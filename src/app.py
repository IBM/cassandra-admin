from flask import Flask, request, jsonify, send_from_directory
from flask.json.provider import DefaultJSONProvider
import sys
import os
from io import StringIO
import json
from decimal import Decimal
from uuid import UUID
from datetime import datetime, date
import ipaddress

# Add cqlsh pylib directly to path
sys.path.insert(0, '/opt/cassandra/pylib')

from cqlshlib.cqlshmain import Shell
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from cassandra.util import SortedSet, OrderedMapSerializedKey

app = Flask(__name__)
shells = {}
paging_states = {}

# Check if we should auto-connect on startup
AUTO_CONNECT = os.getenv('CASSANDRA_HOST') is not None


# Custom JSON provider for Flask 2.2+
class CassandraJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, (set, frozenset, SortedSet)):
            return list(obj)
        if isinstance(obj, OrderedMapSerializedKey):
            return dict(obj)
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            return str(obj)
        if isinstance(obj, bytes):
            return obj.hex()
        return super().default(obj)


app.json = CassandraJSONProvider(app)


def serialize_value(value):
    """Convert Cassandra types to JSON-serializable types"""
    if isinstance(value, (set, frozenset, SortedSet)):
        return list(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return str(value)
    if isinstance(value, OrderedMapSerializedKey):
        return dict(value)
    return value


def serialize_row(row):
    """Serialize a single row dict"""
    return {key: serialize_value(value) for key, value in row.items()}


def extract_column_metadata(column_names, table_meta):
    """Extract column metadata from table metadata"""
    if not table_meta:
        return [{'name': name, 'type': None, 'role': 'regular'} for name in column_names]
    
    partition_keys = {col.name for col in table_meta.partition_key}
    clustering_keys = {col.name for col in table_meta.clustering_key}
    
    column_metadata = []
    for col_name in column_names:
        col_meta = {
            'name': col_name,
            'type': None,
            'role': 'regular'
        }
        
        if col_name in partition_keys:
            col_meta['role'] = 'partition_key'
        elif col_name in clustering_keys:
            col_meta['role'] = 'clustering_key'
        elif col_name in table_meta.columns and table_meta.columns[col_name].is_static:
            col_meta['role'] = 'static'
        
        if col_name in table_meta.columns:
            col_meta['type'] = str(table_meta.columns[col_name].cql_type)
        
        column_metadata.append(col_meta)
    
    return column_metadata


def format_result(result, column_names, table_meta):
    """Format a result set into JSON structure"""
    all_rows = [serialize_row(row) for row in result.current_rows] if result.current_rows else []
    column_metadata = extract_column_metadata(column_names, table_meta)
    
    return {
        'keyspace': table_meta.keyspace_name if table_meta else None,
        'rows': all_rows,
        'row_count': len(all_rows),
        'column_metadata': column_metadata,
        'has_more_pages': result.has_more_pages,
        'paging_state': result.paging_state.hex() if result.paging_state else None
    }


def patch_shell_for_json():
    """Monkey-patch Shell class to output JSON instead of formatted text"""
    
    original_print_result = Shell.print_result
    original_writeresult = Shell.writeresult
    original_printerr = Shell.printerr
    
    def json_print_result(self, result, table_meta):
        if not getattr(self, '_json_mode', False):
            return original_print_result(self, result, table_meta)
        
        column_names = result.column_names or (list(table_meta.columns.keys()) if table_meta else [])
        self._json_output = format_result(result, column_names, table_meta)
    
    def json_writeresult(self, text, color=None, newline=True, out=None):
        if not getattr(self, '_json_mode', False):
            return original_writeresult(self, text, color, newline, out)
        
        # Capture all text output
        if not hasattr(self, '_text_output'):
            self._text_output = []
        
        # Convert text to string
        text_str = str(text)
        self._text_output.append(text_str)
    
    def json_printerr(self, text, color=None, newline=True, shownum=None):
        if not getattr(self, '_json_mode', False):
            return original_printerr(self, text, color, newline, shownum)
        
        if not hasattr(self, '_json_output'):
            self._json_output = {}
        if 'errors' not in self._json_output:
            self._json_output['errors'] = []
        self._json_output['errors'].append(str(text))
        self.statement_error = True
    
    def make_noop(command_name, message=None):
        if message is None:
            message = f'{command_name.upper()} command not supported in API mode'
        
        def noop_method(self, parsed=None):
            if not getattr(self, '_json_mode', False):
                # Fallback to original if not in JSON mode
                original = getattr(Shell, f'_original_{command_name}', None)
                if original:
                    return original(self, parsed)
            
            if not hasattr(self, '_json_output'):
                self._json_output = {}
            self._json_output['error'] = message
            self.statement_error = True
        
        return noop_method
    
    Shell.print_result = json_print_result
    Shell.writeresult = json_writeresult
    Shell.printerr = json_printerr
    
    noop_commands = {
        'do_login': 'LOGIN not supported - authenticate at connection time using /connect endpoint',
        'do_exit': 'EXIT not supported - use /disconnect endpoint instead',
        'do_quit': 'QUIT not supported - use /disconnect endpoint instead',
        'do_clear': 'CLEAR not supported in API mode',
        'do_cls': 'CLS not supported in API mode',
        'do_debug': 'DEBUG not supported in API mode',
        'do_help': 'HELP not supported - refer to API documentation',
        'do_history': 'HISTORY not supported in API mode',
        'do_source': 'SOURCE not supported - execute commands directly via API',
        'do_capture': 'CAPTURE not supported in API mode',
    }
    
    for cmd, message in noop_commands.items():
        if hasattr(Shell, cmd):
            setattr(Shell, '_original_' + cmd, getattr(Shell, cmd))
            setattr(Shell, cmd, make_noop(cmd, message))

patch_shell_for_json()


def create_shell(host='127.0.0.1', port=9042, username=None, password=None, keyspace=None):
    """Create a patched Shell instance"""
    
    auth_provider = None
    if username and password:
        auth_provider = PlainTextAuthProvider(username=username, password=password)
    
    desired_args = {
        'hostname': host,
        'port': port,
        'config_file': '/tmp/cqlshrc',
        'color': False,
        'username': username,
        'encoding': 'utf-8',
        'stdin': StringIO(),
        'tty': False,
        'keyspace': keyspace,
        'ssl': False,
        'auth_provider': auth_provider,
        'elapsed_enabled': False,
        'completekey': None
    }
    
    import inspect
    shell_params = inspect.signature(Shell.__init__).parameters

    shell_args = {
        key: value for key, value in desired_args.items() 
        if key in shell_params
    }
    
    try:
        from cqlshlib import cqlshmain
        if cqlshmain.cqlruleset is None:
            from cqlshlib import cql3handling, cqlshhandling
            cqlshmain.setup_cqlruleset(cql3handling)
    except (ImportError, AttributeError):
        pass
    
    shell = Shell(**shell_args)
    
    shell._json_mode = True
    shell._json_output = {}
    shell.session.row_factory = dict_factory
    
    return shell


def execute_cql(shell, command, paging_state=None):
    """Execute a CQL command and return JSON result"""
    
    if not command.strip().endswith(';'):
        command = command.strip() + ';'
    
    shell.statement_error = False
    shell._json_output = {}
    shell._text_output = []
    
    # For paginated queries, use session directly
    if paging_state:
        try:
            from cassandra.query import SimpleStatement
            stmt = SimpleStatement(command.rstrip(';'), fetch_size=shell.page_size)
            stmt.paging_state = bytes.fromhex(paging_state)
            
            result = shell.session.execute(stmt)
            
            # Get table metadata for column info
            table_meta = None
            try:
                table_meta = shell.parse_for_select_meta(command)
            except:
                pass
            
            column_names = result.column_names or []
            return format_result(result, column_names, table_meta), False
            
        except Exception as e:
            return {'error': str(e)}, True
    
    # Capture stdout for commands that use print()
    import sys
    from io import StringIO
    
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    try:
        # Normal execution through shell
        shell.statement.truncate(0)
        shell.statement.seek(0)
        shell.statement.write(command + '\n')
        shell.onecmd(shell.statement.getvalue())
        
        # Capture any print() output
        printed_output = sys.stdout.getvalue()
        if printed_output:
            shell._text_output.append(printed_output.strip())
    finally:
        sys.stdout = old_stdout
    
    # Build result from available data
    result = {}

    result['query'] = command.strip()
    
    # If we have structured data (SELECT queries)
    if shell._json_output:
        result = shell._json_output.copy()
    
    # If we have text output (DESCRIBE, CONSISTENCY, etc)
    if shell._text_output:
        if result:
            # Add text output to existing result
            result['output'] = '\n'.join(shell._text_output).strip()
        else:
            # Text output only
            result = {
                'output': '\n'.join(shell._text_output).strip()
            }
    
    # If there are no results at all, return empty dict
    if not result:
        result = {}
    
    shell.reset_statement()
    
    return result, shell.statement_error

@app.route('/')
@app.route('/keyspace')
@app.route('/keyspace/')
@app.route('/keyspace/<path:subpath>')
def index(subpath=None):
    """Serve the main HTML interface"""
    return send_from_directory('.', 'index.html')

# Only expose /connect endpoint if not auto-connecting
if not AUTO_CONNECT:
    @app.route('/api/connect', methods=['POST'])
    def connect():
        """Connect to Cassandra"""
        data = request.json or {}
        
        try:
            shell = create_shell(
                host=data.get('host', '127.0.0.1'),
                port=data.get('port', 9042),
                username=data.get('username'),
                password=data.get('password'),
                keyspace=data.get('keyspace')
            )
            
            session_id = data.get('session_id', 'default')
            shells[session_id] = shell
            
            return {
                'status': 'connected',
                'session_id': session_id,
                'cluster': shell.get_cluster_name(),
                'keyspace': shell.current_keyspace
            }
        
        except Exception as e:
            return {'error': str(e)}, 500


@app.route('/api/keyspace/<keyspace_name>/table/<table_name>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def table_operations(keyspace_name, table_name):
    """Virtual endpoint for table operations"""
    session_id = request.args.get('session_id', 'default')
    
    shell = shells.get(session_id)
    if not shell:
        return {'error': 'Not connected. Call /connect first'}, 400
    
    try:
        if request.method == 'GET':
            # SELECT query
            where_clause = request.args.get('where', '')
            limit = request.args.get('limit')
            columns = request.args.get('columns', '*')
            paging_state = request.args.get('paging_state')
            page_size = request.args.get('page_size')
            
            # Build SELECT query
            query = f"SELECT {columns} FROM {keyspace_name}.{table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            if limit:
                query += f" LIMIT {limit}"
            
            # Set custom page size if provided
            original_page_size = None
            if page_size:
                original_page_size = shell.page_size
                shell.page_size = int(page_size)
            
            try:
                result, has_error = execute_cql(shell, query, paging_state)
                if has_error:
                    return result, 400
                return result
            finally:
                if original_page_size is not None:
                    shell.page_size = original_page_size
        
        elif request.method == 'POST':
            # INSERT query
            data = request.json or {}
            
            if not data:
                return {'error': 'No data provided for INSERT'}, 400
            
            columns = ', '.join(data.keys())
            values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
            
            query = f"INSERT INTO {keyspace_name}.{table_name} ({columns}) VALUES ({values})"
            
            result, has_error = execute_cql(shell, query)
            if has_error:
                return result, 400
            return {'status': 'inserted', 'query': query}
        
        elif request.method == 'PUT':
            # UPDATE query
            data = request.json or {}
            where_clause = request.args.get('where')
            
            if not data:
                return {'error': 'No data provided for UPDATE'}, 400
            if not where_clause:
                return {'error': 'WHERE clause required for UPDATE'}, 400
            
            set_clause = ', '.join([f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}" 
                                   for k, v in data.items()])
            
            query = f"UPDATE {keyspace_name}.{table_name} SET {set_clause} WHERE {where_clause}"
            
            result, has_error = execute_cql(shell, query)
            if has_error:
                return result, 400
            return {'status': 'updated', 'query': query}
        
        elif request.method == 'DELETE':
            # DELETE query
            where_clause = request.args.get('where')
            
            if not where_clause:
                return {'error': 'WHERE clause required for DELETE'}, 400
            
            query = f"DELETE FROM {keyspace_name}.{table_name} WHERE {where_clause}"
            
            result, has_error = execute_cql(shell, query)
            if has_error:
                return result, 400
            return {'status': 'deleted', 'query': query}
    
    except Exception as e:
        import traceback
        return {
            'error': str(e),
            'traceback': traceback.format_exc()
        }, 500


@app.route('/api/execute', methods=['POST'])
def execute():
    """Execute CQL command with optional pagination"""
    data = request.json or {}
    command = data.get('command')
    session_id = data.get('session_id', 'default')
    paging_state = data.get('paging_state')
    page_size = data.get('page_size')
    
    if not command:
        return {'error': 'No command provided'}, 400
    
    shell = shells.get(session_id)
    if not shell:
        return {'error': 'Not connected. Call /connect first'}, 400
    
    # Set custom page size if provided
    original_page_size = None
    if page_size:
        original_page_size = shell.page_size
        shell.page_size = page_size
    
    try:
        result, has_error = execute_cql(shell, command, paging_state)
        
        # Return error response if there were errors
        if has_error:
            return result, 400
        
        return result
    
    except Exception as e:
        import traceback
        return {
            'error': str(e),
            'traceback': traceback.format_exc()
        }, 500
    
    finally:
        # Restore original page size
        if original_page_size is not None:
            shell.page_size = original_page_size

@app.route('/api/schema', methods=['GET'])
def get_schema():
    """Get full schema tree of keyspaces, tables, and views"""
    session_id = request.args.get('session_id', 'default')
    
    shell = shells.get(session_id)
    if not shell:
        return {'error': 'Not connected. Call /connect first'}, 400
    
    try:
        schema_tree = []
        
        # Get all keyspaces from cluster metadata
        cluster_meta = shell.session.cluster.metadata
        
        # Sort keyspaces alphabetically
        for keyspace_name in sorted(cluster_meta.keyspaces.keys()):
            keyspace_meta = cluster_meta.keyspaces[keyspace_name]
            entities = []
            
            # Add all tables (sorted)
            for table_name in sorted(keyspace_meta.tables.keys()):
                entities.append({
                    'type': 'table',
                    'name': table_name
                })
            
            # Add all materialized views (sorted)
            for view_name in sorted(keyspace_meta.views.keys()):
                entities.append({
                    'type': 'view',
                    'name': view_name
                })
            
            schema_tree.append({
                'keyspace': keyspace_name,
                'entities': entities
            })
        
        return schema_tree
    
    except Exception as e:
        import traceback
        return {
            'error': str(e),
            'traceback': traceback.format_exc()
        }, 500

@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    """Disconnect from Cassandra"""
    data = request.json or {}
    session_id = data.get('session_id', 'default')
    
    shell = shells.get(session_id)
    if shell:
        if shell.owns_connection:
            shell.conn.shutdown()
        del shells[session_id]
    
    paging_states.pop(session_id, None)
    
    return {'status': 'disconnected'}


if __name__ == '__main__':
    print("Starting Flask CQLSH API on 0.0.0.0:5000")
    
    # Auto-connect if environment variables are set
    if AUTO_CONNECT:
        print("Auto-connecting using environment variables...")
        try:
            shell = create_shell(
                host=os.getenv('CASSANDRA_HOST', '127.0.0.1'),
                port=int(os.getenv('CASSANDRA_PORT', '9042')),
                username=os.getenv('CASSANDRA_USERNAME'),
                password=os.getenv('CASSANDRA_PASSWORD'),
                keyspace=os.getenv('CASSANDRA_KEYSPACE')
            )
            shells['default'] = shell
            print(f"Connected to {shell.get_cluster_name()}")
            if shell.current_keyspace:
                print(f"Using keyspace: {shell.current_keyspace}")
        except Exception as e:
            print(f"Failed to auto-connect: {e}")
            import sys
            sys.exit(1)
    else:
        print("No CASSANDRA_HOST environment variable set. Use /connect endpoint to connect.")
    
    app.run(debug=True, host='0.0.0.0', port=5000)