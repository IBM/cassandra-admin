from flask import Flask, request, send_from_directory
import sys
import os
from io import StringIO
sys.path.insert(0, '/opt/cassandra/pylib')
from cqlshlib.cqlshmain import Shell, version as cqlsh_version

try:
    from cqlshlib.cqlshmain import insert_driver_hooks
    insert_driver_hooks()
except (ImportError, AttributeError):
    pass

app = Flask(__name__)
shells = {}

AUTO_CONNECT = os.getenv('CASSANDRA_HOST') is not None

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


def patch_shell_for_json():
    """Monkey-patch Shell class to output JSON instead of formatted text"""
    
    original_print_result = Shell.print_result
    original_writeresult = Shell.writeresult
    original_printerr = Shell.printerr
    
    def json_print_result(self, result, table_meta):
        if not getattr(self, '_json_mode', False):
            return original_print_result(self, result, table_meta)
        
        column_names = result.column_names or (list(table_meta.columns.keys()) if table_meta else [])
        
        cql_types = []
        if result.column_types:
            ks_name = table_meta.keyspace_name if table_meta else self.current_keyspace
            ks_meta = self.conn.metadata.keyspaces.get(ks_name, None)
            from cassandra.cqltypes import cql_typename
            from cqlshlib.formatting import CqlType
            cql_types = [CqlType(cql_typename(t), ks_meta) for t in result.column_types]
        
        all_rows = []
        if result.current_rows:
            from cqlshlib.displaying import get_str
            for row in result.current_rows:
                row_dict = {}
                for idx, col_name in enumerate(column_names):
                    value = row[col_name]
                    cqltype = cql_types[idx] if idx < len(cql_types) else None
                    
                    # Use the shell's own formatting and extract string
                    formatted = self.myformat_value(value, cqltype=cqltype)
                    row_dict[col_name] = get_str(formatted)
                
                all_rows.append(row_dict)
        
        column_metadata = extract_column_metadata(column_names, table_meta)
        
        self._json_output = {
            'keyspace': table_meta.keyspace_name if table_meta else None,
            'table': table_meta.name if table_meta else None,
            'rows': all_rows,
            'row_count': len(all_rows),
            'column_metadata': column_metadata,
            'has_more_pages': result.has_more_pages,
            'paging_state': result.paging_state.hex() if result.paging_state else None
        }
    
    def json_writeresult(self, text, color=None, newline=True, out=None):
        if not getattr(self, '_json_mode', False):
            return original_writeresult(self, text, color, newline, out)
        
        if not hasattr(self, '_text_output'):
            self._text_output = []
        
        self._text_output.append(str(text))
    
    def json_printerr(self, text, color=None, newline=True, shownum=None):
        if not getattr(self, '_json_mode', False):
            return original_printerr(self, text, color, newline, shownum)
        
        if not hasattr(self, '_json_output'):
            self._json_output = {}
        if 'errors' not in self._json_output:
            self._json_output['errors'] = []
        self._json_output['errors'].append(str(text))
        self.statement_error = True
    
    Shell.print_result = json_print_result
    Shell.writeresult = json_writeresult
    Shell.printerr = json_printerr

patch_shell_for_json()


def create_shell(host='127.0.0.1', port=9042, username=None, password=None, keyspace=None):
    """Create a patched Shell instance"""
    
    desired_args = {
        'hostname': host,
        'port': port,
        'config_file': '/tmp/cqlshrc',
        'color': False,
        'username': username,
        'password': password,
        'encoding': 'utf-8',
        'stdin': StringIO(),
        'tty': False,
        'keyspace': keyspace,
        'ssl': False,
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
            from cqlshlib import cql3handling
            cqlshmain.setup_cqlruleset(cql3handling)
    except (ImportError, AttributeError):
        pass
    
    shell = Shell(**shell_args)
    
    shell._json_mode = True
    shell._json_output = {}
    
    return shell


def execute_cql(shell, command):
    """Execute a CQL command and return JSON result"""
    
    if not command.strip().endswith(';'):
        command = command.strip() + ';'
    
    shell.statement_error = False
    shell._json_output = {}
    shell._text_output = []
    
    # Capture stdout for commands that use print()
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    try:
        shell.statement.truncate(0)
        shell.statement.seek(0)
        shell.statement.write(command + '\n')
        shell.onecmd(shell.statement.getvalue())
        
        printed_output = sys.stdout.getvalue()
        if printed_output:
            shell._text_output.append(printed_output.strip())
    finally:
        sys.stdout = old_stdout
    
    # Build result from available data
    result = {'last_query': command.strip() }
    
    if shell._json_output:
        result.update(shell._json_output)
    
    if shell._text_output:
        result['output'] = '\n'.join(shell._text_output).strip()
    
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
    
    # Quote identifiers if they contain uppercase or special characters
    def quote_identifier(name):
        if name != name.lower() or not name.replace('_', '').isalnum():
            return f'"{name}"'
        return name
    
    quoted_keyspace = quote_identifier(keyspace_name)
    quoted_table = quote_identifier(table_name)
    
    try:
        if request.method == 'GET':
            # SELECT query
            where_clause = request.args.get('where', '')
            limit = request.args.get('limit')
            columns = request.args.get('columns', '*')
            
            # Build SELECT query with quoted identifiers
            query = f"SELECT {columns} FROM {quoted_keyspace}.{quoted_table}"
            if where_clause:
                query += f" WHERE {where_clause}"
            if limit:
                query += f" LIMIT {limit}"
            
            result, has_error = execute_cql(shell, query)
            if has_error:
                return result, 400
            return result
        
        elif request.method == 'POST':
            # INSERT query
            data = request.json or {}
            
            if not data:
                return {'error': 'No data provided for INSERT'}, 400
            
            columns = ', '.join([quote_identifier(k) for k in data.keys()])
            values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
            
            query = f"INSERT INTO {quoted_keyspace}.{quoted_table} ({columns}) VALUES ({values})"
            
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
            
            set_clause = ', '.join([f"{quote_identifier(k)} = '{v}'" if isinstance(v, str) else f"{quote_identifier(k)} = {v}" 
                                   for k, v in data.items()])
            
            query = f"UPDATE {quoted_keyspace}.{quoted_table} SET {set_clause} WHERE {where_clause}"
            
            result, has_error = execute_cql(shell, query)
            if has_error:
                return result, 400
            return {'status': 'updated', 'query': query}
        
        elif request.method == 'DELETE':
            # DELETE query
            where_clause = request.args.get('where')
            
            if not where_clause:
                return {'error': 'WHERE clause required for DELETE'}, 400
            
            query = f"DELETE FROM {quoted_keyspace}.{quoted_table} WHERE {where_clause}"
            
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
    """Execute CQL command"""
    data = request.json or {}
    command = data.get('command')
    session_id = data.get('session_id', 'default')
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
        result, has_error = execute_cql(shell, command)
        
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
        cluster_meta = shell.session.cluster.metadata
        
        for keyspace_name in sorted(cluster_meta.keyspaces.keys()):
            keyspace_meta = cluster_meta.keyspaces[keyspace_name]
            entities = []
            
            for table_name in sorted(keyspace_meta.tables.keys()):
                entities.append({
                    'type': 'table',
                    'name': table_name
                })
            
            for view_name in sorted(keyspace_meta.views.keys()):
                entities.append({
                    'type': 'view',
                    'name': view_name
                })
            
            schema_tree.append({
                'keyspace': keyspace_name,
                'entities': entities
            })
        
        # Get connection information
        vers = shell.connection_versions.copy()
        connection_info = {
            'cluster_name': shell.get_cluster_name(),
            'host': shell.hostname,
            'port': shell.port,
            'cql_version': shell.cql_version,
            'cassandra_version': vers.get('build', 'unknown'),
            'protocol_version': vers.get('protocol', 'unknown'),
            'cqlsh_version': cqlsh_version,
        }
        
        return {
            'connection': connection_info,
            'schema': schema_tree
        }
    
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
    
    return {'status': 'disconnected'}


if __name__ == '__main__':
    print("Starting Flask CQLSH API on 0.0.0.0:5000")
    
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
            sys.exit(1)
    else:
        print("No CASSANDRA_HOST environment variable set. Use /connect endpoint to connect.")
    
    app.run(debug=True, host='0.0.0.0', port=5000)