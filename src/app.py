import csv
import sys
import os
from flask import Flask, Response, request, send_from_directory
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
    
    # 4. Patch execute_async to inject paging_state when present
    original_execute_async = shell.session.execute_async
    def execute_async_with_paging(statement, *args, **kwargs):
        paging_state = getattr(shell, '_current_paging_state', None)
        if paging_state:
            kwargs['paging_state'] = paging_state
            shell._current_paging_state = None  # Consume it once so it doesn't affect subsequent queries
        return original_execute_async(statement, *args, **kwargs)
    
    shell.session.execute_async = execute_async_with_paging

    return shell


def execute_cql(shell, command, paging_state=None):
    """Execute a CQL command and return JSON result"""
    if not command.strip().endswith(';'):
        command = command.strip() + ';'
    
    # 3. Store it on the shell instance temporarily 
    if paging_state:
        shell._current_paging_state = bytes.fromhex(paging_state)
    else:
        shell._current_paging_state = None

    shell.statement_error = False
    shell._json_output = {}
    shell._text_output = []

    # Capture output by redirecting both sys.stdout and shell.query_out
    old_stdout = sys.stdout
    old_query_out = shell.query_out
    output_capture = StringIO()
    
    sys.stdout = output_capture
    shell.query_out = output_capture
    
    try:
        shell.statement.truncate(0)
        shell.statement.seek(0)
        shell.statement.write(command + '\n')
        shell.onecmd(shell.statement.getvalue())
        
        captured_output = output_capture.getvalue()
        if captured_output:
            shell._text_output.append(captured_output.strip())
    except Exception as e:
        # Restore outputs even on exception
        sys.stdout = old_stdout
        shell.query_out = old_query_out
        raise
    finally:
        sys.stdout = old_stdout
        shell.query_out = old_query_out
    
    # Build result from available data
    result = {'last_query': command.strip()}
    
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

@app.route('/api/execute', methods=['POST'])
def execute():
    """Execute CQL command"""
    data = request.json or {}
    command = data.get('command')
    session_id = data.get('session_id', 'default')
    page_size = data.get('page_size')
    paging_state = data.get('paging_state') # 1. Capture the paging_state
    
    if not command:
        return {'error': 'No command provided'}, 400
    
    shell = shells.get(session_id)
    if not shell:
        return {'error': 'Not connected. Call /connect first'}, 400
    
    original_page_size = None
    if page_size:
        original_page_size = shell.page_size
        shell.page_size = page_size
    
    try:
        # 2. Pass the paging state into execute_cql
        result, has_error = execute_cql(shell, command, paging_state)
        
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
    """Get full schema tree of keyspaces, grouped by entity type"""
    session_id = request.args.get('session_id', 'default')
    
    shell = shells.get(session_id)
    if not shell:
        return {'error': 'Not connected. Call /connect first'}, 400
    
    try:
        schema_tree = []
        cluster_meta = shell.session.cluster.metadata
        
        for keyspace_name in sorted(cluster_meta.keyspaces.keys()):
            keyspace_meta = cluster_meta.keyspaces[keyspace_name]
            groups = []
            
            # 1. Tables
            tables = [{'type': 'table', 'name': t} for t in sorted(keyspace_meta.tables.keys())]
            if tables:
                groups.append({'label': 'Tables', 'entities': tables})
            
            # 2. Views
            views = [{'type': 'view', 'name': v} for v in sorted(keyspace_meta.views.keys())]
            if views:
                groups.append({'label': 'Views', 'entities': views})
                
            # 3. Triggers
            triggers = []
            for table in keyspace_meta.tables.values():
                if hasattr(table, 'triggers'):
                    for trigger_name in table.triggers.keys():
                        triggers.append({'type': 'trigger', 'name': trigger_name})
            
            if triggers:
                triggers = sorted(triggers, key=lambda x: x['name'])
                groups.append({'label': 'Triggers', 'entities': triggers})
                
            # 4. Functions
            functions = []
            if hasattr(keyspace_meta, 'functions'):
                for f in keyspace_meta.functions.values():
                    functions.append({'type': 'function', 'name': f.name})
                    
            if functions:
                # Deduplicate overloaded functions (same name, different signatures)
                unique_functions = {f['name']: f for f in functions}.values()
                sorted_functions = sorted(unique_functions, key=lambda x: x['name'])
                groups.append({'label': 'Functions', 'entities': sorted_functions})
            
            schema_tree.append({
                'keyspace': keyspace_name,
                'groups': groups  # Replaces the old flat 'entities' list
            })


            # 5. Types
            types = []
            if hasattr(keyspace_meta, 'user_types'):
                for type_name in keyspace_meta.user_types.keys():
                    types.append({'type': 'type', 'name': type_name})
                    
            if types:
                sorted_types = sorted(types, key=lambda x: x['name'])
                groups.append({'label': 'Types', 'entities': sorted_types})
            
            schema_tree.append({
                'keyspace': keyspace_name,
                'groups': groups  
            })
        
        # Get connection information
        vers = shell.connection_versions.copy()
        from cqlshlib.cqlshmain import version as cqlsh_version
        
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

@app.route('/api/keyspaces/<keyspace>/<collection_type>/<entity>/export', methods=['POST'])
def export_entity(keyspace, collection_type, entity):
    """Export data from a table or view"""
    # Map REST collection name back to entity type
    entity_type = 'table' if collection_type == 'tables' else 'view'
    
    export_format = request.form.get('format', 'csv')
    limit = int(request.form.get('limit', 0))
    include_ddl = request.form.get('include_ddl') == '1'
    
    # Retrieve the default shell session 
    # (If using multi-user sessions, pass session_id via a hidden form input)
    shell = shells.get('default')
    if not shell:
        return "Not connected to Cassandra", 400
        
    # 1. Fetch DDL if requested
    cql_ddl = ""
    if include_ddl and export_format == 'cql' and entity_type == 'table':
        ddl_res, _ = execute_cql(shell, f'DESCRIBE TABLE "{keyspace}"."{entity}";')
        cql_ddl = ddl_res.get('output', '') + "\n\n"
        
    # 2. Fetch Data
    query = f'SELECT * FROM "{keyspace}"."{entity}"'
    if limit > 0:
        query += f' LIMIT {limit}'
        
    # Temporarily increase page size to fetch the export payload 
    # without breaking the user's UI pagination limits
    original_page_size = shell.page_size
    shell.page_size = limit if limit > 0 else 10000 
    
    try:
        result, has_error = execute_cql(shell, query)
    finally:
        shell.page_size = original_page_size
        
    if has_error:
        return result.get('output', 'Error executing export query'), 400
        
    rows = result.get('rows', [])
    filename = f"{keyspace}_{entity}_export.{export_format}"
    
    # 3. Format Output
    if export_format == 'json':
        import json
        content = json.dumps(rows, indent=2)
        mimetype = 'application/json'
        
    elif export_format == 'csv':
        output = StringIO()
        if rows:
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        content = output.getvalue()
        mimetype = 'text/csv'
        
    elif export_format == 'cql':
        lines = [cql_ddl] if cql_ddl else []
        for row in rows:
            columns = ', '.join([f'"{k}"' for k in row.keys()])
            values = []
            for v in row.values():
                if v is None or v == 'null':
                    values.append('NULL')
                else:
                    # Escape single quotes for valid CQL strings
                    escaped_v = str(v).replace("'", "''")
                    values.append(f"'{escaped_v}'")
                    
            values_str = ', '.join(values)
            lines.append(f'INSERT INTO "{keyspace}"."{entity}" ({columns}) VALUES ({values_str});')
            
        content = '\n'.join(lines)
        mimetype = 'application/octet-stream'
        
    else:
        return "Invalid export format", 400
        
    # Trigger browser file download
    return Response(
        content,
        mimetype=mimetype,
        headers={"Content-Disposition": f"attachment;filename={filename}"}
    )

@app.route('/api/keyspaces/<keyspace>/export', methods=['POST'])
def export_keyspace(keyspace):
    """Export an entire keyspace schema (equivalent to DESCRIBE KEYSPACE)"""
    shell = shells.get('default')
    if not shell:
        return "Not connected to Cassandra", 400
        
    # Simply execute the DESCRIBE KEYSPACE command
    result, has_error = execute_cql(shell, f'DESCRIBE KEYSPACE "{keyspace}";')
    
    if has_error:
        return result.get('output', 'Error exporting keyspace schema'), 400
        
    # The 'output' key contains the exact string representation from the shell
    content = result.get('output', '')
    filename = f"{keyspace}_schema.cql"
    
    return Response(
        content,
        mimetype='application/octet-stream',
        headers={"Content-Disposition": f"attachment;filename={filename}"}
    )

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