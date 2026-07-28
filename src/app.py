import csv
import json
import os
import re
import sys
import traceback
from functools import wraps
from io import StringIO

from flask import Flask, Response, request, send_from_directory

sys.path.insert(0, "/opt/cassandra/pylib")
from cqlshlib.cqlshmain import Shell
from cqlshlib.cqlshmain import version as cqlsh_version

try:
    from cqlshlib.cqlshmain import insert_driver_hooks

    insert_driver_hooks()
except (ImportError, AttributeError):
    pass

app = Flask(__name__)
shells = {}


# ==========================================
# CONSTANTS
# ==========================================

# App Config
APP_PORT = int(os.getenv("APP_PORT", 5000))
APP_LISTEN = os.getenv("APP_LISTEN", "0.0.0.0")

# Cassandra Config
AUTO_CONNECT = os.getenv("CASSANDRA_HOST") is not None
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_USERNAME = os.getenv("CASSANDRA_USERNAME")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")

# Dictionary to hold metadata for actions that nodes can execute
ACTION_DEF = {
    "describe": {"name": "describe", "label": "Describe", "title": "Describe", "icon": "bi-info-circle", "isDangerous": False},
    "export": {"name": "export", "label": "Export", "title": "Export", "icon": "bi-download", "isDangerous": False},
    "truncate": {"name": "truncate", "label": "Truncate", "title": "Truncate", "icon": "bi-scissors", "isDangerous": True},
    "drop": {"name": "drop", "label": "Drop", "title": "Drop", "icon": "bi-trash", "isDangerous": True},
}

# Export format handlers
EXPORT_FORMATS = {
    "json": lambda rows, _: (json.dumps(rows, indent=2), "application/json"),
    "csv": lambda rows, _: (
        (writer := csv.DictWriter(StringIO(), fieldnames=rows[0].keys()), writer.writeheader(), writer.writerows(rows))[
            0
        ].writer.writerow.__self__.getvalue()
        if rows
        else "",
        "text/csv",
    ),
    "cql": lambda rows, ddl, ks, ent: (
        "\n".join(
            [ddl]
            + [
                f'INSERT INTO "{ks}"."{ent}" ({", ".join(f"{k}" for k in r.keys())}) VALUES ({", ".join("NULL" if v in (None, "null") else f"{chr(39)}{str(v).replace(chr(39), chr(39) * 2)}{chr(39)}" for v in r.values())});'
                for r in rows
            ]
        ),
        "application/octet-stream",
    ),
}

# Map UI Tree node types to their resolver logic (lambdas used to avoid NameErrors for functions defined below)
NODE_RESOLVERS = {
    "keyspace": lambda meta, args: resolve_keyspace(meta, args),
    "group": lambda meta, args: resolve_group(meta, args),
    "table": lambda meta, args: resolve_entity_columns(meta, args),
    "view": lambda meta, args: resolve_entity_columns(meta, args),
    "type": lambda meta, args: resolve_type_fields(meta, args),
}


# ==========================================
# DECORATORS
# ==========================================


def api_safe(f):
    """Wraps route to automatically catch exceptions and return JSON error/traceback."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            return {"error": str(e), "traceback": traceback.format_exc()}, 500

    return wrapper


def require_shell(f):
    """Injects the shell into the route based on session_id, returning 400 if disconnected."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        req = request.json if request.is_json else (request.form or request.args)
        session_id = req.get("session_id", "default")
        shell = shells.get(session_id)
        if not shell:
            return {"error": "Not connected. Call /connect first"}, 400
        return f(shell, *args, **kwargs)

    return wrapper


def json_mode_fallback(original_method):
    """Decorator factory that falls back to the original method if _json_mode is not active."""

    def decorator(custom_impl):
        @wraps(custom_impl)
        def wrapper(self, *args, **kwargs):
            if getattr(self, "_json_mode", False):
                return custom_impl(self, *args, **kwargs)
            return original_method(self, *args, **kwargs)

        return wrapper

    return decorator


# ==========================================
# CORE SHELL PATCHING & EXECUTION
# ==========================================


def patch_shell_for_json():
    """Monkey-patch Shell class to output JSON instead of formatted text"""

    @json_mode_fallback(Shell.print_result)
    def json_print_result(self, result, table_meta):
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
                all_rows.append(
                    {
                        col: get_str(self.myformat_value(row[col], cqltype=(cql_types[i] if i < len(cql_types) else None)))
                        for i, col in enumerate(column_names)
                    }
                )

        self._json_output = {
            "keyspace": table_meta.keyspace_name if table_meta else None,
            "table": table_meta.name if table_meta else None,
            "rows": all_rows,
            "row_count": len(all_rows),
            "column_metadata": extract_column_metadata(column_names, table_meta),
            "has_more_pages": result.has_more_pages,
            "paging_state": result.paging_state.hex() if result.paging_state else None,
        }

    @json_mode_fallback(Shell.writeresult)
    def json_writeresult(self, text, color=None, newline=True, out=None):
        if not hasattr(self, "_text_output"):
            self._text_output = []
        self._text_output.append(str(text))

    @json_mode_fallback(Shell.printerr)
    def json_printerr(self, text, color=None, newline=True, shownum=None):
        if not hasattr(self, "_json_output"):
            self._json_output = {}
        self._json_output.setdefault("errors", []).append(str(text))
        self.statement_error = True

    # Apply the patches
    Shell.print_result = json_print_result
    Shell.writeresult = json_writeresult
    Shell.printerr = json_printerr


# Ensure this gets called to apply the patches
patch_shell_for_json()


def extract_column_metadata(column_names, table_meta):
    if not table_meta:
        return [{"name": name, "type": None, "role": "regular"} for name in column_names]

    pk = {c.name for c in table_meta.partition_key}
    ck = {c.name for c in table_meta.clustering_key}

    def get_role(name, meta):
        if name in pk:
            return "partition_key"
        if name in ck:
            return "clustering_key"
        if name in meta.columns and meta.columns[name].is_static:
            return "static"
        return "regular"

    return [
        {"name": col, "type": str(table_meta.columns[col].cql_type) if col in table_meta.columns else None, "role": get_role(col, table_meta)}
        for col in column_names
    ]


def create_shell(**kwargs):
    """Create a patched Shell instance"""
    if "host" in kwargs:
        kwargs["hostname"] = kwargs.pop("host")

    desired_args = {
        "hostname": "127.0.0.1",
        "port": 9042,
        "config_file": "/tmp/cqlshrc",
        "color": False,
        "encoding": "utf-8",
        "stdin": StringIO(),
        "tty": False,
        "ssl": False,
        "elapsed_enabled": False,
        "completekey": None,
    }
    desired_args.update(kwargs)

    import inspect

    shell_params = inspect.signature(Shell.__init__).parameters
    shell_args = {k: v for k, v in desired_args.items() if k in shell_params}

    try:
        from cqlshlib import cqlshmain

        if cqlshmain.cqlruleset is None:
            from cqlshlib import cql3handling

            cqlshmain.setup_cqlruleset(cql3handling)
    except (ImportError, AttributeError):
        pass

    shell = Shell(**shell_args)
    shell._json_mode = True
    original_execute_async = shell.session.execute_async

    def execute_async_with_paging(statement, *args, **kwargs_inner):
        if getattr(shell, "_current_paging_state", None):
            kwargs_inner["paging_state"] = shell._current_paging_state
            shell._current_paging_state = None
        return original_execute_async(statement, *args, **kwargs_inner)

    shell.session.execute_async = execute_async_with_paging
    return shell


def execute_cql(shell, command, paging_state=None):
    """Execute a CQL command and return JSON result"""
    command = command.strip() if command.strip().endswith(";") else command.strip() + ";"
    shell._current_paging_state = bytes.fromhex(paging_state) if paging_state else None
    shell.statement_error, shell._json_output, shell._text_output = False, {}, []

    old_stdout, old_query_out = sys.stdout, shell.query_out
    output_capture = StringIO()
    sys.stdout = shell.query_out = output_capture

    try:
        shell.statement.truncate(0)
        shell.statement.seek(0)
        shell.statement.write(command + "\n")
        shell.onecmd(shell.statement.getvalue())

        captured = output_capture.getvalue()
        if captured:
            shell._text_output.append(captured.strip())
    finally:
        sys.stdout, shell.query_out = old_stdout, old_query_out

    result = {"last_query": command}
    if shell._json_output:
        result.update(shell._json_output)
    if shell._text_output:
        result["output"] = "\n".join(shell._text_output).strip()

    shell.reset_statement()
    return result, shell.statement_error


# ==========================================
# TREE NODE RESOLVERS
# ==========================================


def build_node(id, label, type, keyspace, entity=None, group=None, column=None, has_children=False, actions=None, click_act=None, export_url=None):
    """DRY builder for UI Tree nodes"""
    node = {
        "id": id,
        "label": label,
        "type": type,
        "keyspace": keyspace,
        "hasChildren": has_children,
        "actions": actions or [],
        "defaultClickAction": click_act,
        "isClickable": click_act is not None,
    }
    for k, v in [("entity", entity), ("group", group), ("column", column), ("exportUrl", export_url)]:
        if v:
            node[k] = v
    return node


def resolve_keyspace(ks_meta, args):
    checks = [
        ("table", "Tables", lambda m: bool(m.tables)),
        ("view", "Views", lambda m: bool(m.views)),
        ("type", "Types", lambda m: getattr(m, "user_types", None)),
        ("function", "Functions", lambda m: getattr(m, "functions", None)),
        ("trigger", "Triggers", lambda m: any(getattr(t, "triggers", None) for t in m.tables.values())),
    ]
    return [
        build_node(f"{ks_meta.name}_grp_{g_id}", label, "group", ks_meta.name, group=g_id, has_children=True, click_act="expand")
        for g_id, label, cond in checks
        if cond(ks_meta)
    ]


def resolve_group(ks_meta, args):
    group, ks = args.get("group"), ks_meta.name

    entity_map = {
        "table": (ks_meta.tables, [ACTION_DEF["describe"], ACTION_DEF["export"], ACTION_DEF["truncate"], ACTION_DEF["drop"]], "select", True),
        "view": (ks_meta.views, [ACTION_DEF["describe"], ACTION_DEF["export"], ACTION_DEF["drop"]], "select", True),
        "type": (getattr(ks_meta, "user_types", {}), [ACTION_DEF["describe"], ACTION_DEF["drop"]], "describe", True),
        "function": (getattr(ks_meta, "functions", {}), [ACTION_DEF["describe"], ACTION_DEF["drop"]], "describe", False),
    }

    if group in entity_map:
        items, actions, click_act, has_children = entity_map[group]
        items = {f.name: f for f in items.values()} if group == "function" else items

        return [
            build_node(
                f"{ks}_{group}_{k}",
                k,
                group,
                ks,
                entity=k,
                has_children=has_children,
                actions=actions,
                click_act=click_act,
                export_url=f"/api/keyspaces/{ks}/{group}s/{k}/export" if group in ["table", "view"] else None,
            )
            for k in sorted(items.keys())
        ]

    elif group == "trigger":
        triggers = {trg for t in ks_meta.tables.values() if getattr(t, "triggers", None) for trg in t.triggers.keys()}
        return [
            build_node(
                f"{ks}_trigger_{trg}", trg, "trigger", ks, entity=trg, actions=[ACTION_DEF["describe"], ACTION_DEF["drop"]], click_act="describe"
            )
            for trg in sorted(triggers)
        ]
    return []


def resolve_entity_columns(ks_meta, args):
    node_type, entity, ks = args.get("type"), args.get("entity"), ks_meta.name
    meta = ks_meta.tables.get(entity) if node_type == "table" else ks_meta.views.get(entity)
    if not meta:
        return []

    return [
        build_node(f"{ks}_{node_type}_{entity}_col_{c}", f"{c} ({c_meta.cql_type})", "column", ks, entity=entity, column=c)
        for c, c_meta in meta.columns.items()
    ]


def resolve_type_fields(ks_meta, args):
    entity, ks = args.get("entity"), ks_meta.name
    user_type = ks_meta.user_types.get(entity)
    if not user_type:
        return []

    return [
        build_node(f"{ks}_type_{entity}_field_{f}", f"{f} ({user_type.field_types[i]})", "column", ks, entity=entity, column=f)
        for i, f in enumerate(user_type.field_names)
    ]


# ==========================================
# API ROUTES
# ==========================================


@app.route("/")
def root():
    return send_from_directory(".", "index.html")

if not AUTO_CONNECT:
    @app.route("/api/connect", methods=["POST"])
    @api_safe
    def connect():
        data = request.json or {}
        shell = create_shell(**{k: data.get(k) for k in ["host", "port", "username", "password", "keyspace"] if data.get(k) is not None})
        session_id = data.get("session_id", "default")
        shells[session_id] = shell
        return {"status": "connected", "session_id": session_id, "cluster": shell.get_cluster_name(), "keyspace": shell.current_keyspace}


@app.route("/api/execute", methods=["POST"])
@api_safe
@require_shell
def execute(shell):
    data = request.json or {}
    if not (command := data.get("command")):
        return {"error": "No command provided"}, 400

    original_page_size = shell.page_size
    if page_size := data.get("page_size"):
        shell.page_size = page_size

    try:
        result, has_error = execute_cql(shell, command, data.get("paging_state"))
        return result, (400 if has_error else 200)
    finally:
        shell.page_size = original_page_size


@app.route("/api/action", methods=["POST"])
@api_safe
@require_shell
def handle_action(shell):
    data = request.json or {}
    action, keyspace, entity_type, entity = map(data.get, ("action", "keyspace", "type", "entity"))

    def quote_ident(n):
        return f'"{n.replace(chr(34), chr(34) + chr(34))}"' if n and not re.match(r"^[a-z_][a-z0-9_]*$", n) else (n or "")

    ks_q, ent_q = quote_ident(keyspace), quote_ident(entity)
    queries = {
        "drop": f"DROP KEYSPACE {ks_q};" if entity_type == "keyspace" else f"DROP {entity_type.upper()} {ks_q}.{ent_q};",
        "truncate": f"TRUNCATE {ks_q}.{ent_q};",
    }

    if action not in queries:
        return {"error": f"Unsupported action: {action}"}, 400

    result, has_error = execute_cql(shell, queries[action])
    return result if has_error else {"success": True, "message": f"Action '{action}' executed successfully."}, (400 if has_error else 200)


@app.route("/api/schema", methods=["GET"])
@api_safe
@require_shell
def get_schema(shell):
    keyspaces = [
        build_node(
            f"ks_{ks}",
            ks,
            "keyspace",
            ks,
            has_children=True,
            actions=[ACTION_DEF["describe"], ACTION_DEF["export"], ACTION_DEF["drop"]],
            click_act="expand",
            export_url=f"/api/keyspaces/{ks}/export",
        )
        for ks in sorted(shell.session.cluster.metadata.keyspaces.keys())
    ]

    vers = shell.connection_versions.copy()
    return {
        "connection": {
            "cluster_name": shell.get_cluster_name(),
            "host": shell.hostname,
            "port": shell.port,
            "cql_version": shell.cql_version,
            "cassandra_version": vers.get("build", "unknown"),
            "protocol_version": vers.get("protocol", "unknown"),
            "cqlsh_version": cqlsh_version,
        },
        "schema": keyspaces,
    }


@app.route("/api/schema/children", methods=["GET"])
@api_safe
@require_shell
def get_schema_children(shell):
    node_type, keyspace_name = request.args.get("type"), request.args.get("keyspace")
    ks_meta = shell.session.cluster.metadata.keyspaces.get(keyspace_name)

    # Calling the lambda from our constants dictionary
    resolver = NODE_RESOLVERS.get(node_type) if ks_meta else None
    return {"children": resolver(ks_meta, request.args) if resolver else []}


@app.route("/api/disconnect", methods=["POST"])
def disconnect():
    session_id = (request.json or {}).get("session_id", "default")
    if shell := shells.pop(session_id, None):
        if shell.owns_connection:
            shell.conn.shutdown()
    return {"status": "disconnected"}


@app.route("/api/keyspaces/<keyspace>/<collection_type>/<entity>/export", methods=["POST"])
@api_safe
@require_shell
def export_entity(shell, keyspace, collection_type, entity):
    fmt, limit, include_ddl = (request.form.get("format", "csv"), int(request.form.get("limit", 0)), request.form.get("include_ddl") == "1")
    entity_type = "table" if collection_type == "tables" else "view"

    cql_ddl = (
        f"{execute_cql(shell, f'DESCRIBE TABLE {keyspace}.{entity};')[0].get('output', '')}\n\n"
        if include_ddl and fmt == "cql" and entity_type == "table"
        else ""
    )

    original_page_size, shell.page_size = shell.page_size, limit if limit > 0 else 10000
    try:
        result, has_error = execute_cql(shell, f'SELECT * FROM "{keyspace}"."{entity}"{" LIMIT " + str(limit) if limit > 0 else ""}')
    finally:
        shell.page_size = original_page_size

    if has_error:
        return result.get("output", "Error executing export query"), 400
    if fmt not in EXPORT_FORMATS:
        return "Invalid export format", 400

    content, mimetype = (
        EXPORT_FORMATS[fmt](result.get("rows", []), cql_ddl, keyspace, entity)
        if fmt == "cql"
        else EXPORT_FORMATS[fmt](result.get("rows", []), cql_ddl)
    )

    return Response(content, mimetype=mimetype, headers={"Content-Disposition": f"attachment;filename={keyspace}_{entity}_export.{fmt}"})


@app.route("/api/keyspaces/<keyspace>/export", methods=["POST"])
@api_safe
@require_shell
def export_keyspace(shell, keyspace):
    result, has_error = execute_cql(shell, f'DESCRIBE KEYSPACE "{keyspace}";')
    if has_error:
        return result.get("output", "Error exporting keyspace schema"), 400

    return Response(
        result.get("output", ""), mimetype="application/octet-stream", headers={"Content-Disposition": f"attachment;filename={keyspace}_schema.cql"}
    )


# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":
    print(f"Starting Flask CQLSH API on {APP_LISTEN}:{APP_PORT}")
    if AUTO_CONNECT:
        print("Auto-connecting using environment variables...")
        try:
            shell = create_shell(
                host=CASSANDRA_HOST, port=CASSANDRA_PORT, username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD, keyspace=CASSANDRA_KEYSPACE
            )
            shells["default"] = shell
            print(f"Connected to {shell.get_cluster_name()}. Using keyspace: {shell.current_keyspace}")
        except Exception as e:
            sys.exit(f"Failed to auto-connect: {e}")
    else:
        print("No CASSANDRA_HOST environment variable set. Use /connect endpoint to connect.")

    app.run(debug=True, host=APP_LISTEN, port=APP_PORT)
