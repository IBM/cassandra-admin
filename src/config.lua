local _M = {}

_M.app_name = "Cassandra Admin"

_M.connection = {
    host = os.getenv("CASSANDRA_HOST") or "127.0.0.1",
    port = os.getenv("CASSANDRA_PORT") or 9042,
    username = os.getenv("CASSANDRA_USER") or "cassandra",
    password = os.getenv("CASSANDRA_PASSWORD") or "cassandra",
    auth_provider = "plain_text",
    timeout = os.getenv("CASSANDRA_TIMEOUT") or 5000,
}

_M.page_sizes = {50, 100, 200}
_M.default_page_size = 50

return _M