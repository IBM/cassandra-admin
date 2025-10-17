local _M = {}

_M.defaults = {
    app_name = "Cassandra Admin",
    connection = {
        host = "127.0.0.1",
        port = 9042,
        username = "cassandra",
        password = "cassandra",
        auth_provider = "plain_text",
        timeout = 5000,
    },
    page_sizes = {50, 100, 200},
    default_page_size = 50
}

_M.env_mapping = {
    ["CA_CONNECTION_HOST"] = {"connection", "host"},
    ["CA_CONNECTION_PORT"] = {"connection", "port", "number"},
    ["CA_CONNECTION_USERNAME"] = {"connection", "username"},
    ["CA_CONNECTION_PASSWORD"] = {"connection", "password"},
    ["CA_CONNECTION_TIMEOUT"] = {"connection", "timeout", "number"},
    ["CA_DEBUG_MODE"] = {},
}

_M.settings_file_path = "/etc/cassandra-admin/settings.cfg"

_M.feature_flags = {
    enable_htmx = false,
}

return _M