local _M = {}

local function init_context()
  if not ngx.ctx.template_context then
    ngx.ctx.template_context = {
      initialized = false,
      host = nil,
      keyspace_table_hierarchy = {},
      user_keyspaces = {},
      system_keyspaces = {},
      client = nil
    }
  end
  return ngx.ctx.template_context
end

local function get_client()
  local ctx = init_context()

  if not ctx.client then
    local host = os.getenv("CASSANDRA_HOST") or "127.0.0.1"
    local client = cassandra.new({
      host = host,
      auth = cassandra.auth_providers.plain_text(
        os.getenv("CASSANDRA_USER") or "cassandra",
        os.getenv("CASSANDRA_PASSWORD") or "cassandra"
      )
    })
    
    if not client then
      error_page("Error", "Failed to initialize Cassandra client.")
    end
    
    client:settimeout(5000)
    
    local ok, err = client:connect()
    if not ok then
      error_page("Connection Error", "Failed to connect to Cassandra: " .. tostring(err))
    end

    ctx.client = client
  end

  return ctx.client
end


local function load_keyspace_hierarchy(client)
  local ks_tbls = {}

  local keyspace_rows, err = client:execute("SELECT keyspace_name FROM system_schema.keyspaces")
  if err then
    error_page("Error", "Error fetching keyspaces: " .. tostring(err))
  end

  for _, row in ipairs(keyspace_rows) do
    ks_tbls[row.keyspace_name] = {}
  end

  local table_rows, err2 = client:execute("SELECT keyspace_name, table_name FROM system_schema.tables")
  if err2 then
    error_page("Error", "Error fetching tables: " .. tostring(err2))
  end

  for _, row in ipairs(table_rows) do
    if ks_tbls[row.keyspace_name] then
      table.insert(ks_tbls[row.keyspace_name], row.table_name)
    end
  end

  return ks_tbls
end

local function split_keyspaces(keyspace_table_hierarchy)
  local system_keyspaces = {}
  local user_keyspaces = {}

  for ks, _ in pairs(keyspace_table_hierarchy) do
    if ks:match("^system") then
      table.insert(system_keyspaces, ks)
    else
      table.insert(user_keyspaces, ks)
    end
  end

  table.sort(user_keyspaces)
  table.sort(system_keyspaces)

  return system_keyspaces, user_keyspaces
end

function _M.get_system_local_info()
  local client = get_client()
  local row, err = client:execute("SELECT cluster_name, cql_version, data_center, release_version FROM system.local LIMIT 1")
  if err then
    ngx.log(ngx.ERR, "Error fetching system.local info: ", err)
    return nil
  end
  return row[1]
end

function _M.get_shared_context()
  local ctx = init_context()

  if not ctx.initialized then
    local client = get_client()
    local keyspace_table_hierarchy = load_keyspace_hierarchy(client)
    local system_keyspaces, user_keyspaces = split_keyspaces(keyspace_table_hierarchy)

    ctx.config = config
    ctx.connection_metadata = {
      host = client.host,
      port = client.port,
      protocol_version = client.protocol_version,
      client_version = client._VERSION,
      system_local_info = _M.get_system_local_info()
    }
    ctx.keyspace_table_hierarchy = keyspace_table_hierarchy
    ctx.system_keyspaces = system_keyspaces
    ctx.user_keyspaces = user_keyspaces
    ctx.inspect = inspect
    ctx.initialized = true
  end

  return ctx
end

function _M.get_client()
  local ctx = _M.get_shared_context()
  return ctx.client, ctx.host
end

function _M.get_table_metadata(keyspace_name, table_name)
  local client = _M.get_client()
  local schema_query = string.format("SELECT column_name, position, kind, type FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s'", keyspace_name, table_name)

  local col_rows, err = client:execute(schema_query)
  if err then
    ngx.log(ngx.ERR, "Error fetching schema: ", err)
    return nil
  end

  local columns = {}
  for _, row in ipairs(col_rows) do
    table.insert(columns, {
      column_name = row.column_name,
      kind = row.kind,
      position = row.position,
      type = row.type
    })
  end

  table.sort(columns, function(a, b)
    local kind_order = { partition_key = 1, clustering = 2, regular = 3 }
    local a_order = kind_order[a.kind] or 99
    local b_order = kind_order[b.kind] or 99

    if a_order ~= b_order then
      return a_order < b_order
    end

    if (a.kind == "partition_key" or a.kind == "clustering") and a.kind == b.kind then
      return a.position < b.position
    end

    if a.kind == "regular" and b.kind == "regular" then
      return a.column_name < b.column_name
    end

    return false
  end)

  return {
    columns = columns
  }
end

function _M.validate_table_exists(keyspace_name, table_name)
  local shared = _M.get_shared_context()
  local tables = shared.keyspace_table_hierarchy[keyspace_name]

  if not tables then
    return false
  end

  for _, table in ipairs(tables) do
    if table == table_name then
      return true
    end
  end

  return false
end

function _M.reset_context()
  ngx.ctx.template_context = nil
  ngx.log(ngx.ERR, "Context reset - will reinitialize on next request")
end

function _M.render_template(template_name, page_context)
  page_context = page_context or {}
  local shared = _M.get_shared_context()
  local merged = {}

  for k, v in pairs(shared) do
    if k ~= "client" then
      merged[k] = v
    end
  end

  if page_context then
    for k, v in pairs(page_context) do
      merged[k] = v
    end
  end

  if page_context.page_title then
    merged.page_title = string.format("%s | %s", page_context.page_title, config.app_name)
  else
    merged.page_title = config.app_name
  end

  template.render(template_name, merged)
end

return _M
