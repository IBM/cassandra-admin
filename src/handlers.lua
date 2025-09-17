local context = require("context")

local _M = {}

local function generate_table_rows(rows, table_columns)
  local formatted_rows = {}

  for _, row in ipairs(rows) do
    local formatted_row = {}

    if table_columns then
      for _, col_info in ipairs(table_columns) do
        local column_name = col_info.column_name
        local value = row[column_name]
        local col_type = col_info.type

        local formatted_value = ""
        if value ~= nil then
          if type(value) == "table" then
            formatted_value = cjson.encode(value)
          elseif col_type == "blob" then
            formatted_value = ngx.encode_base64(value)
          else
            formatted_value = tostring(value)
          end
        end

        formatted_row[column_name] = formatted_value
      end
    end

    table.insert(formatted_rows, formatted_row)
  end

  return formatted_rows
end

local function encode_paging_state(paging_state)
  if not paging_state then
    return
  end
  return ngx.encode_base64(paging_state)
end

local function decode_paging_state(encoded_state)
  if not encoded_state or encoded_state == "" then
    return
  end
  local decoded = ngx.decode_base64(encoded_state)
  return decoded
end

local function is_htmx_request()
  local headers = ngx.req.get_headers()
  return headers["Hx-Request"] ~= nil
end


function _M.home()
  ngx.header.content_type = "text/html; charset=utf-8"
  context.render_template("main.html", { page_title = "Home" })
end

function _M.query()
  local keyspace_name, table_name

  if ngx.ctx.route_params then
    keyspace_name = ngx.ctx.route_params.keyspace_name
    table_name = ngx.ctx.route_params.table_name
  end

  ngx.header.content_type = "text/html; charset=utf-8"

  if ngx.var.request_method == "POST" then
    ngx.req.read_body()
    local args = ngx.req.get_post_args()
    local query = args.current_query

    if not query or query == "" then
      error_page_bad_request("Error", "Query cannot be empty.")
    end

    local client = context.get_client()
    local result, err = client:execute(query)

    if err then
      ngx.log(ngx.ERR, "Query error: ", err)
      local page_context = {
        error_message = "Query error: " .. tostring(err),
        current_query = query,
        keyspace_name = keyspace_name,
        table_name = table_name
      }
      context.render_template("table.html", page_context)
      return
    end

    local formatted_rows = {}
    local columns = {}

    if result and result.type == 'ROWS' and #result > 0 then
      if not result.columns then
        for col_name, _ in pairs(result[1]) do
          table.insert(columns, { column_name = col_name })
        end
      else
        local first_col = result.columns[1]
        if first_col.keyspace and first_col.table then
          local table_metadata = context.get_table_metadata(first_col.keyspace, first_col.table)
          columns = table_metadata.columns or {}
        else
          for _, col in ipairs(result.columns) do
            table.insert(columns, { column_name = col.name, type = col.type })
          end
        end
      end
      formatted_rows = generate_table_rows(result, columns)
    end

    local page_context = {
      static_data = true,
      rows = formatted_rows,
      columns = columns,
      current_query = query,
      row_count = #formatted_rows,
      keyspace_name = keyspace_name,
      table_name = table_name
    }

    context.render_template("table.html", page_context)
    return
  end

  if not keyspace_name or not table_name then
    error_page_bad_request("Error", "Keyspace and table name are required.")
  end

  if not context.validate_table_exists(keyspace_name, table_name) then
    error_page_not_found("Error", "Keyspace or table not found.")
  end

  local table_metadata = context.get_table_metadata(keyspace_name, table_name)
  local current_query = string.format("SELECT * FROM %s.%s;", keyspace_name, table_name)
  local headers = ngx.req.get_headers()

  if is_htmx_request() then
    local args = ngx.req.get_uri_args()

    if args.limit or args.paging_state then
      local page_size = tonumber(args.limit) or 50
      local encoded_paging_state = args.paging_state

      if page_size < 1 or page_size > 200 then
        page_size = 50
      end

      local query_options = {
        page_size = page_size
      }

      if encoded_paging_state and encoded_paging_state ~= "" then
        local paging_state = decode_paging_state(encoded_paging_state)
        if paging_state then
          query_options.paging_state = paging_state
        else
          ngx.log(ngx.ERR, "Invalid paging state provided: " .. tostring(encoded_paging_state))
          ngx.status = 400
          return
        end
      end

      local client = context.get_client()
      local result, err = client:execute(current_query, nil, query_options)

      if err then
        ngx.log(ngx.ERR, "Cassandra query error: ", err)
        ngx.status = 500
        ngx.say("Error fetching data")
        return
      end

      local formatted_rows = generate_table_rows(result, table_metadata.columns)

      local page_context = {
        rows = formatted_rows,
        columns = table_metadata.columns or {},
        current_query = current_query
      }

      ngx.header["Cache-Control"] = "no-cache"

      if result.meta then
        if result.meta.has_more_pages then
          ngx.header["X-Has-More-Pages"] = "true"
          if result.meta.paging_state then
            local encoded_state = encode_paging_state(result.meta.paging_state)
            if encoded_state then
              ngx.header["X-Paging-State"] = encoded_state
            end
          end
        else
          ngx.header["X-Has-More-Pages"] = "false"
          ngx.header["HX-Trigger"] = "noMoreData"
        end
      end

      context.render_template("table_rows.html", page_context)
      return
    end
  end

  local client = context.get_client()
  local count_query = string.format("SELECT COUNT(*) FROM %s.%s", keyspace_name, table_name)
  local count_result, count_err = client:execute(count_query)
  local row_count = 0
  if not count_err and count_result and count_result[1] then
    row_count = count_result[1]["count"] or 0
  end

  context.render_template("main.html", {
    page_title = string.format("%s - %s", keyspace_name, table_name),
    keyspace_name = keyspace_name,
    table_name = table_name,
    columns = table_metadata.columns or {},
    selected_keyspace = keyspace_name,
    selected_table = table_name,
    comment = "",
    current_query = current_query,
    row_count = row_count
  })
end

function _M.new_keyspace()
  ngx.header.content_type = "text/html; charset=utf-8"
  if not config.enable_add_keyspace then
    error_page("Adding new keyspaces is disabled in the configuration.")
  end
  
  if is_htmx_request() then
    context.render_template("new_keyspace.html")
  else
    context.render_template("main.html", { page_title = "Create New Keyspace", show_new_keyspace_form = true })
  end
end

function _M.create_keyspace_handler()
  if not config.enable_add_keyspace then
    error_page("Adding new keyspaces is disabled in the configuration.")
  end
  if ngx.var.request_method ~= "POST" then
    error_page_bad_request("Error", "Invalid request method.")
  end

  ngx.req.read_body()
  local args = ngx.req.get_post_args()

  local keyspace_name = args.keyspace_name
  local replication_strategy = args.replication_strategy
  local replication_factor = args.replication_factor
  local durable_writes = args.durable_writes and args.durable_writes == "on"
  local comment = args.comment or ""

  if not keyspace_name or keyspace_name == "" then
    alert_error("Keyspace name is required.")
    return
  end

  if not replication_strategy or replication_strategy == "" then
    alert_error("Replication strategy is required.")
    return
  end

  if not keyspace_name:match("^[a-zA-Z][a-zA-Z0-9_]*$") then
    alert_error(
      "Invalid keyspace name. Must start with a letter and contain only letters, numbers, and underscores.")
    return
  end

  local replication_config = ""

  if replication_strategy == "SimpleStrategy" then
    local factor = tonumber(replication_factor) or 3
    if factor < 1 or factor > 10 then
      factor = 3
    end
    replication_config = string.format("{'class': 'SimpleStrategy', 'replication_factor': %d}", factor)
  elseif replication_strategy == "NetworkTopologyStrategy" then
    local datacenter_names = args.datacenter_name
    local datacenter_replicas = args.datacenter_replicas

    local dc_configs = {}

    if type(datacenter_names) == "string" then
      datacenter_names = { datacenter_names }
      datacenter_replicas = { datacenter_replicas }
    end

    if datacenter_names and #datacenter_names > 0 then
      for i, dc_name in ipairs(datacenter_names) do
        local replicas = tonumber(datacenter_replicas[i]) or 3
        if dc_name and dc_name ~= "" then
          table.insert(dc_configs, string.format("'%s': %d", dc_name, replicas))
        end
      end
    end

    if #dc_configs == 0 then
      table.insert(dc_configs, "'datacenter1': 1")
    end

    replication_config = string.format("{'class': 'NetworkTopologyStrategy', %s}", table.concat(dc_configs, ", "))
  else
    alert_error("Invalid replication strategy: " .. tostring(replication_strategy))
    return
  end

  local durable_writes_str = durable_writes and "true" or "false"
  local create_query = string.format("CREATE KEYSPACE %s WITH REPLICATION = %s AND DURABLE_WRITES = %s", keyspace_name, replication_config, durable_writes_str)

  local client = context.get_client()
  local result, err = client:execute(create_query)

  if err then
    alert_error("Failed to create keyspace: " .. tostring(err))
    return
  end

  context.reset_context()

  alert_success("Keyspace '" .. keyspace_name .. "' has been created successfully.")
end

function _M.new_table()
  if not config.enable_add_table then
    error_page("Adding new tables is disabled in the configuration.")
  end
  ngx.header.content_type = "text/html; charset=utf-8"
  
  if is_htmx_request() then
    context.render_template("new_table.html", { page_title = "Create New Table" })
  else
    context.render_template("main.html", { page_title = "Create New Table", show_new_table_form = true })
  end
end

return _M
