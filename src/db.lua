local cjson = require("cjson")
local cassandra = require("cassandra")
local config = require("config")
local formatting = require("formatting")
local utils = require("utils")

local _M = {}

local system_keyspaces = {"system", "system_auth", "system_distributed", "system_schema", "system_traces"}

local function handle_error(err)
    local error_msg = string.format("Database error: %s", tostring(err))
    ngx.log(ngx.ERR, error_msg)
    
    ngx.header.content_type = "application/json"
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say(cjson.encode({
        error = true,
        message = error_msg
    }))
    ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
end

local function connect()
    local peer = cassandra.new({
        host = config.connection.host,
        port = utils.coerce_positive_integer(config.connection.port, 9042),
        auth = cassandra.auth_providers[config.connection.auth_provider](config.connection.username, config.connection.password),
    })
    peer:settimeout(config.connection.timeout)
    local ok, err = peer:connect()
    if not ok then
        handle_error(err)
    end
    return peer
end

local function execute(query, params, options)
    local peer = connect()

    if not peer then
        handle_error("No database connection")
    end
    local result, err = peer:execute(query, params, options)
    if not result then
        handle_error(err)
    end
    return result
end

function _M.getSchema()
    local peer = connect()
    if not peer then
        handle_error("No database connection")
    end
    
    local keyspaces_query = "SELECT keyspace_name FROM system_schema.keyspaces;"
    local keyspaces = peer:execute(keyspaces_query)
    if not keyspaces then
        handle_error("Failed to fetch keyspaces")
    end

    local tables_query = "SELECT * FROM system_schema.tables;"
    local tables = peer:execute(tables_query)
    if not tables then
        handle_error("Failed to fetch tables")
    end

    local views_query = "SELECT * FROM system_schema.views;"
    local views = peer:execute(views_query)
    if not views then
        handle_error("Failed to fetch views")
    end

    local keyspace_entities = {}
    for _, tbl in ipairs(tables) do
        if not keyspace_entities[tbl.keyspace_name] then
            keyspace_entities[tbl.keyspace_name] = {}
        end
        table.insert(keyspace_entities[tbl.keyspace_name], { name = tbl.table_name, type = "table", comment = tbl.comment })
    end
    for _, view in ipairs(views) do
        if not keyspace_entities[view.keyspace_name] then
            keyspace_entities[view.keyspace_name] = {}
        end
        table.insert(keyspace_entities[view.keyspace_name], { name = view.view_name, type = "view", comment = view.comment })
    end

    local schema = {}
    for _, ks in ipairs(keyspaces) do
        local entities = keyspace_entities[ks.keyspace_name] or {}
        table.sort(entities, function(a, b)
            return a.name < b.name
        end)
        table.insert(schema, {
            keyspace = ks.keyspace_name,
            entities = entities
        })
    end

    table.sort(schema, function(a, b)
        return a.keyspace < b.keyspace
    end)

    return schema
end

local function getTableColumns(keyspace, table_name)
    local query = string.format([[
        SELECT column_name, type, kind, clustering_order, position
        FROM system_schema.columns 
        WHERE keyspace_name = '%s' AND table_name = '%s'
    ]], keyspace, table_name)

    local result = execute(query)
    if not result then
        return nil, "Failed to fetch columns"
    end

    local columns = formatting.get_sorted_columns(result)
    return columns
end

function _M.getTableData(keyspace, table_name, page_size, paging_state_encoded)
    page_size = utils.coerce_positive_integer(page_size, config.default_page_size)
    
    local query_options = {
        page_size = page_size
    }
    
    if paging_state_encoded and paging_state_encoded ~= "" then
        local paging_state = ngx.decode_base64(paging_state_encoded)
        if paging_state then
            query_options.paging_state = paging_state
        end
    end
    
    local columns = getTableColumns(keyspace, table_name)

    local query = string.format("SELECT * FROM %s.%s", keyspace, table_name)
    local result = execute(query, nil, query_options)
    
    local has_more_pages = false
    local next_paging_state = nil
    
    if result then
        if result.meta then
            has_more_pages = result.meta.has_more_pages or false
            if has_more_pages and result.meta.paging_state then
                next_paging_state = ngx.encode_base64(result.meta.paging_state)
            end
        end
    end
    
    local formatted_rows = formatting.format_rows(result)

    return {
        keyspace = keyspace,
        table = table_name,
        rows = formatted_rows,
        columns = columns,
        has_more_pages = has_more_pages,
        paging_state = next_paging_state,
        page_size = page_size
    }
end

function _M.truncateTable(keyspace, table_name)
    if utils.table_contains(system_keyspaces, keyspace) then
        return nil, "System keyspaces are not user-modifiable."
    end

    local query = string.format("TRUNCATE %s.%s", keyspace, table_name)
    local result = execute(query)
    
    return true
end

function _M.dropEntity(entity_type, keyspace, table_name)
    if utils.table_contains(system_keyspaces, keyspace) then
        return nil, "System keyspaces are not user-modifiable."
    end

    local entity_types = {
        table = "TABLE",
        view = "MATERIALIZED VIEW"
    }
    
    local result = execute(string.format("DROP %s %s.%s", entity_types[entity_type], keyspace, table_name))

    return true
end

function _M.dropKeyspace(keyspace)
    if utils.table_contains(system_keyspaces, keyspace) then
        return nil, "System keyspaces are not user-modifiable."
    end

    local result = execute(string.format("DROP KEYSPACE %s", keyspace))

    return true
end

function _M.getTableDDL(keyspace, table_name)
    local table_info_query = string.format([[
        SELECT * FROM system_schema.tables 
        WHERE keyspace_name = '%s' AND table_name = '%s' LIMIT 1
    ]], keyspace, table_name)

    local table_info = execute(table_info_query)
    if #table_info == 0 then
        return nil, "Table does not exist"
    end
    
    local tbl = table_info[1]
    
    local columns = getTableColumns(keyspace, table_name)
    
    local cql = {}
    table.insert(cql, string.format("CREATE TABLE IF NOT EXISTS %s.%s (", keyspace, table_name))
    
    local col_defs = {}
    local partition_keys = {}
    local clustering_keys = {}
    
    for _, col in ipairs(columns) do
        table.insert(col_defs, string.format("    %s %s", col.column_name, col.type))
        if col.kind == "partition_key" then
            table.insert(partition_keys, col.column_name)
        elseif col.kind == "clustering" then
            table.insert(clustering_keys, col.column_name)
        end
    end
    
    table.insert(cql, table.concat(col_defs, ",\n") .. ",")
    
    local pk = "    PRIMARY KEY ("
    if #partition_keys > 1 then
        pk = pk .. "(" .. table.concat(partition_keys, ", ") .. ")"
    else
        pk = pk .. partition_keys[1]
    end
    if #clustering_keys > 0 then
        pk = pk .. ", " .. table.concat(clustering_keys, ", ")
    end
    pk = pk .. ")"
    
    table.insert(cql, pk)
    table.insert(cql, ")")

    local column_type_map = {}
    if table_info.columns and type(table_info.columns) == "table" then
        for _, col_meta in ipairs(table_info.columns) do
            if col_meta and col_meta.name then
                column_type_map[col_meta.name] = col_meta.type
            end
        end
    end
    
    local with_clauses = {}
    
    if #clustering_keys > 0 then
        local orders = {}
        for _, col in ipairs(columns) do
            if col.kind == "clustering" then
                table.insert(orders, string.format("%s %s", col.column_name, col.clustering_order:upper()))
            end
        end
        if #orders > 0 then
            table.insert(cql, "WITH CLUSTERING ORDER BY (" .. table.concat(orders, ", ") .. ")")
        end
    end
    
    local skip_columns = {
        keyspace_name = true,
        table_name = true,
        id = true,
        flags = true
    }
    
    local properties = {}
    for column_name, value in pairs(tbl) do
        if not skip_columns[column_name] and value ~= nil and value ~= "" then
            local type_info = column_type_map[column_name]
            local formatted_value = formatting.format_cql_value(value, type_info)
            table.insert(properties, {
                name = column_name,
                value = formatted_value
            })
        end
    end
    
    table.sort(properties, function(a, b)
        return a.name < b.name
    end)
    
    for _, prop in ipairs(properties) do
        table.insert(with_clauses, "AND " .. prop.name .. " = " .. prop.value)
    end
    
    if #with_clauses > 0 then
        table.insert(cql, table.concat(with_clauses, "\n"))
    end
    
    table.insert(cql, ";")
    return table.concat(cql, "\n")
end

function _M.exportTableData(keyspace, table_name, format, limit, include_ddl)
    if utils.table_contains({"cql", "csv", "json"}, format) == false then
        return nil, "Unsupported export format: " .. tostring(format)
    end

    local limit = utils.coerce_positive_integer(limit, 100)
    local columns = getTableColumns(keyspace, table_name)
    local result = execute(string.format("SELECT * FROM %s.%s LIMIT %d", keyspace, table_name, limit))
    local formatted_rows = formatting.format_rows(result)

    if format == "json" then
        return cjson.encode(formatted_rows)
    end
    
    local output = {}
    
    if format == "cql" then
        if include_ddl then
            local ddl, ddl_err = _M.getTableDDL(keyspace, table_name)
            if ddl then
                table.insert(output, ddl)
                table.insert(output, "")
            end
        end
        
        local column_type_map = {}
        if result.columns and type(result.columns) == "table" then
            for _, col_meta in ipairs(result.columns) do
                if col_meta and col_meta.name then
                    column_type_map[col_meta.name] = col_meta.type
                end
            end
        end
        
        for _, row in ipairs(result) do
            if type(row) == "table" then
                local insert_stmt = formatting.format_cql_insert(
                    keyspace, 
                    table_name, 
                    row, 
                    columns, 
                    column_type_map
                )
                if insert_stmt then
                    table.insert(output, insert_stmt)
                end
            end
        end
    end
    
    if format == "csv" then
        local headers = {}
        for _, col in ipairs(columns) do
            table.insert(headers, col.column_name)
        end
        table.insert(output, table.concat(headers, ","))
        
        for _, formatted_row in ipairs(formatted_rows) do
            local values = {}
            for _, col in ipairs(columns) do
                local val = formatted_row[col.column_name]
                if val == nil or val == "" then
                    table.insert(values, "")
                elseif type(val) == "string" then
                    table.insert(values, '"' .. val:gsub('"', '""') .. '"')
                else
                    table.insert(values, tostring(val))
                end
            end
            table.insert(output, table.concat(values, ","))
        end
    end
    return table.concat(output, "\n")
end


return _M