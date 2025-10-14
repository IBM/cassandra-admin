local cjson = require("cjson")
local cassandra = require("cassandra")
local config = require("config")
local formatting = require("formatting")
local utils = require("utils")

local _M = {}

local system_keyspaces = {
    system = true,
    system_auth = true,
    system_distributed = true,
    system_schema = true,
    system_traces = true,
}


local function handle_error(err)
    local error_msg = string.format("Database error: %s", tostring(err))
    ngx.log(ngx.ERR, error_msg)
    
    ngx.header.content_type = "application/json"
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

local function execute_query(query, params, options)
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

    -- Build unified relations list per keyspace
    local keyspace_entities = {}
    for _, tbl in ipairs(tables) do
        if not keyspace_entities[tbl.keyspace_name] then
            keyspace_entities[tbl.keyspace_name] = {}
        end
        table.insert(keyspace_entities[tbl.keyspace_name], { name = tbl.table_name, type = "table" })
    end
    for _, view in ipairs(views) do
        if not keyspace_entities[view.keyspace_name] then
            keyspace_entities[view.keyspace_name] = {}
        end
        table.insert(keyspace_entities[view.keyspace_name], { name = view.view_name, type = "view" })
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

function _M.get_table_columns(keyspace, table_name)
    local query = string.format([[
        SELECT column_name, type, kind, clustering_order, position
        FROM system_schema.columns 
        WHERE keyspace_name = '%s' AND table_name = '%s'
    ]], keyspace, table_name)

    local result = execute_query(query)
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
    
    local columns = _M.get_table_columns(keyspace, table_name)

    local query = string.format("SELECT * FROM %s.%s", keyspace, table_name)
    local result = execute_query(query, nil, query_options)
    
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
    if system_keyspaces[keyspace] then
        return nil, "System keyspaces are not user-modifiable."
    end

    local query = string.format("TRUNCATE %s", table_name)
    local result = execute_query(query)
    
    return true
end

function _M.dropTable(keyspace, table_name)
    if system_keyspaces[keyspace] then
        return nil, "System keyspaces are not user-modifiable."
    end

    local result = execute_query(string.format("DROP TABLE %s", table_name))

    return true
end

function _M.dropKeyspace(keyspace)
    if system_keyspaces[keyspace] then
        return nil, "System keyspaces are not user-modifiable."
    end

    local result = execute_query(string.format("DROP KEYSPACE %s", keyspace))

    return true
end

function _M.getTableDDL(keyspace, table_name)
    local table_info_query = string.format([[
        SELECT * FROM system_schema.tables 
        WHERE keyspace_name = '%s' AND table_name = '%s'
    ]], keyspace, table_name)

    local table_info = execute_query(table_info_query)
    if #table_info == 0 then
        return nil, "Table does not exist"
    end
    
    local columns = _M.get_table_columns(keyspace, table_name)
    
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

    local formatted_info = formatting.format_rows(table_info)
    if formatted_info and #formatted_info > 0 then
        local info = formatted_info[1]
        local options = {}
        
        local skip_columns = {
            keyspace_name = true,
            table_name = true,
            id = true,
            flags = true
        }
        
        for column, value in pairs(info) do
            if not skip_columns[column] and value ~= nil and value ~= "" then
                table.insert(options, string.format("%s = %s", column, value))
            end
        end
        
        if #options > 0 then
            table.sort(options)
            local options_str = table.concat(options, "\n    AND ")
            if #clustering_keys == 0 then
                table.insert(cql, "WITH " .. options_str .. ";")
            else
                table.insert(cql, "AND " .. options_str .. ";")
            end
        else
            table.insert(cql, ";")
        end
    else
        table.insert(cql, ";")
    end
    
    return table.concat(cql, "\n")
end

function _M.exportTableData(keyspace, table_name, format, limit, include_ddl)
    if utils.table_contains({"cql", "csv", "json"}, format) == false then
        return nil, "Unsupported export format: " .. tostring(format)
    end

    local limit = utils.coerce_positive_integer(limit, 100)

    local columns = _M.get_table_columns(keyspace, table_name)
    
    local data_query = string.format("SELECT * FROM %s.%s", keyspace, table_name)
    
    data_query = data_query .. string.format(" LIMIT %d", limit)
    
    local result = execute_query(data_query)
    
    if not result then
        result = {}
    end

    local formatted_rows = formatting.format_rows(result)
    
    local output = {}
    
    if format == "cql" then
        if include_ddl then
            local ddl, ddl_err = _M.getTableDDL(keyspace, table_name)
            if ddl then
                table.insert(output, ddl)
                table.insert(output, "")
            end
        end
        
        for _, formatted_row in ipairs(formatted_rows) do
            local col_names = {}
            local values = {}
            
            for _, col in ipairs(columns) do
                local val = formatted_row[col.column_name]
                if val ~= nil and val ~= "" then
                    table.insert(col_names, col.column_name)
                    -- fix this
                    if type(val) == "string" and not val:match("^0x") then
                        table.insert(values, string.format("'%s'", val:gsub("'", "''")))
                    else
                        table.insert(values, val)
                    end
                end
            end
            
            if #col_names > 0 then
                table.insert(output, string.format(
                    "INSERT INTO %s.%s (%s) VALUES (%s);",
                    keyspace,
                    table_name,
                    table.concat(col_names, ", "),
                    table.concat(values, ", ")
                ))
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

    if format == "json" then
        return cjson.encode(formatted_rows)
    end
    
    return table.concat(output, "\n")
end


return _M