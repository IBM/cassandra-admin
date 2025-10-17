local _M = {}

local cql_types = {
  custom    = 0x00,
  ascii     = 0x01,
  bigint    = 0x02,
  blob      = 0x03,
  boolean   = 0x04,
  counter   = 0x05,
  decimal   = 0x06,
  double    = 0x07,
  float     = 0x08,
  int       = 0x09,
  text      = 0x0A,
  timestamp = 0x0B,
  uuid      = 0x0C,
  varchar   = 0x0D,
  varint    = 0x0E,
  timeuuid  = 0x0F,
  inet      = 0x10,
  list      = 0x20,
  map       = 0x21,
  set       = 0x22,
  udt       = 0x30,
  tuple     = 0x31,
}


local function format_list(list_data, type_value)
    if list_data == nil then
        return ""
    end
    
    if type(list_data) ~= "table" then
        return tostring(list_data)
    end
    
    local parts = {}
    for _, v in ipairs(list_data) do
        table.insert(parts, _M.format(v, type_value))
    end
    
    return "[" .. table.concat(parts, ", ") .. "]"
end

local function format_set(set_data, type_value)
    if set_data == nil then
        return ""
    end
    
    if type(set_data) ~= "table" then
        return tostring(set_data)
    end
    
    local parts = {}
    for _, v in ipairs(set_data) do
        table.insert(parts, _M.format(v, type_value))
    end
    
    return "{" .. table.concat(parts, ", ") .. "}"
end

local function format_map(map_data, type_value)
    if map_data == nil then
        return ""
    end
    
    if type(map_data) ~= "table" then
        return tostring(map_data)
    end
    
    local parts = {}
    local key_type = type_value and type_value[1]
    local val_type = type_value and type_value[2]
    
    for k, v in pairs(map_data) do
        local key_str = _M.format(k, key_type)
        local val_str = _M.format(v, val_type)
        table.insert(parts, key_str .. ": " .. val_str)
    end
    
    table.sort(parts)
    return "{" .. table.concat(parts, ", ") .. "}"
end

function _M.blob_to_hex(blob_data)
    if not blob_data then return nil end
    local hex = {}
    for i = 1, #blob_data do
        table.insert(hex, string.format("%02x", string.byte(blob_data, i)))
    end
    return "0x" .. table.concat(hex)
end

function _M.format(value, type_info)
    if value == nil then
        return ""
    end
    
    if not type_info then
        return tostring(value)
    end
    
    local cql_type = type_info.__cql_type
    
    if cql_type == cql_types.blob then
        return _M.blob_to_hex(value)
    elseif cql_type == cql_types.uuid or cql_type == cql_types.timeuuid then
        return tostring(value)
    elseif cql_type == cql_types.varchar or cql_type == cql_types.ascii or cql_type == cql_types.text then
        return tostring(value)
    elseif cql_type == cql_types.inet then
        return tostring(value)
    elseif cql_type == cql_types.map then
        return format_map(value, type_info.__cql_type_value)
    elseif cql_type == cql_types.set then
        return format_set(value, type_info.__cql_type_value)
    elseif cql_type == cql_types.list then
        return format_list(value, type_info.__cql_type_value)
    else
        return tostring(value)
    end
end

function _M.format_cql_value(value, type_info)
    if value == nil or value == "" then
        return "null"
    end
    
    if not type_info then
        -- No type info, attempt to format safely
        if type(value) == "string" then
            return "'" .. value:gsub("'", "''") .. "'"
        end
        return tostring(value)
    end
    
    local cql_type = type_info.__cql_type
    
    -- Numeric types - no quotes
    if cql_type == cql_types.int or 
       cql_type == cql_types.bigint or 
       cql_type == cql_types.counter or
       cql_type == cql_types.varint or
       cql_type == cql_types.float or
       cql_type == cql_types.double or
       cql_type == cql_types.decimal then
        return tostring(value)
    end
    
    -- Boolean - no quotes
    if cql_type == cql_types.boolean then
        return tostring(value)
    end
    
    -- Blob - hex format, no quotes
    if cql_type == cql_types.blob then
        return _M.blob_to_hex(value)
    end
    
    -- UUID/TimeUUID - no quotes
    if cql_type == cql_types.uuid or cql_type == cql_types.timeuuid then
        return tostring(value)
    end
    
    -- Text types - with quotes
    if cql_type == cql_types.text or 
       cql_type == cql_types.varchar or 
       cql_type == cql_types.ascii then
        return "'" .. tostring(value):gsub("'", "''") .. "'"
    end
    
    -- Timestamp - with quotes
    if cql_type == cql_types.timestamp then
        return "'" .. tostring(value) .. "'"
    end
    
    -- Inet - with quotes
    if cql_type == cql_types.inet then
        return "'" .. tostring(value) .. "'"
    end
    
    -- Collections
    if cql_type == cql_types.list then
        if type(value) ~= "table" then
            return "[]"
        end
        local parts = {}
        local elem_type = type_info.__cql_type_value
        for _, v in ipairs(value) do
            table.insert(parts, _M.format_cql_value(v, elem_type))
        end
        return "[" .. table.concat(parts, ", ") .. "]"
    end
    
    if cql_type == cql_types.set then
        if type(value) ~= "table" then
            return "{}"
        end
        local parts = {}
        local elem_type = type_info.__cql_type_value
        for _, v in ipairs(value) do
            table.insert(parts, _M.format_cql_value(v, elem_type))
        end
        return "{" .. table.concat(parts, ", ") .. "}"
    end
    
    if cql_type == cql_types.map then
        if type(value) ~= "table" then
            return "{}"
        end
        local parts = {}
        local key_type = type_info.__cql_type_value and type_info.__cql_type_value[1]
        local val_type = type_info.__cql_type_value and type_info.__cql_type_value[2]
        
        for k, v in pairs(value) do
            local key_str = _M.format_cql_value(k, key_type)
            local val_str = _M.format_cql_value(v, val_type)
            table.insert(parts, key_str .. ": " .. val_str)
        end
        table.sort(parts)
        return "{" .. table.concat(parts, ", ") .. "}"
    end
    
    -- UDT and Tuple - treat as generic with quotes
    if cql_type == cql_types.udt or cql_type == cql_types.tuple then
        return "'" .. tostring(value):gsub("'", "''") .. "'"
    end
    
    -- Default fallback - quote it
    return "'" .. tostring(value):gsub("'", "''") .. "'"
end

-- Generate CQL INSERT statement from a row
function _M.format_cql_insert(keyspace, table_name, row, columns, column_type_map)
    local col_names = {}
    local values = {}
    
    for _, col in ipairs(columns) do
        local val = row[col.column_name]
        
        if val ~= nil and val ~= "" then
            table.insert(col_names, col.column_name)
            
            local type_info = column_type_map and column_type_map[col.column_name]
            table.insert(values, _M.format_cql_value(val, type_info))
        end
    end
    
    if #col_names == 0 then
        return nil
    end
    
    return string.format(
        "INSERT INTO %s.%s (%s) VALUES (%s);",
        keyspace,
        table_name,
        table.concat(col_names, ", "),
        table.concat(values, ", ")
    )
end

function _M.get_sorted_columns(col_rows)
    local columns = {}
    for _, row in ipairs(col_rows) do
        table.insert(columns, {
            column_name = row.column_name,
            kind = row.kind,
            position = row.position,
            type = row.type,
            clustering_order = row.clustering_order
        })
    end
    
    table.sort(columns, function(a, b)
        local kind_order = {partition_key = 1, clustering = 2, regular = 3}
        local a_order = kind_order[a.kind] or 99
        local b_order = kind_order[b.kind] or 99
        
        if a_order ~= b_order then
            return a_order < b_order
        end
        
        if (a.kind == "partition_key" or a.kind == "clustering") and a.kind == b.kind then
            return (a.position or 0) < (b.position or 0)
        end
        
        if a.kind == "regular" and b.kind == "regular" then
            return a.column_name < b.column_name
        end
        
        return false
    end)
    
    return columns
end

function _M.format_rows(result)
    if not result or type(result) ~= "table" then
        return {}
    end
    
    local rows_array = {}
    for k, v in pairs(result) do
        if type(k) == "number" then
            table.insert(rows_array, v)
        end
    end
    
    if #rows_array == 0 then
        for _, row in ipairs(result) do
            table.insert(rows_array, row)
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

    local formatted_rows = {}
    
    for _, row in ipairs(rows_array) do
        if type(row) == "table" then
            local formatted_row = {}
            
            for column_name, value in pairs(row) do
                if value ~= nil then
                    local type_info = column_type_map[column_name]
                    formatted_row[column_name] = _M.format(value, type_info)
                else
                    formatted_row[column_name] = ""
                end
            end
            
            table.insert(formatted_rows, formatted_row)
        end
    end
    
    return formatted_rows
end

return _M