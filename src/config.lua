local constants = require("constants")

local _M = {}

local function load_settings()
    local file = io.open(constants.settings_file_path, "r")
    if not file then
        return nil
    end
    
    local content = file:read("*all")
    file:close()
    
    local sandbox = {
        tonumber = tonumber,
        tostring = tostring,
        type = type,
        pairs = pairs,
        ipairs = ipairs,
    }
    
    local chunk, err = load("return " .. content, "settings", "t", sandbox)
    
    if not chunk then
        ngx.log(ngx.ERR, "Failed to parse settings: ", err)
        return nil
    end
    
    local success, settings = pcall(chunk)
    if not success then
        ngx.log(ngx.ERR, "Failed to load settings: ", settings)
        return nil
    end
    
    return settings
end

for k, v in pairs(constants.defaults) do
    if type(v) == "table" then
        _M[k] = {}
        for nested_k, nested_v in pairs(v) do
            _M[k][nested_k] = nested_v
        end
    else
        _M[k] = v
    end
end

local saved_settings = load_settings()
if saved_settings then
    for k, v in pairs(saved_settings) do
        if type(v) == "table" and type(_M[k]) == "table" then
            for nested_k, nested_v in pairs(v) do
                _M[k][nested_k] = nested_v
            end
        else
            _M[k] = v
        end
    end
end

local function apply_env_overrides()
    for env_var, path in pairs(constants.env_mapping) do
        local value = os.getenv(env_var)
        if value then
            local key1, key2, type_cast = path[1], path[2], path[3]
            
            if type_cast == "number" then
                value = tonumber(value)
            elseif type_cast == "boolean" then
                value = (value == "true" or value == "1")
            end
            
            if key2 then
                _M[key1] = _M[key1] or {}
                _M[key1][key2] = value
            elseif key1 then
                _M[key1] = value
            end
        end
    end
end

apply_env_overrides()

return _M