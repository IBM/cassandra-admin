local template = require("resty.template")
local config = require("config")
local reqargs = require("resty.reqargs")
local cjson = require("cjson")
cjson.encode_empty_table_as_object(false)

local _M = {}
local routes = { GET = {}, POST = {}, PUT = {}, DELETE = {} }
local global_middleware = {}

function _M.use(middleware)
    table.insert(global_middleware, middleware)
end

function _M.get(path, ...)
    local args = {...}
    local handler = table.remove(args)
    routes.GET[path] = { handler = handler, middleware = args }
end

function _M.post(path, ...)
    local args = {...}
    local handler = table.remove(args)
    routes.POST[path] = { handler = handler, middleware = args }
end

function _M.put(path, ...)
    local args = {...}
    local handler = table.remove(args)
    routes.PUT[path] = { handler = handler, middleware = args }
end

function _M.delete(path, ...)
    local args = {...}
    local handler = table.remove(args)
    routes.DELETE[path] = { handler = handler, middleware = args }
end

local function create_context()    
    local get_args, post_args, files = reqargs()
    
    local query = {}
    if get_args and type(get_args) == "table" then
        for key, value in pairs(get_args) do
            if type(value) ~= "function" and type(value) ~= "userdata" then
                query[key] = value
            end
        end
    end
    
    local body = {}
    if post_args and type(post_args) == "table" then
        for key, value in pairs(post_args) do
            if type(value) ~= "function" and type(value) ~= "userdata" then
                body[key] = value
            end
        end
    end
    
    local uploaded_files = {}
    if files and type(files) == "table" then
        for key, value in pairs(files) do
            if type(value) ~= "function" and type(value) ~= "userdata" then
                uploaded_files[key] = value
            end
        end
    end

    return {
        req = {
            method = ngx.var.request_method,
            path = ngx.var.uri,
            query = query,
            body = body,
            files = uploaded_files,
            params = {},
            headers = ngx.req.get_headers(),
            state = {}
        },
        res = {
            header = function(key, value)
                ngx.header[key] = value
            end,
            json = function(data)
                ngx.header["Content-Type"] = "application/json"
                ngx.say(cjson.encode(data))
            end,
            send = function(text)
                ngx.say(text)
            end,
            render = function(view, data)
                ngx.header["Content-Type"] = "text/html"
                data = data or {}
                data.config = config
                template.render(view, data)
            end,
            status = function(code)
                ngx.status = code
                return {
                    json = function(data)
                        ngx.header["Content-Type"] = "application/json"
                        ngx.say(cjson.encode(data))
                    end,
                    send = function(text)
                        ngx.say(text)
                    end
                }
            end,
            redirect = function(url, code)
                ngx.redirect(url, code or 302)
            end
        }
    }
end

local function match_route(pattern, path)
    local params = {}
    local p = "^" .. pattern:gsub(":([^/]+)", function(name)
        return "([^/]+)"
    end) .. "$"
    
    local matches = {path:match(p)}
    if #matches == 0 then return nil end
    
    local i = 1
    for name in pattern:gmatch(":([^/]+)") do
        params[name] = matches[i]
        i = i + 1
    end
    
    return params
end

local function execute_middleware(middleware_list, req, res, callback)
    local index = 1
    
    local function next()
        if index > #middleware_list then
            callback()
            return
        end
        
        local middleware = middleware_list[index]
        index = index + 1
        middleware(req, res, next)
    end
    
    next()
end

function _M.run()
    local method = ngx.var.request_method
    local path = ngx.var.uri
    local route_info = routes[method][path]
    
    local ctx = create_context()
    
    if route_info then
        local all_middleware = {}
        for _, mw in ipairs(global_middleware) do
            table.insert(all_middleware, mw)
        end
        for _, mw in ipairs(route_info.middleware) do
            table.insert(all_middleware, mw)
        end
        
        execute_middleware(all_middleware, ctx.req, ctx.res, function()
            route_info.handler(ctx.req, ctx.res)
        end)
        return
    end
    
    for pattern, info in pairs(routes[method]) do
        local params = match_route(pattern, path)
        if params then
            ctx.req.params = params
            
            local all_middleware = {}
            for _, mw in ipairs(global_middleware) do
                table.insert(all_middleware, mw)
            end
            for _, mw in ipairs(info.middleware) do
                table.insert(all_middleware, mw)
            end
            
            execute_middleware(all_middleware, ctx.req, ctx.res, function()
                info.handler(ctx.req, ctx.res)
            end)
            return
        end
    end
    
    ngx.status = 404
    ngx.say("Not Found")
end

return _M