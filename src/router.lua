local _M = {}

local routes = {}

local function match_route(pattern, uri)
  if pattern == uri then
    return true, {}
  end

  local pattern_parts = {}
  local uri_parts = {}
  local params = {}

  for part in string.gmatch(pattern, "([^/]+)") do
    table.insert(pattern_parts, part)
  end

  for part in string.gmatch(uri, "([^/]+)") do
    table.insert(uri_parts, part)
  end

  if #pattern_parts ~= #uri_parts then
    return false, {}
  end

  for i = 1, #pattern_parts do
    local pattern_part = pattern_parts[i]
    local uri_part = uri_parts[i]

    if string.sub(pattern_part, 1, 1) == ":" then
      local param_name = string.sub(pattern_part, 2)
      params[param_name] = uri_part
    elseif pattern_part ~= uri_part then
      return false, {}
    end
  end

  return true, params
end

function _M.register(method, path, handler)
  if not routes[method] then
    routes[method] = {}
  end
  table.insert(routes[method], {
    pattern = path,
    handler = handler
  })
end

function _M.handle()
  local method = ngx.var.request_method
  local uri = ngx.var.uri

  if not routes[method] then
    ngx.status = 405
    return
  end

  for _, route in ipairs(routes[method]) do
    local matched, params = match_route(route.pattern, uri)
    if matched then
      ngx.ctx.route_params = params
      route.handler()
      return
    end
  end
  
  error_page_not_found("The requested path was not found.")
end

function _M.get(path, handler)
  _M.register("GET", path, handler)
end

function _M.post(path, handler)
  _M.register("POST", path, handler)
end

function _M.put(path, handler)
  _M.register("PUT", path, handler)
end

function _M.delete(path, handler)
  _M.register("DELETE", path, handler)
end

return _M
