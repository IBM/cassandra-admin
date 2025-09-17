jit.opt.start("minstitch=10")
require("resty.core")
config = require("config")
cjson = require("cjson")
inspect = require("inspect")
cassandra = require("cassandra")
template = require("resty.template").new({ root = "/app/templates" })
print = ngx.say

local error_page_types = {
  [""] = { title = "Error", log_level = ngx.ERR, exit_code = ngx.HTTP_INTERNAL_SERVER_ERROR },
  ["_not_found"] = { title = "Not Found", log_level = ngx.WARN, exit_code = ngx.HTTP_NOT_FOUND },
  ["_bad_request"] = { title = "Bad Request", log_level = ngx.WARN, exit_code = ngx.HTTP_BAD_REQUEST }
}

local alert_types = {
  success = { title = "Success", type = "success", level = ngx.INFO },
  info = { title = "Information", type = "info", level = ngx.INFO },
  warning = { title = "Warning", type = "warning", level = ngx.WARN },
  error = { title = "Error", type = "danger", level = ngx.ERR }
}

for name, def in pairs(alert_types) do
  _G["alert_" .. name] = function(message, details)
    template.render("alert.html", {
      alert_title = def.title,
      alert_type = def.type,
      alert_message = message,
      alert_details = details
    })

    ngx.log(def.level, message)
  end
end

for name, def in pairs(error_page_types) do
  _G["error_page" .. name] = function(message, details)
    ngx.header["Content-Type"] = "text/html"
    template.render("error.html", {
      page_title = def.title,
      error_message = message,
      error_details = details,
      error_type = def.type
    })

    ngx.log(def.log_level, message)
    ngx.exit(def.exit_code)
  end
end