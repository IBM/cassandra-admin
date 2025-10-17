#!/usr/bin/env /usr/local/openresty/luajit/bin/luajit
local constants = require("constants")
local template = require("resty.template").new({
    root = "/app/config"
})

print(template.compile("nginx.conf")({ constants = constants }))
