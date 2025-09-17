local router = require("router")
local handlers = require("handlers")

router.get("/", handlers.home)
router.get("/view/:keyspace_name/:table_name", handlers.query)
router.post("/query", handlers.query)
if config.enable_add_keyspace then
    router.get("/new/keyspace", handlers.new_keyspace)
    router.post("/keyspaces/create", handlers.create_keyspace_handler)
end 
if config.enable_add_table then
    router.get("/new/table/:keyspace_name", handlers.new_table)
end

router.handle()