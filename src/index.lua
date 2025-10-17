local app = require("framework")
local db = require("db")
local config = require("config")
local utils = require("utils")

app.configure_template({
    template_root = config.template_path or "templates",
    caching = not (os.getenv("CA_DEBUG_MODE") == "true")
})

local function getSchemaHandler(req, res)
    local schema = db.getSchema()
    if not schema then
        return res.status(500).json({ error = "Failed to fetch schema" })
    end
    return res.json(schema)
end

local function getTableDataHandler(req, res)
    local view_table = req.params.table or req.params.view
    local data, err = db.getTableData(req.params.keyspace, view_table, req.query.page_size, req.query.paging_state)
    if not data then
        return res.status(500).json({ error = "Failed to fetch table data: " .. tostring(err) })
    end
    return res.json(data)
end

local function truncateTableHandler(req, res)
    local ok, err = db.truncateTable(req.params.keyspace, req.params.table)
    if not ok then
        return res.status(500).json({ error = "Failed to truncate table: " .. tostring(err) })
    end
    return res.json({ success = true, message = "Table truncated" })
end

local function dropEntityHandler(req, res)
    if not utils.table_contains({"table", "view"}, req.params.entity) then
        return res.status(400).json({ error = "Invalid entity type" })
    end
    local ok, err = db.dropEntity(req.params.entity, req.params.keyspace, req.params.table)
    if not ok then
        return res.status(500).json({ error = "Failed to drop table: " .. tostring(err) })
    end
    return res.json({ success = true, message = string.format("Dropped %s %s.%s", req.params.entity, req.params.keyspace, req.params.table) })
end

local function exportTableHandler(req, res)
    local cql, err = db.exportTableData(req.params.keyspace, req.params.table, req.body.format, req.body.limit, req.body.include_ddl == "1")
    
    if not cql then
        return res.status(500).json({ error = "Failed to export table: " .. tostring(err) })
    end
    
    res.header("Content-Type", "text/plain")
    --res.header("Content-Disposition", string.format('attachment; filename="%s_%s.%s"', keyspace, table_name, format))
    return res.send(cql)
end

local function dropKeyspaceHandler(req, res)
    local ok, err = db.dropKeyspace(req.params.keyspace)
    if not ok then
        return res.status(500).json({ error = "Failed to drop keyspace: " .. tostring(err) })
    end
    return res.json({ success = true, message = "Keyspace dropped" })
end 

local function mainHandler(req, res)
    local entity_type = req.params.entity_type
    res.render("main.html")
end

app.get("/", mainHandler)
app.get("/table/:keyspace/:table", mainHandler)
app.get("/view/:keyspace/:table", mainHandler)

app.get("/api/schema", getSchemaHandler)
app.get("/api/table/:keyspace/:table", getTableDataHandler)
app.get("/api/view/:keyspace/:view", getTableDataHandler)
app.post("/api/table/:keyspace/:table/truncate", truncateTableHandler)
app.post("/api/:entity/:keyspace/:table/drop", dropEntityHandler)
app.post("/api/table/:keyspace/:table/export", exportTableHandler)
app.post("/api/view/:keyspace/:table/export", exportTableHandler)
app.post("/api/keyspace/:keyspace/drop", dropKeyspaceHandler)
app.get("/settings", function(req, res)
    res.render("settings.html", { config = config, inspect = require("inspect") })
end)
app.run()