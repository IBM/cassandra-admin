curl -X POST http://localhost:5001/connect \
  -H "Content-Type: application/json" \
  -d '{
    "host": "pp-cassandra",
    "port": 9042
  }'


curl -X POST http://localhost:5001/disconnect \
  -H "Content-Type: application/json" \
  -d '{
    "host": "cassandra",
    "port": 9042
  }'


curl http://localhost:5001/schema

curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d '{
    "command": "SELECT * FROM system_schema.keyspaces;",
    "session_id": "default",
    "page_size": 10
  }'


curl -X POST http://localhost:5001/execute \
  -H "Content-Type: application/json" \
  -d '{
    "command": "DESCRIBE KEYSPACE cross_business;",
    "session_id": "default"
  }'

curl -X POST http://localhost:5001/execute \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
  "command": "INSERT INTO cross_business.gd_id_to_acid (gd_id, acid, timestamp) VALUES ('aa2', 'bb2', 'cc2');",
  "session_id": "default"
}
EOF

curl -X POST http://localhost:5001/execute \
  -H "Content-Type: application/json" \
  -d '{
    "command": "TRUNCATE ;",
    "session_id": "default"
  }'


curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d '{
    "command": "SELECT * FROM system_schema.columns;",
    "session_id": "default",
    "page_size": 5
  }'

curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d '{
    "command": "SELECT * FROM system_schema.columns;",
    "session_id": "default",
    "page_size": 5,
    "paging_state": "CXRlc3Rwb2ludCUAGGJlaGF2aW9yX3Jlc3VsdHNfbWFwcGluZwpjcmVhdGVkX2F08H////rwf///+g=="
  }'
