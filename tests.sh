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


curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
    "command": "CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };",
    "session_id": "default"
}
EOF

curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
    "command": "CREATE TYPE mykeyspace.address ( street text, city text, zip_code int, phones set<text> );",
    "session_id": "default"
}
EOF

curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
    "command": "CREATE TYPE mykeyspace.fullname ( firstname text, lastname text );",
    "session_id": "default"
}
EOF

curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
    "command": "CREATE TABLE mykeyspace.users ( id uuid PRIMARY KEY, name frozen <fullname>, direct_reports set<frozen <fullname>>, addresses map<text, frozen <address>>  );",
    "session_id": "default"
}
EOF

curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
    "command": "INSERT INTO mykeyspace.users (id, name) VALUES (62c36092-82a1-3a00-93d1-46196ee77204, {firstname: 'Marie-Claude', lastname: 'Josset'});",
    "session_id": "default"
}
EOF


curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
    "command": "UPDATE mykeyspace.users SET addresses = addresses + {'home': { street: '191 Rue St. Charles', city: 'Paris', zip_code: 75015, phones: {'33 6 78 90 12 34'}}} WHERE id=62c36092-82a1-3a00-93d1-46196ee77204;",
    "session_id": "default"
}
EOF

curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
    "command": "SELECT * FROM mykeyspace.users;"
}
EOF

curl -X POST http://localhost:5001/api/execute \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
  "command": "DESCRIBE KEYSPACE mykeyspace;",
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
