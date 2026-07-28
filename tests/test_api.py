import os
import socket
import subprocess
import sys
import threading
import time

import pytest
import requests


def get_free_port():
    """Finds an available open port on the host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def forward_logs(stream, prefix=""):
    """Reads from a subprocess stream and writes to sys.stdout."""
    for line in iter(stream.readline, b""):
        sys.stdout.write(f"{prefix}{line.decode(errors='replace')}")
        sys.stdout.flush()


@pytest.fixture(scope="session")
def app_url():
    """Spawns the Flask app in a separate process for the test session."""
    port = get_free_port()
    base_url = f"http://localhost:{port}"
    
    env = os.environ.copy()
    for key in list(env.keys()):
        if key.startswith("CASSANDRA_"):
            del env[key]
            
    env["APP_PORT"] = str(port)
    env["APP_LISTEN"] = "127.0.0.1"
    
    app_path = "/app/app.py"

    proc = subprocess.Popen(
        [sys.executable, "-u", app_path],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    threading.Thread(target=forward_logs, args=(proc.stdout, "[Flask] "), daemon=True).start()
    threading.Thread(target=forward_logs, args=(proc.stderr, "[Flask ERR] "), daemon=True).start()
    
    server_up = False
    for _ in range(30):
        try:
            response = requests.get(f"{base_url}/")
            if response.status_code in [200, 404]:
                server_up = True
                break
        except requests.ConnectionError:
            time.sleep(0.2)
            
    if not server_up:
        proc.terminate()
        raise RuntimeError("Flask app failed to start in time. Check the logs above.")
        
    yield base_url
    
    proc.terminate()
    proc.wait()


# --- Base API Tests ---

def test_index_route(app_url):
    response = requests.get(f"{app_url}/")
    assert response.status_code in [200, 404], f"Index failed: {response.text}"


def test_execute_without_connection(app_url):
    response = requests.post(f"{app_url}/api/execute", json={
        "command": "SELECT * FROM system.local;"
    })
    
    assert response.status_code == 400, f"Expected 400, got: {response.text}"
    data = response.json()
    assert data.get("error") == "Not connected. Call /connect first"


def test_connect_invalid_host(app_url):
    response = requests.post(f"{app_url}/api/connect", json={
        "host": "invalid-host-that-does-not-exist",
        "session_id": "test_session"
    })
    
    assert response.status_code == 500, f"Expected 500, got: {response.text}"
    assert "error" in response.json()


# --- Integration Test Fixtures ---

@pytest.fixture
def db_session_id(app_url):
    """Establishes a connection to Cassandra and handles keyspace cleanup."""
    session_id = "lifecycle_session"
    host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    
    res = requests.post(f"{app_url}/api/connect", json={"host": host, "session_id": session_id})
    assert res.status_code == 200, f"Connect failed: {res.text}"
    
    # Ensure clean slate before test
    requests.post(f"{app_url}/api/execute", json={
        "command": "DROP KEYSPACE IF EXISTS mykeyspace;",
        "session_id": session_id
    })
    
    yield session_id
    
    # Teardown after test
    requests.post(f"{app_url}/api/execute", json={
        "command": "DROP KEYSPACE IF EXISTS mykeyspace;",
        "session_id": session_id
    })


@pytest.fixture
def schema_setup(app_url, db_session_id):
    """Sets up the initial schema (keyspace, types, tables) for tests that require them."""
    commands = [
        "CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };",
        "CREATE TYPE mykeyspace.address ( street text, city text, zip_code int, phones set<text> );",
        "CREATE TYPE mykeyspace.fullname ( firstname text, lastname text );",
        "CREATE TABLE mykeyspace.users ( id uuid PRIMARY KEY, name frozen <fullname>, direct_reports set<frozen <fullname>>, addresses map<text, frozen <address>> );"
    ]
    
    for cmd in commands:
        res = requests.post(f"{app_url}/api/execute", json={"command": cmd, "session_id": db_session_id})
        assert res.status_code == 200, f"Schema setup failed on '{cmd[:20]}...': {res.text}"
        
    time.sleep(1)  # Allow brief moment for Cassandra schema propagation
    return db_session_id


@pytest.fixture
def test_uuid():
    return "123e4567-e89b-12d3-a456-426614174000"


@pytest.fixture
def seeded_data(app_url, schema_setup, test_uuid):
    """Seeds the database with initial row data."""
    insert_cql = f"INSERT INTO mykeyspace.users (id, name) VALUES ({test_uuid}, {{firstname: 'John', lastname: 'Doe'}});"
    res = requests.post(f"{app_url}/api/execute", json={"command": insert_cql, "session_id": schema_setup})
    assert res.status_code == 200, f"Seed data insert failed: {res.text}"
    return schema_setup


# --- Split Lifecycle Tests ---

def test_cassandra_basic_query(app_url, db_session_id):
    """Tests executing a simple system query post-connection."""
    res = requests.post(f"{app_url}/api/execute", json={
        "command": "SELECT * FROM system.local;",
        "session_id": db_session_id
    })
    assert res.status_code == 200, f"Query failed: {res.text}"
    assert len(res.json()["rows"]) > 0


def test_cassandra_schema_traversal(app_url, schema_setup):
    """Tests that the API accurately reflects the created Cassandra schema."""
    # Check keyspace groups
    res = requests.get(f"{app_url}/api/schema/children", params={"type": "keyspace", "keyspace": "mykeyspace", "session_id": schema_setup})
    assert res.status_code == 200, f"Schema traversal failed: {res.text}"
    
    groups = {child.get("group") for child in res.json().get("children", []) if child.get("type") == "group"}
    assert "table" in groups
    assert "type" in groups
    
    # Check table lists
    res = requests.get(f"{app_url}/api/schema/children", params={"type": "group", "keyspace": "mykeyspace", "group": "table", "session_id": schema_setup})
    assert res.status_code == 200, f"Table schema retrieval failed: {res.text}"
    
    tables = {child.get("entity") for child in res.json().get("children", [])}
    assert "users" in tables


def test_cassandra_crud_operations(app_url, schema_setup, test_uuid):
    """Tests Insert, Select, Update, and Delete data lifecycle."""
    session_id = schema_setup
    
    # Insert
    insert_cql = f"INSERT INTO mykeyspace.users (id, name) VALUES ({test_uuid}, {{firstname: 'John', lastname: 'Doe'}});"
    requests.post(f"{app_url}/api/execute", json={"command": insert_cql, "session_id": session_id})
    
    # Verify Insert
    res = requests.post(f"{app_url}/api/execute", json={"command": f"SELECT * FROM mykeyspace.users WHERE id={test_uuid};", "session_id": session_id})
    assert len(res.json()["rows"]) == 1

    # Update
    update_cql = f"UPDATE mykeyspace.users SET name = {{firstname: 'Jane', lastname: 'Doe'}} WHERE id={test_uuid};"
    res = requests.post(f"{app_url}/api/execute", json={"command": update_cql, "session_id": session_id})
    assert res.status_code == 200, f"Update failed: {res.text}"

    # Delete
    delete_cql = f"DELETE FROM mykeyspace.users WHERE id={test_uuid};"
    res = requests.post(f"{app_url}/api/execute", json={"command": delete_cql, "session_id": session_id})
    assert res.status_code == 200, f"Delete failed: {res.text}"
    
    # Verify Deletion
    res = requests.post(f"{app_url}/api/execute", json={"command": f"SELECT * FROM mykeyspace.users WHERE id={test_uuid};", "session_id": session_id})
    assert len(res.json()["rows"]) == 0


def test_cassandra_export(app_url, seeded_data, test_uuid):
    """Tests the table export API functionality."""
    res = requests.post(f"{app_url}/api/keyspaces/mykeyspace/tables/users/export", data={
        "format": "json",
        "session_id": seeded_data
    })
    
    assert res.status_code == 200, f"Export failed: {res.text}"
    assert "application/json" in res.headers.get("Content-Type", "")
    assert test_uuid in res.text