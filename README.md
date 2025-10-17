# Cassandra Admin

A web-based admin interface for Apache Cassandra, powered by [alpine.js](https://github.com/alpinejs/alpine). Built with OpenResty and Lua, using the [lua-cassandra](https://github.com/thibaultcha/lua-cassandra) library, a Cassandra driver written in pure Lua.

![screenshot](/docs/screenshot.jpeg)

## Features

- **Schema Management** - Sidebar displaying all keyspaces, tables, and views in your Cassandra instance
- **Data Viewing** - Browse table and view data with customizable page sizes (50, 100, 200 rows)
- **Cursor-based pagination** - Cursor-based pagination using Cassandra's native paging states
- **Column Type Display** - Visual indicators showing data types for each column and icons distinguishing partition keys and clustering keys
- **Formatted Output** - `cqlsh`-like formatting for complex types
- **Safeguards** - Built-in safeguards prevent accidental modifications to system keyspaces
- **Data Export** - Export data as CQL, CSV, or JSON and configure row limits and choose whether to include DDL statements
- **Quick table operations** - Truncate or drop tables and views directly from the UI (more to come)
- **Quick keyspace Operations** - Drop keyspaces (more to come)
- **Environment Configuration** - Flexible configuration via environment variables or a configuration file
- **Dark Mode** - Dark mode support for comfortable viewing in low-light environments

## Quick Setup

### Using Docker Compose

```bash
git clone https://github.com/IBM/cassandra-admin.git
cd cassandra-admin

docker-compose up -d

# Wait for Cassandra to fully start (30-60 seconds)
docker-compose logs -f cassandra
```

Access the admin interface at `http://localhost:8002`

### Environment Variables

The application can be configured via the following environment variables:
```
CA_CONNECTION_HOST        # default: 127.0.0.1
CA_CONNECTION_PORT        # default: 9042
CA_CONNECTION_USERNAME    # default: cassandra
CA_CONNECTION_PASSWORD    # default: cassandra
CA_CONNECTION_TIMEOUT     # default: 5 (seconds)
```

### Configuration File
You can also create a `settings.cfg` file in the same directory as the application with the following format (mapped to `/etc/cassandra-admin/settings.cfg` in the Docker container):

```lua
{
  connection = {
    host = "cassandra_container",
    port = 9042,
    username = "cassandra",
    password = "cassandra"
  },
  page_sizes = {50, 100, 200},
  default_page_size = 50,
}
```

## Limitations
This list is not exhaustive, but here are some limitations of the current version:

- **No multi-node support** - Multi-host/multi-datacenter cluster support is planned for a future release. Currently connects to one contact point only.
- **No user authentication** - The app does not implement user authentication or role-based access control (also no LDAP, Kerberos, etc.). It is recommended to run behind a secure proxy if exposed publicly.
- **No user/role management** - User and role management support is planned for a future release.
- **No search/filter** - Cannot search or filter rows within tables. This is planned for a future release.
- **No data editing** - Currently read-only.
- **No data import** - Data import functionality is planned for a future release.

## Credits
This project makes use of the following open-source libraries:

- [thibaultcha/lua-cassandra](https://github.com/thibaultcha/lua-cassandra)
- [bungle/lua-resty-template](https://github.com/bungle/lua-resty-template)
- [bungle/lua-resty-reqargs](https://github.com/bungle/lua-resty-reqargs)

## Roadmap

* Datatable instead of plain table
* Test with Cassandra-compatible DBs like ScyllaDB and different Cassandra versions. Make a testbed of sorts.
* Finish off UI for keyspace and table creation.

## Disclaimer

This project is an independent, open-source tool created to help administer Apache Cassandra databases. **This software is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation or the Apache Cassandra project.**

"Apache Cassandra" and "Cassandra" are trademarks of the Apache Software Foundation. This project uses these terms solely to indicate compatibility and functionality with the Apache Cassandra database system.

The Apache Software Foundation has not reviewed, approved, or been involved in the development of this tool. For official Apache Cassandra resources, documentation, and support, please visit the [official Apache Cassandra website](https://cassandra.apache.org/).

This project is provided "as is" without warranty of any kind. Use at your own risk.

## License

`cassandra-admin` uses an MIT License, since the main library it depends on (lua-cassandra) is also MIT licensed.

```
MIT License

Copyright (c) 2025 International Business Machines

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

```
