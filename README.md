# Cassandra Admin

A web-based admin interface for Apache Cassandra, powered by [alpine.js](https://github.com/alpinejs/alpine). Built with OpenResty and Lua, using the [lua-cassandra](https://github.com/thibaultcha/lua-cassandra) library, a Cassandra driver written in pure Lua.

![screenshot](/docs/screenshot.jpeg)

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
