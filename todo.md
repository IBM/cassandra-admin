1. Paging state not working [OK]
2. Serialize complex types in UI 
3. Add 'TRACING' support
4. Add 'CONSISTENCY' support
5. Add download/upload support (COPY)
6. More entity support in UI
7. (optional) Expand support
8. 'HISTORY' support?
9. No-op PAGING, CLEAR, CLS, HELP command, add support for paging
10. Serial consistency support
11. show_host() and show_version()
12. No-op SOURCE
13. Intercept SELECT JSON?
14. Support for JSON insert (why?)
INSERT INTO mykeyspace.users JSON '{
  "id": "1234567890",
  "name": {"firstname": "John", "lastname": "Doe"},
  "addresses": {
    "home": {"street": "123 Main St", "city": "Springfield", "zip_code": "12345", "phones": ["555-1234", "555-5678"]},
    "work": {"street": "456 Office Blvd", "city": "Metropolis", "zip_code": "67890", "phones": ["555-8765"]}
  },
  "direct_reports": [
    {"firstname": "Jane", "lastname": "Smith"},
    {"firstname": "Bob", "lastname": "Johnson"}
  ]
}';
15. Enriched display (and editing) of complex types in UI (user-configurable?)
16. When doing CREATE|TRUNCATE|INSERT|UPDATE, update UI
17. Risky query detection
18. Add healthcheck to init.sh to wait for cassandra DB