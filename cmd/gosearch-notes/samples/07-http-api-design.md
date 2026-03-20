---
title: REST API Design Guidelines
tags: api, rest, http, design
category: best-practices
priority: 5
---
A well-designed REST API uses HTTP methods consistently:

- GET for reading resources (idempotent, safe)
- POST for creating resources
- PUT for full replacement of a resource
- PATCH for partial updates
- DELETE for removing resources

Use plural nouns for resource paths: /users, /notes, /tags. Nest related resources logically: /users/123/notes.

Return appropriate HTTP status codes: 200 for success, 201 for creation, 400 for bad requests, 404 for not found, 500 for server errors.

Pagination is essential for list endpoints. Use query parameters like ?page=2&limit=20 or cursor-based pagination for large datasets.

Version your API from the start (/v1/users) to allow breaking changes in future versions without disrupting existing clients.
