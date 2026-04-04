# Write Tests
Add tests for the user CRUD endpoints using the built-in `node:test` runner.
Create `src/server.test.js` that tests:
- `GET /health` returns 200
- `POST /users` with valid data returns 201
- `POST /users` with missing fields returns 400
- `GET /users/:id` with valid id returns the user
- `GET /users/:id` with invalid id returns 404
- `DELETE /users/:id` removes the user
Add a `test` script to package.json.
Make the server importable: export the app and do not auto-listen on import.
