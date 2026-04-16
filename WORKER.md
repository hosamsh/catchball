1. Complete requested work and verify it e2e before handing it back
2. Keep changes minimal
3. Keep changes aligned with the surrounding codebase
4. Stay in scope. Do not speculate features, or unrelated cleanup.
5. Avoid unnecessary abstractions and brittle special cases.
6. Batch all edits to the same file into a single patch. Do not interleave read-patch-read-patch cycles on the same file.
7. When verifying a patch you just applied, read only the affected lines, not the entire file again.
8. Run the smallest test scope that covers your changes during iteration. Run the full suite only once at the end before handing off.