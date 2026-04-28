# Managed Schema Files

`app.db.init_db()` still executes the current schema for compatibility, but new
database work should be captured here first as managed schema fragments before
being wired into runtime migrations.

The intent is to move away from one giant inline DDL block and toward reviewable
schema slices grouped by service boundary.
