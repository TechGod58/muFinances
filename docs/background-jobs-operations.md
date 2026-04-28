# Background Jobs And Operations Verification

B80 adds a durable operations layer for background work.

## Capabilities

- Queue jobs with type, payload, priority, and run time.
- Lease jobs to workers.
- Retry failed jobs with exponential backoff.
- Move exhausted jobs to dead letter.
- Cancel queued or running jobs.
- Record worker heartbeats.
- Record job logs.
- Provide health probes.
- Provide admin diagnostics.

## Files

- `services/background_jobs.py`
- `tests/test_background_jobs.py`
- `schema/postgresql/0080_background_jobs_operations.up.sql`
- `schema/postgresql/0080_background_jobs_operations.down.sql`

## Operational Rule

Long-running exports, imports, syncs, forecast recalculations, and backup drills should run as background jobs instead of blocking HTTP requests.

