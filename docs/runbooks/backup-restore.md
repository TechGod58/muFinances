# muFinances Backup And Restore Runbook

## Create Backup

Use the Operations workspace or call `POST /api/operations/backups`.

Backups are stored under `C:\muFinances\data\backups`.

## Restore Test

Run a restore test before restoring production data. The test opens the selected SQLite backup and runs `PRAGMA integrity_check`.

## Restore

Use the Foundation restore endpoint only after confirming a valid backup. A pre-restore backup is created automatically.
