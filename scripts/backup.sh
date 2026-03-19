#!/bin/bash
# Usage: ./backup.sh [container_name] [db_user] [db_name]

CONTAINER=${1:-tisane-relay-postgres-1}
USER=${2:-tisane_admin}
DB=${3:-tisane_relay}
DATE=$(date +%Y%m%d_%H%M%S)

echo "Backing up database '$DB' from container '$CONTAINER'..."
docker exec -t $CONTAINER pg_dump -U $USER $DB > "backup_${DB}_${DATE}.sql"

if [ $? -eq 0 ]; then
  echo "✅ Backup created: backup_${DB}_${DATE}.sql"
else
  echo "❌ Backup failed."
  rm "backup_${DB}_${DATE}.sql"
  exit 1
fi
