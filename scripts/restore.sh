#!/bin/bash
# Usage: ./restore.sh <backup_file> [container_name] [db_user] [db_name]

if [ -z "$1" ]; then
  echo "Usage: ./restore.sh <backup_file> [container_name] [db_user] [db_name]"
  exit 1
fi

BACKUP_FILE=$1
CONTAINER=${2:-tisane-relay-postgres-1}
USER=${3:-tisane_admin}
DB=${4:-tisane_relay}

if [ ! -f "$BACKUP_FILE" ]; then
  echo "❌ File not found: $BACKUP_FILE"
  exit 1
fi

echo "WARNING: This will overwrite data in '$DB' on container '$CONTAINER'."
read -p "Are you sure? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

echo "Restoring from $BACKUP_FILE..."
cat "$BACKUP_FILE" | docker exec -i $CONTAINER psql -U $USER $DB

if [ $? -eq 0 ]; then
  echo "✅ Restore complete."
else
  echo "❌ Restore failed."
  exit 1
fi
