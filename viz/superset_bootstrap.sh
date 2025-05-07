#!/bin/bash
set -e

echo "🔧 Initializing Superset..."

# Configurar variables de usuario
ADMIN_USERNAME="${SUPERSET_ADMIN_USERNAME:-admin}"

superset db upgrade
superset fab create-admin \
    --username "$ADMIN_USERNAME" \
    --firstname "${SUPERSET_ADMIN_FIRST_NAME:-Admin}" \
    --lastname "${SUPERSET_ADMIN_LAST_NAME:-User}" \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}"

superset init

echo "📦 Importing Superset assets..."

# Import dashboards
for zip_file in /app/superset_exports/*.zip; do
  echo "📈 Importing dashboard: $zip_file"
  superset import-dashboards -p "$zip_file" -u "$ADMIN_USERNAME" || echo "⚠️ Error importing $zip_file (it might already exist)"
done

echo "🚀 Starting Superset..."
exec gunicorn \
    -w 1 \
    -b 0.0.0.0:8088 \
    --timeout 120 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
