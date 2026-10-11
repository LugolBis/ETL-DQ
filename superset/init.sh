#!/bin/sh
set -e
echo "Migrate meta data database"
superset db upgrade

echo "Create the admin user (ignored if it's already exist)"
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" --firstname Admin --lastname User \
  --email "${SUPERSET_ADMIN_EMAIL}" --password "${SUPERSET_ADMIN_PASSWORD}" \
  || echo "Admin already exist."

echo "Init roles and permissions."
superset init

echo "Add the DuckDB connexion Déclaration de la connexion DuckDB"
superset set-database-uri -d "DuckDB" -u "duckdb:///:memory:"

echo "Init completed."