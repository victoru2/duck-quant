openssl rand -base64 42


docker-compose up -d

# Ejecutar migraciones de la base de datos
docker-compose exec superset superset db upgrade

# Crear usuario administrador
docker-compose exec superset superset fab create-admin \
  --username victoru \
  --firstname Victor \
  --lastname U \
  --email victicol@example.com \
  --password victoru2

# Inicializar Superset
docker-compose exec superset superset init


http://localhost:8088



duckdb:////app/data/database.duckdb

