# Superset Setup with Docker and DuckDB

This guide explains how to set up Apache Superset using Docker and connect it to a local DuckDB database.

<!-- ## Generate a Secret Key

Run the following command to generate a secret key (used by Superset for session management):

```bash
openssl rand -base64 42
``` -->

## Start Superset with Docker Compose

Start the Superset services in detached mode:

```bash
docker-compose up -d
```

## Access the Superset UI

You can now access Superset in your browser at:

[http://localhost:8088](http://localhost:8088)

## Connect to DuckDB

To connect to a DuckDB database located inside your container:

```
duckdb:////app/data/database.duckdb
```
![Ref](image.png)

Make sure the `.duckdb` file is available in the correct path inside the container.
