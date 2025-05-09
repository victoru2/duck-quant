### [Docker Image for Apache Airflow](https://airflow.apache.org/docs/docker-stack/index.html)

### [Fetching docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml)


```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.0/docker-compose.yaml'
```

### [Setting the right Airflow user](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user)

```bash
mkdir -p ./dags ./logs ./config
```

### Generate `requirements.txt` file
```bash
uv pip compile ../pyproject.toml --extra=extract --extra=transform --extra=orchestration --output-file requirements.txt
```

### 🔐 Creating a Custom Admin User

1. Access the APIserver Container
```bash
docker exec -it orchestration-airflow-apiserver-1 bash
```

2. Create the Admin User
```bash
airflow users create \
  --username airflow_user \
  --firstname Airflow \
  --lastname Apache \
  --role Admin \
  --email admin@example.com

You'll be prompted to enter a password for the new user.
```

3. Type `exit` to leave the container shell.

4. Access the Airflow UI: http://localhost:8080/
