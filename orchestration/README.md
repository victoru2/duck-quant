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
uv pip compile ../pyproject.toml --extra=extract --extra=transform --extra=orchestrate --output-file requirements.txt
```
