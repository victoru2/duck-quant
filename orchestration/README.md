[Fetching docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml)


```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml'
```

[Setting the right Airflow user](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user)

```bash
mkdir -p ./dags ./logs ./config
```

Generate `requirements.txt` file

uv pip compile ../pyproject.toml --extra=extract --extra=transform --extra=orchestrate --output-file requirements.txt

