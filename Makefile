# Makefile

export FLASK_APP=superset
export SUPERSET_CONFIG_PATH=$(PWD)/viz/superset_config.py

superset-init:
	uv pip install -e ".[viz]"
	superset db upgrade
	superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@example.com --password admin
	superset init

superset-run:
	superset run -p 8088 --with-threads --reload --debugger
