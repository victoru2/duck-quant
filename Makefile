# Makefile
.PHONY: help up-all down-all stop-all

help:
	@echo "\nDuckDB Quant Pipeline - Available commands:\n"
	@echo "make help       - Show this help"
	@echo "make up-all     - Start all services (Airflow + Superset)"
	@echo "make down-all   - Stop services preserving data"
	@echo "make stop-all   - Destroy EVERYTHING (containers, volumes, images)\n"

up-all:
	@echo "\n🚀 Launching infrastructure..."
	@echo "====================================="
	@cd orchestration && docker compose up -d --build
	@echo "\n✅ Airflow ready at http://localhost:8080"
	@cd viz && docker compose up -d --build
	@echo "✅ Superset ready at http://localhost:8088\n"

down-all:
	@echo "\n🛑 Stopping services..."
	@echo "====================================="
	@cd orchestration && docker compose down
	@cd viz && docker compose down
	@echo "\n✅ All services stopped (data preserved)\n"

stop-all:
	@echo "\n🧹🏠 Deep cleaning..."
	@echo "====================================="
	@cd orchestration && docker compose down --volumes --rmi all --remove-orphans
	@cd viz && docker compose down --volumes --rmi all --remove-orphans
	@echo "\n🏠✨ All resources destroyed.\n"
