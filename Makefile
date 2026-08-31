.PHONY: all download build up down logs verify

all: download build up verify

download:
	./download_jars.sh

build:
	docker compose build --no-cache

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

verify:
	@echo "Verificando containers e arquivos essenciais..."
	@docker exec -it airflow-webserver which spark-submit || true
	@docker exec -it docker-spark-master-1 ls -lah /opt/spark/jars || true
