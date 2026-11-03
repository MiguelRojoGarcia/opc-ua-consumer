COMPOSE := docker compose
SERVICE := kepserver
ENDPOINT ?= opc.tcp://localhost:49320
SECONDS ?= 5
ARGS ?=

.DEFAULT_GOAL := help
.PHONY: help build up down restart logs ps check shell clean run client consumer-logs consumer-status consumer-restart kafka-ui kafka-tail

help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Construye la imagen del simulador
	$(COMPOSE) build

up: ## Arranca el simulador en segundo plano (reconstruyendo si hace falta)
	$(COMPOSE) up -d --build

down: ## Para y elimina el contenedor
	$(COMPOSE) down

restart: ## Reinicia el simulador (p. ej. tras editar kepserver-sim/config/tags.yaml)
	$(COMPOSE) restart $(SERVICE)

logs: ## Muestra los logs en vivo
	$(COMPOSE) logs -f $(SERVICE)

ps: ## Estado del contenedor y healthcheck
	$(COMPOSE) ps

check: ## Ejecuta el cliente de comprobación (ENDPOINT=..., SECONDS=...)
	$(COMPOSE) exec $(SERVICE) python check_client.py $(ENDPOINT) $(SECONDS)

shell: ## Abre una shell dentro del contenedor
	$(COMPOSE) exec $(SERVICE) sh

clean: ## Para el simulador y borra la imagen construida
	$(COMPOSE) down --rmi local --volumes

run: ## Ejecuta el consumidor en local (ARGS="--center-id madrid-01 --log-level debug")
	go run . run $(ARGS)

client: ## Compila el cliente Go en bin/opc-ua-consumer
	go build -o bin/opc-ua-consumer .

consumer-logs: ## Logs en vivo del consumidor dockerizado
	$(COMPOSE) logs -f consumer

consumer-status: ## Estado del proceso dentro de supervisord
	$(COMPOSE) exec consumer supervisorctl status

consumer-restart: ## Reinicia el consumidor (p. ej. tras editar config/tags.yaml)
	$(COMPOSE) restart consumer

kafka-ui: ## Abre la interfaz web de Kafka (http://localhost:8085)
	@echo "Kafka UI: http://localhost:8085"
	@xdg-open http://localhost:8085 >/dev/null 2>&1 || true

kafka-tail: ## Muestra en vivo los mensajes del tópico (TOPIC=measurements)
	docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
		--topic $(or $(TOPIC),measurements) --property print.key=true --property print.partition=true