.DEFAULT_GOAL := help

COMPOSE_FILE := docker-compose.yml

up:
	docker compose -f $(COMPOSE_FILE) up --detach

down:
	docker compose -f $(COMPOSE_FILE) down

logs:
	docker compose -f $(COMPOSE_FILE) logs

build:
	docker compose -f $(COMPOSE_FILE) build

rebuild:
	down build up

status:
	docker compose -f $(COMPOSE_FILE) ps

help:
	@echo " Usage:"
	@echo "  make up      # Run the container"
	@echo "  make down 	  # Stop the container"
	@echo "  make logs    # Show container logs"
	@echo "  make build   # Build the image"
	@echo "  make rebuild # Rebuild the service (down, build, up)"
	@echo "  make status  # Show status of the service"

