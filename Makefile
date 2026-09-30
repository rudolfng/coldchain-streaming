.PHONY: install dataset up replay show logs down down-volumes test lint smoke demo clean-data

install:
	uv sync --frozen

dataset:
	uv run python -m coldchain_streaming.dataset

up:
	docker compose up -d --build

# --build: el servicio producer está en el perfil replay, así que `make up` no lo construye y
# `run` reusaría una imagen vieja con código desactualizado.
replay:
	docker compose --profile replay run --build --rm producer

# report.py corre en el host: el nombre `kafka` solo resuelve dentro de la red de Compose,
# así que se usa el listener publicado para el host (PLAINTEXT_HOST en docker-compose.yml).
show: export KAFKA_BOOTSTRAP_SERVERS ?= localhost:29092
show:
	uv run python scripts/report.py

logs:
	docker compose logs --follow --tail=100

down:
	docker compose down

down-volumes:
	docker compose down --volumes

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .
	docker compose config --quiet

# Sin `status=$$?` ni líneas continuadas: esta forma corre igual con sh (Git Bash/WSL) y con
# cmd (make de Windows sin sh). Baja todo el proyecto al terminar, pase o falle.
smoke:
	docker compose --profile smoke up --build --abort-on-container-exit --exit-code-from smoke smoke || (docker compose --profile smoke down && exit 1)
	docker compose --profile smoke down

# Sin comillas, paréntesis, `#` ni `;`: el make de Windows sin sh (cmd) imprimiría las
# comillas, y sh tomaría el resto como comentario o como otro comando.
demo:
	@echo 1. make up - levanta Kafka, Flink, el job server, el pipeline y el consumer
	@echo 2. esperar a que el job aparezca en http://localhost:8081
	@echo 3. make replay - reproduce el tramo con las inyecciones deterministas
	@echo 4. make show - ventanas, panes y DLQ, antes y despues de la rafaga de b07
	@echo 5. make down - detiene todo

clean-data:
	uv run python -c "from pathlib import Path; keep = {'.gitkeep', 'manifest.json'}; [p.unlink() for d in ('data/processed', 'data/out') for p in Path(d).iterdir() if p.is_file() and p.name not in keep]"
