.PHONY: up down restart logs

run:
	docker-compose up -d

stop:
	docker-compose down

restart: down up

logs:
	docker-compose logs -f redis

test:
	coverage run -m pytest -s -vv && coverage report -m