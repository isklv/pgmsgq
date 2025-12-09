SHELL := /bin/bash

.PHONY: setup test lint migrate cli

setup:
	docker-compose up -d
	sleep 3
	go mod download

migrate:
	docker-compose exec postgres psql -U postgres -c "CREATE DATABASE pgmsgq_test;" || true
	migrate -database "postgresql://postgres:pass@localhost:5432/pgmsgq_test?sslmode=disable" -path migrations up

test:
	INTEGRATION=1 go test -v ./tests/integration/... -race -cover

lint:
	golangci-lint run ./...

cli:
	go build -o bin/pgmsgq ./cmd/cli

example-basic:
	go run ./cmd/basic

example-push:
	go run ./cmd/push

.PHONY: clean
clean:
	rm -rf bin/
	docker-compose down -v