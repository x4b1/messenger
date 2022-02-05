BINARY_NAME=messenger
VERSION?=0.0.0
DOCKER_REGISTRY?= #if set it should finished by /

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

.PHONY: all test build

default: lint test

all: help

## Deps:
tools: ## Install development tools
	cd tools && go install $(shell cd tools && go list -f '{{ join .Imports " " }}' -tags=tools)

tidy: ## Fix dependencies
	@go mod tidy -compat=1.17
	cd tools && go mod tidy -compat=1.17

## Generate:
generate: ## Generate
	@go generate ./...

## Test:
test: ## Run the tests of the project
	@go test -race -v -count 1 ./...

coverage: cov-report ## Displays coverage per func on cli
	@go tool cover -func=report.cov

coverage-html: cov-report ## Displays the coverage results in the browser
	@go tool cover -html=report.cov

.PHONY: cov-report
cov-report:
	@go test -count 1 ./... -coverpkg=./... -coverprofile=report.cov


## Lint:
lint: ## Lint go code
	@golangci-lint run
lint-fix: ## Lint go code and try to fix issues
	@golangci-lint run --fix


## Build:
build: ## Build project and put the output binary in bin/
	mkdir -p bin
	@go build -o bin/$(BINARY_NAME) .

docker-build: ## Use the dockerfile to build the container
	docker build --rm --tag $(BINARY_NAME) .

## Help:
help: ## Show this help.
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) {printf "    ${YELLOW}%-20s${GREEN}%s${RESET}\n", $$1, $$2} \
		else if (/^## .*$$/) {printf "  ${CYAN}%s${RESET}\n", substr($$1,4)} \
		}' $(MAKEFILE_LIST)
