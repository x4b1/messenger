SHELL=/bin/bash -e -o pipefail
PWD = $(shell pwd)

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

.PHONY: all test tools
set_opts: .PHONY

default: tidy tools

## Tools:
generate: ## Generate
	$(call print-target)
	@go generate ./...

tools: ## go development tools
	$(call print-target)
	@cd tools && go install $(shell cd tools && go list -f '{{ join .Imports " " }}' -tags=tools)

download: ## Downloads the dependencies
	$(call print-target)
	@go mod download

tidy: ## Install development tools
	$(call print-target)
	@go mod tidy -compat=1.17

## Lint:
lint: download ## Lint go code
	$(call print-target)
	@golangci-lint run
lint-fix: download ## Lint go code and try to fix issues
	$(call print-target)
	@golangci-lint run --fix

## Test:
test: ## Runs all tests
	$(call print-target)
	@go test -count 1 ./...

coverage: out/report ## Displays coverage per func on cli
	$(call print-target)
	@go tool cover -func=out/cover.out

html-coverage: out/report ## Displays the coverage results in the browser
	$(call print-target)
	@go tool cover -html=out/cover.out

.PHONY: out/report
out/report:
	$(call print-target)
	@mkdir -p out
	@go test -count 1 ./... -coverprofile=out/cover.out

clean: ## Cleans up everything
	$(call print-target)
	@rm -rf out

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

define print-target
    @printf "Executing target: \033[36m$@\033[0m\n"
endef
