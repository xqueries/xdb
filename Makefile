.PHONY: watch
watch: ## Start a file watcher to run tests on change. (requires: watchexec)
	watchexec -c "go test -failfast ./..."

.PHONY: all
all: lint test build ## test -> lint -> build

.PHONY: verify
verify: ## Verify dependencies
	go mod verify

.PHONY: deps
## Verify and then Setup or Update linters
deps:
	go mod download
	cd && \
	go get -u gotest.tools/gotestsum && \
	go get -u github.com/kisielk/errcheck && \
	go get -u golang.org/x/lint/golint && \
	go get -u github.com/securego/gosec/cmd/gosec && \
	go get -u honnef.co/go/tools/cmd/staticcheck

.PHONY: test
test: ## Runs the unit test suite
	gotestsum --debug --format pkgname-and-test-fails -- -race ./...

.PHONY: lint
lint: ## Runs the linters (including internal ones)
	# internal analysis tools
	go run ./internal/tool/analysis ./...;
	# external analysis tools
	golint -set_exit_status ./...;
	errcheck ./...;
	gosec -quiet ./...;
	staticcheck ./...;

.PHONY: build
build: ## Build an xdb binary that is ready for prod
	go build -o xdb -ldflags="-s -w -X 'main.Version=$(shell date +%Y%m%d)'" ./cmd/xdb

.PHONY: fuzz
fuzz: ## Starts fuzzing the database
	go-fuzz-build -o xdb-fuzz.zip ./internal/test
	go-fuzz -bin xdb-fuzz.zip -workdir internal/test/testdata/fuzz

## Help display.
## Pulls comments from beside commands and prints a nicely formatted
## display with the commands and their usage information.

.DEFAULT_GOAL := help

help: ## Prints this help
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
