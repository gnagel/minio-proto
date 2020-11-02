.PHONY: setup

setup:
	brew update
	brew install pre-commit || brew upgrade pre-commit
	pre-commit install
	go get -u golang.org/x/lint/golint

vendor:
	go mod vendor

test:
	go test -mod=vendor -v ./...
