
# init environment variables
export PATH        := $(shell go env GOPATH)/bin:$(PATH)
export GOPATH      := $(shell go env GOPATH)
export GO111MODULE := on

# make make all
.PHONY: all
all: build


.PHONY: build
# make build, Build the binary file
build:
	go build -o consistent_hash main.go


.PHONY: test
# make test
test:
	go test -w -all ./...
