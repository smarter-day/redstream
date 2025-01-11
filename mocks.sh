#!/bin/bash

go install go.uber.org/mock/mockgen@latest

export PATH=$PATH:$(go env GOPATH)/bin

# External
mockgen -package=mocks -destination=./mocks/redis_mock.go github.com/redis/go-redis/v9 Cmdable,Pipeliner,UniversalClient

# Local
mockgen -source=./types.go \
        -destination=./mocks/redstream_mock.go \
        -package=mocks
