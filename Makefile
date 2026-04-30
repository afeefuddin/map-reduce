.PHONY: build proto worker master tools clean

.DEFAULT_GOAL := build

PROTOC ?= protoc
PROTO_DIR := proto
GEN_DIR := gen
BIN_DIR := .bin

tools:
	GOBIN=$(PWD)/$(BIN_DIR) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
	GOBIN=$(PWD)/$(BIN_DIR) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

worker: tools
	mkdir -p $(GEN_DIR)/worker
	PATH="$(PWD)/$(BIN_DIR):$$PATH" $(PROTOC) -I=$(PROTO_DIR) \
		--go_out=$(GEN_DIR)/worker --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR)/worker --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/worker.proto

master: tools
	mkdir -p $(GEN_DIR)/master
	PATH="$(PWD)/$(BIN_DIR):$$PATH" $(PROTOC) -I=$(PROTO_DIR) \
		--go_out=$(GEN_DIR)/master --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR)/master --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/master.proto

proto: worker master

build: proto

clean:
	rm -rf $(GEN_DIR)
