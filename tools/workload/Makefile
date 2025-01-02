BINARY_NAME := workload

OUTPUT_DIR := ./bin

all: build

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(OUTPUT_DIR)
	@go build -o $(OUTPUT_DIR)/$(BINARY_NAME) .

clean:
	@echo "Cleaning up..."
	@rm -rf $(OUTPUT_DIR)

.PHONY: all build clean