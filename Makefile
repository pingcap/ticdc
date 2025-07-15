# Phony targets are targets that are not associated with files.
# Add new phony targets here to make them available in the `make` command.
.PHONY: clean fmt check tidy \
	generate-protobuf generate_mock \
	cdc kafka_consumer storage_consumer pulsar_consumer filter_helper \
	prepare_test_binaries \
	unit_test_in_verify_ci integration_test_build integration_test_mysql integration_test_kafka integration_test_storage integration_test_pulsar \


FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1  }  }'

PROJECT=ticdc
.DEFAULT_GOAL := cdc
CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(CURDIR)/bin:$(CURDIR)/tools/bin:$(path_to_add):$(PATH)
TIFLOW_CDC_PKG := github.com/pingcap/tiflow
CDC_PKG := github.com/pingcap/ticdc
# DBUS_SESSION_BUS_ADDRESS pulsar client use dbus to detect the connection status,
# but it will not exit when the connection is closed.
# I try to use leak_helper to detect goroutine leak,but it does not work.
# https://github.com/benthosdev/benthos/issues/1184 suggest to use environment variable to disable dbus.
export DBUS_SESSION_BUS_ADDRESS := /dev/null

SHELL := /usr/bin/env bash

TEST_DIR := /tmp/tidb_cdc_test
DM_TEST_DIR := /tmp/dm_test
ENGINE_TEST_DIR := /tmp/engine_test

GO       := GO111MODULE=on go
ifeq (${CDC_ENABLE_VENDOR}, 1)
GOVENDORFLAG := -mod=vendor
endif

BUILDTS := $(shell date -u '+%Y-%m-%d %H:%M:%S')
GITHASH := $(shell git rev-parse HEAD)
GITBRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GOVERSION := $(shell go version)

# Since TiDB add a new dependency on github.com/cloudfoundry/gosigar,
# We need to add CGO_ENABLED=1 to make it work when build TiCDC in Darwin OS.
# These logic is to check if the OS is Darwin, if so, add CGO_ENABLED=1.
# ref: https://github.com/cloudfoundry/gosigar/issues/58#issuecomment-1150925711
# ref: https://github.com/pingcap/tidb/pull/39526#issuecomment-1407952955
OS := "$(shell go env GOOS)"
SED_IN_PLACE ?= $(shell which sed)
IS_ALPINE := $(shell if [ -f /etc/os-release ]; then grep -qi Alpine /etc/os-release && echo 1; else echo 0; fi)
ifeq (${OS}, "linux")
	CGO := 0
	SED_IN_PLACE += -i
else ifeq (${OS}, "darwin")
	CGO := 1
	SED_IN_PLACE += -i ''
endif

GOTEST := CGO_ENABLED=1 $(GO) test -p 3 --race --tags=intest

BUILD_FLAG =
GOEXPERIMENT=
ifeq ("${ENABLE_FIPS}", "1")
	BUILD_FLAG = -tags boringcrypto
	GOEXPERIMENT = GOEXPERIMENT=boringcrypto
	CGO = 1
endif

RELEASE_VERSION =
ifeq ($(RELEASE_VERSION),)
	RELEASE_VERSION := $(shell git describe --tags --dirty)
endif
ifeq ($(RELEASE_VERSION),)
	RELEASE_VERSION := v9.0.0-alpha
endif

# Version LDFLAGS.
LDFLAGS += -X "$(CDC_PKG)/pkg/version.ReleaseVersion=$(RELEASE_VERSION)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GitHash=$(GITHASH)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GitBranch=$(GITBRANCH)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GoVersion=$(GOVERSION)"
LDFLAGS += -X "github.com/pingcap/tidb/pkg/parser/mysql.TiDBReleaseVersion=$(RELEASE_VERSION)"

# For Tiflow CDC
LDFLAGS += -X "$(TIFLOW_CDC_PKG)/pkg/version.ReleaseVersion=$(RELEASE_VERSION)"
LDFLAGS += -X "$(TIFLOW_CDC_PKG)/pkg/version.GitHash=$(GITHASH)"
LDFLAGS += -X "$(TIFLOW_CDC_PKG)/pkg/version.GitBranch=$(GITBRANCH)"
LDFLAGS += -X "$(TIFLOW_CDC_PKG)/pkg/version.BuildTS=$(BUILDTS)"

CONSUMER_BUILD_FLAG=
ifeq ("${IS_ALPINE}", "1")
	CONSUMER_BUILD_FLAG = -tags musl
endif
GOBUILD  := $(GOEXPERIMENT) CGO_ENABLED=$(CGO) $(GO) build $(BUILD_FLAG) -trimpath $(GOVENDORFLAG)
CONSUMER_GOBUILD  := $(GOEXPERIMENT) CGO_ENABLED=1 $(GO) build $(CONSUMER_BUILD_FLAG) -trimpath $(GOVENDORFLAG)

PACKAGE_LIST := go list ./... | grep -vE 'vendor|proto|ticdc/tests|integration|testing_utils|pb|pbmock|ticdc/bin'
PACKAGES := $$($(PACKAGE_LIST))

FILES := $$(find . -name '*.go' -type f | grep -vE 'vendor|_gen|proto|pb\.go|pb\.gw\.go|_mock.go')

# MAKE_FILES is a list of make files to lint.
# We purposefully avoid MAKEFILES as a variable name as it influences
# the files included in recursive invocations of make
MAKE_FILES = $(shell find . \( -name 'Makefile' -o -name '*.mk' \) -print)

FAILPOINT_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/pingcap/$(PROJECT)/"}|grep -v "github.com/pingcap/$(PROJECT)"; done)
FAILPOINT := tools/bin/failpoint-ctl
FAILPOINT_ENABLE  := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) enable >/dev/null)
FAILPOINT_DISABLE := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) disable >/dev/null)

# gotestsum -p parameter for unit tests
P=3

# The following packages are used in unit tests.
# Add new packages here if you want to include them in unit tests.
UT_PACKAGES_DISPATCHER := ./pkg/sink/cloudstorage/... ./pkg/sink/mysql/... ./pkg/sink/util/... ./downstreamadapter/sink/... ./downstreamadapter/dispatcher/... ./downstreamadapter/eventcollector/... ./pkg/sink/codec/avro/... ./pkg/sink/codec/open/... ./pkg/sink/codec/csv/... ./pkg/sink/codec/canal/... ./pkg/sink/codec/debezium/... ./pkg/sink/codec/simple/...
UT_PACKAGES_MAINTAINER := ./maintainer/...
UT_PACKAGES_COORDINATOR := ./coordinator/...
UT_PACKAGES_LOGSERVICE := ./logservice/...
UT_PACKAGES_OTHERS := ./pkg/eventservice/... ./pkg/version/... ./utils/dynstream/... ./pkg/common/event/...

include tools/Makefile

generate-protobuf:
	@echo "generate-protobuf"
	./scripts/generate-protobuf.sh

generate_mock: ## Generate mock code.
generate_mock: tools/bin/mockgen
	scripts/generate-mock.sh

cdc:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc

kafka_consumer:
	$(CONSUMER_GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_kafka_consumer ./cmd/kafka-consumer

storage_consumer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_storage_consumer ./cmd/storage-consumer/main.go

pulsar_consumer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_pulsar_consumer ./cmd/pulsar-consumer/main.go

oauth2_server:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/oauth2-server ./cmd/oauth2-server/main.go

filter_helper:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_filter_helper ./cmd/filter-helper/main.go

config-converter:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_config_converter ./cmd/config-converter/main.go

fmt: tools/bin/gofumports tools/bin/shfmt tools/bin/gci
	@echo "run gci (format imports)"
	tools/bin/gci write $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
	@echo "run gofumports"
	tools/bin/gofumports -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
	@echo "run shfmt"
	tools/bin/shfmt -d -w .
	@echo "check log style"
	scripts/check-log-style.sh
	@make check-diff-line-width

prepare_test_binaries:
	./tests/scripts/download-integration-test-binaries.sh "$(branch)" "$(community)" "$(ver)" "$(os)" "$(arch)"

check_third_party_binary:
	@which bin/tidb-server
	@which bin/tikv-server
	@which bin/pd-server
	@which bin/tiflash
	@which bin/pd-ctl
	@which bin/sync_diff_inspector
	@which bin/go-ycsb
	@which bin/etcdctl
	@which bin/jq
	@which bin/minio
	@which bin/bin/schema-registry-start

integration_test_build: check_failpoint_ctl storage_consumer kafka_consumer pulsar_consumer oauth2_server
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/ticdc/... \
		-o bin/cdc.test github.com/pingcap/ticdc/cmd/cdc \
	|| { $(FAILPOINT_DISABLE); echo "Failed to build cdc.test"; exit 1; }
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc/main.go \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

failpoint-enable: check_failpoint_ctl
	$(FAILPOINT_ENABLE)

failpoint-disable: check_failpoint_ctl
	$(FAILPOINT_DISABLE)

check_failpoint_ctl: tools/bin/failpoint-ctl

integration_test: integration_test_mysql

integration_test_mysql: check_third_party_binary
	tests/integration_tests/run.sh mysql "$(CASE)" "$(START_AT)"

integration_test_kafka: check_third_party_binary
	tests/integration_tests/run.sh kafka "$(CASE)" "$(START_AT)"

integration_test_storage: check_third_party_binary
	tests/integration_tests/run.sh storage "$(CASE)" "$(START_AT)"

integration_test_pulsar: check_third_party_binary
	tests/integration_tests/run.sh pulsar "$(CASE)" "$(START_AT)"

unit_test: check_failpoint_ctl generate-protobuf
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -cover -covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

unit_test_in_verify_ci: check_failpoint_ctl tools/bin/gotestsum tools/bin/gocov tools/bin/gocov-xml
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@echo "Running unit tests..."
	@export log_level=error;\
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile cdc-junit-report.xml -- -v -timeout 300s -p $(P) --race --tags=intest \
	-parallel=16 \
	-covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" \
	$(UT_PACKAGES_DISPATCHER) \
	$(UT_PACKAGES_MAINTAINER) \
	$(UT_PACKAGES_COORDINATOR) \
	$(UT_PACKAGES_LOGSERVICE) \
	$(UT_PACKAGES_OTHERS) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	tools/bin/gocov convert "$(TEST_DIR)/cov.unit.out" | tools/bin/gocov-xml > cdc-coverage.xml
	$(FAILPOINT_DISABLE)

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

check-copyright:
	@echo "check-copyright"
	@./scripts/check-copyright.sh

check-static: tools/bin/golangci-lint
	tools/bin/golangci-lint run --timeout 10m0s --exclude-dirs "^tests/"

check-ticdc-dashboard:
	@echo "check-ticdc-dashboard"
	@./scripts/check-ticdc-dashboard.sh

check-diff-line-width:
ifneq ($(shell echo $(RELEASE_VERSION) | grep master),)
	@echo "check-file-width"
	@./scripts/check-diff-line-width.sh
endif

check-makefiles: format-makefiles
	@git diff --exit-code -- $(MAKE_FILES) || (echo "Please format Makefiles by running 'make format-makefiles'" && false)

format-makefiles: $(MAKE_FILES)
	$(SED_IN_PLACE) -e 's/^\(\t*\)  /\1\t/g' -e 's/^\(\t*\) /\1/' -- $?

check: check-copyright fmt tidy check-diff-line-width check-ticdc-dashboard check-makefiles
	@git --no-pager diff --exit-code || (echo "Please add changed files!" && false)

clean:
	go clean -i ./...
	rm -rf *.out
	rm -rf bin
	rm -rf tools/bin
	rm -rf tools/include

workload: tools/bin/workload
