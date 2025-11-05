#!/usr/bin/env bash
# Copyright 2025 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

# download-integration-test-binaries-next-gen.sh will
# * download all third party binaries needed for integration testing
# the tikv/tidb/pd/tiflash binaries download by EE script

set -euo pipefail

# Default values
OS=${1:-linux}
ARCH=${2:-amd64}

# Constants
FILE_SERVER_URL="http://fileserver.pingcap.net"
TMP_DIR="tmp"
THIRD_BIN_DIR="third_bin"
BIN_DIR="bin"

# ANSI color codes
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Functions
log_green() {
	echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

download_file() {
	local url=$1
	local file_name=$2
	local file_path=$3
	if [[ -f "${file_path}" ]]; then
		echo "File ${file_name} already exists, skipping download"
		return
	fi
	echo ">>> Downloading ${file_name} from ${url}"
	wget --no-verbose --retry-connrefused --waitretry=1 -t 3 -O "${file_path}" "${url}"
}

download_binaries() {
	log_green "Downloading binaries..."

	# Get sha1 based on branch name

	# Define download URLs
	# local minio_download_url="${FILE_SERVER_URL}/download/minio.tar.gz"
	local go_ycsb_download_url="${FILE_SERVER_URL}/download/builds/pingcap/go-ycsb/test-br/go-ycsb"
	local etcd_download_url="${FILE_SERVER_URL}/download/builds/pingcap/cdc/etcd-v3.4.7-linux-amd64.tar.gz"
	local sync_diff_inspector_url="${FILE_SERVER_URL}/download/builds/pingcap/cdc/sync_diff_inspector_hash-a129f096_linux-amd64.tar.gz"
	local jq_download_url="${FILE_SERVER_URL}/download/builds/pingcap/test/jq-1.6/jq-linux64"
	local schema_registry_url="${FILE_SERVER_URL}/download/builds/pingcap/cdc/schema-registry.tar.gz"

	# Download and extract binaries
	# download_and_extract "$minio_download_url" "minio.tar.gz"
	download_and_extract "$etcd_download_url" "etcd.tar.gz" "etcd-v3.4.7-linux-amd64/etcdctl"
	download_and_extract "$sync_diff_inspector_url" "sync_diff_inspector.tar.gz"
	download_and_extract "$schema_registry_url" "schema-registry.tar.gz"

	download_file "$go_ycsb_download_url" "go-ycsb" "${THIRD_BIN_DIR}/go-ycsb"
	download_file "$jq_download_url" "jq" "${THIRD_BIN_DIR}/jq"

	chmod a+x ${THIRD_BIN_DIR}/*
}

download_and_extract() {
	local url=$1
	local file_name=$2
	local extract_path=${3:-""}

	download_file "$url" "$file_name" "${TMP_DIR}/$file_name"

	local tar_cmd_args=("-xz")

	# For GNU tar, use --wildcards. For BSD tar (macOS), this option is not available.
	local tar_opts=()
	local local_os=$(uname -s)
	if [[ "$local_os" != "Darwin" ]]; then
		tar_opts+=("--wildcards")
	fi

	# Arguments are added in a standard order: options, archive, members.
	tar_cmd_args+=(-C "${THIRD_BIN_DIR}")
	tar_cmd_args+=(-f "${TMP_DIR}/$file_name")

	if [ -n "$extract_path" ]; then
		tar_cmd_args+=("$extract_path")
	fi

	tar "${tar_cmd_args[@]}"

	# Move extracted files if necessary
	case $file_name in
	"etcd.tar.gz")
		mv ${THIRD_BIN_DIR}/etcd-v3.4.7-linux-amd64/etcdctl ${THIRD_BIN_DIR}/
		rm -rf ${THIRD_BIN_DIR}/etcd-v3.4.7-linux-amd64
		;;
	"schema-registry.tar.gz")
		mv ${THIRD_BIN_DIR}/schema-registry ${THIRD_BIN_DIR}/_schema_registry
		mv ${THIRD_BIN_DIR}/_schema_registry/* ${THIRD_BIN_DIR}/ && rm -rf ${THIRD_BIN_DIR}/_schema_registry
		;;
	esac
}

# Main execution
cleanup() {
	rm -rf ${TMP_DIR} ${THIRD_BIN_DIR}
}

setup() {
	cleanup
	# rm -rf ${BIN_DIR}
	mkdir -p ${THIRD_BIN_DIR} ${TMP_DIR} ${BIN_DIR}
}

main() {
	log_green "Start downloading"
	setup

	download_binaries

	# Move binaries to final location
	mv ${THIRD_BIN_DIR}/* ./${BIN_DIR}

	cleanup
	log_green "Download SUCCESS"
}

main
