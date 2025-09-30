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
set -euo pipefail
# ANSI color codes for styling the output
RED='\033[0;31m'    # Sets text to red
GREEN='\033[0;32m'  # Sets text to green
YELLOW='\033[0;33m' # Sets text to yellow
BLUE='\033[0;34m'   # Sets text to blue
NC='\033[0m'        # Resets the text color to default, no color

ver=
disable_tidb=0

show_help() {
	echo -e "${GREEN}Usage: $0 [OPTIONS]${NC}"
	echo -e "Downloads integration test binaries for macOS"
	echo -e "\n${BLUE}Options:${NC}"
	echo -e "  -v, --version VERSION  Set TiDB component version (default: latest)"
	echo -e "  --disable-tidb         Skip downloading TiDB components"
	echo -e "  -h, --help             Show this help message"
	echo -e "\n${YELLOW}Downloads:${NC}"
	echo -e "  - minio, jq, confluent"
	echo -e "  - go-ycsb, etcdctl, sync_diff_inspector"
	echo -e "  - TiDB components (unless --disable-tidb is set):"
	echo -e "    tidb, tikv, cdc, pd, tiflash, ctl"
}

get_latest_tidb_version() {
	local latest_version="v8.5.2" # fallback version
	# Try to get latest version from GitHub API
	if command -v curl &>/dev/null; then
		latest_version=$(curl -s https://api.github.com/repos/pingcap/tidb/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
	elif command -v wget &>/dev/null; then
		latest_version=$(wget -qO- https://api.github.com/repos/pingcap/tidb/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
	fi
	# If version extraction failed, use fallback
	if [[ ! "$latest_version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]]; then
		latest_version="v8.5.2"
	fi
	echo "$latest_version"
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	-v | --version)
		ver="$2"
		shift 2
		;;
	--disable-tidb)
		disable_tidb=1
		shift
		;;
	-h | --help)
		show_help
		exit 0
		;;
	*) ;;
	esac

done

if [ -z "$ver" ]; then
	ver=$(get_latest_tidb_version)
fi

mkdir -p bin

arch=$(uname -m)

if [ ! -x bin/minio ]; then
	# Download minio
	echo -e "${YELLOW}downloading minio...${NC}"
	wget -O bin/minio "https://dl.min.io/server/minio/release/darwin-$arch/minio"
	chmod +x bin/minio
fi

if [ ! -x bin/jq ]; then
	# Download jq using curl
	echo -e "${YELLOW}downloading jq...${NC}"
	wget -O bin/jq "https://github.com/jqlang/jq/releases/download/jq-1.8.0/jq-macos-$arch"
	chmod +x bin/jq
fi

if [ ! -x bin/bin/kafka-server-start ]; then
	echo -e "${YELLOW}downloading confluent...${NC}"
	wget -O bin/confluent-7.5.2.tar.gz https://packages.confluent.io/archive/7.5/confluent-7.5.2.tar.gz
	tar -C bin/ -xzf bin/confluent-7.5.2.tar.gz
	rm bin/confluent-7.5.2.tar.gz
	mv bin/confluent-7.5.2/bin/ bin/
	rm -rf bin/confluent-7.5.2
fi

if [ ! -x bin/go-ycsb ]; then
	echo -e "${YELLOW}downloading go-ycsb...${NC}"
	wget -O "bin/go-ycsb-darwin-$arch.tar.gz" "https://github.com/pingcap/go-ycsb/releases/latest/download/go-ycsb-darwin-$arch.tar.gz"
	tar -C bin/ -xzf "bin/go-ycsb-darwin-$arch.tar.gz"
	rm -rf "bin/go-ycsb-darwin-$arch.tar.gz"
fi

if [ ! -x bin/etcdctl ]; then
	echo -e "${YELLOW}downloading etcd...${NC}"
	wget -O "bin/etcd-v3.6.1-darwin-$arch.zip" "https://github.com/etcd-io/etcd/releases/download/v3.6.1/etcd-v3.6.1-darwin-$arch.zip"
	unzip -d bin/ "bin/etcd-v3.6.1-darwin-$arch.zip"
	rm "bin/etcd-v3.6.1-darwin-$arch.zip"
	mv "bin/etcd-v3.6.1-darwin-$arch/etcdctl" bin/
	rm -rf "bin/etcd-v3.6.1-darwin-$arch"
fi

if [ ! -x bin/sync_diff_inspector ]; then
	echo -e "${YELLOW}downloading sync-diff-inspector...${NC}"
	wget -O "bin/sync-diff-inspector-v9.0.0-beta.1-darwin-$arch.tar.gz" "https://tiup-mirrors.pingcap.com/sync-diff-inspector-v9.0.0-beta.1-darwin-$arch.tar.gz"
	tar -C bin/ -xzf "bin/sync-diff-inspector-v9.0.0-beta.1-darwin-$arch.tar.gz"
	rm -rf "bin/sync-diff-inspector-v9.0.0-beta.1-darwin-$arch.tar.gz"
fi

if [[ "$disable_tidb" == "1" ]]; then
	echo -e "${RED}You should copy the tidb binaries to the bin/ directory on your own.${NC}"
	exit 0
fi

if [ ! -x bin/tidb-server ]; then
	echo -e "${YELLOW}downloading tidb...${NC}"
	wget -O "bin/tidb-$ver-darwin-$arch.tar.gz" "https://tiup-mirrors.pingcap.com/tidb-$ver-darwin-$arch.tar.gz"
	tar -C bin/ -xvf "bin/tidb-$ver-darwin-$arch.tar.gz"
	rm "bin/tidb-$ver-darwin-$arch.tar.gz"
fi

if [ ! -x bin/tikv-server ]; then
	echo -e "${YELLOW}downloading tikv...${NC}"
	wget -O "bin/tikv-$ver-darwin-$arch.tar.gz" "https://tiup-mirrors.pingcap.com/tikv-$ver-darwin-$arch.tar.gz"
	tar -C bin/ -xvf "bin/tikv-$ver-darwin-$arch.tar.gz"
	rm "bin/tikv-$ver-darwin-$arch.tar.gz"
fi

if [ ! -x bin/pd-server ]; then
	echo -e "${YELLOW}downloading pd...${NC}"
	wget -O "bin/pd-$ver-darwin-$arch.tar.gz" "https://tiup-mirrors.pingcap.com/pd-$ver-darwin-$arch.tar.gz"
	tar -C bin/ -xvf "bin/pd-$ver-darwin-$arch.tar.gz"
	rm "bin/pd-$ver-darwin-$arch.tar.gz"
fi

if [ ! -x bin/tiflash ]; then
	echo -e "${YELLOW}downloading tiflash...${NC}"
	wget -O "bin/tiflash-$ver-darwin-$arch.tar.gz" "https://tiup-mirrors.pingcap.com/tiflash-$ver-darwin-$arch.tar.gz"
	tar -C bin/ -xvf "bin/tiflash-$ver-darwin-$arch.tar.gz"
	mv bin/tiflash "bin/tiflash-bin"
	mv bin/tiflash-bin/* bin/
	rm -rf "bin/tiflash-$ver-darwin-$arch.tar.gz"
	rm -rf bin/tiflash-bin
fi

if [ ! -x bin/pd-ctl ]; then
	echo -e "${YELLOW}downloading ctl...${NC}"
	wget -O "bin/ctl-$ver-darwin-$arch.tar.gz" "https://tiup-mirrors.pingcap.com/ctl-$ver-darwin-$arch.tar.gz"
	tar -C bin/ -xvf "bin/ctl-$ver-darwin-$arch.tar.gz"
	rm -rf "bin/ctl-$ver-darwin-$arch.tar.gz"
fi
