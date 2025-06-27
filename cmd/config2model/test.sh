#!/usr/bin/env bash

RED='\033[0;31m'    # Sets text to red
GREEN='\033[0;32m'  # Sets text to green
YELLOW='\033[0;33m' # Sets text to yellow
NC='\033[0m'        # Resets the text color to default, no color

check_port_available() {
    local port=$1
    while ! nc -z localhost "$port"; do
        echo "Waiting for port $port to be available..."
        sleep 1
    done
}

setup() {
    echo -e "${YELLOW}Setting up...${NC}"
    echo -e "${YELLOW}Deploy upstream TiDB cluster${NC}"
    nohup tiup playground v8.5.2 --tag config2model-upstream --pd 1 --kv 1 --db 1 --ticdc 1 &
    check_port_available 4000

    echo -e "${YELLOW}Deploy downstream TiDB cluster...${NC}"
    nohup tiup playground v8.5.2 --tag config2model-downstream --pd 1 --kv 1 --db 1 --ticdc 1 --port-offset 10000 &
    check_port_available 14000
}

teardown() {
    ps -ef | grep tiup | grep config2model | awk '{print $2}' | xargs kill -9 >/dev/null 2>/dev/null
    tiup clean config2model-upstream >/dev/null 2>/dev/null
    tiup clean config2model-downstream >/dev/null 2>/dev/null
}

should_eq() {
    local expected=$1
    local actual=$2
    if [ "$expected" != "$actual" ]; then
        echo -e "${RED}Expected $expected, but got $actual${NC}"
        exit 1
    fi
}

trap teardown EXIT

setup

# Create changefeed
# Set case_sensitive=true, force_replicate=true
echo -e "${YELLOW}Create changefeed${NC}"
curl -X POST 'http://127.0.0.1:8300/api/v2/changefeeds' -H 'Content-type: application/json' \
    -d '{ "changefeed_id": "1", "sink_uri": "mysql://root@127.0.0.1:14000/", "start_ts": 0, "replica_config": { "memory_quota": 0, "case_sensitive": true, "force_replicate": true, "ignore_ineligible_table": false, "check_gc_safe_point": false, "filter": null, "mounter": null, "sink": null, "scheduler": null } }'

echo ""

# verify the result
data=$(curl -s 'http://127.0.0.1:8300/api/v2/changefeeds/1')

memory_quota=$(echo "$data" | jq '.config.memory_quota')
should_eq 1073741824 "$memory_quota"

case_sensitive=$(echo "$data" | jq '.config.case_sensitive')
should_eq true "$case_sensitive"

force_replicate=$(echo "$data" | jq '.config.force_replicate')
should_eq true "$force_replicate"

rules=$(echo "$data" | jq '.config.filter.rules[0]')
should_eq '"*.*"' "$rules"

echo -e "${GREEN}Test passed${NC}"

teardown
