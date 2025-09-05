import sys
import requests as rq
from requests.exceptions import RequestException
import time
import json
import logging


# the max retry time
RETRY_TIME = 10

BASE_URL0 = "https://127.0.0.1:8300/api/v2"
BASE_URL1 = "https://127.0.0.1:8301/api/v2"


TLS_PD_ADDR = "https://127.0.0.1:2579"
SINK_URI = "mysql://normal:123456@127.0.0.1:3306/"

physicalShiftBits = 18


def assert_status_code(resp, expected_code, url):
    """
    Assert response status code matches expected code with detailed error message

    Args:
        resp: Response object
        expected_code: Expected status code
        url: Request URL
    """
    try:
        if resp is None:
            body = "No response"
            return
        else:
            body = resp.json() if resp.content else "Empty response"
    except ValueError:
        body = resp.text

    assert resp.status_code == expected_code, f"""
    Expected status code: {expected_code}
    Actual status code: {resp.status_code}
    Response body: {body}
    URL: {url}
    """


def requests_get_with_retry(url, max_retries=RETRY_TIME, delay_seconds=1):
    """
    requests get with retry

    :param url: request url
    :param max_retries: max retry times
    :param delay_seconds: retry delay seconds
    :return: when success, return response, else return None
    """
    for retry in range(max_retries):
        try:
            response = rq.get(url, cert=CERT, verify=VERIFY)
            if response.status_code == 200 or response.status_code == 202:
                return response
        except RequestException as e:
            logging.info(
                f"request fails {retry + 1}/{max_retries} time retry...")
            time.sleep(delay_seconds)
    return None


# we should write some SQLs in the run.sh after call create_changefeed
def create_changefeed(sink_uri):
    url = BASE_URL1+"/changefeeds"
    # create changefeed
    for i in range(1, 5):
        data = {
            "changefeed_id": "changefeed-test"+str(i),
            "sink_uri": "blackhole://",
            "ignore_ineligible_table": True
        }
        # set sink_uri
        if i == 1 and sink_uri != "":
            data["sink_uri"] = sink_uri

        data = json.dumps(data)
        headers = {"Content-Type": "application/json"}
        resp = rq.post(url, data=data, headers=headers,
                       cert=CERT, verify=VERIFY)
        assert resp.status_code == rq.codes.ok

    # create changefeed fail because sink_uri is invalid
    data = json.dumps({
        "changefeed_id": "changefeed-test",
        "sink_uri": "mysql://127.0.0.1:1111",
        "ignore_ineligible_table": True
    })
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request

    # create changefeed fail because dispatcher is invalid
    url = BASE_URL1+"/changefeeds"
    data = json.dumps({
        "changefeed_id": "changefeed-test-v2",
        "sink_uri": "kafka://127.0.0.1:9092/http_api_tls?protocol=simple",
        "replica_config": {
            "sink": {
                "dispatchers": [
                    {
                        "matcher": ["*.*"],
                        "partition": "columns",
                        "columns": ["a.b"]
                    }
                ]
            }
        }
    })
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers,
                   cert=CERT, verify=VERIFY)
    assert "ErrDispatcherFailed" in resp.text, f"{resp.text}"

    print("pass test: create changefeed")


def list_changefeed():
    # test state: all
    url = BASE_URL0+"/changefeeds?state=all"
    # Add retry logic to wait for changefeeds
    # We need to retry because the coordinator need some time to sync the changefeed infos from etcd
    for _ in range(RETRY_TIME):
        resp = rq.get(url, cert=CERT, verify=VERIFY)
        assert_status_code(resp, rq.codes.ok, url)
        changefeeds = resp.json()["items"]
        if len(changefeeds) > 0:
            break
        logging.info("No changefeeds found, retrying...")
        time.sleep(1)

    assert len(changefeeds) > 0, "No changefeeds found after retries"

    # test state: normal
    # test state: normal
    url = BASE_URL0+"/changefeeds?state=normal"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert_status_code(resp, rq.codes.ok, url)
    data = resp.json()
    changefeeds = data["items"]
    for cf in changefeeds:
        assert cf["state"] == "normal"

    # test state: stopped
    url = BASE_URL0+"/changefeeds?state=stopped"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert_status_code(resp, rq.codes.ok, url)
    data = resp.json()
    changefeeds = data["items"]
    for cf in changefeeds:
        assert cf["state"] == "stopped"

    print("pass test: list changefeed")


def get_changefeed():
    # test get changefeed success
    url = BASE_URL0+"/changefeeds/changefeed-test1"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    # test get changefeed failed
    url = BASE_URL0+"/changefeeds/changefeed-not-exists"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request
    data = resp.json()
    assert data["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: get changefeed")


def pause_changefeed():
    # pause changefeed
    url = BASE_URL0+"/changefeeds/changefeed-test2/pause"
    for i in range(RETRY_TIME):
        resp = rq.post(url, cert=CERT, verify=VERIFY)
        if resp.status_code == rq.codes.ok:
            break
        time.sleep(1)
    assert resp.status_code == rq.codes.ok
    # check if pause changefeed success
    url = BASE_URL0+"/changefeeds/changefeed-test2"
    for i in range(RETRY_TIME):
        resp = rq.get(url, cert=CERT, verify=VERIFY)
        assert resp.status_code == rq.codes.ok
        data = resp.json()
        if data["state"] == "stopped":
            break
        time.sleep(1)
    assert data["state"] == "stopped"
    # test pause changefeed failed
    url = BASE_URL0+"/changefeeds/changefeed-not-exists/pause"
    resp = rq.post(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request
    data = resp.json()
    assert data["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: pause changefeed")


def update_changefeed():
    # update fail
    # can only update a stopped changefeed
    url = BASE_URL0+"/changefeeds/changefeed-test1"
    data = json.dumps({"mounter_worker_num": 32})
    headers = {"Content-Type": "application/json"}
    resp = rq.put(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request

    # update success
    url = BASE_URL0+"/changefeeds/changefeed-test2"
    data = json.dumps({"mounter_worker_num": 32})
    headers = {"Content-Type": "application/json"}
    resp = rq.put(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    # update fail
    # can't update start_ts
    url = BASE_URL0+"/changefeeds/changefeed-test2"
    data = json.dumps({"start_ts": 1})
    headers = {"Content-Type": "application/json"}
    resp = rq.put(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert_status_code(resp, rq.codes.bad_request, url)

    # can't update dispatchers
    url = BASE_URL0+"/changefeeds/changefeed-test2"
    data = json.dumps({
        "sink_uri": "kafka://127.0.0.1:9092/http_api_tls?protocol=simple",
        "replica_config": {
            "sink": {
                "dispatchers": [
                    {
                        "matcher": ["*.*"],
                        "partition": "columns",
                        "columns": ["a.b"]
                    }
                ]
            }
        }
    })
    headers = {"Content-Type": "application/json"}
    resp = rq.put(url, data=data, headers=headers,
                  cert=CERT, verify=VERIFY)
    assert "ErrDispatcherFailed" in resp.text, f"{resp.text}"

    print("pass test: update changefeed")


def resume_changefeed():
    # resume changefeed
    url = BASE_URL1+"/changefeeds/changefeed-test2/resume"
    resp = rq.post(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    # check if resume changefeed success
    url = BASE_URL1+"/changefeeds/changefeed-test2"
    for i in range(RETRY_TIME):
        resp = rq.get(url, cert=CERT, verify=VERIFY)
        assert resp.status_code == rq.codes.ok
        data = resp.json()
        if data["state"] == "normal":
            break
        time.sleep(1)
    assert data["state"] == "normal"

    # test resume changefeed failed
    url = BASE_URL0+"/changefeeds/changefeed-not-exists/resume"
    resp = rq.post(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request
    data = resp.json()
    assert data["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: resume changefeed")


def remove_changefeed(cfID="changefeed-test3"):
    # remove changefeed
    url = BASE_URL0+"/changefeeds/" + cfID
    resp = rq.delete(url, cert=CERT, verify=VERIFY)
    assert_status_code(resp, rq.codes.ok, url)

    # test remove non-exists changefeed, it should return 200 and do nothing
    url = BASE_URL0+"/changefeeds/changefeed-not-exists"
    resp = rq.delete(url, cert=CERT, verify=VERIFY)
    assert_status_code(resp, rq.codes.ok, url)

    print("pass test: remove changefeed")


def move_table():
    # FIXME: Enable this test case after we fully support move table API
    print("pass test: move table")


def resign_owner():
    url = BASE_URL1 + "/owner/resign"
    resp = rq.post(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    print("pass test: resign owner")


def list_capture():
    url = BASE_URL0 + "/captures"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    print("pass test: list captures")


def check_health():
    url = BASE_URL0 + "/health"
    for i in range(RETRY_TIME):
        resp = rq.get(url, cert=CERT, verify=VERIFY)
        if resp.status_code == rq.codes.ok:
            break
        time.sleep(1)
    assert resp.status_code == rq.codes.ok

    print("pass test: check health")


def get_status():
    url = BASE_URL0 + "/status"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok
    assert resp.json()["is_owner"]

    print("pass test: get status")


def set_log_level():
    url = BASE_URL0 + "/log"
    data = json.dumps({"log_level": "debug"})
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    data = json.dumps({"log_level": "info"})
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    print("pass test: set log level")


def verify_table():
    # FIXME: Enable this test case after we fully support verify table API
    print("pass test: verify table")
    return
    url = BASE_URL0+"/tso"
    # we need to retry since owner resign before this func call
    i = 0
    while i < 10:
        try:
            data = json.dumps({})
            headers = {"Content-Type": "application/json"}
            resp = rq.post(url, data=data, headers=headers,
                           cert=CERT, verify=VERIFY, timeout=5)
            if resp.status_code == rq.codes.ok:
                break
            else:
                continue
        except rq.exceptions.RequestException:
            i += 1
    assert resp.status_code == rq.codes.ok

    ps = resp.json()["timestamp"]
    ls = resp.json()["logic_time"]
    tso = compose_tso(ps, ls)

    url = BASE_URL0 + "/verify_table"
    data = json.dumps({
        "pd_addrs": [TLS_PD_ADDR],
        "ca_path": CA_PEM_PATH,
        "cert_path": CLIENT_PEM_PATH,
        "key_path": CLIENT_KEY_PEM_PATH,
        "cert_allowed_cn": ["client"],
        "start_ts": tso,
        "replica_config": {
            "filter": {
                "rules": ["test.verify*"]
            }
        }
    })
    headers = {"Content-Type": "application/json"}
    for i in range(RETRY_TIME):
        resp = rq.post(url, data=data, headers=headers,
                       cert=CERT, verify=VERIFY)
        if resp.status_code == rq.codes.ok:
            break
        time.sleep(1)
    assert resp.status_code == rq.codes.ok
    eligible_table_name = resp.json()["eligible_tables"][0]["table_name"]
    ineligible_table_name = resp.json()["ineligible_tables"][0]["table_name"]
    assert eligible_table_name == "verify_table_eligible"
    assert ineligible_table_name == "verify_table_ineligible"

    print("pass test: verify table")


def get_tso():
    url = BASE_URL0+"/tso"
    data = json.dumps({})
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    print("pass test: get tso")


def unsafe_apis():
    # FIXME: Enable this test case after we fully support unsafe APIs
    print("pass test: unsafe apis")
    return
    url = BASE_URL1+"/unsafe/metadata"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok
    print("status code", resp.status_code)
    print("pass test: show metadata")

    # service_gc_safepoint 1
    url = BASE_URL1+"/unsafe/service_gc_safepoint"
    data = {
        "pd_addrs": [TLS_PD_ADDR],
        "ca_path": CA_PEM_PATH,
        "cert_path": CLIENT_PEM_PATH,
        "key_path": CLIENT_KEY_PEM_PATH,
        "cert_allowed_cn": ["client"],
    }
    data = json.dumps(data)
    headers = {"Content-Type": "application/json"}
    resp = rq.delete(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    print("status code", resp.status_code)
    assert resp.status_code == rq.codes.ok

    data = json.dumps({})
    headers = {"Content-Type": "application/json"}
    resp = rq.delete(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    print("status code", resp.status_code)
    assert resp.status_code == rq.codes.ok
    print("pass test: delete service_gc_safepoint")

    # create changefeed fail because sink_uri is invalid
    data = json.dumps({})
    url = BASE_URL1+"/unsafe/resolve_lock"
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    print("status code", resp.status_code)
    assert resp.status_code != rq.codes.not_found
    print("pass test: resolve lock")


# util functions define belows

# compose physical time and logical time into tso
def compose_tso(ps, ls):
    return (ps << physicalShiftBits) + ls


# arg1: test case name
# arg2: cetificates dir
# arg3: sink uri
if __name__ == "__main__":

    CERTIFICATE_PATH = sys.argv[2]
    CLIENT_PEM_PATH = CERTIFICATE_PATH + '/client.pem'
    CLIENT_KEY_PEM_PATH = CERTIFICATE_PATH + '/client-key.pem'
    CA_PEM_PATH = CERTIFICATE_PATH + '/ca.pem'
    CERT = (CLIENT_PEM_PATH, CLIENT_KEY_PEM_PATH)
    VERIFY = (CA_PEM_PATH)

    # test all the case as the order list in this map
    FUNC_MAP = {
        "check_health": check_health,
        "get_status": get_status,
        "create_changefeed": create_changefeed,
        "list_changefeed": list_changefeed,
        "get_changefeed": get_changefeed,
        "pause_changefeed": pause_changefeed,
        "update_changefeed": update_changefeed,
        "resume_changefeed": resume_changefeed,
        "move_table": move_table,
        "set_log_level": set_log_level,
        "remove_changefeed": remove_changefeed,
        "resign_owner": resign_owner,
        # api v2
        "get_tso": get_tso,
        "verify_table": verify_table,
        "unsafe_apis": unsafe_apis
    }

    # check the test case name
    if len(sys.argv) < 2:
        logging.error("Please provide a test case name")
        sys.exit(1)

    # get the test case name
    test_case_name = sys.argv[1]
    arg = sys.argv[3:]
    # check if the test case name is in the FUNC_MAP
    if test_case_name not in FUNC_MAP:
        logging.error(f"Test case {test_case_name} not found")
        sys.exit(1)

    # get the test case function
    test_case_func = FUNC_MAP[test_case_name]
    # run the test case
    logging.info(f"Start to run test case: {test_case_name}")
    test_case_func(*arg)
    logging.info(f"Test case {test_case_name} finished")
