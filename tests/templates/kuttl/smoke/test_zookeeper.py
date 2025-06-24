#!/usr/bin/env python3
import argparse
import requests
import time
import sys

sys.tracebacklimit = 0


def print_request_error_and_sleep(message, err, retry_count):
    print("[" + str(retry_count) + "] " + message, err)
    time.sleep(5)


def try_get(url):
    retries = 3
    for i in range(retries):
        try:
            r = requests.get(url, timeout=5)
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as errh:
            print_request_error_and_sleep("Http Error: ", errh, i)
        except requests.exceptions.ConnectionError as errc:
            print_request_error_and_sleep("Error Connecting: ", errc, i)
        except requests.exceptions.Timeout as errt:
            print_request_error_and_sleep("Timeout Error: ", errt, i)
        except requests.exceptions.RequestException as err:
            print_request_error_and_sleep("Error: ", err, i)

    exit(-1)


def check_ruok(hosts):
    cmd_ruok = "ruok"

    for host in hosts:
        url = host + ":8080/commands/" + cmd_ruok
        response = try_get(url).json()

        if (
            "command" in response
            and response["command"] == cmd_ruok
            and "error" in response
            and response["error"] is None
        ):
            continue
        else:
            print(
                "Error["
                + cmd_ruok
                + "] for ["
                + url
                + "]: received "
                + str(response)
                + " - expected {'command': 'ruok', 'error': None} "
            )
            exit(-1)


def check_monitoring(hosts):
    for host in hosts:
        # test for the jmx exporter metrics
        url = host + ":9505"
        response = try_get(url)

        if not response.ok:
            print("Error for [" + url + "]: could not access monitoring")
            exit(-1)

        # test for the native metrics
        url = host + ":7000/metrics"
        response = try_get(url)

        if response.ok:
            # arbitrary metric was chosen to test if metrics are present in the response
            if "quorum_size" not in response.text:
                print("Error for [" + url + "]: missing metrics")
                exit(-1)
        else:
            print("Error for [" + url + "]: could not access monitoring")
            exit(-1)


if __name__ == "__main__":
    all_args = argparse.ArgumentParser(description="Test ZooKeeper.")
    all_args.add_argument(
        "-n", "--namespace", help="The namespace to run in", required=True
    )
    args = vars(all_args.parse_args())
    namespace = args["namespace"]

    host_primary_0 = (
        "http://test-zk-server-primary-0.test-zk-server-primary."
        + namespace
        + ".svc.cluster.local"
    )
    host_primary_1 = (
        "http://test-zk-server-primary-1.test-zk-server-primary."
        + namespace
        + ".svc.cluster.local"
    )
    host_secondary = (
        "http://test-zk-server-secondary-0.test-zk-server-secondary."
        + namespace
        + ".svc.cluster.local"
    )

    hosts = [host_primary_0, host_primary_1, host_secondary]

    check_ruok(hosts)
    check_monitoring(hosts)

    print("Test successful!")
