import json
import os
import sys

import requests

url_view_parameter = "?view=FULL"


def get_query_input():
    url = os.getenv("TES_URL")

    if not url:
        print("TES_URL environment variable is not set.")
        sys.exit(1)

    user = os.getenv("TES_USER")
    password = os.getenv("TES_PASSWORD")

    if user and password:
        auth = (user, password)
    else:
        print("TES_USER and TES_PASSWORD environment variables are not set.")
        sys.exit(1)

    #    url = f"{url}{task_id}{url_parameter}"
    return url, auth


def get_json(url, auth):
    response = requests.get(url, auth=auth)

    if response.status_code == 200:
        json_data = response.json()
        if json_data.get("state") == "COMPLETE":
            print("Task is complete")
            json_string = json.dumps(json_data, indent=2)
            print(json_string)
            # json_string = json.dumps(json_data)
            # print(json_string)

            return json_data
        else:
            print("Task not completed successfully")
    else:
        print(f"Error: {response.status_code}, {response.text}")

    return None


def get_sequence_size(url):

    response = requests.head(url)
    file_size = response.headers.get("Content-Length")

    if file_size:
        return file_size
    else:
        return 0


def get_appinputs(json_data):

    received_appinputs = json_data["inputs"]

    num_sequences = 0
    appinputs = {}
    appinputs["sequence_sizes"] = []
    for appinput in received_appinputs:
        print("name: ", appinput["name"])
        url = appinput["url"]
        print("size = ", get_sequence_size(url))
        num_sequences += 1
        appinputs["sequence_sizes"].append(get_sequence_size(url))

    appinputs["num_sequences"] = num_sequences

    return appinputs


def get_deployment():

    return os.getenv("TES_DEPLOYMENT") or "unknown"


def generate_hpcadvisor_json(json_data):
    print("Registering data to HPCAdvisor")
    # print(json_data)

    appinputs = get_appinputs(json_data)
    deployment = get_deployment()

    # TODO: not all tasks have the right data
    try:
        sku = json_data["resources"]["backend_parameters"]["vm_size"]
    except KeyError:
        sku = "unknown"

    new_json = {
        "deployment": deployment,
        "appname": "tes",
        "total_cores": json_data["resources"]["cpu_cores"],
        "sku": sku,
        "nnodes": 1,
        "appinputs": appinputs,
    }
    new_json["tags"] = {}
    new_json["tags"]["tes_experiment_id"] = json_data["id"]
    print(json.dumps(new_json, indent=4))


def extract_data_for_task_id(url, auth, task_id):

    url = f"{url}{task_id}{url_view_parameter}"
    json_data = get_json(url, auth)

    if json_data:
        generate_hpcadvisor_json(json_data)


def get_valid_task_ids(data):

    valid_task_ids = []
    for task in data["tasks"]:
        if task["state"] == "COMPLETE":
            valid_task_ids.append(task["id"])
    return valid_task_ids


def get_all_valid_task_ids(url, auth):
    # TODO: add test for the rest api call

    params = {"page_token": ""}

    valid_task_ids = []
    while True:
        response = requests.get(url, auth=auth, params=params)

        data = response.json()
        new_ids = get_valid_task_ids(data)
        valid_task_ids.extend(new_ids)

        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break

        params["page_token"] = next_page_token

    return valid_task_ids


def extract_data_all_tasks(url, auth):

    valid_task_ids = get_all_valid_task_ids(url, auth)
    print("Valid task ids: ", valid_task_ids)

    for task_id in valid_task_ids:
        extract_data_for_task_id(url, auth, task_id)


def extract_data(url, auth, task_id):

    if task_id != "all":
        extract_data_for_task_id(url, auth, task_id)
    else:
        extract_data_all_tasks(url, auth)


if __name__ == "__main__":
    # pass "all" or "id" as argument
    task_id = "all"
    if len(sys.argv) > 1:
        if sys.argv[1] == "all":
            print("Registering all data")
        else:
            print("Registering data for task id: ", sys.argv[1])
            task_id = sys.argv[1]
    else:
        print('<task id> or "all" is not provided.')
        sys.exit(1)

    url, auth = get_query_input()

    extract_data(url, auth, task_id)
