import json
import os
import sys
from datetime import datetime

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


def get_execution_time(json_data):

    log_entry = json_data["logs"][0]["logs"][0]

    start_time = log_entry["start_time"]
    end_time = log_entry["end_time"]

    start_dt = datetime.strptime(start_time[:-1][:26], "%Y-%m-%dT%H:%M:%S.%f")
    end_dt = datetime.strptime(end_time[:-1][:26], "%Y-%m-%dT%H:%M:%S.%f")

    total_seconds = (end_dt - start_dt).total_seconds()

    return total_seconds


def generate_hpcadvisor_json(json_data):
    print("Registering data to HPCAdvisor")
    # print(json_data)

    if len(json_data["inputs"]) == 0:
        print("No inputs found. Ignoring this task.")
        return []

    print("input urls: ", json_data["inputs"][0]["url"])

    if not json_data["resources"]["backend_parameters"]:
        print("No backend parameters found. Ignoring this task.")
        return []

    appinputs = get_appinputs(json_data)
    deployment = get_deployment()
    exectime = get_execution_time(json_data)

    sku = json_data["resources"]["backend_parameters"]["vm_size"]

    new_json = {
        "deployment": deployment,
        "appname": "tes",
        "total_cores": json_data["resources"]["cpu_cores"],
        "sku": sku,
        "nnodes": 1,
        "appinputs": appinputs,
        "exec_time": exectime,
    }
    new_json["tags"] = {}
    new_json["tags"]["tes_experiment_id"] = json_data["id"]

    # print(json.dumps(new_json, indent=4))

    return new_json


def extract_data_for_task_id(url, auth, task_id):

    url = f"{url}{task_id}{url_view_parameter}"
    json_data = get_json(url, auth)

    generated_json = []
    if json_data:
        generated_json = generate_hpcadvisor_json(json_data)

    return generated_json


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

    recorded_task_ids = 0
    not_recorded_task_ids = 0
    new_entries_json = []
    for task_id in valid_task_ids:
        generated_json = extract_data_for_task_id(url, auth, task_id)
        if generated_json:
            recorded_task_ids += 1
            new_entries_json.append(generated_json)
            # if recorded_task_ids == 2:
            # print("Stopping after 2 tasks")
            # break
        else:
            not_recorded_task_ids += 1

    print("Recorded task ids: ", recorded_task_ids)
    print("Not recorded task ids: ", not_recorded_task_ids)

    return new_entries_json


def extract_data(url, auth, task_id):

    new_entries_json = []

    if task_id != "all":
        new_entries_json = extract_data_for_task_id(url, auth, task_id)
    else:
        new_entries_json = extract_data_all_tasks(url, auth)

    return new_entries_json


def store_new_entries(new_entries_json):
    print("Storing new entries to HPCAdvisor")

    datapoints_label = "datapoints"

    file_path = os.getenv("HPCADVISOR_FILE_PATH")

    if not file_path:
        print("HPCADVISOR_FILE_PATH environment variable is not set.")
        sys.exit(1)

    if not os.path.exists(file_path):
        print("HPCAdvisor file path does not exist.")
        sys.exit(1)

    existing_data = {}

    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            existing_data = json.load(file)

    if not datapoints_label in existing_data:
        existing_data[datapoints_label] = []

    # Ensure the new data points are lists of dictionaries and append
    #    if isinstance(new_entries_json, list) and all(
    #       isinstance(entry, dict) for entry in new_entries_json
    #  ):
    print(new_entries_json)
    existing_data[datapoints_label].extend(new_entries_json)
    # else:
    #    print("New data is not in the expected format")

    with open(file_path, "w") as file:
        json.dump(existing_data, file, indent=2)


if __name__ == "__main__":
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

    new_entries_json = extract_data(url, auth, task_id)
    store_new_entries(new_entries_json)
