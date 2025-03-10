import json
import os
import sys

import requests

url_parameter = "?view=FULL"


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

    if len(sys.argv) > 1:
        task_id = sys.argv[1]
    else:
        print("Task ID is not provided.")
        sys.exit(1)

    url = f"{url}{task_id}{url_parameter}"
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


def generate_hpcadvisor_json(json_data):
    print("Registering data to HPCAdvisor")
    print(json_data)
    new_json = {
        "deployment": None,
        "appname": "tes",
        "total_cores": json_data["resources"]["cpu_cores"],
        "sku": json_data["resources"]["backend_parameters"]["vm_size"],
        "nnodes": 1,
    }
    print(new_json)


url, auth = get_query_input()
json_data = get_json(url, auth)

if json_data:
    generate_hpcadvisor_json(json_data)
