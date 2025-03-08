import os
import sys

import requests

url_parameter = "?view=FULL"

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
print(f"URL: {url}")


response = requests.get(url, auth=auth)

if response.status_code == 200:
    json_data = response.json()
    if json_data.get("status") == "COMPLETE":
        print("Task is complete")
    else:
        # print(json_data)
        print("Task not completed successfully")
else:
    print(f"Error: {response.status_code}, {response.text}")
