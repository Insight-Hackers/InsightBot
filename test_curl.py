import requests
import pandas as pd
import io

csv_url = "https://files.slack.com/files-pri/T08TR1VA5JS-F091DFRQ9U1/csv/list"
api_token = 'xoxb-8943063345638-9002480214405-Vbh7kHjdgcLrfYoQKq34kJO9'

headers = {
    "Authorization": f"Bearer {api_token}"
}

response = requests.get(csv_url, headers=headers)
response.raise_for_status()

df = pd.read_csv(io.StringIO(response.text))

print(df)
