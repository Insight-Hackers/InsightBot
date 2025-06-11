import requests
import pandas as pd
import io


url = 'https://files.slack.com/files-pri/T08TR1VA5JS-F0910FJJRU2/download/list'
api_token = 'xoxb-8943063345638-9002480214405-Vbh7kHjdgcLrfYoQKq34kJO9'
headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}
res = requests.get(url, headers=headers)
if res.status_code == 200:
    df = pd.read_csv(io.StringIO(res.text))
    print("Status code:", res.status_code)
    print("DataFrame:")
    print(df)
else:
    print("Failed to download list.")
    print("Status code:", res.status_code)
    print("Response text:", res.text)