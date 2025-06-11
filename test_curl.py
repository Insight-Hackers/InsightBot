import requests
import pandas as pd
import io


url = 'https://files.slack.com/files-pri/T08TR1VA5JS-F091DFRQ9U1/download/list'
api_token = 'xoxb-8943063345638-9002480214405-SsIbcsHZTsw4LorLdRbeYZ4d'
headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}
res = requests.get(url, headers=headers)
csv_url = res.json()['list_csv_download_url']
# Download the CSV file
csv_res = requests.get(url=csv_url,headers=headers)
csv_res.raise_for_status() 
csv_data = csv_res.content.decode('utf-8').splitlines()
total_csv = [dict(zip(csv_data[0].split(','), line.split(','))) for line in csv_data[1:]]
from pprint import pprint
pprint(total_csv)


