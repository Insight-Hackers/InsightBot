import requests

url = 'https://files.slack.com/files-pri/T08TR1VA5JS-F0910FJJRU2/download/list'
api_token = 'xoxb-8943063345638-9002480214405-Vbh7kHjdgcLrfYoQKq34kJO9'
headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}
res = requests.get(url, headers=headers)
print(res.status_code)
print(res.json())