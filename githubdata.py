import pandas as pd
import os
import requests

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # טען את הטוקן ממשתני הסביבה

headers = {
    "Authorization": f"token {GITHUB_TOKEN}"
}


def fetch_github_prs(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state=all&per_page=100"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


# דוגמה לשימוש
if __name__ == "__main__":
    prs = fetch_github_prs("OWNER_OR_ORG_NAME", "REPO_NAME")
    for pr in prs:
        print(pr["number"], pr["state"], pr["created_at"])


GITHUB_TOKEN = "הטוקן שלך"
headers = {"Authorization": f"token {GITHUB_TOKEN}"}


def fetch_github_prs(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state=all&per_page=100"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    prs = fetch_github_prs("OWNER", "REPO")

    # כאן ממירים את ה-JSON ל-DataFrame
    prs_df = pd.json_normalize(prs)

    print(prs_df.head())  # תצוגת חמשת השורות הראשונות
