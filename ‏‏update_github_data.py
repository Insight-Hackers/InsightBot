import os
import requests
import pandas as pd
import psycopg2
import json

# ========================
# הגדרות בסיסיות
# ========================

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # הגדרת הטוקן כמשתנה סביבה

headers = {
    "Authorization": f"token {GITHUB_TOKEN}"
}


def get_db_connection():
    """חיבור למסד Supabase"""
    return psycopg2.connect(
        dbname="postgres",
        user="postgres.apphxbmngxlclxromyvt",
        password="insightbot2025",
        host="aws-0-eu-north-1.pooler.supabase.com",
        port="6543"
    )


def save_dataframe_to_db(df, table_name):
    """שמירת DataFrame לטבלה במסד הנתונים"""
    if df.empty:
        print(f"⚠️ הטבלה {table_name} ריקה - לא נשמר כלום")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                df[column] = df[column].dt.to_pydatetime()
            elif pd.api.types.is_object_dtype(df[column]):
                df[column] = df[column].astype(str)

        for _, row in df.iterrows():
            cols = ','.join(df.columns)
            placeholders = ','.join(['%s'] * len(df.columns))
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET "
            sql += ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != 'id'])

            cursor.execute(sql, tuple(row))

        conn.commit()
        print(f"✅ נשמרו {len(df)} שורות לטבלה {table_name}")

    except Exception as e:
        print(f"❌ שגיאה בשמירה לטבלה {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# ========================
# פונקציות קריאה ל־GitHub API
# ========================


def fetch_github_prs(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state=all&per_page=100"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def fetch_github_issues(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/issues?state=all&per_page=100"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def fetch_github_commits(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/commits?per_page=100"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def fetch_github_reviews(owner, repo, pull_number):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pull_number}/reviews"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# ========================
# פונקציות עיבוד נתונים ושמירה למסד
# ========================


def process_and_save_prs(owner, repo):
    prs = fetch_github_prs(owner, repo)
    df = pd.json_normalize(prs)
    # בחירת עמודות רלוונטיות
    df = df[[
        'id', 'number', 'state', 'title', 'created_at', 'closed_at',
        'merged_at', 'user.login', 'repository_url', 'html_url'
    ]].rename(columns={
        'user.login': 'user_id',
        'repository_url': 'repository',
        'html_url': 'url'
    })
    # המרת תאריכים
    for col in ['created_at', 'closed_at', 'merged_at']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    save_dataframe_to_db(df, 'github_prs_raw')


def process_and_save_issues(owner, repo):
    issues = fetch_github_issues(owner, repo)
    # מסננים PRs כי Issues API מחזיר גם PRs, מוותרים על PRs כאן
    issues = [issue for issue in issues if 'pull_request' not in issue]
    df = pd.json_normalize(issues)
    df = df[[
        'id', 'user.login', 'title', 'body', 'state', 'created_at',
        'closed_at', 'repository_url', 'html_url'
    ]].rename(columns={
        'user.login': 'user_id',
        'repository_url': 'repository',
        'html_url': 'url'
    })
    for col in ['created_at', 'closed_at']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    save_dataframe_to_db(df, 'github_issues_raw')


def process_and_save_commits(owner, repo):
    commits = fetch_github_commits(owner, repo)
    df = pd.json_normalize(commits)
    df = df[[
        'sha', 'commit.author.name', 'commit.message', 'commit.author.date',
        'html_url', 'url'
    ]].rename(columns={
        'commit.author.name': 'author',
        'commit.message': 'message',
        'commit.author.date': 'timestamp',
        'html_url': 'url'
    })
    df['repository'] = f"https://github.com/{owner}/{repo}"
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    save_dataframe_to_db(df, 'github_commits_raw')


def process_and_save_reviews(owner, repo):
    prs = fetch_github_prs(owner, repo)
    all_reviews = []
    for pr in prs:
        reviews = fetch_github_reviews(owner, repo, pr['number'])
        for rev in reviews:
            rev['pull_request_id'] = pr['id']
            all_reviews.append(rev)
    df = pd.json_normalize(all_reviews)
    if df.empty:
        print("⚠️ אין ביקורות לשמירה")
        return
    df = df[[
        'id', 'pull_request_id', 'user.login', 'state',
        'body', 'created_at', 'html_url'
    ]].rename(columns={
        'user.login': 'user_id',
        'html_url': 'url'
    })
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    save_dataframe_to_db(df, 'github_reviews_raw')

# ========================
# דוגמת הרצה כוללת
# ========================


if __name__ == "__main__":
    OWNER = "YOUR_GITHUB_ORG_OR_USER"
    REPO = "YOUR_REPOSITORY_NAME"

    print("מתחיל למשוך ולעבד נתוני GitHub...")

    process_and_save_prs(OWNER, REPO)
    process_and_save_issues(OWNER, REPO)
    process_and_save_commits(OWNER, REPO)
    process_and_save_reviews(OWNER, REPO)

    print("✅ סיום עדכון GitHub ל-Supabase")
