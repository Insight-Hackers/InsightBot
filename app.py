from flask import Flask, request, abort
import hmac
import hashlib
import json
import pandas as pd
import psycopg2
import os
import traceback
from io import BytesIO
import requests
import threading
from openai import OpenAI
import re
from dotenv import load_dotenv
from agent_monitor import agent_monitor

load_dotenv()

app = Flask(__name__)

GITHUB_SECRET = os.getenv("GITHUB_SECRET")
if GITHUB_SECRET is None:
    raise RuntimeError("GITHUB_SECRET לא מוגדר בסביבת הריצה")
GITHUB_SECRET = GITHUB_SECRET.encode()  # המרה ל-כbytes



def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres.apphxbmngxlclxromyvt",
            password="insightbot2025",
            host="aws-0-eu-north-1.pooler.supabase.com",
            port="6543"
        )
        print("🟢 התחברות למסד הצליחה")
        return conn
    except Exception as e:
        print("❌ שגיאה בהתחברות למסד:", e)
        raise


def verify_signature(payload_body, signature_header):
    if signature_header is None:
        print("❌ לא נמצא header של חתימה")
        return False
    try:
        sha_name, signature = signature_header.split('=')
    except Exception as e:
        print(f"❌ שגיאה בפיצול חתימה: {e}")
        return False
    if sha_name != 'sha256':
        print(f"❌ סוג חתימה לא נתמך: {sha_name}")
        return False
    mac = hmac.new(GITHUB_SECRET, msg=payload_body, digestmod=hashlib.sha256)
    valid = hmac.compare_digest(mac.hexdigest(), signature)
    print(f"🔐 חתימה תקינה? {valid}")
    return valid


def save_dataframe_to_db(df, table_name, pk_column):
    if df.empty:
        print(f"⚠ הטבלה {table_name} ריקה - לא נשמר כלום")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 🛠 תיקון מרכזי: להמיר כל NaT / NaN ל־None
        df = df.where(pd.notnull(df), None)

      # המרת NaT לערכי None בפועל
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                df[column] = df[column].astype(
                    object).where(df[column].notna(), None)
            elif pd.api.types.is_object_dtype(df[column]):
                df[column] = df[column].apply(
                    lambda x: str(x) if x is not None else None)

        for _, row in df.iterrows():
            cols = ','.join(df.columns)
            placeholders = ','.join(['%s'] * len(df.columns))
            update_cols = ', '.join(
                [f"{col}=EXCLUDED.{col}" for col in df.columns if col != pk_column])
            sql = f"""
                INSERT INTO {table_name} ({cols}) VALUES ({placeholders})
                ON CONFLICT ({pk_column}) DO UPDATE SET {update_cols}
            """
            cursor.execute(sql, tuple(row))

        conn.commit()
        print(f"✅ נשמרו {len(df)} שורות לטבלה {table_name}")
        
           
    except Exception as e:
        print(f"❌ שגיאה בשמירה לטבלה {table_name}: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def filter_columns_for_table(df, table_name):
    table_columns = {
        'slack_messages_raw': ['id', 'channel_id', 'user_id', 'text', 'ts', 'thread_ts', 'raw', 'event_type', 'parent_id', 'is_list', 'list_items', 'num_list_items'],
        'alerts': ['id', 'user_id', 'type', 'message', 'severity', 'created_at'],
        'github_commits_raw': ['sha', 'author', 'message', 'timestamp', 'repository', 'url'],
        'github_issues_raw': ['id', 'user_id', 'title', 'body', 'state', 'created_at', 'closed_at', 'repository', 'url', 'is_critical'],
        'github_prs_raw': ['id', 'user_id', 'title', 'state', 'created_at', 'closed_at', 'merged_at', 'repository', 'url'],
        'github_reviews_raw': ['id', 'pull_request_id', 'user_id', 'state', 'body', 'created_at', 'url'],
        'slack_reports_raw': ['id', 'user_id', 'text', 'ts', 'channel_id', 'report_type', 'status'],
        'user_daily_summary': ['user_id', 'day', 'total_messages', 'help_requests', 'stuck_passive', 'stuck_active', 'resolved', 'completed_tasks', 'open_tasks', 'commits', 'reviews']
    }
    cols_to_keep = table_columns.get(table_name, [])
    return df.loc[:, df.columns.intersection(cols_to_keep)]


# מיפוי מפתח ראשי לכל טבלה
PRIMARY_KEYS = {
    'slack_messages_raw': 'id',
    'alerts': 'id',
    'github_commits_raw': 'sha',
    'github_issues_raw': 'id',
    'github_prs_raw': 'id',
    'github_reviews_raw': 'id',
    'slack_reports_raw': 'id',
    # Composite key in DB, כאן עשוי להיות צורך בהתאמה מיוחדת
    'user_daily_summary': 'user_id',
}
slack_message_columns = [
    "id",
    "event_type",
    "user_id",
    "channel_id",
    "text",
    "ts",
    "parent_id",
    "is_list",
    "list_items",
    "num_list_items",
    "raw"
]


def get_user_email(user_id):
    slack_token = os.getenv("api_token")
    if not slack_token:
        print("❌ לא נמצא api_token בקובץ .env")
        return None

    url = f"https://slack.com/api/users.info?user={user_id}"
    headers = {
        "Authorization": f"Bearer {slack_token}"
    }

    print(f"📡 שולחת בקשה ל-Slack עבור המשתמש: {user_id}")
    res = requests.get(url, headers=headers)

    try:
        data = res.json()
        print("📥 תגובה מה-API:", data)
    except Exception as e:
        print("⚠️ שגיאה בפיענוח JSON:", e)
        return None

    if not data.get("ok"):
        print(f"⚠️ Slack החזיר שגיאה: {data.get('error')}")
        return None

    if "user" not in data:
        print("⚠️ מפתח 'user' לא קיים בתגובה מ-Slack")
        return None

    profile = data["user"].get("profile", {})
    email = profile.get("email")

    if not email:
        print(f"ℹ️ לא נמצא אימייל עבור המשתמש {user_id}")
    else:
        print(f"✅ נמצא אימייל: {email} עבור המשתמש {user_id}")

    return email


@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json
    print("📥 Slack event received:")
    # print(json.dumps(data, indent=2))
    
    event = data.get("event", {})
    if (event.get("type") == "message" and 
        event.get("subtype") == "file_share" and 
        "files" in event):
        
        print("📋📎 התקבלה הודעת קובץ מסוג list (file_share)")
        
        url = event.get("files", [{}])[0].get("url_private_download")
        if not url:
            print("⚠️ לא נמצא URL להורדת הקובץ")
            return "", 400
        api_token = os.getenv("api_token")
        print(f"🔑 משתמשת ב־api_token: {api_token}")
        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }

        res = requests.get(url, headers=headers)
        csv_url = res.json()['list_csv_download_url']

        # Download the CSV file
        csv_res = requests.get(url=csv_url, headers=headers)
        csv_res.raise_for_status()
        csv_data = csv_res.content.decode('utf-8').splitlines()
        total_csv = [dict(zip(csv_data[0].split(','), line.split(',')))
                     for line in csv_data[1:]]
        print( total_csv)
        email = get_user_email(event.get("user"))

        df = pd.DataFrame([[
            event.get("client_msg_id") or event.get("ts"),
            "list",
            email,
            event.get("channel"),
            total_csv,
            float(event.get("ts", 0)),
            event.get("thread_ts") if event.get("thread_ts") != event.get("ts") else None,
            True,
            total_csv,
            event.get("files", [{}])[0].get("list_limits", {}).get("row_count", 0),
            True
        ]], columns=slack_message_columns)
        
        df_filtered = filter_columns_for_table(df, 'slack_messages_raw')
        save_dataframe_to_db(df_filtered, 'slack_messages_raw', PRIMARY_KEYS['slack_messages_raw'])
        print("📋 Slack list saved to DB")
        threading.Thread(
            agent_monitor,
            daemon=True,
        ).start()
        
        return "", 200
    
    
    # 🎯 הודעה מסוג message עם קובץ רשימה
    if event.get("type") == "message" and "files" in event:
        print("📎 we are clever")
        for f in event["files"]:
            filetype = f.get("filetype")
            if filetype == "list" and f.get("mode") == "list":
                load_dotenv()

                url = os.getenv("SLACK_FILE_URL")
                api_token = os.getenv("api_token")
                headers = {
                    'Authorization': f'Bearer {api_token}',
                    'Content-Type': 'application/json'
                }

                res = requests.get(url, headers=headers)
                csv_url = res.json()['list_csv_download_url']

                # הורדת קובץ ה־CSV
                csv_res = requests.get(url=csv_url, headers=headers)
                csv_res.raise_for_status()
                csv_data = csv_res.content.decode('utf-8').splitlines()

                total_csv = [
                    dict(zip(csv_data[0].split(','), line.split(',')))
                    for line in csv_data[1:]
                ]
                email = get_user_email(event.get("user"))
                df = pd.DataFrame([[
                    event.get("client_msg_id") or event.get("ts"),
                    "list",
                    email,
                    event.get("channel"),
                    total_csv,
                    float(event.get("ts", 0)),
                    event.get("thread_ts") if event.get(
                        "thread_ts") != event.get("ts") else None,
                    True,
                    total_csv,
                    f["list_limits"]["row_count"],
                    json.dumps(event)
                ]], columns=slack_message_columns)

                df_filtered = filter_columns_for_table(
                    df, 'slack_messages_raw')
                save_dataframe_to_db(
                    df_filtered, 'slack_messages_raw', PRIMARY_KEYS['slack_messages_raw'])

                print("📋 Slack list saved to DB")
                return "", 200

    # ✉️ הודעת טקסט רגילה (כולל בדיקת רשימות)
    if event.get("type") == "message":
        text = event.get("text", "")

        def extract_list_items(text):
            if not isinstance(text, str):
                return None
            lines = text.splitlines()
            items = []
            for line in lines:
                if line.strip().startswith(("* ", "- ", "• ")):
                    items.append(line[2:].strip())
            return items if items else None

        list_items = extract_list_items(text)
        is_list = bool(list_items)
        num_list_items = len(list_items) if list_items else 0
        email = get_user_email(event.get("user"))

        df = pd.DataFrame([[
            event.get("client_msg_id") or event.get("ts"),
            "message",
            email,
            event.get("channel"),
            text,
            float(event.get("ts", 0)),
            event.get("thread_ts") if event.get(
                "thread_ts") != event.get("ts") else None,
            is_list,
            list_items,
            num_list_items,
            json.dumps(event)
        ]], columns=slack_message_columns)

        df_filtered = filter_columns_for_table(df, 'slack_messages_raw')
        save_dataframe_to_db(df_filtered, 'slack_messages_raw',
                             PRIMARY_KEYS['slack_messages_raw'])

        print("📝 הודעת טקסט רגילה נשמרה למסד (כולל בדיקת רשימה)")
        return "", 200

    # חזרה על בדיקה של טקסט סניפט (ייתכן מיותר, אבל שמרתי כפי שביקשת)
    if event.get("type") == "message" and "files" in event:
        for f in event["files"]:
            if f.get("filetype") == "text" and f.get("mode") == "snippet":
                snippet_text = f.get("preview") or "[שגיאה בקריאת סניפט]"
                email = get_user_email(event.get("user"))
                df = pd.DataFrame([{
                    "id": event.get("client_msg_id") or event.get("ts") + "_snippet",
                    "event_type": "text_snippet",
                    "user_id": email,
                    "channel_id": event.get("channel"),
                    "text": snippet_text,
                    "ts": float(event.get("ts", 0)),
                    "parent_id": event.get("client_msg_id") or event.get("ts"),
                    "is_list": False,
                    "list_items": None,
                    "num_list_items": 0,
                    "raw": json.dumps(event)
                }])
                df_filtered = filter_columns_for_table(
                    df, 'slack_messages_raw')
                save_dataframe_to_db(
                    df_filtered, 'slack_messages_raw', PRIMARY_KEYS['slack_messages_raw'])
                print("📄 סניפט טקסט נשמר למסד")
                return "", 200

    if event.get("type") == "message" and event.get("subtype") == "message_deleted":
        deleted_message = event.get("previous_message", {})
        user_id = deleted_message.get("user")
        email = get_user_email(user_id) if user_id else None
        df = pd.DataFrame([{
            "id": event.get("event_ts"),
            "event_type": "message_deleted",
            "user_id": email,
            "channel_id": event.get("channel"),
            "text": deleted_message.get("text", "[לא נמצא טקסט]"),
            "ts": float(event.get("event_ts")),
            "parent_id": deleted_message.get("ts"),  # מזהה ההודעה שנמחקה
            "is_list": False,
            "list_items": None,
            "num_list_items": 0,
            "raw": json.dumps(event)
        }])

        df_filtered = filter_columns_for_table(df, 'slack_messages_raw')
        save_dataframe_to_db(df_filtered, 'slack_messages_raw',
                             PRIMARY_KEYS['slack_messages_raw'])

        print("🗑 הודעה שנמחקה נשמרה במסד")
        return "", 200

    if event.get("type") in ["reaction_added", "reaction_removed"]:
        item = event.get("item", {})
        email = get_user_email(event.get("user"))
        df = pd.DataFrame([{
            "id": event.get("event_ts"),  # מזהה ייחודי של האירוע (הריאקציה)
            "event_type": event.get("type"),
            "user_id": email,
            "channel_id": item.get("channel"),
            "parent_id": item.get("ts"),  # ההודעה שאליה נוספה הריאקציה
            "text": event.get("reaction"),  # שם הריאקציה (למשל 'thumbsup')
            "ts": float(event.get("event_ts", 0)),  # זמן האירוע עצמו
            "is_list": False,
            "list_items": None,
            "num_list_items": 0,
            "raw": json.dumps(event)
        }])

        # סינון עמודות מיותרות
        df_filtered = filter_columns_for_table(df, 'slack_messages_raw')
        df_filtered = df_filtered.sort_values(by="ts", ascending=True)

        save_dataframe_to_db(df_filtered, 'slack_messages_raw',
                             PRIMARY_KEYS['slack_messages_raw'])

        print(f"✅ Reaction ({event.get('type')}) נשמר למסד")
        return "", 200

    df = pd.json_normalize([event])

    if 'client_msg_id' in df.columns:
        df['id'] = df['client_msg_id'].astype(str)
    elif 'ts' in df.columns:
        df['id'] = df['ts'].astype(str)
    else:
        df['id'] = pd.util.hash_pandas_object(df).astype(str)

    df.rename(columns={
        'user': 'user_id',
        'channel': 'channel_id',
        'type': 'event_type'
    }, inplace=True)

    if 'ts' in df.columns:
        df['ts'] = pd.to_numeric(df['ts'], errors='coerce')

    df['raw'] = df.apply(lambda row: json.dumps(event), axis=1)

    def extract_list_items(text):
        if not isinstance(text, str):
            return None
        lines = text.splitlines()
        pattern = re.compile(r'^\s*[\*\-\•\d+\.]+\s*(.+)')
        items = [match.group(1).strip()
                 for line in lines if (match := pattern.match(line))]
        return items if items else None

    if 'text' in df.columns:
        df['list_items'] = df['text'].apply(extract_list_items)
        df['is_list'] = df['list_items'].apply(lambda x: bool(x))
        df['num_list_items'] = df['list_items'].apply(
            lambda x: len(x) if x else 0)
    else:
        df['list_items'] = None
        df['is_list'] = False
        df['num_list_items'] = 0

    df_filtered = filter_columns_for_table(df, 'slack_messages_raw')

    save_dataframe_to_db(df_filtered, 'slack_messages_raw',
                         PRIMARY_KEYS['slack_messages_raw'])
    print("✅ Slack message נשמר למסד")

    return "", 200



@app.route("/github/webhook", methods=["POST"])
def github_webhook():
    signature = request.headers.get('X-Hub-Signature-256')
    payload = request.data

    if not verify_signature(payload, signature):
        print("❌ חתימת webhook שגויה - דחה את הבקשה")
        abort(400, "Invalid signature")

    event_type = request.headers.get("X-GitHub-Event")
    data = request.json

    print(f"📢 GitHub event received: {event_type}")

    if event_type == "ping":
        print("✅ Received ping event from GitHub")
        return "", 200

    elif event_type == "pull_request":
        action = data.get("action", "")
        pr = data.get("pull_request")
        repository = data.get("repository", {})

        print(f"📦 פעולה על Pull Request: {action}")

        if not pr:
            print("⚠ אין מידע על pull_request באירוע - דילוג")
            return "", 200  # או return "", 400 אם אתה רוצה לתעד חריגה

        df = pd.json_normalize([pr])
        df['action'] = action

        if 'id' not in df.columns:
            df['id'] = df.get('number', [None])[0]
            if df['id'].isna().all():
                print("⚠ PR בלי id או number - דילוג")
                return "", 400

        df.rename(columns={
            'user.login': 'user_id',
            'repository.full_name': 'repository',
            'html_url': 'url'
        }, inplace=True)

        if 'repository' not in df.columns and 'full_name' in repository:
            df['repository'] = repository['full_name']

        for col in ['created_at', 'closed_at', 'merged_at']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

            df_filtered = filter_columns_for_table(df, 'github_prs_raw')
            save_dataframe_to_db(df_filtered, 'github_prs_raw',
                                 PRIMARY_KEYS['github_prs_raw'])

            print(f"💾 PR #{pr.get('number', '')} ({action}) נשמר/עודכן במסד")
            return "", 200

    elif event_type == "issues":
        action = data.get("action", "")
        issue = data.get("issue", {})
        repository = data.get("repository", {})

        print(f"📌 פעולה על Issue: {action}")

        if not issue:
            print("⚠ אין מידע על issue באירוע - דילוג")
            return "", 200

        df = pd.json_normalize([issue])
        df['action'] = action

        if 'id' not in df.columns:
            if 'number' in issue:
                df['id'] = str(issue['number'])
            else:
                print("⚠ Issue בלי id או number - דילוג")
                return "", 400

        df.rename(columns={
            'user.login': 'user_id',
            'repository.full_name': 'repository',
            'html_url': 'url'
        }, inplace=True)

        if 'repository' not in df.columns and 'full_name' in repository:
            df['repository'] = repository['full_name']

        df = df.loc[:, ~df.columns.duplicated()]

        for col in ['created_at', 'closed_at']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        df_filtered = filter_columns_for_table(df, 'github_issues_raw')
        save_dataframe_to_db(df_filtered, 'github_issues_raw',
                             PRIMARY_KEYS['github_issues_raw'])
        print(f"💾 Issue #{issue.get('number', '')} ({action}) נשמר למסד")
        return "", 200

    elif event_type == "push":
        commits = data.get("commits", [])
        repository = data.get("repository", {})
        if commits:
            df = pd.json_normalize(commits)

            df.rename(columns={
                'id': 'sha',
                'author.name': 'author',
                'message': 'message',
                'timestamp': 'timestamp'
            }, inplace=True)

            df['repository'] = repository.get('full_name', '')
            df['url'] = None

            df = df.loc[:, ~df.columns.duplicated()]

            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(
                    df['timestamp'], errors='coerce')

            df_filtered = filter_columns_for_table(df, 'github_commits_raw')
            save_dataframe_to_db(
                df_filtered, 'github_commits_raw', PRIMARY_KEYS['github_commits_raw'])
            print(f"💾 נשמרו {len(df_filtered)} קומיטים במסד")

    elif event_type == "pull_request_review":
        review = data.get("review")
        pr = data.get("pull_request", {})
        if review:
            df = pd.json_normalize([review])
            pr_id = pr.get('id', None)
            df['pull_request_id'] = str(pr_id) if pr_id is not None else None

            if 'id' not in df.columns:
                df['id'] = None

            df.rename(columns={
                'user.login': 'user_id',
                'state': 'state',
                'body': 'body',
                'created_at': 'created_at',
                'html_url': 'url'
            }, inplace=True)

            df = df.loc[:, ~df.columns.duplicated()]

            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(
                    df['created_at'], errors='coerce')

            df_filtered = filter_columns_for_table(df, 'github_reviews_raw')
            save_dataframe_to_db(
                df_filtered, 'github_reviews_raw', PRIMARY_KEYS['github_reviews_raw'])
            print(f"💾 Review #{review.get('id', '')} נשמר במסד")

    else:
        print(f"⚠ אירוע לא מטופל: {event_type}")

    return "", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    print(f"✅ הקובץ app.py התחיל לרוץ ב-port {port}")
    app.run(host="0.0.0.0", port=port)
