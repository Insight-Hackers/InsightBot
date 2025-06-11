from flask import Flask, request, abort
import hmac
import hashlib
import json
import pandas as pd
import psycopg2
import os
import traceback
import openai
from io import BytesIO
import requests
import threading




app = Flask(__name__)

GITHUB_SECRET = os.getenv("GITHUB_SECRET")
if GITHUB_SECRET is None:
    raise RuntimeError("GITHUB_SECRET לא מוגדר בסביבת הריצה")
GITHUB_SECRET = GITHUB_SECRET.encode()  # המרה ל-כbytes

# הוספה אם לא יעבוד נמחק
# openai.api_key = os.getenv("OPENAI_API_KEY")
# def handle_voice_message_in_background(event, audio_url):
#     transcription = transcribe_audio_from_url(audio_url)
#     if transcription is None:
#         transcription = "[שגיאה בתמלול]"

#     # הכנת רשומה חדשה עם תמלול
#     df = pd.DataFrame([{
#         "id": event.get("client_msg_id") or event.get("ts") + "_transcribed",  # מזהה ייחודי חדש
#         "event_type": "voice_message_transcribed",
#         "user_id": event.get("user"),
#         "channel_id": event.get("channel"),
#         "text": transcription,
#         "ts": float(event.get("ts", 0)),
#         "parent_id": event.get("client_msg_id") or event.get("ts"),
#         "is_list": False,
#         "list_items": None,
#         "num_list_items": 0,
#         "raw": json.dumps(event)
#     }])

#     df_filtered = filter_columns_for_table(df, 'slack_messages_raw')
#     save_dataframe_to_db(df_filtered, 'slack_messages_raw', PRIMARY_KEYS['slack_messages_raw'])
#     print("🗣️ תמלול רקע נוסף למסד כהודעה חדשה")

# def transcribe_audio_from_url(audio_url):
#     try:
#         headers = {'Authorization': f"Bearer {os.getenv('SLACK_BOT_TOKEN')}"}
#         response = requests.get(audio_url, headers=headers)
#         if response.status_code != 200:
#             print(f"❌ שגיאה בהורדת הקובץ הקולי: {response.status_code}")
#             return None

#         audio_file = BytesIO(response.content)
#         audio_file.name = "audio.mp3"

#         transcript = openai.Audio.transcribe("whisper-1", audio_file)
#         return transcript.get("text", "")

#     except Exception as e:
#         print("❌ שגיאה בתמלול:", e)
#         return None
 #עד פה

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
        print(f"⚠️ הטבלה {table_name} ריקה - לא נשמר כלום")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                # החלפת NaT ב-None
                df[column] = df[column].where(df[column].notna(), None)
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


@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json
    print("📥 Slack event received:")
    print(json.dumps(data, indent=2))

    event = data.get("event", {})
    # רשימה
    if event.get("type") == "message" and "subtype" not in event:
        # זהו מקרה של הודעה רגילה - נבדוק אם היא רשימה ונשמור למסד
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

        df = pd.DataFrame([{
            "id": event.get("client_msg_id") or event.get("ts"),
            "event_type": "message",
            "user_id": event.get("user"),
            "channel_id": event.get("channel"),
            "text": text,
            "ts": float(event.get("ts", 0)),
            "parent_id": event.get("thread_ts") if event.get("thread_ts") != event.get("ts") else None,
            "is_list": is_list,
            "list_items": list_items,
            "num_list_items": num_list_items,
            "raw": json.dumps(event)
        }])

        df_filtered = filter_columns_for_table(df, 'slack_messages_raw')
        save_dataframe_to_db(df_filtered, 'slack_messages_raw', PRIMARY_KEYS['slack_messages_raw'])
        print("📝 הודעת טקסט רגילה נשמרה למסד (כולל בדיקת רשימה)")
        return "", 200

    if event.get("type") == "message" and "files" in event:
     for f in event["files"]:
        if f.get("filetype") == "text" and f.get("mode") == "snippet":
            snippet_text = f.get("preview") or "[שגיאה בקריאת סניפט]"
            df = pd.DataFrame([{
                "id": event.get("client_msg_id") or event.get("ts") + "_snippet",
                "event_type": "text_snippet",
                "user_id": event.get("user"),
                "channel_id": event.get("channel"),
                "text": snippet_text,
                "ts": float(event.get("ts", 0)),
                "parent_id": event.get("client_msg_id") or event.get("ts"),
                "is_list": False,
                "list_items": None,
                "num_list_items": 0,
                "raw": json.dumps(event)
            }])
            df_filtered = filter_columns_for_table(df, 'slack_messages_raw')
            save_dataframe_to_db(df_filtered, 'slack_messages_raw', PRIMARY_KEYS['slack_messages_raw'])
            print("📄 סניפט טקסט נשמר למסד")
            return "", 200

    # הוספה שאולי נמחק
    # if event.get("type") == "message" and "files" in event:
    #   for f in event["files"]:
    #     if f.get("mimetype", "").startswith("audio/"):
    #         audio_url = f.get("url_private")

    #         # שמור הודעה ראשונית עם טקסט זמני [בתהליך תמלול]
    #         df = pd.DataFrame([{
    #             "id": event.get("client_msg_id") or event.get("ts"),
    #             "event_type": "voice_message",
    #             "user_id": event.get("user"),
    #             "channel_id": event.get("channel"),
    #             "text": "[בתהליך תמלול]",
    #             "ts": float(event.get("ts", 0)),
    #             "parent_id": None,
    #             "is_list": False,
    #             "list_items": None,
    #             "num_list_items": 0,
    #             "raw": json.dumps(event)
    #         }])
    #         df_filtered = filter_columns_for_table(df, 'slack_messages_raw')
    #         save_dataframe_to_db(df_filtered, 'slack_messages_raw', PRIMARY_KEYS['slack_messages_raw'])

    #         # הרץ את התמלול ברקע
    #         threading.Thread(
    #             target=handle_voice_message_in_background,
    #             args=(event, audio_url),
    #             daemon=True
    #         ).start()

    #         print("🎙️ תמלול קולית נשלח לרקע")
    #         return "", 200

        # עד פה 
        
    if event.get("type") == "message" and event.get("subtype") == "message_deleted":
        deleted_message = event.get("previous_message", {})

        df = pd.DataFrame([{
            "id": event.get("event_ts"),
            "event_type": "message_deleted",
            "user_id": deleted_message.get("user"),
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
        save_dataframe_to_db(df_filtered, 'slack_messages_raw', PRIMARY_KEYS['slack_messages_raw'])

        print("🗑️ הודעה שנמחקה נשמרה במסד")
        return "", 200

    if event.get("type") in ["reaction_added", "reaction_removed"]:
       item = event.get("item", {})

       df = pd.DataFrame([{
           "id": event.get("event_ts"),  # מזהה ייחודי של האירוע (הריאקציה)
           "event_type": event.get("type"),
           "user_id": event.get("user"),
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

       save_dataframe_to_db(df_filtered, 'slack_messages_raw', PRIMARY_KEYS['slack_messages_raw'])

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
        items = []
        for line in lines:
            if line.strip().startswith(("* ", "- ", "• ")):
                items.append(line[2:].strip())
        return items if items else None

    if 'text' in df.columns:
         df['list_items'] = df['text'].apply(extract_list_items)
         df['is_list'] = df['list_items'].apply(lambda x: bool(x))
         df['num_list_items'] = df['list_items'].apply(lambda x: len(x) if x else 0)
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
        pr = data.get("pull_request")
        if pr:
            df = pd.json_normalize([pr])

            if 'id' not in df.columns:
                if 'number' in df.columns:
                    df['id'] = df['number'].astype(str)
                else:
                    print("⚠️ PR בלי id או number - דילוג")
                    return "", 400

            df.rename(columns={
                'user.login': 'user_id',
                'repository.full_name': 'repository',
                'html_url': 'url'
            }, inplace=True)

            df = df.loc[:, ~df.columns.duplicated()]

            for col in ['created_at', 'closed_at', 'merged_at']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            df_filtered = filter_columns_for_table(df, 'github_prs_raw')
            save_dataframe_to_db(df_filtered, 'github_prs_raw',
                                 PRIMARY_KEYS['github_prs_raw'])
            print(f"💾 PR #{pr.get('number', '')} נשמר במסד")

    elif event_type == "issues":
        issue = data.get("issue")
        if issue:
            df = pd.json_normalize([issue])

            if 'id' not in df.columns:
                if 'number' in df.columns:
                    df['id'] = df['number'].astype(str)
                else:
                    print("⚠️ Issue בלי id או number - דילוג")
                    return "", 400

            df.rename(columns={
                'user.login': 'user_id',
                'repository.full_name': 'repository',
                'html_url': 'url'
            }, inplace=True)

            df = df.loc[:, ~df.columns.duplicated()]

            for col in ['created_at', 'closed_at']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            df_filtered = filter_columns_for_table(df, 'github_issues_raw')
            save_dataframe_to_db(
                df_filtered, 'github_issues_raw', PRIMARY_KEYS['github_issues_raw'])
            print(f"💾 Issue #{issue.get('number', '')} נשמר במסד")

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
                if 'id' in df.columns:
                    df['id'] = df['id'].astype(str)
                else:
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
        print(f"⚠️ אירוע לא מטופל: {event_type}")

    return "", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    print(f"✅ הקובץ app.py התחיל לרוץ ב-port {port}")
    app.run(host="0.0.0.0", port=port)
