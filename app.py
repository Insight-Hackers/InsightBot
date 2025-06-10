from flask import Flask, request, abort
import hmac
import hashlib
import json
import pandas as pd
import psycopg2
import os
import traceback

app = Flask(__name__)

# טען את ה־Secret מהמשתנה סביבתי (ללא hardcoding)
GITHUB_SECRET = os.getenv("GITHUB_SECRET")
if GITHUB_SECRET is None:
    raise RuntimeError("GITHUB_SECRET לא מוגדר בסביבת הריצה")
GITHUB_SECRET = GITHUB_SECRET.encode()  # המרה ל-bytes


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
    return hmac.compare_digest(mac.hexdigest(), signature)


def save_dataframe_to_db(df, table_name):
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
            update_cols = ', '.join(
                [f"{col}=EXCLUDED.{col}" for col in df.columns if col != 'id'])
            sql = f"""
                INSERT INTO {table_name} ({cols}) VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET {update_cols}
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


@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json
    print("📥 Slack event received:")
    print(json.dumps(data, indent=2))

    event = data.get("event", {})
    df = pd.json_normalize([event])

    if 'id' not in df.columns:
        if 'ts' in df.columns:
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

    df_filtered = filter_columns_for_table(df, 'slack_messages_raw')

    save_dataframe_to_db(df_filtered, 'slack_messages_raw')
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

    if event_type == "pull_request":
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
                'repository_url': 'repository',
                'html_url': 'url'
            }, inplace=True)

            for col in ['created_at', 'closed_at', 'merged_at']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            df_filtered = filter_columns_for_table(df, 'github_prs_raw')

            save_dataframe_to_db(df_filtered, 'github_prs_raw')
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
                'repository_url': 'repository',
                'html_url': 'url'
            }, inplace=True)

            for col in ['created_at', 'closed_at']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            df_filtered = filter_columns_for_table(df, 'github_issues_raw')

            save_dataframe_to_db(df_filtered, 'github_issues_raw')
            print(f"💾 Issue #{issue.get('number', '')} נשמר במסד")

    # הוסף טיפול לאירועים נוספים לפי הצורך

    return "", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    print(f"✅ הקובץ app.py התחיל לרוץ ב-port {port}")
    app.run(host="0.0.0.0", port=port)
