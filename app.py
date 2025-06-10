from flask import Flask, request, abort
import pandas as pd
from flask import request, abort
import hashlib
import hmac
import os
from flask import Flask, request
import psycopg2
import json

app = Flask(__name__)
print("✅ הקובץ app.py התחיל לרוץ")

# ========================
# 🔌 התחברות למסד הנתונים
# ========================


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

# ========================
# 🌐 נקודת קצה ל־Slack Events
# ========================


@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json
    print("📥 התקבלה בקשה מ-Slack:")
    print(json.dumps(data, indent=2))

    if "challenge" in data:
        return data["challenge"], 200

    event = data.get("event", {})
    event_type = event.get("type")

    if event_type == "message" and "subtype" not in event:
        try:
            save_to_db(event, data)
            print("✅ הודעה נשמרה במסד בהצלחה")
        except Exception as e:
            print("❌ שגיאה בשמירת הודעה:", e)
            import traceback
            traceback.print_exc()

    elif event_type == "message" and event.get("subtype") == "message_deleted":
        try:
            save_deleted_message_to_db(event, data)
            print("🗑 הודעה שנמחקה נשמרה בהצלחה")
        except Exception as e:
            print("❌ שגיאה בשמירת הודעת מחיקה:", e)
            import traceback
            traceback.print_exc()

    elif event_type in ["reaction_added", "reaction_removed"]:
        try:
            save_to_db(event, data)
            print(f"✅ תגובה מסוג {event_type} נשמרה בהצלחה")
        except Exception as e:
            print(f"❌ שגיאה בשמירת תגובה ({event_type}):", e)
            import traceback
            traceback.print_exc()

    return "", 200

# ========================
# 💾 שמירה למסד
# ========================


def save_deleted_message_to_db(event, full_payload):
    conn = get_db_connection()
    cur = conn.cursor()

    # מזהה האירוע החדש
    event_id = event.get("event_ts")  # זה ה-id החדש לאירוע המחיקה
    ts = float(event_id) if event_id else None

    # פרטי ההודעה המקורית שנמחקה
    previous_message = event.get("previous_message", {})
    parent_id = previous_message.get("ts")
    user_id = previous_message.get("user")
    channel_id = event.get("channel")
    text = "[message deleted]"
    event_type = "deleted"

    is_list = False
    list_items = []
    num_list_items = 0

    print("🗑 מחיקת הודעה:")
    print("🔹 id (event):", event_id)
    print("🔹 parent_id (ts של ההודעה שנמחקה):", parent_id)

    if not event_id or not parent_id:
        print("⚠ event_id או parent_id חסרים – מדלג")
        cur.close()
        conn.close()
        return

    # בדיקה אם האירוע כבר נשמר
    cur.execute("SELECT 1 FROM slack_messages_raw WHERE id = %s", (event_id,))
    if cur.fetchone():
        print("⛔ אירוע מחיקה כבר קיים – לא מכניס שוב")
        cur.close()
        conn.close()
        return

    try:
        cur.execute("""
            INSERT INTO slack_messages_raw (
                id, channel_id, user_id, text, ts, thread_ts,
                raw, event_type, parent_id,
                is_list, list_items, num_list_items
            )
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (
            event_id,
            channel_id,
            user_id,
            text,
            ts,
            None,
            json.dumps(full_payload),
            event_type,
            parent_id,
            is_list,
            None,
            num_list_items
        ))

        conn.commit()
        print("✅ אירוע מחיקה נשמר עם commit")
    except Exception as e:
        print("❌ שגיאה בשמירת אירוע מחיקה:", e)
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def save_to_db(event, full_payload):
    conn = get_db_connection()
    cur = conn.cursor()

    event_type = event.get("type")
    is_reaction = event_type in ["reaction_added", "reaction_removed"]

    # זיהוי הודעה שהיא תגובה בשרשור
    if event_type == "message" and event.get("thread_ts") and event.get("thread_ts") != event.get("ts"):
        event_type = "concatenation"

    event_id = event.get("ts") or event.get("event_ts")
    if not event_id:
        print("⚠ event_id חסר, מדלג")
        return

    ts = float(event_id)
    parent_event_id = None
    text = event.get("text")

    if is_reaction:
        text = f":{event.get('reaction')}: by {event.get('user')}"
        parent_event_id = event.get("item", {}).get("ts")

    # ניתוח רשימה
    is_list = False
    list_items = []
    if text:
        lines = text.splitlines()
        for line in lines:
            if line.strip().startswith(("* ", "- ", "• ")):
                is_list = True
                list_items.append(line[2:].strip())
    num_list_items = len(list_items) if is_list else 0

    print("🧾 פרטי ההודעה:")
    print("🔹 id:", event_id)
    print("🔹 type:", event_type)
    print("🔹 text:", text[:50] if text else None)
    print("🔹 parent:", parent_event_id)
    print("🔹 is_list:", is_list, "| פריטים:", num_list_items)

    # בדיקה אם ההודעה כבר קיימת
    cur.execute("SELECT 1 FROM slack_messages_raw WHERE id = %s", (event_id,))
    if cur.fetchone():
        print("⛔ ההודעה כבר קיימת במסד – לא מכניס מחדש")
        cur.close()
        conn.close()
        return

    # הכנסת הנתונים לטבלה
    try:
        cur.execute("""
            INSERT INTO slack_messages_raw (
                id, channel_id, user_id, text, ts, thread_ts,
                raw, event_type, parent_id,
                is_list, list_items, num_list_items
            )
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (
            event_id,
            event.get("item", {}).get(
                "channel") if is_reaction else event.get("channel"),
            event.get("user"),
            text,
            ts,
            event.get("thread_ts") if not is_reaction else None,
            json.dumps(full_payload),
            event_type,
            parent_event_id,
            is_list,
            json.dumps(list_items) if list_items else None,
            num_list_items
        ))

        conn.commit()
        print("💾 commit בוצע")
    except Exception as e:
        print("❌ שגיאה בביצוע INSERT או commit:", e)
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cur.close()
        conn.close()


GITHUB_SECRET = b"YOUR_WEBHOOK_SECRET"  # מפתח סודי שתגדירי ב-GitHub ובקוד


def verify_signature(payload_body, signature_header):
    """בודק את חתימת ה־GitHub Webhook לפי ה־secret."""
    if signature_header is None:
        return False
    sha_name, signature = signature_header.split('=')
    if sha_name != 'sha256':
        return False
    mac = hmac.new(GITHUB_SECRET, msg=payload_body, digestmod=hashlib.sha256)
    return hmac.compare_digest(mac.hexdigest(), signature)


app = Flask(__name__)

# טען את ה־Secret מהמשתנה סביבתי (ללא hardcoding)
GITHUB_SECRET = os.getenv("GITHUB_SECRET")
if GITHUB_SECRET is None:
    raise RuntimeError("GITHUB_SECRET לא מוגדר בסביבת הריצה")
GITHUB_SECRET = GITHUB_SECRET.encode()  # המרה ל-bytes

# התחברות למסד


def get_db_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres.apphxbmngxlclxromyvt",
        password="insightbot2025",
        host="aws-0-eu-north-1.pooler.supabase.com",
        port="6543"
    )

# שמירת DataFrame למסד עם עדכון (upsert)


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
            # DO UPDATE for all columns except PK 'id'
            update_cols = ', '.join(
                [f"{col}=EXCLUDED.{col}" for col in df.columns if col != 'id'])
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {update_cols}"
            cursor.execute(sql, tuple(row))

        conn.commit()
        print(f"✅ נשמרו {len(df)} שורות לטבלה {table_name}")
    except Exception as e:
        print(f"❌ שגיאה בשמירה לטבלה {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# אימות חתימה


def verify_signature(payload_body, signature_header):
    if signature_header is None:
        return False
    sha_name, signature = signature_header.split('=')
    if sha_name != 'sha256':
        return False
    mac = hmac.new(GITHUB_SECRET, msg=payload_body, digestmod=hashlib.sha256)
    return hmac.compare_digest(mac.hexdigest(), signature)

# ה־endpoint לטיפול ב־GitHub Webhook


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

    # טיפול באירוע PR
    if event_type == "pull_request":
        pr = data.get("pull_request")
        if pr:
            df = pd.json_normalize([pr])
            df.rename(columns={
                'user.login': 'user_id',
                'repository_url': 'repository',
                'html_url': 'url'
            }, inplace=True)
            for col in ['created_at', 'closed_at', 'merged_at']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            save_dataframe_to_db(df, 'github_prs_raw')
            print(f"💾 PR #{pr['number']} נשמר במסד")

    # טיפול באירוע Issues
    elif event_type == "issues":
        issue = data.get("issue")
        if issue:
            df = pd.json_normalize([issue])
            df.rename(columns={
                'user.login': 'user_id',
                'repository_url': 'repository',
                'html_url': 'url'
            }, inplace=True)
            for col in ['created_at', 'closed_at']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            save_dataframe_to_db(df, 'github_issues_raw')
            print(f"💾 Issue #{issue['number']} נשמר במסד")

    # הוסיפי טיפול באירועים נוספים במידת הצורך

    return "", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))


# ========================
# 🚀 הרצת השרת
# ========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
