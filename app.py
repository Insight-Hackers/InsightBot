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
# 🌐 נקודת קצה ל־Slack Interact
# ========================

@app.route('/slack/interact', methods=['POST'])
def handle_interactive():
    payload = json.loads(request.form['payload'])

    event_type = payload.get("type")
    user_id = payload.get("user", {}).get("id")
    channel_id = None
    ts = None
    text = None
    parent_ts = None

    # הגדרת event בסיסי
    event = {
        "type": "interaction",  # שם כללי ל־event_type
        "user": user_id
    }

    # ⚙ סוגים שונים של אינטראקציות
    if event_type == "block_actions":
        action = payload["actions"][0]
        action_id = action["action_id"]
        value = action.get("value")
        channel_id = payload.get("channel", {}).get("id")
        ts = payload.get("message", {}).get("ts")
        text = f"[block action] {action_id} = {value}"
        parent_ts = ts

    elif event_type == "view_submission":
        view = payload["view"]
        callback_id = view.get("callback_id")
        state_values = view.get("state", {}).get("values", {})
        ts = payload.get("container", {}).get("message_ts") or view.get("id")
        channel_id = payload.get("view", {}).get("private_metadata")  # אפשר להעביר את channel דרך metadata
        text = f"[view submission] {callback_id} with values: {json.dumps(state_values)}"

    elif event_type == "view_closed":
        callback_id = payload.get("view", {}).get("callback_id")
        ts = payload.get("view", {}).get("id")
        channel_id = payload.get("view", {}).get("private_metadata")
        text = f"[view closed] {callback_id}"

    elif event_type == "message_action":
        action_ts = payload["message"].get("ts")
        channel_id = payload.get("channel", {}).get("id")
        message_text = payload["message"].get("text")
        ts = action_ts
        parent_ts = action_ts
        text = f"[message action] on: {message_text}"

    else:
        print("⚠ סוג אינטראקציה לא ידוע:", event_type)
        return '', 200

    # מילוי שדות באובייקט ה־event שנשלח ל־save_to_db
    event["channel"] = channel_id
    event["ts"] = ts
    event["text"] = text
    if parent_ts:
        event["thread_ts"] = parent_ts  # כדי שיתועד כהמשך הודעה

    try:
        save_to_db(event, payload)
        print("✅ אינטראקציה נשמרה במסד בהצלחה")
    except Exception as e:
        print("❌ שגיאה בשמירת אינטראקציה:", e)
        import traceback
        traceback.print_exc()

    return '', 200


def save_assignment_to_db(task_id, user_id, parent_message_ts):
    conn = psycopg2.connect(...)
    cur = conn.cursor()
    
    cur.execute("""
        UPDATE slack_checklist_tasks
        SET assigned_user_id = %s
        WHERE task_id = %s AND parent_message_ts = %s
    """, (user_id, task_id, parent_message_ts))

    conn.commit()
    cur.close()
    conn.close()

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
        # הודעה שנמחקה
        deleted_ts = event.get("previous_message", {}).get("ts")
        channel_id = event.get("channel")
        user_id = event.get("previous_message", {}).get("user")

        deleted_event = {
            "type": "message_removed",
            "ts": deleted_ts,
            "channel": channel_id,
            "user": user_id,
            "text": "[message removed]"
        }

        try:
            save_to_db(deleted_event, data)
            print("🗑 הודעה שנמחקה נשמרה במסד")
        except Exception as e:
            print("❌ שגיאה בשמירת הודעה שנמחקה:", e)
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

    if not text and event_type == "interaction":
        text = "[interaction]"

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
            event.get("item", {}).get("channel") if is_reaction else event.get("channel"),
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


# ========================
# 🚀 הרצת השרת
# ========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)