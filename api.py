from flask import Flask, request, jsonify, render_template
import psycopg2
import json
from datetime import datetime

app = Flask(__name__)

print(":white_check_mark: The app.py file started running")

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres.apphxbmngxlclxromyvt",
            password="insightbot2025",
            host="aws-0-eu-north-1.pooler.supabase.com",
            port="5432"
        )
        print(":large_green_circle: The connection to the database was successful")
        return conn
    except Exception as e:
        print(":x: Error connecting to the database:", e)
        raise

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/stats')
def stats():
    return render_template('stats.html')

@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json
    print(":inbox_tray: Received a request from Slack:", json.dumps(data, indent=2))

    if "challenge" in data:
        return data["challenge"], 200

    event = data.get("event", {})
    if event.get("type") == "message" and "subtype" not in event:
        try:
            save_to_db(event, data)
            print(":white_check_mark: Message saved to database successfully")
        except Exception as e:
            print(":x: Error saving message:", e)

    return "", 200

def save_to_db(event, full_payload):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO slack_messages_raw (event_id, channel_id, user_id, text, ts, thread_ts, raw)
        VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """, (
        event.get("ts"),  # משמש כ־event_id
        event.get("channel"),
        event.get("user"),
        event.get("text"),
        float(event.get("ts")),
        event.get("thread_ts"),
        json.dumps(full_payload)
    ))
    conn.commit()
    cur.close()
    conn.close()

@app.route('/api/slack_messages_raw')
def slack_messages_raw():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT event_id, channel_id, user_id, text, extract(epoch from ts) as ts_epoch, thread_ts
        FROM slack_messages_raw
        ORDER BY ts DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    data = []
    for row in rows:
        ts_val = ''
        if row[4]:
            try:
                ts_val = datetime.fromtimestamp(row[4]).isoformat()
            except Exception:
                ts_val = str(row[4])
        data.append({
            'event_id': row[0],
            'channel_id': row[1],
            'user_id': row[2],
            'text': row[3],
            'ts': ts_val,
            'thread_ts': row[5]
        })

    return jsonify(data)

if __name__ == "__main__":
    print(":rocket: Running the server in http://localhost:5000")
    app.run(port=5000, debug=True)
