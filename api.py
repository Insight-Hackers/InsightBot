from flask import Flask, request, jsonify, render_template
import psycopg2
import json
from datetime import datetime, date  # תיקון: מוסיף גם date

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

@app.route('/commits')
def commits():
    return render_template('commits.html')


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


@app.route('/api/user_daily_summary')
def user_daily_summary_api():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
    SELECT
        user_id,
        day,
        total_messages,
        help_requests,
        stuck_passive,
        stuck_active,
        resolved,
        completed_tasks,
        open_tasks,
        commits,
        reviews
    FROM user_daily_summary
    ORDER BY day DESC, user_id
    LIMIT 50
""")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    data = []
    for row in rows:
        data.append({
            'user_id': row[0],
            'day': row[1].isoformat() if row[1] else None,
            'total_messages': row[2],
            'help_requests': row[3],
            'stuck_passive': row[4],
            'stuck_active': row[5],
            'resolved': row[6],
            'completed_tasks': row[7],
            'open_tasks': row[8],
            'commits': row[9],
            'reviews': row[10]
        })
    return jsonify(data)

@app.route('/api/github_commits_raw')
def github_commits_raw():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT sha, author, message, timestamp, repository, url
        FROM github_commits_raw
        ORDER BY timestamp DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    data = []
    for row in rows:
        data.append({
            'sha': row[0],
            'author': row[1],
            'message': row[2],
            'timestamp': row[3].isoformat() if row[3] else None,
            'repository': row[4],
            'url': row[5]
        })
    return jsonify(data)

@app.route('/api/alerts')
def alerts_api():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM alerts
        ORDER BY created_at DESC
        LIMIT 50
    """)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]  # שמות העמודות
    cur.close()
    conn.close()

    data = []
    for row in rows:
        item = {}
        for col, val in zip(colnames, row):
            if isinstance(val, (datetime, date)):  # תיקון כאן: בודק גם date
                item[col] = val.isoformat()
            else:
                item[col] = val
        data.append(item)

    return jsonify(data)


if __name__ == "__main__":
    print(":rocket: Running the server in http://localhost:5000")
    app.run(port=5000, debug=True)
