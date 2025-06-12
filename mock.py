import psycopg2
from datetime import datetime
import uuid


def gen_id():
    return str(uuid.uuid4())


# תאריך אחיד לתסריט
today = datetime(2025, 6, 10)

# ---------- חיבור ל-Supabase ----------


def get_db_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres.apphxbmngxlclxromyvt",
        password="insightbot2025",
        host="aws-0-eu-north-1.pooler.supabase.com",
        port="6543"
    )

# ---------- Slack messages (user_1 + user_2) ----------


def insert_slack_messages(conn):
    messages = [
        ("user_1", "מישהו יכול לעזור לי? אני נתקעתי בשלב האחרון"),
        ("user_1", "לא מצליח להריץ את הקוד, מה עושים?"),
        ("user_1", "שלב 3 ממש מוזר"),
        ("user_1", "יש רעיונות?"),
        ("user_1", "בדקתם את זה פעם?"),
        ("user_2", "תנסה לבדוק את השם של הקובץ"),
        ("user_2", "תנסה להוסיף print"),
        ("user_2", "נראה לי שזה קשור לגרסה של פייתון"),
        ("user_2", "זה קרה גם לי פעם")
    ]
    with conn.cursor() as cur:
        for user_id, text in messages:
            cur.execute("""
                INSERT INTO slack_messages_raw (id, channel_id, user_id, text, ts, thread_ts, raw, event_type, parent_id, is_list, list_items, num_list_items)
                VALUES (%s, 'dev', %s, %s, %s, NULL, '{}'::jsonb, 'message', NULL, FALSE, NULL, NULL)
            """, (gen_id(), user_id, text, int(today.timestamp())))
    conn.commit()

# ---------- GitHub commits (user_3) ----------


def insert_github_commits(conn):
    commits = [
        ("user_3", "Refactor DB class"),
        ("user_3", "Add tests"),
        ("user_3", "Fix bug in login"),
        ("user_3", "Improve logs")
    ]
    with conn.cursor() as cur:
        for author, message in commits:
            cur.execute("""
                INSERT INTO github_commits_raw (sha, author, message, timestamp, repository, url)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (gen_id(), author, message, today, "api-core", "https://example.com/" + gen_id()[:8]))
    conn.commit()

# ---------- GitHub issues (user_5) ----------


def insert_github_issues(conn):
    issues = [
        ("user_5", "Crash on login screen", "App crashes on mobile", True),
        ("user_5", "Spacing issue in navbar", "Padding missing on right", False)
    ]
    with conn.cursor() as cur:
        for user_id, title, body, is_critical in issues:
            cur.execute("""
                INSERT INTO github_issues_raw (id, user_id, title, body, state, created_at, closed_at, repository, url, is_critical, action)
                VALUES (%s, %s, %s, %s, 'open', %s, NULL, 'ui', %s, %s, 'opened')
            """, (
                gen_id(), user_id, title, body, today,
                "https://example.com/" + gen_id()[:8], is_critical
            ))
    conn.commit()

# ---------- GitHub PRs וביקורות ----------


def insert_github_prs_and_reviews(conn):
    pr_id_old = gen_id()
    pr_id_new = gen_id()
    with conn.cursor() as cur:
        # PR פתוח ישן (user_4)
        cur.execute("""
            INSERT INTO github_prs_raw (id, user_id, title, state, created_at, closed_at, merged_at, repository, url, event_action)
            VALUES (%s, %s, %s, 'open', %s, NULL, NULL, 'ui', %s, 'opened')
        """, (
            pr_id_old, "user_4", "Redesign UI", datetime(
                2025, 6, 7), "https://example.com/" + pr_id_old[:8]
        ))

        # PR חדש (user_6)
        cur.execute("""
            INSERT INTO github_prs_raw (id, user_id, title, state, created_at, closed_at, merged_at, repository, url, event_action)
            VALUES (%s, %s, %s, 'open', %s, NULL, NULL, 'ui', %s, 'opened')
        """, (
            pr_id_new, "user_6", "Fix CSS alignment", today, "https://example.com/" +
            pr_id_new[:8]
        ))

        # ביקורת שלילית על PR של user_6 מ־user_3
        cur.execute("""
            INSERT INTO github_reviews_raw (id, pull_request_id, user_id, state, body, created_at, url)
            VALUES (%s, %s, %s, 'changes_requested', %s, %s, %s)
        """, (
            gen_id(), pr_id_new, "user_3", "This breaks on mobile!", today, "https://example.com/review1"
        ))
    conn.commit()

# ---------- דיווח Slack (user_7) ----------


def insert_slack_report(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO slack_reports_raw (id, user_id, text, ts, channel_id, report_type, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            gen_id(), "user_7", "סיימתי את תיקון הבעיה בתצוגה", today,
            "dev", "progress", "done"
        ))
    conn.commit()

# ---------- הפעלת הכל ----------


def run_full_simulation():
    conn = get_db_connection()
    try:
        insert_slack_messages(conn)
        print("✅ Slack messages inserted.")
        insert_github_commits(conn)
        print("✅ GitHub commits inserted.")
        insert_github_issues(conn)
        print("✅ GitHub issues inserted.")
        insert_github_prs_and_reviews(conn)
        print("✅ GitHub PRs and reviews inserted.")
        insert_slack_report(conn)
        print("✅ Slack report inserted.")
    finally:
        conn.close()
        print("🔒 Connection closed.")


# ---- הרצה בפועל ----
if __name__ == "__main__":
    run_full_simulation()
