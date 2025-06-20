from datetime import timezone
import pandas as pd
import re
from datetime import datetime
from functools import reduce
from tabulate import tabulate
import uuid
import psycopg2
from thefuzz import fuzz
from datetime import date
import time

import os

LAST_PROCESSED_FILE = "last_processed.txt"

SLACK_TO_GIT_USERNAME_MAP = {
    "efrat.wilinger@gmail.com": "EfratWilinger",
    "yafit3278@gmail.com": "YafitCohen3278",
    "aditoubin@gmail.com": "AdiToubin",
    "avitalhoyzer@gmail.com": "AvitalHoyzer",
    "meitav.bin@gmail.com": "meitav1",
    "eszilber29@gmail.com": "EtiZilberlicht",
    "ayala62005@gmail.com": "AyalaTrachtman",
    "y7697086@gmail.com": "yaelshneor2004"
}


def get_canonical_username(slack_email: str = None, git_username: str = None) -> str:
    if slack_email and slack_email in SLACK_TO_GIT_USERNAME_MAP:
        return SLACK_TO_GIT_USERNAME_MAP[slack_email]
    if git_username and git_username in SLACK_TO_GIT_USERNAME_MAP.values():
        return git_username
    return None


def add_canonical_user_column(df: pd.DataFrame, slack_col: str = "user_id", git_col: str = "author") -> pd.DataFrame:
    def map_user(row):
        return get_canonical_username(row.get(slack_col), row.get(git_col))
    df["canonical_username"] = df.apply(map_user, axis=1)
    return df


def load_filtered_github_commits():
    df = load_github_commits()
    last_ts = get_last_processed_time("github_commits_raw")
    if last_ts:
        # הפוך את last_ts ל־UTC אם צריך
        if last_ts.tzinfo is None:
            last_ts = pd.Timestamp(last_ts).tz_localize("UTC")

        df['ts_dt'] = pd.to_datetime(df['timestamp'], utc=True)
        df = df[df['ts_dt'] > last_ts].copy()
        df = df.drop(columns=['ts_dt'])
        print(f"🧹 סוננו קומיטים לפני {last_ts} - נותרו {len(df)}")
    return df


def load_filtered_github_issues():
    df = load_github_issues()
    last_ts = get_last_processed_time("github_issues_raw")
    if last_ts:
        if last_ts.tzinfo is None:
            last_ts = pd.Timestamp(last_ts)
            if last_ts.tzinfo is None:
                last_ts = last_ts.tz_localize("UTC")

        df['ts_dt'] = pd.to_datetime(df['created_at'], utc=True)
        df = df[df['ts_dt'] > last_ts].copy()
        df = df.drop(columns=['ts_dt'])
        print(f"🧹 סוננו Issues לפני {last_ts} - נותרו {len(df)}")

    return df


def load_filtered_github_reviews():
    df = load_github_reviews()
    last_ts = get_last_processed_time("github_reviews_raw")
    if last_ts:
        if last_ts.tzinfo is None:
            last_ts = pd.Timestamp(last_ts)
            if last_ts.tzinfo is None:
                last_ts = last_ts.tz_localize("UTC")

        df['ts_dt'] = pd.to_datetime(df['created_at'], utc=True)
        df = df[df['ts_dt'] > last_ts].copy()
        df = df.drop(columns=['ts_dt'])
        print(f"🧹 סוננו Reviews לפני {last_ts} - נותרו {len(df)}")
    return df


def load_filtered_github_prs():
    df = load_github_prs()
    last_ts = get_last_processed_time("github_prs_raw")

    if last_ts is not None:
        last_ts = pd.Timestamp(last_ts)
        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize("UTC")

        df['ts_dt'] = pd.to_datetime(df['created_at'], utc=True)
        df = df[df['ts_dt'] > last_ts].copy()
        df = df.drop(columns=['ts_dt'])
        print(f"🧹 סוננו PRs לפני {last_ts} - נותרו {len(df)}")

    return df


# --- פונקציות חיבורים לדאטא בייס ---


def update_last_processed_time(table_name, last_time):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO agent_progress (table_name, last_processed_at)
            VALUES (%s, %s)
            ON CONFLICT (table_name)
            DO UPDATE SET last_processed_at = EXCLUDED.last_processed_at
        """, (table_name, last_time))
        conn.commit()
    conn.close()


def get_last_processed_time(table_name):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_processed_at FROM agent_progress WHERE table_name = %s", (table_name,))
        result = cur.fetchone()
    conn.close()
    return result[0] if result else None


def get_db_connection():
    """מקים חיבור למסד הנתונים של Supabase."""
    return psycopg2.connect(
        dbname="postgres",
        user="postgres.apphxbmngxlclxromyvt",
        password="insightbot2025",
        host="aws-0-eu-north-1.pooler.supabase.com",
        port="6543"
    )

# --- פונקציות טעינת נתונים גולמיים מ-Supabase ---


def load_slack_messages():
    conn = get_db_connection()
    query = "SELECT * FROM slack_messages_raw"
    try:
        df = pd.read_sql(query, conn)
        if 'user' in df.columns and 'canonical_username' not in df.columns:
            df = df.rename(columns={'user': 'canonical_username'})
        df = normalize_user_ids(df)
        return df
    finally:
        conn.close()


def load_filtered_slack_messages():
    """טוען הודעות Slack מסוננות לפי תאריך אחרון שטופל מהטבלה agent_progress."""
    df = load_slack_messages()
    last_ts = get_last_processed_time("slack_messages_raw")

    if last_ts:
        df['ts_dt'] = pd.to_datetime(df['ts'], unit='s', utc=True)
        last_ts = pd.Timestamp(last_ts)
        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize("UTC")

            print(f"🧹 סוננו הודעות לפני {last_ts} - נותרו {len(df)}")
            df = df.drop(columns=['ts_dt'])

    # סינון הודעות שנמחקו
    if 'deleted' in df.columns:
        before = len(df)
        df = df[df['deleted'] != True].copy()
        print(f"🗑 סוננו {before - len(df)} הודעות שנמחקו")
        replies_df = add_canonical_user_column(replies_df, slack_col="user_id")
        slack_reports_df = add_canonical_user_column(
            slack_reports_df, slack_col="user_id")

    return df


def load_slack_reports():
    conn = get_db_connection()
    query = "SELECT * FROM slack_reports_raw"
    try:
        df = pd.read_sql(query, conn)
        df = normalize_user_ids(df)
        if df.empty:
            return pd.DataFrame(columns=['id', 'canonical_username', 'text', 'ts', 'channel_id', 'report_type', 'status'])
        return df
    finally:
        conn.close()


def load_github_issues():
    """טוען גיליונות (issues) מ-GitHub מטבלת github_issues_raw."""
    conn = get_db_connection()
    query = "SELECT * FROM github_issues_raw"
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            # הגדר עמודות צפויות עבור DataFrame ריק
            return pd.DataFrame(columns=['id', 'canonical_username', 'title', 'body', 'state', 'created_at', 'closed_at', 'repository', 'url', 'is_critical'])
        return df
    finally:
        conn.close()


def load_github_commits():
    """טוען קומיטים מ-GitHub מטבלת github_commits_raw."""
    conn = get_db_connection()
    query = "SELECT * FROM github_commits_raw"
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            # הגדר עמודות צפויות עבור DataFrame ריק
            return pd.DataFrame(columns=['sha', 'author', 'message', 'timestamp', 'repository', 'url'])
        # ודא שעמודת 'author' משונה ל-'user_id' אם יש צורך
        if 'author' in df.columns and 'canonical_username' not in df.columns:
            df = df.rename(columns={'author': 'canonical_username'})
        return df
    finally:
        conn.close()


def analyze_pull_requests(github_prs_df):
    if github_prs_df.empty:
        return pd.DataFrame(columns=['canonical_username', 'date', 'pull_requests'])
    github_prs_df['date'] = pd.to_datetime(github_prs_df['created_at']).dt.date
    return github_prs_df.groupby(['canonical_username', 'date']).size().reset_index(name='pull_requests')


def load_github_reviews():
    """טוען ביקורות (reviews) מ-GitHub מטבלת github_reviews_raw."""
    conn = get_db_connection()
    query = "SELECT * FROM github_reviews_raw"
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            # הגדר עמודות צפויות עבור DataFrame ריק
            return pd.DataFrame(columns=['id', 'pull_request_id', 'canonical_username', 'state', 'body', 'created_at', 'url'])
        return df
    finally:
        conn.close()


def load_github_prs():
    """טוען בקשות משיכה (pull requests) מ-GitHub מטבלת github_prs_raw."""
    conn = get_db_connection()
    query = "SELECT * FROM github_prs_raw"
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            # הגדר עמודות צפויות עבור DataFrame ריק
            return pd.DataFrame(columns=['id', 'canonical_username', 'title', 'state', 'created_at', 'closed_at', 'merged_at', 'repository', 'url'])
        return df
    finally:
        conn.close()

# --- פונקציות אנליזה ---


def analyze_total_messages(slack_df):
    """מנתח את סך ההודעות שנשלחו על ידי כל משתמש ביום."""
    slack_df['date'] = pd.to_datetime(slack_df['ts'], unit='s').dt.date
    return slack_df.groupby(['canonical_username', 'date']).size().reset_index(name='total_messages')


def normalize_user_ids(df):
    """אם יש עמודת user – שנה את שמה ל־user_id"""
    if 'user' in df.columns and 'canonical_username' not in df.columns:
        df = df.rename(columns={'user': 'canonical_username'})
    return df


def analyze_help_requests(slack_df):
    """מזהה ומנתח בקשות עזרה מהודעות Slack, כולל זיהוי שגיאות כתיב."""
    help_keywords = [
        "עזרה", "בעיה", "שאלה", "לא מצליח", "נתקע", "תקוע",
        "איזה שלב", "איך ממשיכים", "מה עושים", "מישהו יכול לעזור",
        "לא עובד", "משהו לא תקין", "צריך עזרה", "איך ממשיכים",
        "איך מתקדם", "מה השלב הבא", "מה לעשות", "מה הבעיה",
        "help", "stuck", "issue", "problem", "need help", "can't", "error",
        "🆘", "❓", "🙋‍♀"
    ]

    # שלב ראשון: נזהה הודעות שמכילות ביטוי רגיל
    regex_pattern = '|'.join(map(re.escape, help_keywords))
    basic_matches = slack_df['text'].str.contains(
        regex_pattern, case=False, na=False)

    # שלב שני: נזהה הודעות עם שגיאות כתיב – לפי fuzzy match
    def fuzzy_contains_help(text):
        if not isinstance(text, str):
            return False
        words = text.split()
        for word in words:
            for keyword in help_keywords:
                if fuzz.partial_ratio(word.lower(), keyword.lower()) >= 85:
                    return True
        return False

    fuzzy_matches = slack_df['text'].apply(fuzzy_contains_help)

    # שילוב שני המסלולים
    help_msgs = slack_df[basic_matches | fuzzy_matches].copy()
    help_msgs['type'] = 'help_request'
    help_msgs['date'] = pd.to_datetime(help_msgs['ts'], unit='s').dt.date

    return help_msgs


def analyze_help_requests_count(slack_df):
    """סופר את מספר בקשות העזרה לכל משתמש ביום."""
    help_df = analyze_help_requests(slack_df)
    return help_df.groupby(['canonical_username', 'date']).size().reset_index(name='help_requests')


def analyze_message_replies(messages_df, replies_df, slack_reports_df, github_issues_df):
    """מנתח תגובות להודעות ומנסה לקבוע סטטוס פתרון."""
    if 'parent_id' in replies_df.columns and not replies_df.empty:
        replies_count = replies_df.groupby(
            'parent_id').size().reset_index(name='num_replies')
    else:
        replies_count = pd.DataFrame(columns=['parent_id', 'num_replies'])

    messages = messages_df.merge(
        replies_count, how='left', left_on='id', right_on='parent_id')
    messages['num_replies'] = messages['num_replies'].fillna(0)

    def is_resolved(row):
        text = str(row['text']).lower()  # ודא שזה מחרוזת
        resolved_keywords = ['תודה', 'הסתדרתי', 'נפתר', 'works']
        if any(k in text for k in resolved_keywords):
            return True

        # בדיקה מול slack_reports_df
        # ודא ש-slack_reports_df לא ריק ושיש בו את העמודות הנדרשות
        if not slack_reports_df.empty and all(col in slack_reports_df.columns for col in ['canonical_username', 'ts', 'text']):
            user_reports = slack_reports_df[
                (slack_reports_df['canonical_username'] == row['canonical_username']) &
                (pd.to_datetime(
                    slack_reports_df['ts'], unit='s').dt.date == row['date'])
            ]
            if not user_reports.empty and any(k in user_reports['text'].str.lower().str.cat(sep=' ') for k in ['הבעיה נפתרה', 'טופל', 'נפתרה']):
                return True

        # בדיקה מול github_issues_df
        # ודא ש-github_issues_df לא ריק ושיש בו את העמודות הנדרשות
        if not github_issues_df.empty and all(col in github_issues_df.columns for col in ['canonical_username', 'created_at', 'closed_at', 'state']):
            issue_matches = github_issues_df[
                (github_issues_df['canonical_username'] == row['canonical_username']) &
                (pd.to_datetime(github_issues_df['created_at']).dt.date <= row['date']) &
                ((pd.to_datetime(github_issues_df['closed_at'], errors='coerce').dt.date == row['date']) |
                 (github_issues_df['state'] == 'closed'))
            ]
            if not issue_matches.empty:
                return True
        return False

    def classify(row):
        if row['num_replies'] == 0:
            return 'open'
        elif is_resolved(row):
            return 'resolved'
        return 'needs_attention'

    # ודא ש-messages_df מכיל את העמודות הנדרשות לפני ה-apply
    if not messages_df.empty and 'text' in messages_df.columns and 'canonical_username' in messages_df.columns and 'date' in messages_df.columns:
        messages['status'] = messages.apply(classify, axis=1)
    else:
        # אם messages_df ריק או חסרות עמודות, צור עמודת 'status' ריקה
        messages['status'] = None  # או 'unknown' או ערך אחר שמתאים לכם

    messages['date'] = pd.to_datetime(messages['ts'], unit='s').dt.date
    return messages[['id', 'canonical_username', 'text', 'num_replies', 'status', 'date']]


def analyze_stuck_status(slack_df, replies_df, slack_reports_df, github_issues_df):
    """מנתח את סטטוס המשתמשים ('תקועים', 'פעילים', 'נפתרו')."""
    help_df = analyze_help_requests(slack_df)
    replies_analysis = analyze_message_replies(
        help_df, replies_df, slack_reports_df, github_issues_df)

    # ודא ש-replies_analysis מכיל את העמודות הנדרשות
    if replies_analysis.empty or not all(col in replies_analysis.columns for col in ['id', 'status']):
        return pd.DataFrame(columns=['canonical_username', 'date', 'stuck_passive', 'stuck_active', 'resolved'])

    merged = help_df[['id', 'canonical_username', 'date']].merge(
        replies_analysis[['id', 'status']], on='id')

    # ודא ש-merged לא ריק לפני pivot_table
    if merged.empty:
        return pd.DataFrame(columns=['canonical_username', 'date', 'stuck_passive', 'stuck_active', 'resolved'])

    summary = merged.pivot_table(
        index=['canonical_username', 'date'], columns='status', aggfunc='size', fill_value=0).reset_index()
    return summary.rename(columns={
        'open': 'stuck_passive',
        'needs_attention': 'stuck_active',
        'resolved': 'resolved'
    })


def analyze_completed_tasks(github_issues_df):
    """מנתח משימות GitHub שהושלמו."""
    if github_issues_df.empty:
        return pd.DataFrame(columns=['canonical_username', 'date', 'completed_tasks'])

    github_issues_df['date'] = pd.to_datetime(
        github_issues_df['closed_at'], errors='coerce').dt.date
    filtered = github_issues_df[(
        github_issues_df['state'] == 'closed') & github_issues_df['date'].notna()]
    return filtered.groupby(['canonical_username', 'date']).size().reset_index(name='completed_tasks')


def analyze_open_tasks(github_issues_df):
    """מנתח משימות GitHub פתוחות."""
    if github_issues_df.empty:
        return pd.DataFrame(columns=['canonical_username', 'date', 'open_tasks'])

    github_issues_df['date'] = pd.to_datetime(
        github_issues_df['created_at']).dt.date
    open_issues = github_issues_df[github_issues_df['state'] == 'open']
    return open_issues.groupby(['canonical_username', 'date']).size().reset_index(name='open_tasks')


def analyze_commits(github_commits_df):
    """מנתח קומיטים של GitHub."""
    if github_commits_df.empty:
        return pd.DataFrame(columns=['canonical_username', 'date', 'commits'])

    github_commits_df['date'] = pd.to_datetime(
        github_commits_df['timestamp']).dt.date
    # ודא שהעמודה 'author' קיימת לפני ה-groupby
    if 'author' not in github_commits_df.columns:
        # אם 'author' לא קיימת, כבר שינית אותה ל-user_id ב-load_github_commits, או שהיא פשוט חסרה
        # במקרה כזה נחזיר DataFrame ריק עם העמודות הצפויות
        return pd.DataFrame(columns=['canonical_username', 'date', 'commits'])

    return github_commits_df.groupby(['author', 'date']).size().reset_index(name='commits').rename(columns={'author': 'canonical_username'})


def analyze_reviews(github_reviews_df):
    """מנתח ביקורות קוד של GitHub."""
    if github_reviews_df.empty:
        return pd.DataFrame(columns=['canonical_username', 'date', 'reviews'])

    github_reviews_df['date'] = pd.to_datetime(
        github_reviews_df['created_at']).dt.date
    return github_reviews_df.groupby(['canonical_username', 'date']).size().reset_index(name='reviews')

# --- מיזוג תוצאות הניתוח לטבלה אחת (user_daily_summary) ---


def build_user_daily_summary(slack_df, replies_df, slack_reports_df,
                             github_commits_df, github_reviews_df,
                             github_issues_df, github_prs_df):
    """בנאי סיכום יומי עבור כל משתמש על בסיס כל הנתונים המנותחים, כולל PR נפרד."""
    dfs = [
        analyze_total_messages(slack_df),
        analyze_help_requests_count(slack_df),
        analyze_stuck_status(slack_df, replies_df,
                             slack_reports_df, github_issues_df),
        analyze_completed_tasks(github_issues_df),
        analyze_open_tasks(github_issues_df),
        analyze_commits(github_commits_df),
        analyze_reviews(github_reviews_df),
        analyze_pull_requests(github_prs_df)  # ✅ נוספה העמודה pull_requests
    ]

    # מיזוג כל הטבלאות לפי user_id + date
    user_summary_df = reduce(
        lambda left, right: pd.merge(
            left, right, on=['canonical_username', 'date'], how='outer'),
        dfs
    ).fillna(0)

    # המרת עמודות מספריות ל־int
    for col in user_summary_df.columns:
        if col not in ['canonical_username', 'date']:
            user_summary_df[col] = user_summary_df[col].astype(int)

    # שינוי שם 'date' ל־'day'
    user_summary_df = user_summary_df.rename(columns={'date': 'day'})

    return user_summary_df


# --- יצירת טבלת project_status_daily ---


def build_project_status_daily(github_prs_df, github_issues_df, user_summary_df):
    """בונה טבלת סיכום יומית של סטטוס הפרויקט לפי PRs, Issues ופעילות משתמשים."""

    # --- הוספת עמודות תאריך במידת הצורך ---
    def ensure_day_column(df, source_col, target_col='day'):
        if not df.empty and source_col in df.columns:
            if target_col not in df.columns:
                df[target_col] = pd.to_datetime(
                    df[source_col], errors='coerce').dt.date
        else:
            df[target_col] = pd.Series(dtype='datetime64[ns]')

    ensure_day_column(github_prs_df, 'created_at', 'day')
    ensure_day_column(github_prs_df, 'closed_at', 'closed_day')
    ensure_day_column(github_issues_df, 'created_at', 'created_day')
    ensure_day_column(github_issues_df, 'closed_at', 'closed_day')

    # --- איסוף כל הימים האפשריים מכל מקורות הנתונים ---
    days = set()
    for df, col in [
        (github_prs_df, 'day'),
        (github_prs_df, 'closed_day'),
        (github_issues_df, 'created_day'),
        (github_issues_df, 'closed_day'),
        (user_summary_df, 'day')
    ]:
        if not df.empty and col in df.columns:
            days.update(df[col].dropna().unique())

    if not days:
        return pd.DataFrame(columns=[
            'day', 'open_prs', 'stale_prs', 'closed_prs',
            'critical_issues', 'active_contributors'
        ])

    # --- חישוב פר יום ---
    results = []
    for day in sorted(days):
        prs_on_day = github_prs_df[
            github_prs_df['day'] == day
        ] if 'day' in github_prs_df.columns else pd.DataFrame()

        prs_closed_on_day = github_prs_df[
            github_prs_df['closed_day'] == day
        ] if 'closed_day' in github_prs_df.columns else pd.DataFrame()

        stale_threshold = pd.Timestamp(day, tz="UTC") - pd.Timedelta(days=3)
        stale_prs = github_prs_df[
            (github_prs_df.get('state') == 'open') &
            (pd.to_datetime(github_prs_df.get('created_at'),
             errors='coerce') < stale_threshold)
        ] if not github_prs_df.empty else pd.DataFrame()

        critical_issues = github_issues_df[
            (github_issues_df.get('created_day') == day) &
            (github_issues_df.get('is_critical') == True) &
            (github_issues_df.get('state') == 'open')
        ] if not github_issues_df.empty else pd.DataFrame()

        active_users = user_summary_df[
            user_summary_df.get('day') == day
        ]['canonical_username'].nunique() if 'day' in user_summary_df.columns else 0

        results.append({
            'day': day,
            'open_prs': prs_on_day[prs_on_day.get('state') == 'open'].shape[0],
            'closed_prs': prs_closed_on_day[prs_closed_on_day.get('state') == 'closed'].shape[0],
            'stale_prs': stale_prs.shape[0],
            'critical_issues': critical_issues.shape[0],
            'active_contributors': active_users
        })

    return pd.DataFrame(results)

# --- יצירת טבלת alerts ---


def build_alerts(user_summary_df):
    """בנאי התראות על בסיס סיכום המשתמשים היומי."""
    alerts = []
    # ודא ש-user_summary_df לא ריק ושיש בו את העמודות הנדרשות
    if user_summary_df.empty:
        return pd.DataFrame(columns=['id', 'canonical_username', 'type', 'message', 'severity', 'created_at'])

    for _, row in user_summary_df.iterrows():
        # בדיקות עם .get() כדי למנוע KeyError אם עמודה חסרה מאיזושהי סיבה
        if row.get('stuck_passive', 0) > 0:
            alerts.append({
                'id': str(uuid.uuid4()),
                'user_id': row['canonical_username'],
                'type': 'stuck_passive',
                'message': f"{row['canonical_username']} לא התקדם במשימה במשך זמן מה.",
                'severity': 'medium',
                'created_at': row['day']
            })
        if row.get('help_requests', 0) > 0 and row.get('resolved', 0) == 0:
            alerts.append({
                'id': str(uuid.uuid4()),
                'user_id': row['canonical_username'],
                'type': 'unanswered_help',
                'message': f"{row['canonical_username']} ביקש עזרה אך לא קיבל מענה.",
                'severity': 'high',
                'created_at': row['day']
            })
        # אם כלל העמודות הללו הן 0, אז יש חוסר פעילות
        # הוספתי גם משימות
        if all(row.get(col, 0) == 0 for col in ['total_messages', 'commits', 'reviews', 'completed_tasks', 'open_tasks']):
            alerts.append({
                'id': str(uuid.uuid4()),
                'user_id': row['canonical_username'],
                'type': 'inactivity',
                'message': f"{row['canonical_username']} לא היה פעיל כלל ביום {row['day']}.",
                'severity': 'low',
                'created_at': row['day']
            })

    return pd.DataFrame(alerts)

# --- פונקציית שמירה למסד הנתונים ---


def save_dataframe_to_db(df, table_name, conflict_columns=None):
    """שומר DataFrame לטבלה במסד הנתונים של Supabase, כולל עדכון במקרה של CONFLICT."""
    if df.empty:
        print(f"⚠ הטבלה {table_name} ריקה - לא נשמר כלום")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # המרת סוגי נתונים
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                df[column] = df[column].dt.to_pydatetime()
            elif pd.api.types.is_object_dtype(df[column]):
                df[column] = df[column].astype(str)

        for _, row in df.iterrows():
            cols = ','.join(df.columns)
            placeholders = ','.join(['%s'] * len(df.columns))
            values = tuple(row)

            if conflict_columns:
                conflict_clause = ', '.join(conflict_columns)
                update_clause = ', '.join([
                    f"{col} = EXCLUDED.{col}"
                    for col in df.columns if col not in conflict_columns
                ])
                sql = f"""
                INSERT INTO {table_name} ({cols})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}
                """
            else:
                sql = f"""
                INSERT INTO {table_name} ({cols})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
                """

            cursor.execute(sql, values)

        conn.commit()
        print(f"✅ נשמרו {len(df)} שורות לטבלה {table_name}")

    except Exception as e:
        print(f"❌ שגיאה בשמירה לטבלה {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# def load_github_commits():
    # conn = get_db_connection()
    # df = pd.read_sql("SELECT * FROM github_commits_raw", conn)
    # conn.close()
    # return df


def build_alerts_v2(user_summary_df, github_prs_df, github_reviews_df, github_issues_df):
    """יוצר התראות חכמות ובעלות ערך עסקי מתוך כלל הנתונים הזמינים."""
    alerts = []
    today = datetime.now(timezone.utc).date()

    # ✅ תיקון: לשם אחידות משתמשים ב־user_id (העמודה תועתק מ-canonical_username)
    user_summary_df = user_summary_df.rename(
        columns={"canonical_username": "user_id"})

    # 1. PR פתוח מעל 3 ימים וללא ביקורת
    if not github_prs_df.empty:
        github_prs_df['created_date'] = pd.to_datetime(
            github_prs_df['created_at'], errors='coerce')

        github_prs_df['days_open'] = (
            today - github_prs_df['created_date']).dt.days
        open_prs = github_prs_df[(github_prs_df['state'] == 'open') & (
            github_prs_df['days_open'] > 3)]
        reviewed_pr_ids = set(
            github_reviews_df['pull_request_id']) if not github_reviews_df.empty else set()

        for _, row in open_prs.iterrows():
            if row['id'] not in reviewed_pr_ids:
                alerts.append({
                    'id': str(uuid.uuid4()),
                    'user_id': row['user_id'],
                    'type': 'unreviewed_pr',
                    'message': f"PR של {row['user_id']} פתוח כבר {row['days_open']} ימים ללא בדיקה.",
                    'severity': 'high',
                    'created_at': today
                })

    # 2. חוסר פעילות של יומיים רצופים לפחות
    user_summary_df_sorted = user_summary_df.sort_values(['user_id', 'day'])
    grouped = user_summary_df_sorted.groupby('user_id')

    for user, group in grouped:
        group = group.set_index('day').sort_index()
        group['activity'] = group[['total_messages', 'commits',
                                   'reviews', 'completed_tasks']].sum(axis=1)
        inactive_days = group['activity'] == 0
        if inactive_days.empty:
            continue
        max_consec = (inactive_days != inactive_days.shift()).cumsum()
        counts = inactive_days.groupby(max_consec).sum()
        if (counts >= 2).any():
            alerts.append({
                'id': str(uuid.uuid4()),
                'user_id': user,
                'type': 'inactive_multiple_days',
                'message': f"{user} לא ביצע כל פעולה במשך לפחות יומיים רצופים.",
                'severity': 'medium',
                'created_at': today
            })

    # 3. בקשות עזרה ללא מענה
    if 'help_requests' in user_summary_df.columns and 'resolved' in user_summary_df.columns:
        help_issues = user_summary_df[(user_summary_df['help_requests'] >= 2) & (
            user_summary_df['resolved'] == 0)]
        for _, row in help_issues.iterrows():
            alerts.append({
                'id': str(uuid.uuid4()),
                'user_id': row['user_id'],
                'type': 'unanswered_help_repeated',
                'message': f"{row['user_id']} ביקש עזרה {row['help_requests']} פעמים ביום {row['day']} ללא מענה.",
                'severity': 'high',
                'created_at': row['day']
            })

    # 4. משתמש עם עומס יתר בפעילות
    user_summary_df['total_actions'] = user_summary_df[[
        'total_messages', 'commits', 'reviews', 'pull_requests', 'help_requests'
    ]].sum(axis=1)

    overloaded_users = user_summary_df[user_summary_df['total_actions'] >= 15]
    for _, row in overloaded_users.iterrows():
        alerts.append({
            'id': str(uuid.uuid4()),
            'user_id': row['user_id'],
            'type': 'overloaded_user',
            'message': f"{row['user_id']} ביצע {row['total_actions']} פעולות ביום אחד ({row['day']}).",
            'severity': 'medium',
            'created_at': row['day']
        })

    # 5. Issue קריטית פתוחה
    if not github_issues_df.empty:
        github_issues_df['created_date'] = pd.to_datetime(
            github_issues_df['created_at']).dt.date
        critical_issues = github_issues_df[
            (github_issues_df['is_critical'] == True) &
            (github_issues_df['state'] == 'open')
        ]
        for _, row in critical_issues.iterrows():
            alerts.append({
                'id': str(uuid.uuid4()),
                'user_id': row['user_id'],
                'type': 'critical_issue_unresolved',
                'message': f"Issue קריטית נפתחה ע״י {row['user_id']} ביום {row['created_date']} ועדיין פתוחה.",
                'severity': 'high',
                'created_at': row['created_date']
            })

    return pd.DataFrame(alerts)


def agent_monitor():
    print("🚀 מתחיל לנתח נתונים מ־Supabase...")
    time.sleep(10)
    try:

        # --- 1. טעינת כל ה-DataFrames הנדרשים ממסד הנתונים ---
        from slack_deletion_sync import load_filtered_slack_messages
        slack_df = load_filtered_slack_messages()

        print(f"📊 נטענו {len(slack_df)} הודעות מ-Slack")

        if slack_df.empty:
            print("⚠ לא נמצאו הודעות ב-Slack - מסיים")
            return

        # replies_df מבוסס על slack_df
        replies_df = slack_df[slack_df['parent_id'].notna()].copy()

        # טוען דוחות סלאק, יחזיר DF עם עמודות גם אם הטבלה ריקה
        slack_reports_df = load_slack_reports()

        # טוען נתוני GitHub
        github_commits_df = load_filtered_github_commits()
        github_reviews_df = load_filtered_github_reviews()
        github_issues_df = load_filtered_github_issues()
        github_prs_df = load_filtered_github_prs()
        print(f"📊 נטענו {len(github_issues_df)} גיליונות מ-GitHub")
        print(f"📊 נטענו {len(github_commits_df)} קומיטים מ-GitHub")
        print(f"📊 נטענו {len(github_reviews_df)} ביקורות מ-GitHub")
        print(f"📊 נטענו {len(github_prs_df)} בקשות משיכה מ-GitHub")
        github_commits_df = add_canonical_user_column(
            github_commits_df, git_col="author")
        github_reviews_df = add_canonical_user_column(
            github_reviews_df, git_col="user_id")
        github_issues_df = add_canonical_user_column(
            github_issues_df, git_col="user_id")
        github_prs_df = add_canonical_user_column(
            github_prs_df, git_col="user_id")
        slack_df = add_canonical_user_column(slack_df, slack_col="user_id")
        replies_df = add_canonical_user_column(replies_df, slack_col="user_id")
        slack_reports_df = add_canonical_user_column(
            slack_reports_df, slack_col="user_id")

        # --- 2. ביצוע הניתוח ---
        print("🔍 מבצע ניתוח נתונים...")
        user_summary_df = build_user_daily_summary(
            slack_df,
            replies_df,
            slack_reports_df,
            github_commits_df,
            github_reviews_df,
            github_issues_df,
            github_prs_df
        )
        # שמירה לפי תאריך מקסימלי עבור כל טבלה

        if not github_commits_df.empty:
            latest_commits = pd.to_datetime(
                github_commits_df['timestamp']).max()
            update_last_processed_time("github_commits_raw", latest_commits)

        if not github_reviews_df.empty:
            latest_reviews = pd.to_datetime(
                github_reviews_df['created_at']).max()
            update_last_processed_time("github_reviews_raw", latest_reviews)

        if not github_issues_df.empty:
            latest_issues = pd.to_datetime(
                github_issues_df['created_at']).max()
            update_last_processed_time("github_issues_raw", latest_issues)

        if not github_prs_df.empty:
            latest_prs = pd.to_datetime(github_prs_df['created_at']).max()
            update_last_processed_time("github_prs_raw", latest_prs)

        # --- 3. עדכון תאריך אחרון שטופל (לפני שמירה) ---
        if not user_summary_df.empty:
            latest_date = user_summary_df['day'].max()
            if not user_summary_df.empty:
                latest_ts = slack_df['ts'].max()
            latest_dt = datetime.fromtimestamp(float(latest_ts))
            update_last_processed_time("slack_messages_raw", latest_dt)
            print(
                f"🕓 עודכן התאריך האחרון שטופל בטבלה agent_progress: {latest_dt}")

            print(f"🕓 נשמר תאריך אחרון שטופל: {latest_date}")

        project_status_daily_df = build_project_status_daily(
            github_prs_df, github_issues_df, user_summary_df
        )

        alerts_df = build_alerts_v2(
            user_summary_df,
            github_prs_df,
            github_reviews_df,
            github_issues_df
        )

        # --- 3. הדפסת תוצאות ---
        print("\n📈 סיכום משתמשים יומי:")
        print(tabulate(user_summary_df.head(), headers='keys', tablefmt='grid'))

        print("\n📊 סיכום סטטוס פרויקט יומי:")
        print(tabulate(project_status_daily_df.head(),
              headers='keys', tablefmt='grid'))

        print(f"\n🚨 נמצאו {len(alerts_df)} התראות")

        # --- 4. שמירה למסד הנתונים ---
        print("\n💾 שומר נתונים למסד הנתונים...")

# ✅ הוספת בדיקות לפני שמירה:
        print("✅ טיפוסים:")
        print(user_summary_df.dtypes)

        print("🔍 דוגמה לשורה:")
        print(user_summary_df.head(1).to_dict())

        assert user_summary_df['day'].apply(
            lambda d: isinstance(d, date)).all(), "❌ טיפוס שגוי ב-day"
        assert user_summary_df['canonical_username'].notna(
        ).all(), "❌ user_id חסר"


# המשך שמירה
        user_summary_df["user_id"] = user_summary_df["canonical_username"]
        user_summary_df = user_summary_df.drop(columns=["canonical_username"])

        save_dataframe_to_db(
            user_summary_df,
            'user_daily_summary',
            conflict_columns=['user_id', 'day']
        )

        save_dataframe_to_db(
            project_status_daily_df,
            'project_status_daily',
            conflict_columns=['day']
        )

        save_dataframe_to_db(alerts_df, 'alerts')

        print("✅ הנתונים נותחו ונשמרו לטבלאות Supabase בהצלחה")

    except Exception as e:
        print(f"❌ שגיאה כללית: {e}")
        import traceback
        traceback.print_exc()


# אם מריצים את הקובץ ישירות, הפעל את הפונקציה
if __name__ == "__main__":
    agent_monitor()
