from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import feedparser
import urllib.parse
import logging
import requests

# --- CONFIGURATION ---
# 1. URL ที่คัดลอกมาจาก Discord (Webhooks) 
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1478580196314452050/Rbo7zMAcUw3csQWV5mpoj3JCSBDtjRLbkvTw69H5G1OC18yKgx59-QR3fFtKlE_rPA7t" 

# 2. ตั้งค่าการค้นหาข่าว
KEYWORDS_LIST = ["น้ำท่วม", "ไฟไหม้", "แผ่นดินไหว", "สึนามิ", "ดินถล่ม"]
SEARCH_QUERY = " ".join(KEYWORDS_LIST)
ENCODED_QUERY = urllib.parse.quote(SEARCH_QUERY)
RSS_URLS = [
    f"https://news.google.com/rss/search?q={ENCODED_QUERY}&hl=th-TH&gl=TH&ceid=TH:th",
    "http://www.tmd.go.th/service/rss"
]

def send_discord_alert(title, link, severity):
    """ฟังก์ชันส่งการแจ้งเตือนไปยัง Discord ในชื่อ Warning Center"""
    # กำหนดสีแถบข้างข้อความ (แดง = High, ส้ม = Medium)
    color = 15158332 if severity == 'High' else 15105570
    
    payload = {
        "username": "Warning Center",
        "avatar_url": "https://cdn-icons-png.flaticon.com/512/564/564619.png",
        "embeds": [{
            "title": "🚨 ตรวจพบภัยพิบัติใหม่!",
            "description": f"**หัวข้อ:** {title}\n**ระดับความรุนแรง:** {severity}",
            "url": link,
            "color": color,
            "footer": {"text": "ระบบแจ้งเตือนอัตโนมัติ โดย Warning Center"},
            "timestamp": datetime.now().isoformat()
        }]
    }
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Discord Alert Failed: {e}")

def fetch_and_load_to_postgres():
    """ดึงข่าวจาก RSS, บันทึกลง DB และแจ้งเตือนข่าวใหม่"""
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    new_count = 0

    for url in RSS_URLS:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            title = entry.title
            link = entry.link
            # วิเคราะห์ความรุนแรงเบื้องต้นจาก Keywords
            severity = 'High' if any(kw in title for kw in ["ด่วน", "วิกฤต", "สึนามิ", "ตาย", "พัง"]) else 'Medium'
            
            # ตรวจสอบว่าข่าวนี้มีอยู่แล้วในฐานข้อมูลหรือไม่ (เช็คจาก link)
            check_sql = "SELECT EXISTS(SELECT 1 FROM disaster_alerts WHERE external_id = %s)"
            already_exists = pg_hook.get_first(check_sql, parameters=(link,))[0]

            if not already_exists:
                # 1. บันทึกลง PostgreSQL
                insert_sql = """
                    INSERT INTO disaster_alerts (external_id, title, description, severity)
                    VALUES (%s, %s, %s, %s);
                """
                pg_hook.run(insert_sql, parameters=(link, title, title, severity))
                
                # 2. ส่งแจ้งเตือนไปยัง Discord
                send_discord_alert(title, link, severity)
                new_count += 1
            
    logging.info(f"Successfully processed {new_count} new items.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'disaster_warning_system_v_final',
    default_args=default_args,
    description='Pipeline ดึงข้อมูลภัยพิบัติและแจ้งเตือนผ่าน Discord',
    schedule_interval='*/15 * * * *', # รันทุก 15 นาที
    catchup=False
) as dag:

    # TASK 1: สร้าง Table ใน Postgres 
    setup_db = PostgresOperator(
        task_id='setup_database',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS disaster_alerts (
                id SERIAL PRIMARY KEY,
                external_id TEXT UNIQUE,
                title TEXT,
                description TEXT,
                severity VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # TASK 2: รัน ETL และแจ้งเตือน
    run_etl = PythonOperator(
        task_id='fetch_news_rss',
        python_callable=fetch_and_load_to_postgres
    )

    setup_db >> run_etl