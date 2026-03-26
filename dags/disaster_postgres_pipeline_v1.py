from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import feedparser
import pandas as pd
import requests
import os

# --- CONFIGURATION ---
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1478580196314452050/Rbo7zMAcUw3csQWV5mpoj3JCSBDtjRLbkvTw69H5G1OC18yKgx59-QR3fFtKlE_rPA7t"
POSTGRES_CONN_ID = 'my_postgres_conn'

# รายชื่อจังหวัดสำหรับ Match ข้อมูล (ครบ 77 จังหวัด)
PROVINCES_LIST = [
    "กรุงเทพมหานคร", "กระบี่", "กาญจนบุรี", "กาฬสินธุ์", "กำแพงเพชร", 
    "ขอนแก่น", "จันทบุรี", "ฉะเชิงเทรา", "ชลบุรี", "ชัยนาท", 
    "ชัยภูมิ", "ชุมพร", "เชียงราย", "เชียงใหม่", "ตรัง", 
    "ตราด", "ตาก", "นครนายก", "นครปฐม", "นครพนม", 
    "นครราชสีมา", "นครศรีธรรมราช", "นครสวรรค์", "นนทบุรี", "นราธิวาส", 
    "น่าน", "บึงกาฬ", "บุรีรัมย์", "ปทุมธานี", "ประจวบคีรีขันธ์", 
    "ปราจีนบุรี", "ปัตตานี", "พระนครศรีอยุธยา", "พะเยา", "พังงา", 
    "พัทลุง", "พิจิตร", "พิษณุโลก", "เพชรบุรี", "เพชรบูรณ์", 
    "แพร่", "ภูเก็ต", "มหาสารคาม", "มุกดาหาร", "แม่ฮ่องสอน", 
    "ยโสธร", "ยะลา", "ร้อยเอ็ด", "ระนอง", "ระยอง", 
    "ราชบุรี", "ลพบุรี", "ลำปาง", "ลำพูน", "เลย", 
    "ศรีสะเกษ", "สกลนคร", "สงขลา", "สตูล", "สมุทรปราการ", 
    "สมุทรสงคราม", "สมุทรสาคร", "สระแก้ว", "สระบุรี", "สิงห์บุรี", 
    "สุโขทัย", "สุพรรณบุรี", "สุราษฎร์ธานี", "สุรินทร์", "หนองคาย", 
    "หนองบัวลำภู", "อ่างทอง", "อำนาจเจริญ", "อุดรธานี", "อุตรดิตถ์", 
    "อุทัยธานี", "อุบลราชธานี"
]

# ==========================================
# 📥 TASK 1: Ingestion (ดึงข่าวลง Postgres)
# ==========================================
def run_ingestion():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS disaster_news (
            id SERIAL PRIMARY KEY,
            title TEXT UNIQUE,
            link TEXT,
            province TEXT,
            severity_score INTEGER DEFAULT 0,
            disaster_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    RSS_URL = "https://news.google.com/rss/search?q=น้ำท่วม+แผ่นดินไหว&hl=th-TH&gl=TH&ceid=TH:th"
    feed = feedparser.parse(RSS_URL)
    for entry in feed.entries:
        title = entry.title
        matched_prov = next((p for p in PROVINCES_LIST if p in title), "ไม่ระบุ")
        sql = "INSERT INTO disaster_news (title, link, province) VALUES (%s, %s, %s) ON CONFLICT (title) DO NOTHING"
        pg_hook.run(sql, parameters=(title, entry.link, matched_prov))

def run_ai_and_notify():
    # เปลี่ยนจาก AI เป็น Keyword Logic เพื่อประหยัด RAM และป้องกัน Error
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = pg_hook.get_pandas_df("SELECT * FROM disaster_news WHERE severity_score = 0")
    
    if df.empty: 
        print("No new news to analyze.")
        return

    for _, row in df.iterrows():
        title = row['title']
        d_type = "Earthquake" if "แผ่นดินไหว" in title else "Flood" if "น้ำท่วม" in title else "Disaster"
        
        # วิเคราะห์ความรุนแรงด้วยคำสำคัญ
        score = 1
        if any(kw in title for kw in ["เสียชีวิต", "สึนามิ", "วิกฤต", "ถล่ม", "รุนแรง"]): score = 5
        elif any(kw in title for kw in ["บาดเจ็บ", "อพยพ", "เตือนภัย"]): score = 3

        pg_hook.run("UPDATE disaster_news SET severity_score = %s, disaster_type = %s WHERE id = %s", 
                   parameters=(score, d_type, row['id']))

        color = 15158332 if score >= 4 else 15105570 if score >= 2 else 16776960
        label = "🔴 High" if score >= 4 else "🟠 Medium" if score >= 2 else "🟡 Low"
        
        payload = {
            "username": "Warning Center (Postgres)",
            "embeds": [{
                "title": f"🚨 {d_type} Report: {row['province']}",
                "description": f"**หัวข้อ:** {title}\n**ระดับความรุนแรง:** {label}",
                "color": color,
                "url": row['link']
            }]
        }
        try:
            requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        except Exception as e:
            print(f"Failed to send Discord: {e}")

def run_map_update():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = pg_hook.get_pandas_df("SELECT province, severity_score, disaster_type FROM disaster_news")
    if not df.empty:
        print(df.groupby('province').size().reset_index(name='count'))

default_args = {
    'owner': 'crisis_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 26, 22, 0), 
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'disaster_postgres_full_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1
) as dag:

    t1 = PythonOperator(task_id='ingest_to_postgres', python_callable=run_ingestion)
    t2 = PythonOperator(task_id='ai_analysis_and_discord', python_callable=run_ai_and_notify)
    t3 = PythonOperator(task_id='update_disaster_map', python_callable=run_map_update)

t1 >> t2 >> t3