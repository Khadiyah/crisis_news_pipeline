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

# พิกัดสำหรับแสดงแผนที่ในการ์ด Discord (ละติจูด, ลองจิจูด)
PROVINCE_COORDS = {
    "เชียงใหม่": [18.7883, 98.9853], "เชียงราย": [19.9105, 99.8406], "แม่ฮ่องสอน": [19.3020, 97.9654], "ลำปาง": [18.2888, 99.4930], "ลำพูน": [18.5745, 99.0087], "พะเยา": [19.1666, 99.9022], "แพร่": [18.1446, 100.1403], "น่าน": [18.7756, 100.7730], "อุตรดิตถ์": [17.6201, 100.0993],
    "กาฬสินธุ์": [16.4333, 103.5069], "ขอนแก่น": [16.4322, 102.8236], "ชัยภูมิ": [15.8066, 102.0315], "นครพนม": [17.4061, 104.7862], "นครราชสีมา": [14.9799, 102.0978], "บึงกาฬ": [18.3608, 103.6520], "บุรีรัมย์": [14.9930, 103.1029], "มหาสารคาม": [16.1852, 103.3020], "มุกดาหาร": [16.5443, 104.7194], "ยโสธร": [15.7926, 104.1333], "ร้อยเอ็ด": [16.0538, 103.6520], "เลย": [17.4860, 101.7223], "สกลนคร": [17.1664, 104.1486], "สุรินทร์": [14.8818, 103.4936], "ศรีสะเกษ": [15.1186, 104.3220], "หนองคาย": [17.8785, 102.7420], "หนองบัวลำภู": [17.2045, 102.4406], "อำนาจเจริญ": [15.8657, 104.6258], "อุดรธานี": [17.4138, 102.7872], "อุบลราชธานี": [15.2287, 104.8564],
    "กรุงเทพมหานคร": [13.7563, 100.5018], "กำแพงเพชร": [16.4828, 99.5227], "ชัยนาท": [15.1852, 100.1251], "นครนายก": [14.2069, 101.2131], "นครปฐม": [13.8199, 100.0443], "นครสวรรค์": [15.7051, 100.1373], "นนทบุรี": [13.8620, 100.5144], "ปทุมธานี": [14.0208, 100.5250], "พระนครศรีอยุธยา": [14.3510, 100.5781], "พิจิตร": [16.4428, 100.3488], "พิษณุโลก": [16.8211, 100.2659], "เพชรบูรณ์": [16.4184, 101.1550], "ลพบุรี": [14.7995, 100.6534], "สมุทรปราการ": [13.5991, 100.5968], "สมุทรสงคราม": [13.4098, 100.0023], "สมุทรสาคร": [13.5475, 100.2744], "สิงห์บุรี": [14.8936, 100.3967], "สุโขทัย": [17.0055, 99.8263], "สุพรรณบุรี": [14.4742, 100.1123], "สระบุรี": [14.5289, 100.9101], "อ่างทอง": [14.5896, 100.4550], "อุทัยธานี": [15.3787, 100.0256],
    "จันทบุรี": [12.6114, 102.1039], "ฉะเชิงเทรา": [13.6904, 101.0780], "ชลบุรี": [13.3611, 100.9847], "ตราด": [12.2428, 102.5104], "ปราจีนบุรี": [14.0510, 101.3722], "ระยอง": [12.6814, 101.2816], "สระแก้ว": [13.8240, 102.0646],
    "กาญจนบุรี": [14.0041, 99.5330], "ตาก": [16.8840, 99.1258], "ประจวบคีรีขันธ์": [11.8105, 99.7971], "เพชรบุรี": [13.1119, 99.9441], "ราชบุรี": [13.5283, 99.8128],
    "กระบี่": [8.0855, 98.9063], "ชุมพร": [10.4930, 99.1800], "ตรัง": [7.5563, 99.6114], "นครศรีธรรมราช": [8.4304, 99.9595], "นราธิวาส": [6.4255, 101.8253], "ปัตตานี": [6.8673, 101.2501], "พังงา": [8.4501, 98.5283], "พัทลุง": [7.6167, 100.0740], "ภูเก็ต": [7.8804, 98.3923], "ยะลา": [6.5401, 101.2813], "ระนอง": [9.9658, 98.6348], "สงขลา": [7.1898, 100.5954], "สตูล": [6.6228, 100.0658], "สุราษฎร์ธานี": [9.1342, 99.3334]
}

PROVINCES_LIST = list(PROVINCE_COORDS.keys())

# TASK 1: Ingestion
def run_ingestion():
    import feedparser
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
    
    RSS_URL = "https://news.google.com/rss/search?q=น้ำท่วม+แผ่นดินไหว+ประเทศไทย&hl=th-TH&gl=TH&ceid=TH:th"
    feed = feedparser.parse(RSS_URL)
    
    for entry in feed.entries:
        title = entry.title
        matched_prov = next((p for p in PROVINCES_LIST if p in title), "ไม่ระบุ")
        
        if matched_prov == "ไม่ระบุ":
            continue 
            
        sql = "INSERT INTO disaster_news (title, link, province) VALUES (%s, %s, %s) ON CONFLICT (title) DO NOTHING"
        pg_hook.run(sql, parameters=(title, entry.link, matched_prov))

# TASK 2: Scoring & Rich Discord Notification
def run_ai_and_notify():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = pg_hook.get_pandas_df("SELECT * FROM disaster_news WHERE severity_score = 0")
    
    if df.empty: 
        print("No new news to analyze.")
        return

    for _, row in df.iterrows():
        title = row['title']
        prov = row['province']
        d_type = "Earthquake" if "แผ่นดินไหว" in title else "Flood" if "น้ำท่วม" in title else "Disaster"
        
        score = 1
        if any(kw in title for kw in ["เสียชีวิต", "สึนามิ", "วิกฤต", "ถล่ม", "รุนแรง"]): score = 5
        elif any(kw in title for kw in ["บาดเจ็บ", "อพยพ", "เตือนภัย"]): score = 3

        pg_hook.run("UPDATE disaster_news SET severity_score = %s, disaster_type = %s WHERE id = %s", 
                   parameters=(score, d_type, row['id']))

        color = 15158332 if score >= 4 else 15105570 if score >= 2 else 16776960
        label = "🔴 วิกฤต (High)" if score >= 4 else "🟠 ปานกลาง (Medium)" if score >= 2 else "🟡 เฝ้าระวัง (Low)"
        pt_color = "rd" if score >= 4 else "or" if score >= 2 else "gr"

        coords = PROVINCE_COORDS.get(prov, [13.7563, 100.5018]) # ถ้าไม่เจอให้ปักที่ กทม.
        map_url = f"https://static-maps.yandex.ru/1.x/?lang=en_US&ll={coords[1]},{coords[0]}&z=10&l=map&size=450,250&pt={coords[1]},{coords[0]},pm2{pt_color}m"

        payload = {
            "username": "Crisis Warning Center", 
            "avatar_url": "https://cdn-icons-png.flaticon.com/512/564/564619.png", 
            "embeds": [{
                "title": f"🚨 รายงานภัยพิบัติ: {prov}",
                "description": f"**📌 หัวข้อ:** {title}\n**⚠️ ระดับความรุนแรง:** {label}",
                "color": color,
                "url": row['link'],
                "image": {"url": map_url},
                "footer": {"text": "Data Pipeline Verified | Source: Google News RSS"}
            }]
        }
        try:
            requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=15)
        except Exception as e:
            print(f"Failed to send Discord: {e}")

# TASK 3: Map Update
def run_map_update():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = pg_hook.get_pandas_df("SELECT province, severity_score, disaster_type FROM disaster_news")
    if not df.empty:
        print("--- Summary of current disasters in database ---")
        print(df.groupby('province').size().reset_index(name='count'))

# DAG DEFINITION
default_args = {
    'owner': 'crisis_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 26, 0, 0), 
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