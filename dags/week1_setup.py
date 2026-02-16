from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlite3
import feedparser
import pandas as pd
import urllib.parse
from tabulate import tabulate
import os

# --- CONFIGURATION ---
# ใช้ Path แบบ Absolute เพื่อให้ Airflow หาไฟล์เจอแน่นอน
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_PATH, "disaster.db")

SEARCH_QUERY = "น้ำท่วม ไฟไหม้ แผ่นดินไหว สึนามิ ดินถล่ม"
ENCODED_QUERY = urllib.parse.quote(SEARCH_QUERY)
RSS_URL = f"https://news.google.com/rss/search?q={ENCODED_QUERY}&hl=th-TH&gl=TH&ceid=TH:th"

PROVINCES_LIST = [
    "กระบี่", "กรุงเทพมหานคร", "กาญจนบุรี", "กาฬสินธุ์", "กำแพงเพชร",
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

# --- FUNCTIONS (LOGIC) ---
def run_pipeline():
    # 1. Init DB
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS provinces (id INTEGER PRIMARY KEY AUTOINCREMENT, name_th TEXT UNIQUE NOT NULL)')
    c.execute('''CREATE TABLE IF NOT EXISTS news (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, link TEXT UNIQUE,
                    published_date TEXT, source TEXT, risk_level INTEGER DEFAULT 0,
                    province_id INTEGER, FOREIGN KEY (province_id) REFERENCES provinces(id))''')
    
    # 2. Seed Provinces
    for province in PROVINCES_LIST:
        c.execute("INSERT OR IGNORE INTO provinces (name_th) VALUES (?)", (province,))
    
    # 3. Fetch Data
    print(f"🔄 Fetching: {SEARCH_QUERY}")
    feed = feedparser.parse(RSS_URL)
    new_count = 0
    for entry in feed.entries:
        title = entry.title
        link = entry.link
        published = entry.published
        source = entry.source.title if 'source' in entry else 'Google News'
        
        matched_province_id = None
        for prov in PROVINCES_LIST:
            if prov in title:
                c.execute("SELECT id FROM provinces WHERE name_th=?", (prov,))
                result = c.fetchone()
                if result: matched_province_id = result[0]
                break
        
        try:
            c.execute('INSERT OR IGNORE INTO news (title, link, published_date, source, province_id) VALUES (?, ?, ?, ?, ?)', 
                      (title, link, published, source, matched_province_id))
            if c.rowcount > 0: new_count += 1
        except: pass
    
    conn.commit()
    print(f"✅ Saved {new_count} new entries.")
    
    # 4. Show Data (Log to Airflow)
    df = pd.read_sql_query('SELECT * FROM news ORDER BY id DESC LIMIT 5', conn)
    print(tabulate(df, headers='keys', tablefmt='psql'))
    conn.close()

# --- DAG DEFINITION ---
default_args = {
    'owner': 'crisis_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1), # เริ่มวันที่ 1 ก.พ. ตามแผน
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'week1_crisis_ingestion',
    default_args=default_args,
    description='Pipeline ดึงข่าวภัยพิบัติ สัปดาห์ที่ 1',
    schedule=timedelta(minutes=15),  # ✅ แก้จาก schedule_interval เป็น schedule
    catchup=False
) as dag:

    task_fetch_news = PythonOperator(
        task_id='fetch_crisis_news',
        python_callable=run_pipeline
    )