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

KEYWORDS_LIST = ["น้ำท่วม", "ไฟไหม้", "แผ่นดินไหว", "สึนามิ", "ดินถล่ม"]
SEARCH_QUERY = " ".join(KEYWORDS_LIST)
ENCODED_QUERY = urllib.parse.quote(SEARCH_QUERY)
RSS_URLS = [
    f"https://news.google.com/rss/search?q={ENCODED_QUERY}&hl=th-TH&gl=TH&ceid=TH:th",
    "https://www.thaipbs.or.th/rss/news",
    "https://www.thairath.co.th/rss/news",
    "http://www.tmd.go.th/service/rss",
    "https://www.springnews.co.th/rss/news"
]

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
    

    # 3. Loop Fetch & Filter
    total_new_count = 0
    for url in RSS_URLS:
        print(f"🔄 Fetching from: {url}")
        feed = feedparser.parse(url)
        
        for entry in feed.entries:
            # ✅ แก้ไขที่ 2: นิยามตัวแปรให้ครบถ้วนก่อนใช้งาน
            title = entry.title
            link = entry.link
            
            # ✅ แก้ไขที่ 3: ปรับตรรกะการกรอง (Filter Logic)
            is_disaster = False
            if "google.com" in url or "tmd.go.th" in url:
                is_disaster = True # แหล่งข้อมูลเหล่านี้กรองมาให้แล้วระดับหนึ่ง
            else:
                # ตรวจสอบว่ามี "คำ" ใน KEYWORDS_LIST อยู่ในชื่อข่าวหรือไม่
                if any(kw in title for kw in KEYWORDS_LIST):
                    is_disaster = True
            
            if not is_disaster:
                continue # ข้ามข่าวการเมือง/บันเทิงไปทันที
            
            # --- บันทึกลง Database ---
            published = entry.published if 'published' in entry else str(datetime.now())
            source = entry.source.title if 'source' in entry else 'News Source'
            
            # Logic หาจังหวัด (Matching)
            matched_province_id = None
            for prov in PROVINCES_LIST:
                if prov in title:
                    c.execute("SELECT id FROM provinces WHERE name_th=?", (prov,))
                    res = c.fetchone()
                    if res: matched_province_id = res[0]
                    break
            
            try:
                c.execute('INSERT OR IGNORE INTO news (title, link, published_date, source, province_id) VALUES (?, ?, ?, ?, ?)', 
                          (title, link, published, source, matched_province_id))
                if c.rowcount > 0: total_new_count += 1
            except: pass
    
    conn.commit()
    conn.close()
    print(f"✅ Finished! Added {total_new_count} relevant news items.")

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