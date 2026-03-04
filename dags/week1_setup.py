# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta, timezone
# import time
# import sqlite3
# import feedparser
# import pandas as pd
# import urllib.parse
# from tabulate import tabulate
# import os

# # --- CONFIGURATION ---
# BASE_PATH = os.path.dirname(os.path.abspath(__file__))
# DB_NAME = os.path.join(BASE_PATH, "disaster.db")

# KEYWORDS_LIST = ["น้ำท่วม", "ไฟไหม้", "แผ่นดินไหว", "สึนามิ", "ดินถล่ม"]
# SEARCH_QUERY = " ".join(KEYWORDS_LIST)
# ENCODED_QUERY = urllib.parse.quote(SEARCH_QUERY)

# RSS_URLS = [
#     f"https://news.google.com/rss/search?q={ENCODED_QUERY}&hl=th-TH&gl=TH&ceid=TH:th",
#     "https://www.thaipbs.or.th/rss/news",
#     "https://www.thairath.co.th/rss/news",
#     "http://www.tmd.go.th/service/rss",
#     "https://www.springnews.co.th/rss/news"
# ]

# PROVINCES_LIST = [
#     "กระบี่", "กรุงเทพมหานคร", "กาญจนบุรี", "กาฬสินธุ์", "กำแพงเพชร",
#     "ขอนแก่น", "จันทบุรี", "ฉะเชิงเทรา", "ชลบุรี", "ชัยนาท",
#     "ชัยภูมิ", "ชุมพร", "เชียงราย", "เชียงใหม่", "ตรัง",
#     "ตราด", "ตาก", "นครนายก", "นครปฐม", "นครพนม",
#     "นครราชสีมา", "นครศรีธรรมราช", "นครสวรรค์", "นนทบุรี", "นราธิวาส",
#     "น่าน", "บึงกาฬ", "บุรีรัมย์", "ปทุมธานี", "ประจวบคีรีขันธ์",
#     "ปราจีนบุรี", "ปัตตานี", "พระนครศรีอยุธยา", "พะเยา", "พังงา",
#     "พัทลุง", "พิจิตร", "พิษณุโลก", "เพชรบุรี", "เพชรบูรณ์",
#     "แพร่", "ภูเก็ต", "มหาสารคาม", "มุกดาหาร", "แม่ฮ่องสอน",
#     "ยโสธร", "ยะลา", "ร้อยเอ็ด", "ระนอง", "ระยอง",
#     "ราชบุรี", "ลพบุรี", "ลำปาง", "ลำพูน", "เลย",
#     "ศรีสะเกษ", "สกลนคร", "สงขลา", "สตูล", "สมุทรปราการ",
#     "สมุทรสงคราม", "สมุทรสาคร", "สระแก้ว", "สระบุรี", "สิงห์บุรี",
#     "สุโขทัย", "สุพรรณบุรี", "สุราษฎร์ธานี", "สุรินทร์", "หนองคาย",
#     "หนองบัวลำภู", "อ่างทอง", "อำนาจเจริญ", "อุดรธานี", "อุตรดิตถ์",
#     "อุทัยธานี", "อุบลราชธานี"
# ]

# # --- FUNCTIONS (LOGIC) ---
# def run_pipeline():
#     # 1. Init DB
#     conn = sqlite3.connect(DB_NAME)
#     c = conn.cursor()
#     c.execute('CREATE TABLE IF NOT EXISTS provinces (id INTEGER PRIMARY KEY AUTOINCREMENT, name_th TEXT UNIQUE NOT NULL)')
#     c.execute('''CREATE TABLE IF NOT EXISTS news (
#                     id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, link TEXT UNIQUE,
#                     published_date TEXT, source TEXT, risk_level INTEGER DEFAULT 0,
#                     province_id INTEGER, FOREIGN KEY (province_id) REFERENCES provinces(id))''')
    
#     # 2. Seed Provinces
#     for province in PROVINCES_LIST:
#         c.execute("INSERT OR IGNORE INTO provinces (name_th) VALUES (?)", (province,))
        
#     # 3. Loop Fetch & Filter
#     total_new_count = 0
    
#     # ดึงย้อนหลัง 12 ชั่วโมง
#     freshness_threshold = datetime.now(timezone.utc) - timedelta(hours=12)

#     for url in RSS_URLS:
#         print(f"🔄 Fetching from: {url}")
#         feed = feedparser.parse(url)
        
#         for entry in feed.entries:
#             # 1. TIME FILTER: ตรวจสอบเวลาของข่าว
#             entry_date_struct = entry.get('published_parsed')
#             if entry_date_struct:
#                 entry_date = datetime(*entry_date_struct[:6], tzinfo=timezone.utc)
#                 if entry_date < freshness_threshold:
#                     continue

#             title = entry.title
#             link = entry.link
            
#             # 1. DATA CLEANING
#             clean_title = title.replace("-", "").replace(" ", "").replace("\u200b", "")
            
#             # 2. KEYWORD FILTER
#             is_disaster = False
#             if "tmd.go.th" in url:
#                 is_disaster = True 
#             else:
#                 if any(kw in clean_title for kw in KEYWORDS_LIST):
#                     is_disaster = True
            
#             if not is_disaster:
#                 continue 

#             # 3. THAILAND ONLY FILTER
#             matched_province_id = None
#             for prov in PROVINCES_LIST:
#                 if prov in title:
#                     c.execute("SELECT id FROM provinces WHERE name_th=?", (prov,))
#                     res = c.fetchone()
#                     if res: 
#                         matched_province_id = res[0]
#                     break 
            
#             # ไม่มีจังหวัด = ทิ้ง
#             if matched_province_id is None:
#                 continue 

#             # --- บันทึกลง Database ---
#             published = entry.published if 'published' in entry else str(datetime.now())
#             source = entry.source.title if 'source' in entry else 'News Source'
            
#             try:
#                 c.execute('INSERT OR IGNORE INTO news (title, link, published_date, source, province_id) VALUES (?, ?, ?, ?, ?)', 
#                           (title, link, published, source, matched_province_id))
#                 if c.rowcount > 0: total_new_count += 1
#             except: pass
            
#     conn.commit()
#     conn.close()
#     print(f"✅ Finished! Added {total_new_count} relevant & FRESH news items.")

# # --- DAG DEFINITION ---
# default_args = {
#     'owner': 'crisis_team',
#     'depends_on_past': False,
#     'start_date': datetime(2026, 2, 1), 
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     'week1_crisis_ingestion',
#     default_args=default_args,
#     description='Pipeline ดึงข่าวภัยพิบัติ สัปดาห์ที่ 1',
#     schedule=timedelta(minutes=15),
#     catchup=False
# ) as dag:

#     task_fetch_news = PythonOperator(
#         task_id='fetch_crisis_news',
#         python_callable=run_pipeline
#     )

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import time
import sqlite3
import feedparser
import pandas as pd
import urllib.parse
from tabulate import tabulate
import os

# --- CONFIGURATION ---
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

# ==========================================
# 📥 TASK 1: ฟังก์ชันดึงข่าวและคัดกรอง (Ingestion)
# ==========================================
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
    freshness_threshold = datetime.now(timezone.utc) - timedelta(hours=12)

    for url in RSS_URLS:
        print(f"🔄 Fetching from: {url}")
        feed = feedparser.parse(url)
        
        for entry in feed.entries:
            entry_date_struct = entry.get('published_parsed')
            if entry_date_struct:
                entry_date = datetime(*entry_date_struct[:6], tzinfo=timezone.utc)
                if entry_date < freshness_threshold:
                    continue

            title = entry.title
            link = entry.link
            
            clean_title = title.replace("-", "").replace(" ", "").replace("\u200b", "")
            
            is_disaster = False
            if "tmd.go.th" in url:
                is_disaster = True 
            else:
                if any(kw in clean_title for kw in KEYWORDS_LIST):
                    is_disaster = True
            
            if not is_disaster: continue 

            matched_province_id = None
            for prov in PROVINCES_LIST:
                if prov in title:
                    c.execute("SELECT id FROM provinces WHERE name_th=?", (prov,))
                    res = c.fetchone()
                    if res: matched_province_id = res[0]
                    break 
            
            if matched_province_id is None: continue 

            published = entry.published if 'published' in entry else str(datetime.now())
            source = entry.source.title if 'source' in entry else 'News Source'
            
            try:
                c.execute('INSERT OR IGNORE INTO news (title, link, published_date, source, province_id) VALUES (?, ?, ?, ?, ?)', 
                          (title, link, published, source, matched_province_id))
                if c.rowcount > 0: total_new_count += 1
            except: pass
            
    conn.commit()
    conn.close()
    print(f"✅ Finished Ingestion! Added {total_new_count} new items.")

# ==========================================
# 🧠 TASK 2: ฟังก์ชัน AI วิเคราะห์ความรุนแรง (AI Scoring)
# ==========================================
def calculate_keyword_risk(title):
    score = 0
    if "เสียชีวิต" in title or "สลด" in title: score += 2
    if "บาดเจ็บ" in title: score += 1
    if "อพยพ" in title or "วิกฤต" in title: score += 1
    if "เสียหายหนัก" in title: score += 1
    if "เตือนภัย" in title: score += 1
    return score

def classify_disaster(title):
    if "แผ่นดินไหว" in title: return "Earthquake"
    elif "น้ำท่วม" in title or "พายุ" in title: return "Flood"
    elif "ไฟไหม้" in title: return "Fire"
    elif "สึนามิ" in title: return "Tsunami"
    elif "ดินถล่ม" in title: return "Landslide"
    else: return "Other"

def run_ai_scoring():
    # ⚠️ โหลดโมเดลเฉพาะตอน Task ทำงานเท่านั้น
    from transformers import pipeline
    print("⏳ กำลังโหลดโมเดล AI ภาษาไทย (WangchanBERTa)...")
    sentiment_analyzer = pipeline("sentiment-analysis", model="poom-sci/WangchanBERTa-finetuned-sentiment")

    def get_ai_sentiment(text):
        try:
            result = sentiment_analyzer(text[:512])[0]
            label = result['label'].upper()
            if 'NEG' in label: return 'Negative'
            elif 'POS' in label: return 'Positive'
            else: return 'Neutral'
        except: return 'Neutral'

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # เพิ่มคอลัมน์ถ้ายังไม่มี
    try:
        c.execute("ALTER TABLE news ADD COLUMN sentiment TEXT")
        c.execute("ALTER TABLE news ADD COLUMN disaster_type TEXT")
    except sqlite3.OperationalError: pass 

    df = pd.read_sql_query("SELECT n.id, n.title, p.name_th as province, n.province_id FROM news n JOIN provinces p ON n.province_id = p.id", conn)
    
    if df.empty:
        print("✅ ไม่มีข้อมูลข่าวในระบบให้ AI วิเคราะห์")
        conn.close()
        return

    print("🤖 AI และระบบ Hybrid กำลังวิเคราะห์ข้อมูล...")
    df['sentiment'] = df['title'].apply(get_ai_sentiment)
    df['keyword_risk'] = df['title'].apply(calculate_keyword_risk)
    df['disaster_type'] = df['title'].apply(classify_disaster)
    df['sent_score'] = df['sentiment'].map({'Negative': 1, 'Neutral': 0, 'Positive': -1})

    summary = df.groupby(['province_id', 'province', 'disaster_type']).agg(
        frequency=('id', 'count'),          
        total_sent_score=('sent_score', 'sum'),
        total_keyword_risk=('keyword_risk', 'sum')
    ).reset_index()

    summary['raw_severity'] = summary['frequency'] + summary['total_sent_score'] + summary['total_keyword_risk']
    summary['severity_level'] = summary['raw_severity'].apply(lambda x: min(max(int(x), 1), 5))

    for index, row in summary.iterrows():
        c.execute("UPDATE news SET risk_level = ?, sentiment = ?, disaster_type = ? WHERE province_id = ? AND title LIKE ?", 
                 (row['severity_level'], "Calculated", row['disaster_type'], row['province_id'], f"%{row['disaster_type']}%"))

    for index, row in df.iterrows():
        c.execute("UPDATE news SET sentiment = ?, disaster_type = ? WHERE id = ?", (row['sentiment'], row['disaster_type'], row['id']))

    conn.commit()
    conn.close()
    print("✅ ระบบ AI อัปเดต Severity Score เสร็จสมบูรณ์!")


# ==========================================
# ⚙️ AIRFLOW DAG DEFINITION
# ==========================================
default_args = {
    'owner': 'crisis_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crisis_pipeline_full', # เปลี่ยนชื่อ DAG นิดหน่อยให้ดูเป็นตัวเต็ม
    default_args=default_args,
    description='Pipeline ดึงข่าวและวิเคราะห์ความรุนแรงด้วย AI',
    schedule=timedelta(minutes=15),
    catchup=False
) as dag:

    # กล่องที่ 1: ดึงข่าว
    task_fetch_news = PythonOperator(
        task_id='fetch_crisis_news',
        python_callable=run_pipeline
    )

    # กล่องที่ 2: ให้ AI ประเมินความรุนแรง
    task_ai_scoring = PythonOperator(
        task_id='ai_severity_scoring',
        python_callable=run_ai_scoring
    )

    # 🔗 สร้างสายพานการทำงาน: ดึงข่าวเสร็จ (>>) ค่อยส่งให้ AI ประเมิน
    task_fetch_news >> task_ai_scoring