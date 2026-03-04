# test_week2.py
# ไฟล์นี้เอาไว้กด Run ใน VS Code เพื่อเทส Logic (ไม่มี Airflow)

import sqlite3
import feedparser
import pandas as pd
import urllib.parse
from datetime import datetime
from tabulate import tabulate
import os

# --- 1. CONFIGURATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_PATH, "disaster.db")

SEARCH_QUERY = "น้ำท่วม ไฟไหม้ แผ่นดินไหว สึนามิ ดินถล่ม"
ENCODED_QUERY = urllib.parse.quote(SEARCH_QUERY)

# Keyword สำหรับกรองข่าว (เฉพาะ Source ทั่วไป)
FILTER_KEYWORDS = ["น้ำท่วม", "ไฟไหม้", "แผ่นดินไหว", "สึนามิ", "ดินถล่ม", "พายุ", "ระเบิด", "ภัยแล้ง"]

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

# --- 2. LOGIC (จำลองการทำงานของ Airflow Task) ---
def run_pipeline_test():
    print("🚀 เริ่มทดสอบ Pipeline (โหมด Manual)...")
    
    # Init DB
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS provinces (id INTEGER PRIMARY KEY AUTOINCREMENT, name_th TEXT UNIQUE NOT NULL)')
    c.execute('''CREATE TABLE IF NOT EXISTS news (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, link TEXT UNIQUE,
                    published_date TEXT, source TEXT, risk_level INTEGER DEFAULT 0,
                    province_id INTEGER, FOREIGN KEY (province_id) REFERENCES provinces(id))''')
    
    # Seed Provinces
    for province in PROVINCES_LIST:
        c.execute("INSERT OR IGNORE INTO provinces (name_th) VALUES (?)", (province,))
    
    # Loop Fetch & Filter
    total_new = 0
    for url in RSS_URLS:
        print(f"📡 กำลังดึงจาก: {url}...")
        try:
            feed = feedparser.parse(url)
            print(f"   -> เจอ {len(feed.entries)} รายการ")
            
            for entry in feed.entries:
                title = entry.title
                link = entry.link
                
                # 🛑 1. FILTER LOGIC: กรองข่าว
                is_disaster = False
                if "google.com" in url or "tmd.go.th" in url:
                    is_disaster = True # Google กับ กรมอุตุฯ เอาหมด
                else:
                    # ThaiPBS, Thairath, SpringNews ต้องเช็ค keyword
                    for kw in FILTER_KEYWORDS:
                        if kw in title:
                            is_disaster = True
                            break
                
                if not is_disaster:
                    continue # ข้ามข่าวนี้ไป
                
                # ✅ 2. PROVINCE LOGIC: หาจังหวัด
                matched_province_id = None
                for prov in PROVINCES_LIST:
                    if prov in title:
                        c.execute("SELECT id FROM provinces WHERE name_th=?", (prov,))
                        res = c.fetchone()
                        if res: matched_province_id = res[0]
                        break
                
                # 3. SAVE TO DB
                published = entry.published if 'published' in entry else str(datetime.now())
                source = entry.source.title if 'source' in entry else 'Unknown Source'
                
                try:
                    c.execute('INSERT OR IGNORE INTO news (title, link, published_date, source, province_id) VALUES (?, ?, ?, ?, ?)', 
                              (title, link, published, source, matched_province_id))
                    if c.rowcount > 0: total_new += 1
                except: pass
        except Exception as e:
            print(f"❌ Error: {e}")

    conn.commit()
    print(f"\n✅ เสร็จสิ้น! เพิ่มข่าวใหม่: {total_new} ข่าว")
    
    # Show Data
    df = pd.read_sql_query('SELECT id, source, title FROM news ORDER BY id DESC LIMIT 15', conn)
    print(tabulate(df, headers='keys', tablefmt='psql'))
    conn.close()

if __name__ == "__main__":
    run_pipeline_test()