import sqlite3
import feedparser
import pandas as pd
import urllib.parse
from tabulate import tabulate

# --- 1. CONFIGURATION ---
DB_NAME = "disaster.db"

# ✅ ปรับปรุง Keyword: ตัด PM 2.5 ออก เหลือแค่ภัยพิบัติฉุกเฉิน
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

# --- 2. DATABASE SETUP ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS provinces (id INTEGER PRIMARY KEY AUTOINCREMENT, name_th TEXT UNIQUE NOT NULL)')
    c.execute('''CREATE TABLE IF NOT EXISTS news (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, link TEXT UNIQUE,
                    published_date TEXT, source TEXT, risk_level INTEGER DEFAULT 0,
                    province_id INTEGER, FOREIGN KEY (province_id) REFERENCES provinces(id))''')
    conn.commit()
    return conn

def seed_provinces(conn):
    c = conn.cursor()
    for province in PROVINCES_LIST:
        c.execute("INSERT OR IGNORE INTO provinces (name_th) VALUES (?)", (province,))
    conn.commit()

# --- 3. DATA INGESTION ---
def fetch_and_save_news(conn):
    print(f"🔄 กำลังดึงข่าว (น้ำท่วม, ไฟไหม้, แผ่นดินไหว, สึนามิ, ดินถล่ม)...")
    feed = feedparser.parse(RSS_URL)
    
    if len(feed.entries) == 0:
        print("⚠️ ไม่พบข่าว (กรุณาตรวจสอบการเชื่อมต่ออินเทอร์เน็ต)")
        return

    c = conn.cursor()
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
                if result:
                    matched_province_id = result[0]
                break
        
        try:
            c.execute('INSERT OR IGNORE INTO news (title, link, published_date, source, province_id) VALUES (?, ?, ?, ?, ?)', 
                      (title, link, published, source, matched_province_id))
            if c.rowcount > 0: new_count += 1
        except: pass

    conn.commit()
    print(f"✅ บันทึกข่าวใหม่สำเร็จ: {new_count} ข่าว")

# --- 4. PRETTY DISPLAY ---
def show_data(conn):
    print("\n" + "="*80)
    print(" 📊 DIGITAL ANALYTICS: CRISIS DATA PIPELINE (WEEK 1)")
    print("="*80)
    
    # ดึงข้อมูล 10 ข่าวล่าสุด
    df = pd.read_sql_query('''
        SELECT n.id, p.name_th as Province, n.title as Title, n.source as Source, n.published_date as Date
        FROM news n 
        LEFT JOIN provinces p ON n.province_id = p.id 
        ORDER BY n.id DESC LIMIT 10
    ''', conn)
    
    if not df.empty:
        df['Province'] = df['Province'].fillna("ไม่ระบุ")
        df['Title'] = df['Title'].apply(lambda x: x[:50] + "..." if len(x) > 50 else x)
        df['Date'] = df['Date'].apply(lambda x: x[:16])
        
        print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
        print(f"\n💡 Note: ข้อมูลจริงอาจระบุจังหวัดไม่ได้ครบถ้วน (จะแก้ไขด้วย Mock Data ใน Week 2)")
    else:
        print("❌ ยังไม่มีข้อมูลในฐานข้อมูล")

if __name__ == "__main__":
    connection = init_db()
    seed_provinces(connection)
    fetch_and_save_news(connection)
    show_data(connection)
    connection.close()