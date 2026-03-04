import sqlite3
import os

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_PATH, "disaster.db")

def insert_mock_data():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # 1. สร้างตารางใหม่ให้ครบถ้วน
    c.execute('CREATE TABLE IF NOT EXISTS provinces (id INTEGER PRIMARY KEY AUTOINCREMENT, name_th TEXT UNIQUE NOT NULL)')
    c.execute('''CREATE TABLE IF NOT EXISTS news (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, link TEXT UNIQUE,
                    published_date TEXT, source TEXT, risk_level INTEGER DEFAULT 0,
                    province_id INTEGER, FOREIGN KEY (province_id) REFERENCES provinces(id))''')

    # 2. จำลองข้อมูลจังหวัดที่จำเป็นต้องใช้
    provinces = ["กรุงเทพมหานคร", "กาญจนบุรี", "เชียงราย", "เชียงใหม่", "ภูเก็ต", "พระนครศรีอยุธยา"]
    for prov in provinces:
        c.execute("INSERT OR IGNORE INTO provinces (name_th) VALUES (?)", (prov,))

    # ฟังก์ชันช่วยหา ID ของจังหวัด
    def get_prov_id(name):
        c.execute("SELECT id FROM provinces WHERE name_th=?", (name,))
        res = c.fetchone()
        return res[0] if res else None

    # 3. จำลองข่าวภัยพิบัติ 6 ระดับ (เบาไปจนถึงวิกฤต)
    mock_news = [
        ("ด่วน! น้ำท่วมหนักที่เชียงราย อพยพด่วน ประชาชนเสียหายหนัก", "http://test.com/1", "2026-03-01", "Mock", get_prov_id("เชียงราย")), 
        ("แผ่นดินไหว 5.0 เขย่าเชียงใหม่ มีผู้บาดเจ็บหลายราย", "http://test.com/2", "2026-03-01", "Mock", get_prov_id("เชียงใหม่")),
        ("ประกาศเตือนภัย! เฝ้าระวังน้ำท่วมขัง กรุงเทพมหานคร", "http://test.com/3", "2026-03-01", "Mock", get_prov_id("กรุงเทพมหานคร")),
        ("สลด! แผ่นดินไหวรุนแรงที่กาญจนบุรี ตึกถล่ม เสียชีวิต 5 ราย บาดเจ็บอื้อ", "http://test.com/4", "2026-03-02", "Mock", get_prov_id("กาญจนบุรี")), 
        ("วิกฤต! เตือนภัย น้ำท่วมภูเก็ต เสียหายหนัก สั่งอพยพด่วน พบผู้เสียชีวิตและบาดเจ็บ", "http://test.com/5", "2026-03-02", "Mock", get_prov_id("ภูเก็ต")), 
        ("รายงานสถานการณ์น้ำท่วมเล็กน้อยที่พระนครศรีอยุธยา เริ่มคลี่คลาย", "http://test.com/6", "2026-03-02", "Mock", get_prov_id("พระนครศรีอยุธยา"))
    ]

    try:
        # 4. บังคับยัดข้อมูลลง Database
        c.executemany('INSERT OR IGNORE INTO news (title, link, published_date, source, province_id) VALUES (?, ?, ?, ?, ?)', mock_news)
        conn.commit()
        print(f"✅ สร้างข่าวภัยพิบัติจำลองสำเร็จ {c.rowcount} ข่าว! พร้อมให้ AI วิเคราะห์แล้ว")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    insert_mock_data()