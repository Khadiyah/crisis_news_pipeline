import folium
import pandas as pd
import psycopg2 # ใช้สำหรับต่อ PostgreSQL 
from datetime import datetime
import os

# 1. พิกัดของแต่ละจังหวัดในไทย (ใส่ตัวอย่างจังหวัดหลักๆ ไว้ คุณสามารถเพิ่มได้ครับ)
PROVINCE_COORDS = {
    "กรุงเทพมหานคร": [13.7563, 100.5018],
    "เชียงใหม่": [18.7883, 98.9853],
    "เชียงราย": [19.9105, 99.8406],
    "ภูเก็ต": [7.8804, 98.3923],
    "กาญจนบุรี": [14.0041, 99.5330],
    "พระนครศรีอยุธยา": [14.3510, 100.5781],
    "ชลบุรี": [13.3611, 100.9847],
    "สงขลา": [7.1898, 100.5954],
    "นครราชสีมา": [14.9799, 102.0978],
    "ขอนแก่น": [16.4322, 102.8236]
}

def create_disaster_map():
    print("🗺️ กำลังดึงข้อมูลจาก Database เพื่อสร้างแผนที่...")

    # --- ส่วนเชื่อมต่อ Database ---
    # ถ้าคุณใช้ PostgreSQL ให้ใช้โค้ดชุดนี้ (แก้รหัสผ่านและชื่อ DB ให้ตรงของตัวเอง)
    # conn = psycopg2.connect(host="localhost", database="your_db", user="your_user", password="your_password")
    
    # แต่ถ้าคุณทดสอบด้วย SQLite ในเครื่องตัวเอง ให้ใช้ชุดนี้แทน:
    import sqlite3
    BASE_PATH = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(BASE_PATH, "disaster.db"))
    # -----------------------------

    # สมมติว่าดึงจากตารางที่เรามีชื่อข่าวและความรุนแรง
    # ปรับ Query ให้เข้ากับโครงสร้างตารางล่าสุดของคุณนะครับ
    query = """
        SELECT title, risk_level, province_id 
        FROM news 
        WHERE risk_level >= 1
    """
    
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"❌ ดึงข้อมูลไม่สำเร็จ: {e}")
        return

    conn.close()

    if df.empty:
        print("✅ ไม่มีข้อมูลภัยพิบัติให้แสดงบนแผนที่")
        return

    # 2. สร้างแผนที่ตั้งต้น (Center อยู่ที่ประเทศไทย ซูมระดับ 6)
    m = folium.Map(location=[15.8700, 100.9925], zoom_start=6, tiles="CartoDB positron")

    # 3. วนลูปอ่านข้อมูลข่าวทีละบรรทัด แล้วปักหมุดลงแผนที่
    for index, row in df.iterrows():
        title = row['title']
        severity = row['risk_level']  # เช่น 1, 2, 3, 4, 5
        
        # ค้นหาชื่อจังหวัดในพาดหัวข่าว เพื่อเอาพิกัด (หรือถ้าคุณมี Join ตารางจังหวัด ก็ดึงชื่อมาเทียบได้เลย)
        lat_lon = None
        prov_name = "ไม่ระบุ"
        for prov, coords in PROVINCE_COORDS.items():
            if prov in title:
                lat_lon = coords
                prov_name = prov
                break
        
        # ถ้าหาพิกัดไม่เจอ ให้ข้ามไป
        if not lat_lon:
            continue

        # 4. กำหนดสีหมุดตามความรุนแรง (Severity)
        if severity >= 4:
            icon_color = 'red'
            level_text = "วิกฤต (High)"
        elif severity >= 2:
            icon_color = 'orange'
            level_text = "ปานกลาง (Medium)"
        else:
            icon_color = 'lightgreen' # ใช้สีเขียวอ่อนแทนสีเหลืองเพราะ Folium อ่านง่ายกว่า
            level_text = "เฝ้าระวัง (Low)"

        # 5. สร้างกล่องข้อความ Popup เวลากดคลิกที่หมุด
        popup_html = f"""
            <b>🚨 จังหวัด:</b> {prov_name}<br>
            <b>⚠️ ความรุนแรง:</b> {level_text}<br>
            <b>📰 ข่าว:</b> {title}
        """
        
        # ปักหมุดลงบนแผนที่
        folium.Marker(
            location=lat_lon,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{prov_name} - {level_text}",
            icon=folium.Icon(color=icon_color, icon='info-sign')
        ).add_to(m)

    # 6. บันทึกแผนที่เป็นไฟล์ HTML
    map_filename = "disaster_map_thailand.html"
    m.save(map_filename)
    print(f"✅ สร้างแผนที่สำเร็จ! เปิดดูไฟล์นี้ใน Web Browser ได้เลย: {map_filename}")

if __name__ == "__main__":
    create_disaster_map()