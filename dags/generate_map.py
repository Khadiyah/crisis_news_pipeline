import folium
import pandas as pd
import sqlite3
import os
import requests # ✨ เพิ่มไลบรารีสำหรับส่ง Discord

# ✨ ใส่ Webhook URL ของคุณตรงนี้
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1478580196314452050/Rbo7zMAcUw3csQWV5mpoj3JCSBDtjRLbkvTw69H5G1OC18yKgx59-QR3fFtKlE_rPA7t"

# 1. พิกัด 77 จังหวัดแบบจัดเต็ม
PROVINCE_COORDS = {
    "เชียงใหม่": [18.7883, 98.9853], "เชียงราย": [19.9105, 99.8406], "แม่ฮ่องสอน": [19.3020, 97.9654], "ลำปาง": [18.2888, 99.4930], "ลำพูน": [18.5745, 99.0087], "พะเยา": [19.1666, 99.9022], "แพร่": [18.1446, 100.1403], "น่าน": [18.7756, 100.7730], "อุตรดิตถ์": [17.6201, 100.0993],
    "กาฬสินธุ์": [16.4333, 103.5069], "ขอนแก่น": [16.4322, 102.8236], "ชัยภูมิ": [15.8066, 102.0315], "นครพนม": [17.4061, 104.7862], "นครราชสีมา": [14.9799, 102.0978], "บึงกาฬ": [18.3608, 103.6520], "บุรีรัมย์": [14.9930, 103.1029], "มหาสารคาม": [16.1852, 103.3020], "มุกดาหาร": [16.5443, 104.7194], "ยโสธร": [15.7926, 104.1333], "ร้อยเอ็ด": [16.0538, 103.6520], "เลย": [17.4860, 101.7223], "สกลนคร": [17.1664, 104.1486], "สุรินทร์": [14.8818, 103.4936], "ศรีสะเกษ": [15.1186, 104.3220], "หนองคาย": [17.8785, 102.7420], "หนองบัวลำภู": [17.2045, 102.4406], "อำนาจเจริญ": [15.8657, 104.6258], "อุดรธานี": [17.4138, 102.7872], "อุบลราชธานี": [15.2287, 104.8564],
    "กรุงเทพมหานคร": [13.7563, 100.5018], "กำแพงเพชร": [16.4828, 99.5227], "ชัยนาท": [15.1852, 100.1251], "นครนายก": [14.2069, 101.2131], "นครปฐม": [13.8199, 100.0443], "นครสวรรค์": [15.7051, 100.1373], "นนทบุรี": [13.8620, 100.5144], "ปทุมธานี": [14.0208, 100.5250], "พระนครศรีอยุธยา": [14.3510, 100.5781], "พิจิตร": [16.4428, 100.3488], "พิษณุโลก": [16.8211, 100.2659], "เพชรบูรณ์": [16.4184, 101.1550], "ลพบุรี": [14.7995, 100.6534], "สมุทรปราการ": [13.5991, 100.5968], "สมุทรสงคราม": [13.4098, 100.0023], "สมุทรสาคร": [13.5475, 100.2744], "สิงห์บุรี": [14.8936, 100.3967], "สุโขทัย": [17.0055, 99.8263], "สุพรรณบุรี": [14.4742, 100.1123], "สระบุรี": [14.5289, 100.9101], "อ่างทอง": [14.5896, 100.4550], "อุทัยธานี": [15.3787, 100.0256],
    "จันทบุรี": [12.6114, 102.1039], "ฉะเชิงเทรา": [13.6904, 101.0780], "ชลบุรี": [13.3611, 100.9847], "ตราด": [12.2428, 102.5104], "ปราจีนบุรี": [14.0510, 101.3722], "ระยอง": [12.6814, 101.2816], "สระแก้ว": [13.8240, 102.0646],
    "กาญจนบุรี": [14.0041, 99.5330], "ตาก": [16.8840, 99.1258], "ประจวบคีรีขันธ์": [11.8105, 99.7971], "เพชรบุรี": [13.1119, 99.9441], "ราชบุรี": [13.5283, 99.8128],
    "กระบี่": [8.0855, 98.9063], "ชุมพร": [10.4930, 99.1800], "ตรัง": [7.5563, 99.6114], "นครศรีธรรมราช": [8.4304, 99.9595], "นราธิวาส": [6.4255, 101.8253], "ปัตตานี": [6.8673, 101.2501], "พังงา": [8.4501, 98.5283], "พัทลุง": [7.6167, 100.0740], "ภูเก็ต": [7.8804, 98.3923], "ยะลา": [6.5401, 101.2813], "ระนอง": [9.9658, 98.6348], "สงขลา": [7.1898, 100.5954], "สตูล": [6.6228, 100.0658], "สุราษฎร์ธานี": [9.1342, 99.3334]
}

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_PATH, "disaster.db")

def create_full_map_and_send():
    print("🗺️ กำลังดึงข้อมูล 'ข่าวจริง + Mock Data' จาก Database...")

    # 2. เชื่อมต่อ Database และดึงข้อมูลทั้งหมด
    conn = sqlite3.connect(DB_NAME)
    query = """
        SELECT n.title, n.risk_level, n.source, p.name_th as province
        FROM news n
        JOIN provinces p ON n.province_id = p.id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("✅ ไม่มีข้อมูลใน Database เลยครับ")
        return

    m = folium.Map(location=[15.8700, 100.9925], zoom_start=6, tiles="CartoDB positron")

    for index, row in df.iterrows():
        title = row['title']
        severity = row['risk_level'] 
        prov_name = row['province']
        source = row['source'] 

        # ชดเชยคะแนนกรณีที่ข่าวยังไม่ได้ผ่านระบบ AI
        if severity == 0:
            if "วิกฤต" in title or "สลด" in title or "เสียชีวิต" in title: severity = 5
            elif "เตือนภัย" in title or "ด่วน" in title or "น้ำท่วม" in title: severity = 3
            else: severity = 1

        lat_lon = PROVINCE_COORDS.get(prov_name)
        if not lat_lon:
            continue

        if severity >= 4:
            icon_color = 'red'
            level_text = "วิกฤต (High)"
        elif severity >= 2:
            icon_color = 'orange'
            level_text = "ปานกลาง (Medium)"
        else:
            icon_color = 'lightgreen'
            level_text = "เฝ้าระวัง (Low)"

        if source == "Mock":
            source_tag = "<span style='color:blue;'>🧪 ข้อมูลทดสอบ (Mock)</span>"
        else:
            source_tag = f"<span style='color:green;'>📰 ข่าวจริง ({source})</span>"

        popup_html = f"""
            <div style="font-family: Tahoma; min-width: 250px;">
                <h4 style="margin-top:0px; color:{icon_color};">🚨 {prov_name}</h4>
                <b>⚠️ ความรุนแรง:</b> {level_text}<br>
                <b>📌 แหล่งที่มา:</b> {source_tag}<br><br>
                <b>พาดหัวข่าว:</b> <br><i>{title}</i>
            </div>
        """
        
        folium.Marker(
            location=lat_lon,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{prov_name} - {level_text}",
            icon=folium.Icon(color=icon_color, icon='info-sign')
        ).add_to(m)

    # 3. เซฟไฟล์แผนที่
    map_filename = "disaster_map_full.html"
    m.save(map_filename)
    print(f"✅ สร้างแผนที่รวมสำเร็จ! ไฟล์ชื่อ: {map_filename}")
    
    # ✨ 4. ส่วนที่เพิ่มมาใหม่: ส่งไฟล์ HTML ที่เพิ่งเซฟเข้า Discord ทันที!
    print("🚀 กำลังส่งไฟล์แผนที่เข้า Discord...")
    payload = {
        "content": "🗺️ **รายงานสรุปแผนที่ภัยพิบัติทั่วประเทศอัปเดตล่าสุด** 🚨\nทีมงานสามารถดาวน์โหลดไฟล์ `.html` ด้านล่างไปดับเบิลคลิกเพื่อเปิดดูแบบ Interactive ได้เลยครับ 👇"
    }

    try:
        with open(map_filename, 'rb') as f:
            files = {'file': (map_filename, f, 'text/html')}
            response = requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files)
            
            if response.status_code in [200, 204]:
                print("✅ อัปโหลดและส่งแผนที่เข้า Discord สำเร็จแบบหล่อๆ เลยครับ!")
            else:
                print(f"❌ เกิดข้อผิดพลาดจาก Discord: {response.status_code}")
    except Exception as e:
        print(f"❌ ระบบส่งข้อมูลล้มเหลว: {e}")

if __name__ == "__main__":
    create_full_map_and_send()