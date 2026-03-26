from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import feedparser
import pandas as pd
import requests
import os
import folium

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

# พิกัด 77 จังหวัดแบบจัดเต็ม
PROVINCE_COORDS = {
    "เชียงใหม่": [18.7883, 98.9853], "เชียงราย": [19.9105, 99.8406], "แม่ฮ่องสอน": [19.3020, 97.9654], "ลำปาง": [18.2888, 99.4930], "ลำพูน": [18.5745, 99.0087], "พะเยา": [19.1666, 99.9022], "แพร่": [18.1446, 100.1403], "น่าน": [18.7756, 100.7730], "อุตรดิตถ์": [17.6201, 100.0993],
    "กาฬสินธุ์": [16.4333, 103.5069], "ขอนแก่น": [16.4322, 102.8236], "ชัยภูมิ": [15.8066, 102.0315], "นครพนม": [17.4061, 104.7862], "นครราชสีมา": [14.9799, 102.0978], "บึงกาฬ": [18.3608, 103.6520], "บุรีรัมย์": [14.9930, 103.1029], "มหาสารคาม": [16.1852, 103.3020], "มุกดาหาร": [16.5443, 104.7194], "ยโสธร": [15.7926, 104.1333], "ร้อยเอ็ด": [16.0538, 103.6520], "เลย": [17.4860, 101.7223], "สกลนคร": [17.1664, 104.1486], "สุรินทร์": [14.8818, 103.4936], "ศรีสะเกษ": [15.1186, 104.3220], "หนองคาย": [17.8785, 102.7420], "หนองบัวลำภู": [17.2045, 102.4406], "อำนาจเจริญ": [15.8657, 104.6258], "อุดรธานี": [17.4138, 102.7872], "อุบลราชธานี": [15.2287, 104.8564],
    "กรุงเทพมหานคร": [13.7563, 100.5018], "กำแพงเพชร": [16.4828, 99.5227], "ชัยนาท": [15.1852, 100.1251], "นครนายก": [14.2069, 101.2131], "นครปฐม": [13.8199, 100.0443], "นครสวรรค์": [15.7051, 100.1373], "นนทบุรี": [13.8620, 100.5144], "ปทุมธานี": [14.0208, 100.5250], "พระนครศรีอยุธยา": [14.3510, 100.5781], "พิจิตร": [16.4428, 100.3488], "พิษณุโลก": [16.8211, 100.2659], "เพชรบูรณ์": [16.4184, 101.1550], "ลพบุรี": [14.7995, 100.6534], "สมุทรปราการ": [13.5991, 100.5968], "สมุทรสงคราม": [13.4098, 100.0023], "สมุทรสาคร": [13.5475, 100.2744], "สิงห์บุรี": [14.8936, 100.3967], "สุโขทัย": [17.0055, 99.8263], "สุพรรณบุรี": [14.4742, 100.1123], "สระบุรี": [14.5289, 100.9101], "อ่างทอง": [14.5896, 100.4550], "อุทัยธานี": [15.3787, 100.0256],
    "จันทบุรี": [12.6114, 102.1039], "ฉะเชิงเทรา": [13.6904, 101.0780], "ชลบุรี": [13.3611, 100.9847], "ตราด": [12.2428, 102.5104], "ปราจีนบุรี": [14.0510, 101.3722], "ระยอง": [12.6814, 101.2816], "สระแก้ว": [13.8240, 102.0646],
    "กาญจนบุรี": [14.0041, 99.5330], "ตาก": [16.8840, 99.1258], "ประจวบคีรีขันธ์": [11.8105, 99.7971], "เพชรบุรี": [13.1119, 99.9441], "ราชบุรี": [13.5283, 99.8128],
    "กระบี่": [8.0855, 98.9063], "ชุมพร": [10.4930, 99.1800], "ตรัง": [7.5563, 99.6114], "นครศรีธรรมราช": [8.4304, 99.9595], "นราธิวาส": [6.4255, 101.8253], "ปัตตานี": [6.8673, 101.2501], "พังงา": [8.4501, 98.5283], "พัทลุง": [7.6167, 100.0740], "ภูเก็ต": [7.8804, 98.3923], "ยะลา": [6.5401, 101.2813], "ระนอง": [9.9658, 98.6348], "สงขลา": [7.1898, 100.5954], "สตูล": [6.6228, 100.0658], "สุราษฎร์ธานี": [9.1342, 99.3334]
}

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

# ==========================================
# 🧠 TASK 2: AI Scoring & Discord Notify
# ==========================================
def run_ai_and_notify():
    from transformers import pipeline
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    df = pg_hook.get_pandas_df("SELECT * FROM disaster_news WHERE severity_score = 0")
    
    if df.empty: 
        print("No new news to analyze.")
        return

    classifier = pipeline("sentiment-analysis", model="poom-sci/WangchanBERTa-finetuned-sentiment")

    for _, row in df.iterrows():
        d_type = "Earthquake" if "แผ่นดินไหว" in row['title'] else "Flood" if "น้ำท่วม" in row['title'] else "Other"
        
        score = 1
        if any(kw in row['title'] for kw in ["เสียชีวิต", "สึนามิ", "วิกฤต"]): score = 5
        elif any(kw in row['title'] for kw in ["บาดเจ็บ", "อพยพ"]): score = 3

        update_sql = "UPDATE disaster_news SET severity_score = %s, disaster_type = %s WHERE id = %s"
        pg_hook.run(update_sql, parameters=(score, d_type, row['id']))

        color = 15158332 if score >= 4 else 15105570 if score >= 2 else 16776960
        label = "🔴 High" if score >= 4 else "🟠 Medium" if score >= 2 else "🟡 Low"
        
        payload = {
            "username": "Warning Center (Postgres)",
            "embeds": [{
                "title": f"🚨 {d_type} Report",
                "description": f"**หัวข้อ:** {row['title']}\n**จังหวัด:** {row['province']}\n**ระดับ:** {label}",
                "color": color,
                "url": row['link']
            }]
        }
        requests.post(DISCORD_WEBHOOK_URL, json=payload)

# ==========================================
# 🗺️ TASK 3: Update Map 
# ==========================================
def run_map_update():
    """ดึงข้อมูลจาก Postgres มาสร้างแผนที่ HTML และส่งไฟล์เข้า Discord"""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = pg_hook.get_pandas_df("SELECT title, link, province, severity_score, disaster_type FROM disaster_news WHERE province != 'ไม่ระบุ'")
    
    if df.empty:
        print("No data available for map update.")
        return

    m = folium.Map(location=[15.8700, 100.9925], zoom_start=6, tiles="CartoDB positron")

    for index, row in df.iterrows():
        title = row['title']
        severity = row['severity_score'] 
        prov_name = row['province']

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

        popup_html = f"""
            <div style="font-family: Tahoma; min-width: 250px;">
                <h4 style="margin-top:0px; color:{icon_color};">🚨 {prov_name}</h4>
                <b>⚠️ ความรุนแรง:</b> {level_text}<br><br>
                <b>พาดหัวข่าว:</b> <br><i>{title}</i>
            </div>
        """
        
        folium.Marker(
            location=lat_lon,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{prov_name} - {level_text}",
            icon=folium.Icon(color=icon_color, icon='info-sign')
        ).add_to(m)

    map_filename = "/tmp/disaster_map_postgres.html" 
    m.save(map_filename)
    print(f"✅ สร้างแผนที่สำเร็จ! ไฟล์ชื่อ: {map_filename}")

    print("🚀 กำลังส่งไฟล์แผนที่เข้า Discord...")
    payload = {
        "content": "🗺️ **รายงานสรุปแผนที่ภัยพิบัติทั่วประเทศอัปเดตล่าสุด** 🚨\nทีมงานสามารถดาวน์โหลดไฟล์ `.html` ด้านล่างไปดับเบิลคลิกเพื่อเปิดดูแบบ Interactive ได้เลยครับ 👇"
    }

    try:
        with open(map_filename, 'rb') as f:
            files = {'file': (map_filename, f, 'text/html')}
            response = requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files)
            if response.status_code in [200, 204]:
                print("✅ ส่งแผนที่เข้า Discord สำเร็จ!")
            else:
                print(f"❌ เกิดข้อผิดพลาดจาก Discord: {response.status_code}")
    except Exception as e:
        print(f"❌ ระบบส่งข้อมูลล้มเหลว: {e}")

# ==========================================
# ⚙️ AIRFLOW DAG DEFINITION
# ==========================================
default_args = {
    'owner': 'crisis_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'disaster_postgres_full_pipeline',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='ingest_to_postgres', python_callable=run_ingestion)
    t2 = PythonOperator(task_id='ai_analysis_and_discord', python_callable=run_ai_and_notify)
    t3 = PythonOperator(task_id='update_disaster_map', python_callable=run_map_update)

    t1 >> t2 >> t3