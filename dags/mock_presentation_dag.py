from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

# --- เอา Webhook URL ของคุณมาใส่ตรงนี้ ---
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1478580196314452050/Rbo7zMAcUw3csQWV5mpoj3JCSBDtjRLbkvTw69H5G1OC18yKgx59-QR3fFtKlE_rPA7t"

def dummy_ingest():
    print("⏳ แกล้งทำเป็นกำลังดูดข้อมูลจาก Google News RSS...")
    time.sleep(3) # หน่วงเวลา 3 วินาที
    print("✅ ดูดข้อมูลสำเร็จ! ข้อมูลถูกเก็บลง Database (จำลอง) เรียบร้อย")

def dummy_ai_scoring():
    import requests
    import time
    from airflow.providers.postgres.hooks.postgres import PostgresHook 
    
    print("🧠 กำลังวิเคราะห์ AI และบันทึกข้อมูลลง Database (จำลอง)...")
    time.sleep(2)
    
    # --- ส่วนที่ 1: บันทึกข้อมูลลง Database เพื่อให้บอท /check เจอ ---
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')  
        
        # เตรียมข้อมูล Mock เข้า Database
        mock_data_for_db = [
            ("เชียงราย", "ด่วน! น้ำท่วมฉับพลัน แม่น้ำสายทะลักเข้าท่วมตัวเมือง อพยพชาวบ้านวุ่น", 5),
            ("เชียงใหม่", "แผ่นดินไหวขนาด 3.2 ศูนย์กลางสันทราย รับรู้แรงสั่นสะเทือน มีผู้บาดเจ็บเล็กน้อย", 3),
            ("หนองคาย", "ประกาศเตือน เฝ้าระวังระดับน้ำโขงเพิ่มสูงขึ้นต่อเนื่อง", 1)
        ]
        
        for prov, title, score in mock_data_for_db:
            sql = "INSERT INTO disaster_news (province, title, severity_score) VALUES (%s, %s, %s) ON CONFLICT (title) DO NOTHING" #
            pg_hook.run(sql, parameters=(prov, title, score))
        print("✅ บันทึกข้อมูลจำลองลง Postgres เรียบร้อยแล้ว!")
    except Exception as e:
        print(f"❌ บันทึกลง Database ไม่สำเร็จ (แต่จะส่ง Discord ต่อ): {e}")

    # --- ส่วนที่ 2: เตรียมส่งการ์ดเข้า Discord  ---
    mock_news_list = [
        {
            "province": "เชียงราย",
            "headline": "ด่วน! น้ำท่วมฉับพลัน แม่น้ำสายทะลักเข้าท่วมตัวเมือง อพยพชาวบ้านวุ่น",
            "level": "🔴 วิกฤต (High)",
            "color": 15158332, #
            "map_img": "https://static-maps.yandex.ru/1.x/?lang=en_US&ll=99.8406,19.9105&z=10&l=map&size=450,250&pt=99.8406,19.9105,pm2rdm" #
        },
        {
            "province": "เชียงใหม่",
            "headline": "แผ่นดินไหวขนาด 3.2 ศูนย์กลางสันทราย รับรู้แรงสั่นสะเทือน มีผู้บาดเจ็บเล็กน้อย",
            "level": "🟠 ปานกลาง (Medium)",
            "color": 15105570, #
            "map_img": "https://static-maps.yandex.ru/1.x/?lang=en_US&ll=98.9853,18.7883&z=10&l=map&size=450,250&pt=98.9853,18.7883,pm2orm" #
        },
        {
            "province": "หนองคาย",
            "headline": "ประกาศเตือน เฝ้าระวังระดับน้ำโขงเพิ่มสูงขึ้นต่อเนื่อง",
            "level": "🟡 เฝ้าระวัง (Low)",
            "color": 16776960, #
            "map_img": "https://static-maps.yandex.ru/1.x/?lang=en_US&ll=102.7420,17.8785&z=10&l=map&size=450,250&pt=102.7420,17.8785,pm2grm" #
        }
    ]

    for news in mock_news_list:
        payload = {
            "username": "Crisis Warning Center", 
            "avatar_url": "https://cdn-icons-png.flaticon.com/512/564/564619.png",
            "embeds": [{
                "title": f"🚨 รายงานภัยพิบัติ: {news['province']}",
                "description": f"**📌 หัวข้อ:** {news['headline']}\n**⚠️ ระดับความรุนแรง:** {news['level']}",
                "color": news['color'],
                "image": {"url": news['map_img']},
                "footer": {"text": "Data Pipeline Verified | Source: Google News RSS"}
            }]
        }
        requests.post(DISCORD_WEBHOOK_URL, json=payload)
        time.sleep(2) # หน่วงเวลาให้ข้อความค่อยๆ เด้ง
            
    print("✅ ส่งการ์ดแผนที่พร้อมรายละเอียดครบทั้ง 3 จังหวัดเข้า Discord สำเร็จ!")
def dummy_map_update():
    import requests 
    import time
    import os
    
    print("🗺️ ดึงไฟล์แผนที่ Interactive ที่สร้างเตรียมไว้...")
    time.sleep(2)
    
    # ชี้เป้าไปที่ไฟล์แผนที่สวยๆ ของคุณ (ใน Docker โฟลเดอร์ dags จะอยู่ที่ /opt/airflow/dags/)
    map_filename = "/opt/airflow/dags/mock_disaster_map.html"
    
    # ส่งไฟล์เข้า Discord
    payload = {"content": "🗺️ **รายงานสรุปแผนที่ภัยพิบัติทั่วประเทศอัปเดตล่าสุด** 🚨\nทีมงานสามารถดาวน์โหลดไฟล์ `.html` ด้านล่างไปดับเบิลคลิกเพื่อเปิดดูแบบ Interactive ได้เลยครับ 👇"}
    
    try:
        # เช็คว่ามีไฟล์อยู่จริงไหม จะได้ไม่ Error
        if os.path.exists(map_filename):
            with open(map_filename, 'rb') as f:
                # ตั้งชื่อไฟล์ตอนส่งเข้าไปใน Discord ให้ดูโปรเฟสชันแนล
                files = {'file': ('disaster_interactive_map.html', f, 'text/html')}
                requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files)
            print("✅ หยิบไฟล์แผนที่ของจริงส่งเข้า Discord สำเร็จ!")
        else:
            print("❌ หาไฟล์ไม่เจอ! อย่าลืมเอาไฟล์ mock_disaster_map.html ไปใส่ในโฟลเดอร์ dags นะครับ")
    except Exception as e:
        print(f"❌ ระบบส่งไฟล์ล้มเหลว: {e}")
# --- ตั้งค่า DAG ---
default_args = {
    'owner': 'crisis_team',
    'start_date': datetime(2026, 3, 26),
}

with DAG(
    'demo_video_presentation_pipeline', 
    default_args=default_args,
    schedule_interval=None, 
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='ingest_to_postgres', python_callable=dummy_ingest)
    t2 = PythonOperator(task_id='ai_analysis_and_discord', python_callable=dummy_ai_scoring)
    t3 = PythonOperator(task_id='update_disaster_map', python_callable=dummy_map_update)

    t1 >> t2 >> t3