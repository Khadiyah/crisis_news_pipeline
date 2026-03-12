import requests
import os
from html2image import Html2Image

# ลิงก์ Webhook ของคุณ
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1478580196314452050/Rbo7zMAcUw3csQWV5mpoj3JCSBDtjRLbkvTw69H5G1OC18yKgx59-QR3fFtKlE_rPA7t"

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
MAP_HTML = "disaster_map_full.html" # ไฟล์เว็บต้นฉบับ
MAP_PNG = "disaster_map_full.png"   # ชื่อไฟล์รูปภาพที่เราจะเซฟ

def send_map_image_to_discord():
    # 1. เช็คว่ามีไฟล์ HTML ให้ถ่ายรูปไหม
    if not os.path.exists(MAP_HTML):
        print(f"❌ ไม่พบไฟล์ {MAP_HTML} (ลองรันโค้ดสร้างแผนที่ก่อนนะครับ)")
        return

    print("📸 กำลังให้ AI ถ่ายรูปแผนที่... (อาจใช้เวลา 2-3 วินาที)")
    
    try:
        # 2. แปลง HTML เป็นรูปภาพ (PNG)
        hti = Html2Image()
        # จำลองขนาดหน้าจอคอมพิวเตอร์ (กว้าง 800px สูง 600px) เพื่อถ่ายรูป
        hti.screenshot(
            other_file=MAP_HTML, 
            save_as=MAP_PNG, 
            size=(800, 600)
        )
        print("✅ ถ่ายรูปแผนที่สำเร็จ!")
    except Exception as e:
        print(f"❌ แปลงเป็นรูปภาพล้มเหลว: {e}")
        return

    print("🚀 กำลังส่งรูปแผนที่เข้า Discord...")

    # 3. เตรียมข้อความที่จะส่ง
    payload = {
        "content": "🗺️ **รายงานสรุปแผนที่ภัยพิบัติทั่วประเทศ** 🚨\n(แสดงผลในรูปแบบรูปภาพ เพื่อง่ายต่อการติดตามครับ 👇)"
    }

    # 4. แนบไฟล์รูปภาพ (.png) ส่งไปที่ Webhook
    try:
        with open(MAP_PNG, 'rb') as f:
            files = {
                'file': (MAP_PNG, f, 'image/png')
            }
            response = requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files)
            
            if response.status_code in [200, 204]:
                print("✅ อัปโหลดและส่งรูปแผนที่เข้า Discord สำเร็จแบบหล่อๆ เลยครับ!")
            else:
                print(f"❌ เกิดข้อผิดพลาดจาก Discord: {response.status_code}")
    except Exception as e:
        print(f"❌ ระบบส่งข้อมูลล้มเหลว: {e}")

if __name__ == "__main__":
    send_map_image_to_discord()