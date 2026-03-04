import sqlite3
import os

# ชี้ไปที่ไฟล์ DB
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_PATH, "disaster.db")

def calculate_severity(title):
    score = 0
    if "เสียชีวิต" in title:
        score += 2
    if "บาดเจ็บ" in title:
        score += 1
    if "อพยพด่วน" in title:
        score += 1
    if "เสียหายหนัก" in title:
        score += 1
    if "เตือนภัย" in title:
        score += 1
    
    # กำหนดขั้นต่ำ = 1 และสูงสุด = 5
    return min(max(score, 1), 5)

# ✨ ฟังก์ชันใหม่: แปลงคะแนนเป็น สี และ ระดับข้อความ
def get_severity_display(score):
    if score >= 4:
        return "🔴 ระดับวิกฤต (High)"
    elif score >= 2:
        return "🟠 ระดับปานกลาง (Medium)"
    else:
        return "🟡 ระดับเฝ้าระวัง (Low)"

def generate_summary():
    # เช็คว่ามีไฟล์ Database หรือยัง
    if not os.path.exists(DB_NAME):
        print(f"❌ ไม่พบไฟล์ฐานข้อมูลที่: {DB_NAME}")
        return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # ดึงข้อมูลโดย Join ตาราง news กับ provinces
    c.execute("""
        SELECT n.title, p.name_th
        FROM news n
        LEFT JOIN provinces p ON n.province_id = p.id
        WHERE n.title LIKE '%แผ่นดินไหว%'
           OR n.title LIKE '%น้ำท่วม%'
    """)

    rows = c.fetchall()
    conn.close()

    print("\n===== 🚨 DISASTER REPORT 🚨 =====\n")
    
    if not rows:
        print("✅ ปัจจุบันไม่มีรายงานน้ำท่วมหรือแผ่นดินไหวครับ")
        return

    for title, province in rows:
        province_name = province if province else "ไม่ระบุจังหวัด"
        severity_score = calculate_severity(title)
        
        # ✨ เรียกใช้ฟังก์ชันแปลงสีตรงนี้
        severity_display = get_severity_display(severity_score)

        if "แผ่นดินไหว" in title:
            disaster_type = "🌍 แผ่นดินไหว"
        elif "น้ำท่วม" in title:
            disaster_type = "🌊 น้ำท่วม"
        else:
            continue

        print(f"{disaster_type}")
        print(f"📌 สถานที่: {province_name}")
        # ✨ แสดงผลลัพธ์แบบใหม่ที่อ่านง่ายขึ้น
        print(f"⚠️ ความรุนแรง: {severity_display}")
        print(f"📰 หัวข้อข่าว: {title}") 
        print("-" * 50)

if __name__ == "__main__":
    generate_summary()