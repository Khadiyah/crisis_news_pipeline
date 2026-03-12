import sqlite3
import pandas as pd
import os
import requests
from transformers import pipeline

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1478580196314452050/Rbo7zMAcUw3csQWV5mpoj3JCSBDtjRLbkvTw69H5G1OC18yKgx59-QR3fFtKlE_rPA7t"
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_PATH, "disaster.db")

def send_ai_discord_alert(province, dis_type, sev_level):
    """ส่งผลวิเคราะห์ AI เข้า Discord"""
    if sev_level >= 4:
        color, label = 15158332, "High"
    elif sev_level >= 2:
        color, label = 15105570, "Medium"
    else:
        color, label = 16776960, "Low"

    payload = {
        "username": "Warning Center (AI)",
        "embeds": [{
            "title": "🤖 AI Analysis Result",
            "description": f"**จังหวัด:** {province}\n**ประเภทภัย:** {dis_type}\n**ความรุนแรงสุทธิ:** {label}",
            "color": color,
            "footer": {"text": "Analyzed by WangchanBERTa"}
        }]
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload)
    except: pass

sentiment_analyzer = pipeline("sentiment-analysis", model="poom-sci/WangchanBERTa-finetuned-sentiment")

def get_ai_sentiment(text):
    try:
        result = sentiment_analyzer(text[:512])[0]
        label = result['label'].upper()
        if 'NEG' in label: return 'Negative'
        elif 'POS' in label: return 'Positive'
        else: return 'Neutral'
    except:
        return 'Neutral'

# ✨ เพิ่มระบบประเมินความเสี่ยงจากคำศัพท์ (Hybrid System)
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
    else: return "Other"

# ✨ เพิ่มฟังก์ชันแปลงคะแนนเป็นสีตรงนี้ครับ
def get_severity_display(score):
    if score >= 4:
        return "🔴 ระดับวิกฤต (High)"
    elif score >= 2:
        return "🟠 ระดับปานกลาง (Medium)"
    else:
        return "🟡 ระดับเฝ้าระวัง (Low)"

def run_ai_scoring():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    try:
        c.execute("ALTER TABLE news ADD COLUMN sentiment TEXT")
        c.execute("ALTER TABLE news ADD COLUMN disaster_type TEXT")
    except sqlite3.OperationalError: pass 

    df = pd.read_sql_query("""
        SELECT n.id, n.title, p.name_th as province, n.province_id 
        FROM news n 
        JOIN provinces p ON n.province_id = p.id
    """, conn)

    if df.empty:
        print("✅ ไม่มีข้อมูลข่าวในระบบให้วิเคราะห์")
        return

    print("🤖 AI และระบบ Hybrid กำลังวิเคราะห์ข้อมูล...")
    
    # 1. ให้ AI วิเคราะห์ Sentiment
    df['sentiment'] = df['title'].apply(get_ai_sentiment)
    # 2. ให้ระบบ Keyword วิเคราะห์ความรุนแรง
    df['keyword_risk'] = df['title'].apply(calculate_keyword_risk)
    # 3. จำแนกประเภท
    df['disaster_type'] = df['title'].apply(classify_disaster)

    # แปลง AI Sentiment เป็นคะแนนสมทบ
    df['sent_score'] = df['sentiment'].map({'Negative': 1, 'Neutral': 0, 'Positive': -1})

    summary = df.groupby(['province_id', 'province', 'disaster_type']).agg(
        frequency=('id', 'count'),          
        total_sent_score=('sent_score', 'sum'),
        total_keyword_risk=('keyword_risk', 'sum') 
    ).reset_index()

    # 🌟 สูตรคำนวณ Severity ขั้นเทพ: ความถี่ข่าว + AI Score + Keyword Score
    summary['raw_severity'] = summary['frequency'] + summary['total_sent_score'] + summary['total_keyword_risk']
    summary['severity_level'] = summary['raw_severity'].apply(lambda x: min(max(int(x), 1), 5))

    print("\n===== 🚨 สรุปความรุนแรงภัยพิบัติ (HYBRID SEVERITY REPORT) 🚨 =====")
    
    for index, row in summary.iterrows():
        # ส่งเข้า Discord รายจังหวัด
        send_ai_discord_alert(row['province'], row['disaster_type'], row['severity_level'])
        print(f"🤖 AI Alert Sent: {row['province']}")

    for index, row in df.iterrows():
        c.execute("UPDATE news SET sentiment = ?, disaster_type = ? WHERE id = ?", (row['sentiment'], row['disaster_type'], row['id']))

    conn.commit()
    conn.close()
    print("✅ ระบบ Hybrid วิเคราะห์และอัปเดต Severity Score เสร็จสมบูรณ์!")

if __name__ == "__main__":
    run_ai_scoring()