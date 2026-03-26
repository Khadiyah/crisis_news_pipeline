import discord
from discord.ext import commands
import psycopg2 
import os

# --- 1. CONFIGURATION ---
TOKEN = 'MTQ4MTU4NzA0MTYzMTMzODU4Nw.GiS52i.lEFeupjE1p6ogVaakD1fhopvP5TiIZlXKQm6I4'
TARGET_CHANNEL_NAME = "ask-bot"  # <--- ตั้งชื่อห้องที่อยากให้บอทตอบตรงนี้!

intents = discord.Intents.default()
intents.message_content = True 
bot = commands.Bot(command_prefix='/', intents=intents)

# ฟังก์ชันสำหรับเชื่อมต่อฐานข้อมูล
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="airflow",
        user="airflow",
        password="airflow",
        port="5432"
    )

@bot.event
async def on_ready():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # คำสั่งสร้างตารางแบบสมบูรณ์
        cur.execute("""
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
        conn.commit()
        cur.close()
        conn.close()
        print(f'✅ บอท {bot.user} ออนไลน์และเตรียม Database พร้อมแล้ว!')
    except Exception as e:
        print(f'❌ บอทเตรียม Database ไม่สำเร็จ: {e}')

# --- 2. COMMANDS ---

@bot.command()
async def check(ctx, province: str):
    # เช็กว่าพิมพ์ถูกห้องไหม
    if ctx.channel.name != TARGET_CHANNEL_NAME:
        return # ถ้าไม่ใช่ห้องที่กำหนด บอทจะนิ่งเฉย ไม่ตอบโต้

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = "SELECT title, severity_score, link FROM disaster_news WHERE province LIKE %s ORDER BY created_at DESC LIMIT 1"
        cur.execute(query, (f'%{province}%',))
        result = cur.fetchone()

        if result:
            title, score, link = result
            level = "🔴 สูงวิกฤต" if score >= 4 else "🟠 ปานกลาง" if score >= 2 else "🟡 เฝ้าระวัง"
            
            embed = discord.Embed(title=f"🚨 รายงานด่วน: {province}", color=discord.Color.red())
            embed.add_field(name="หัวข้อข่าว", value=title, inline=False)
            embed.add_field(name="ระดับความรุนแรง", value=level, inline=True)
            embed.add_field(name="ลิงก์ข่าว", value=f"[คลิกเพื่ออ่าน]({link})", inline=True)
            await ctx.send(embed=embed)
        else:
            await ctx.send(f"❓ ยังไม่พบรายงานภัยพิบัติในพื้นที่ **{province}** ในระบบครับ")
        
        cur.close()
        conn.close()
    except Exception as e:
        await ctx.send(f"❌ ติดต่อฐานข้อมูลไม่ได้: {e}")

@bot.command()
async def report(ctx, *, detail: str):
    if ctx.channel.name != TARGET_CHANNEL_NAME:
        return
        
    # โชว์ความเทพ: ตอบกลับพร้อมขอบคุณ User
    embed = discord.Embed(title="✅ บันทึกรายงานสำเร็จ", color=discord.Color.green())
    embed.description = f"**รายละเอียด:** {detail}\n\nขอบคุณที่ช่วยแจ้งเหตุครับ ข้อมูลนี้จะถูกนำไปประมวลผลต่อ"
    embed.set_footer(text="Reporting System by Crisis News Pipeline")
    await ctx.send(embed=embed)

# --- 3. ERROR HANDLING (ระบบลบข้อความที่พิมพ์ผิด) ---
@bot.event
async def on_command_error(ctx, error):
    # ถ้าพิมพ์คำสั่งผิด (CommandNotFound) ในห้อง ask-bot ให้ลบทิ้งเพื่อความสะอาด
    if isinstance(error, commands.CommandNotFound) and ctx.channel.name == TARGET_CHANNEL_NAME:
        await ctx.message.delete()
        msg = await ctx.send(f"❌ ไม่พบคำสั่งนี้ครับ {ctx.author.mention} (จะลบข้อความนี้อัตโนมัติใน 5 วินาที)")
        await msg.delete(delay=5)

bot.run(TOKEN)