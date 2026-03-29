import folium

# สร้างแผนที่ประเทศไทย
m = folium.Map(location=[15.8700, 100.9925], zoom_start=6, tiles="CartoDB positron")

# 🔴 หมุดที่ 1: เชียงราย (วิกฤต - แดง)
popup_chiangrai = """
<div style="font-family: Tahoma; min-width: 250px;">
    <h4 style="margin-top:0px; color:red;">🚨 เชียงราย</h4>
    <b>⚠️ ความรุนแรง:</b> วิกฤต (High)<br><br>
    <b>พาดหัวข่าว:</b> <br><i>ด่วน! น้ำท่วมฉับพลัน แม่น้ำสายทะลักเข้าท่วมตัวเมือง อพยพชาวบ้านวุ่น</i>
</div>
"""
folium.Marker(
    location=[19.9105, 99.8406], popup=folium.Popup(popup_chiangrai, max_width=300),
    tooltip="เชียงราย - วิกฤต (High)", icon=folium.Icon(color='red', icon='info-sign')
).add_to(m)

# 🟠 หมุดที่ 2: เชียงใหม่ (ปานกลาง - ส้ม)
popup_chiangmai = """
<div style="font-family: Tahoma; min-width: 250px;">
    <h4 style="margin-top:0px; color:orange;">🚨 เชียงใหม่</h4>
    <b>⚠️ ความรุนแรง:</b> ปานกลาง (Medium)<br><br>
    <b>พาดหัวข่าว:</b> <br><i>แผ่นดินไหวขนาด 3.2 ศูนย์กลางสันทราย มีผู้บาดเจ็บเล็กน้อย</i>
</div>
"""
folium.Marker(
    location=[18.7883, 98.9853], popup=folium.Popup(popup_chiangmai, max_width=300),
    tooltip="เชียงใหม่ - ปานกลาง (Medium)", icon=folium.Icon(color='orange', icon='info-sign')
).add_to(m)

# 🟡 หมุดที่ 3: หนองคาย (เฝ้าระวัง - เขียวอ่อน)
popup_nongkhai = """
<div style="font-family: Tahoma; min-width: 250px;">
    <h4 style="margin-top:0px; color:lightgreen;">🚨 หนองคาย</h4>
    <b>⚠️ ความรุนแรง:</b> เฝ้าระวัง (Low)<br><br>
    <b>พาดหัวข่าว:</b> <br><i>ประกาศเตือน เฝ้าระวังระดับน้ำโขงเพิ่มสูงขึ้นต่อเนื่อง</i>
</div>
"""
folium.Marker(
    location=[17.8785, 102.7420], popup=folium.Popup(popup_nongkhai, max_width=300),
    tooltip="หนองคาย - เฝ้าระวัง (Low)", icon=folium.Icon(color='lightgreen', icon='info-sign')
).add_to(m)

# บันทึกไฟล์
m.save("mock_disaster_map.html")
print("✅ สร้างไฟล์ mock_disaster_map.html สำเร็จ! ดับเบิลคลิกเปิดดูได้เลย")