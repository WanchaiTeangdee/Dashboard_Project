from fastapi import FastAPI, Query, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from typing import Optional
from pydantic import BaseModel
from datetime import datetime
from io import BytesIO
import pandas as pd
from openpyxl.utils import get_column_letter

from etl_engine import process_excel_bytes

app = FastAPI()

# --- CONFIG ---
db_password = quote_plus("teezaza123") # <--- 🔑 แก้รหัสผ่านของคุณ
DB_CONNECTION_STR = f'postgresql://postgres:{db_password}@localhost:5432/safety_db'
engine = create_engine(DB_CONNECTION_STR)

PROVINCES_BY_REGION = {
    "ภาคเหนือ": [
        "เชียงใหม่", "เชียงราย", "ลำพูน", "ลำปาง", "แพร่", "น่าน", "พะเยา", "แม่ฮ่องสอน",
        "ตาก", "สุโขทัย", "พิษณุโลก", "พิจิตร", "เพชรบูรณ์", "อุตรดิตถ์", "กำแพงเพชร",
        "นครสวรรค์", "อุทัยธานี"
    ],
    "ภาคตะวันออกเฉียงเหนือ": [
        "นครราชสีมา", "บุรีรัมย์", "สุรินทร์", "ศรีสะเกษ", "อุบลราชธานี", "ยโสธร", "ชัยภูมิ",
        "อำนาจเจริญ", "บึงกาฬ", "หนองบัวลำภู", "ขอนแก่น", "อุดรธานี", "เลย", "หนองคาย",
        "มหาสารคาม", "ร้อยเอ็ด", "กาฬสินธุ์", "สกลนคร", "นครพนม", "มุกดาหาร"
    ],
    "ภาคกลาง": [
        "กรุงเทพมหานคร", "นนทบุรี", "ปทุมธานี", "พระนครศรีอยุธยา", "อ่างทอง", "ลพบุรี",
        "สิงห์บุรี", "ชัยนาท", "สระบุรี", "นครปฐม", "สมุทรสาคร", "สมุทรสงคราม",
        "สมุทรปราการ", "สุพรรณบุรี"
    ],
    "ภาคตะวันออก": [
        "ชลบุรี", "ระยอง", "จันทบุรี", "ตราด", "ฉะเชิงเทรา", "ปราจีนบุรี", "สระแก้ว", "นครนายก"
    ],
    "ภาคตะวันตก": [
        "กาญจนบุรี", "ราชบุรี", "เพชรบุรี", "ประจวบคีรีขันธ์"
    ],
    "ภาคใต้": [
        "ชุมพร", "ระนอง", "สุราษฎร์ธานี", "พังงา", "ภูเก็ต", "กระบี่", "นครศรีธรรมราช",
        "ตรัง", "พัทลุง", "สตูล", "สงขลา", "ปัตตานี", "ยะลา", "นราธิวาส"
    ]
}
REGIONS = list(PROVINCES_BY_REGION.keys())
PROVINCES = [p for region in PROVINCES_BY_REGION.values() for p in region]

TEMPLATE_COLUMNS = [
    'วันที่/เดือน/ปี เอกสาร', 'เลขที่บิล', 'รหัสลูกค้า', 'ชื่อลูกค้า', 'จังหวัด',
    'รหัสพนักงานขาย', 'ชื่อพนักงาน', 'ทีม', 'รหัสสินค้า', 'กลุ่มสินค้า', 'รายละเอียด',
    'จำนวน', 'หน่วยนับ', '@', '%ส่วนลด', '%ลดท้ายบิล', 'หน่วยละ NON VAT', 'รวมเงิน NON VAT'
]

class TransactionIn(BaseModel):
    document_date: str
    invoice_no: Optional[str] = None
    customer_code: Optional[str] = None
    customer_name: Optional[str] = None
    province: Optional[str] = None
    sales_rep_code: Optional[str] = None
    sales_rep_name: Optional[str] = None
    sales_team: Optional[str] = None
    product_code: Optional[str] = None
    product_group: Optional[str] = None
    product_name: Optional[str] = None
    quantity: Optional[float] = 0
    unit_of_measure: Optional[str] = None
    unit_price: Optional[float] = 0
    discount_percent: Optional[float] = 0
    bill_discount_percent: Optional[float] = 0
    unit_price_non_vat: Optional[float] = 0
    total_amount_non_vat: Optional[float] = 0

# ฟังก์ชันช่วยสร้าง WHERE Clause ตาม Filter ที่เลือก
def build_filter(year, month, team, rep, region, province):
    conditions = []
    params = {}
    
    # Filter บังคับ: ปี (ถ้าไม่ส่งมาจะใช้ปีปัจจุบันในการเปรียบเทียบไม่ได้ แต่ในที่นี้เราจะ filter ตอน query)
    if year:
        conditions.append("EXTRACT(YEAR FROM document_date) = :year")
        params['year'] = year
    
    # Filter ทางเลือก
    if month and month != 'All':
        conditions.append("EXTRACT(MONTH FROM document_date) = :month")
        params['month'] = month
    if team and team != 'All':
        conditions.append("sales_team = :team")
        params['team'] = team
    if rep and rep != 'All':
        conditions.append("sales_rep_name = :rep")
        params['rep'] = rep
    if province and province != 'All':
        conditions.append("province = :province")
        params['province'] = province
    if region and region != 'All':
        region_provinces = PROVINCES_BY_REGION.get(region, [])
        if region_provinces:
            placeholders = []
            for idx, name in enumerate(region_provinces):
                key = f"region_prov_{idx}"
                placeholders.append(f":{key}")
                params[key] = name
            conditions.append(f"province IN ({', '.join(placeholders)})")
        else:
            conditions.append("1 = 0")
        
    where_clause = " AND ".join(conditions)
    if where_clause:
        where_clause = "WHERE " + where_clause
    else:
        where_clause = "" # ระวัง: ถ้าไม่มี filter เลยจะดึงหมด
        
    return where_clause, params

# 1. API สำหรับตัวเลือกใน Dropdown (Filters)
@app.get("/api/options")
def get_options():
    with engine.connect() as conn:
        years = conn.execute(text("SELECT DISTINCT EXTRACT(YEAR FROM document_date) FROM sales_transactions ORDER BY 1 DESC")).fetchall()
        teams = conn.execute(text("SELECT DISTINCT sales_team FROM sales_transactions WHERE sales_team IS NOT NULL ORDER BY 1")).fetchall()
        reps = conn.execute(text("SELECT DISTINCT sales_rep_name FROM sales_transactions WHERE sales_rep_name IS NOT NULL ORDER BY 1")).fetchall()
        
        return {
            "years": [int(row[0]) for row in years if row[0] is not None],
            "teams": [row[0] for row in teams],
            "reps": [row[0] for row in reps],
            "regions": REGIONS,
            "provinces_by_region": PROVINCES_BY_REGION,
            "provinces": PROVINCES
        }

# 1.0 API ดาวน์โหลดไฟล์ตัวอย่าง
@app.get("/api/template")
def download_template():
    df = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="template")
        ws = writer.book["template"]
        ws.freeze_panes = "A2"
        ws.row_dimensions[1].height = 22

        widths = {
            "A": 18, "B": 14, "C": 14, "D": 20, "E": 14, "F": 16, "G": 18,
            "H": 10, "I": 14, "J": 16, "K": 24, "L": 10, "M": 12, "N": 12,
            "O": 10, "P": 12, "Q": 16, "R": 18
        }
        for col in range(1, len(TEMPLATE_COLUMNS) + 1):
            col_letter = get_column_letter(col)
            ws.column_dimensions[col_letter].width = widths.get(col_letter, 14)
    output.seek(0)

    headers = {
        "Content-Disposition": "attachment; filename=sales_template.xlsx"
    }
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)

# 1.1 API อัปโหลดไฟล์ Excel
@app.post("/api/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="รองรับเฉพาะไฟล์ Excel (.xlsx, .xls)")

    content = await file.read()
    result = process_excel_bytes(content)
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "นำเข้าไฟล์ไม่สำเร็จ"))

    return {"success": True, "rows": result.get("rows", 0)}

# 1.2 API เพิ่มรายการขายแบบกรอกฟอร์ม
@app.post("/api/add_transaction")
def add_transaction(payload: TransactionIn):
    try:
        doc_date = datetime.strptime(payload.document_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="รูปแบบวันที่ต้องเป็น YYYY-MM-DD")

    insert_sql = text("""
        INSERT INTO sales_transactions (
            document_date, invoice_no, customer_code, customer_name, province,
            sales_rep_code, sales_rep_name, sales_team,
            product_code, product_group, product_name,
            quantity, unit_of_measure, unit_price, discount_percent, bill_discount_percent,
            unit_price_non_vat, total_amount_non_vat
        ) VALUES (
            :document_date, :invoice_no, :customer_code, :customer_name, :province,
            :sales_rep_code, :sales_rep_name, :sales_team,
            :product_code, :product_group, :product_name,
            :quantity, :unit_of_measure, :unit_price, :discount_percent, :bill_discount_percent,
            :unit_price_non_vat, :total_amount_non_vat
        )
    """)

    params = {
        "document_date": doc_date,
        "invoice_no": payload.invoice_no,
        "customer_code": payload.customer_code,
        "customer_name": payload.customer_name,
        "province": payload.province,
        "sales_rep_code": payload.sales_rep_code,
        "sales_rep_name": payload.sales_rep_name,
        "sales_team": payload.sales_team,
        "product_code": payload.product_code,
        "product_group": payload.product_group,
        "product_name": payload.product_name,
        "quantity": payload.quantity or 0,
        "unit_of_measure": payload.unit_of_measure,
        "unit_price": payload.unit_price or 0,
        "discount_percent": payload.discount_percent or 0,
        "bill_discount_percent": payload.bill_discount_percent or 0,
        "unit_price_non_vat": payload.unit_price_non_vat or 0,
        "total_amount_non_vat": payload.total_amount_non_vat or 0
    }

    with engine.connect() as conn:
        conn.execute(insert_sql, params)
        conn.commit()

    return {"success": True}

# 2. API สำหรับ KPI Cards (ยอดขาย & ยอด Shop)
@app.get("/api/kpi")
def get_kpi(
    year: int,
    month: Optional[str] = 'All',
    team: Optional[str] = 'All',
    rep: Optional[str] = 'All',
    region: Optional[str] = 'All',
    province: Optional[str] = 'All'
):
    where, params = build_filter(year, month, team, rep, region, province)
    
    # ถ้าเลือกเดือน -> ต้องหายอดสะสม (YTD) ถึงเดือนนั้น
    # ถ้าไม่เลือกเดือน -> ยอดสะสมคือทั้งปี
    
    sql = f"""
        SELECT 
            -- 1. ยอดขายรวม (ตาม Filter)
            COALESCE(SUM(total_amount_non_vat), 0) as sales_selected,
            -- 2. จำนวน Shop (ตาม Filter)
            COUNT(DISTINCT customer_code) as shop_selected
        FROM sales_transactions
        {where}
    """
    
    # หา YTD (สะสมตั้งแต่ต้นปี) - กรณีมีการเลือกเดือน
    ytd_where = ""
    ytd_params = params.copy()
    if month and month != 'All':
        # เปลี่ยนเงื่อนไขเดือน เป็น <= เดือนที่เลือก
        base_conditions = [c for c in where.replace("WHERE ", "").split(" AND ") if "EXTRACT(MONTH" not in c]
        base_conditions.append("EXTRACT(MONTH FROM document_date) <= :month")
        ytd_where = "WHERE " + " AND ".join(base_conditions)
    else:
        ytd_where = where # ถ้าไม่เลือกเดือน YTD ก็คือค่าเดียวกับ Selected
        
    sql_ytd = f"""
        SELECT 
            COALESCE(SUM(total_amount_non_vat), 0) as sales_ytd,
            COUNT(DISTINCT customer_code) as shop_ytd
        FROM sales_transactions
        {ytd_where}
    """

    with engine.connect() as conn:
        curr = conn.execute(text(sql), params).fetchone()
        ytd = conn.execute(text(sql_ytd), ytd_params).fetchone()
        
        return {
            "sales_period": float(curr[0]),
            "shop_period": int(curr[1]),
            "sales_accum": float(ytd[0]),
            "shop_accum": int(ytd[1])
        }

# 3. API กราฟเปรียบเทียบปี (Year vs Year)
@app.get("/api/compare_year")
def get_compare_year(
    year: int,
    team: Optional[str] = 'All',
    rep: Optional[str] = 'All',
    region: Optional[str] = 'All',
    province: Optional[str] = 'All'
):
    # สร้าง Filter แบบไม่เอา "ปี" และ "เดือน" (เพราะเราจะดึง 2 ปีมาเทียบกันรายเดือน)
    conditions = []
    params = {'y1': year, 'y2': year - 1}
    
    if team and team != 'All':
        conditions.append("sales_team = :team")
        params['team'] = team
    if rep and rep != 'All':
        conditions.append("sales_rep_name = :rep")
        params['rep'] = rep
    if province and province != 'All':
        conditions.append("province = :province")
        params['province'] = province
    if region and region != 'All':
        region_provinces = PROVINCES_BY_REGION.get(region, [])
        if region_provinces:
            placeholders = []
            for idx, name in enumerate(region_provinces):
                key = f"region_prov_{idx}"
                placeholders.append(f":{key}")
                params[key] = name
            conditions.append(f"province IN ({', '.join(placeholders)})")
        else:
            conditions.append("1 = 0")
        
    base_where = " AND ".join(conditions)
    if base_where: base_where = "AND " + base_where

    sql = f"""
        SELECT 
            EXTRACT(MONTH FROM document_date) as m,
            SUM(CASE WHEN EXTRACT(YEAR FROM document_date) = :y1 THEN total_amount_non_vat ELSE 0 END) as sales_current,
            SUM(CASE WHEN EXTRACT(YEAR FROM document_date) = :y2 THEN total_amount_non_vat ELSE 0 END) as sales_prev
        FROM sales_transactions
        WHERE EXTRACT(YEAR FROM document_date) IN (:y1, :y2)
        {base_where}
        GROUP BY m
        ORDER BY m
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(sql), params).fetchall()
        
        # จัด Data ให้ครบ 12 เดือน (กันเหนียวเผื่อเดือนไหนไม่มีขาย)
        months = list(range(1, 13))
        data_map = {int(row[0]): (float(row[1]), float(row[2])) for row in result}
        
        return {
            "labels": ["ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."],
            "current_year": [data_map.get(m, (0,0))[0] for m in months],
            "prev_year": [data_map.get(m, (0,0))[1] for m in months]
        }

# 4. API Top 10 Ranking
@app.get("/api/ranking")
def get_ranking(
    year: int,
    month: Optional[str] = 'All',
    team: Optional[str] = 'All',
    rep: Optional[str] = 'All',
    region: Optional[str] = 'All',
    province: Optional[str] = 'All'
):
    where, params = build_filter(year, month, team, rep, region, province)
    
    with engine.connect() as conn:
        # Top 10 Products
        prod_sql = f"""
            SELECT product_name, SUM(total_amount_non_vat) as total
            FROM sales_transactions {where}
            GROUP BY product_name ORDER BY total DESC LIMIT 10
        """
        top_products = conn.execute(text(prod_sql), params).fetchall()
        
        # Top 10 Customers
        cust_sql = f"""
            SELECT customer_name, SUM(total_amount_non_vat) as total
            FROM sales_transactions {where}
            GROUP BY customer_name ORDER BY total DESC LIMIT 10
        """
        top_customers = conn.execute(text(cust_sql), params).fetchall()
        
        return {
            "products": [{"label": row[0], "value": float(row[1])} for row in top_products],
            "customers": [{"label": row[0], "value": float(row[1])} for row in top_customers]
        }

app.mount("/", StaticFiles(directory="static", html=True), name="static")