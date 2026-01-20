from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from typing import Optional

app = FastAPI()

# --- CONFIG ---
db_password = quote_plus("teezaza123") # <--- 🔑 แก้รหัสผ่านของคุณ
DB_CONNECTION_STR = f'postgresql://postgres:{db_password}@localhost:5432/safety_db'
engine = create_engine(DB_CONNECTION_STR)

# ฟังก์ชันช่วยสร้าง WHERE Clause ตาม Filter ที่เลือก
def build_filter(year, month, team, rep):
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
            "reps": [row[0] for row in reps]
        }

# 2. API สำหรับ KPI Cards (ยอดขาย & ยอด Shop)
@app.get("/api/kpi")
def get_kpi(year: int, month: Optional[str] = 'All', team: Optional[str] = 'All', rep: Optional[str] = 'All'):
    where, params = build_filter(year, month, team, rep)
    
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
def get_compare_year(year: int, team: Optional[str] = 'All', rep: Optional[str] = 'All'):
    # สร้าง Filter แบบไม่เอา "ปี" และ "เดือน" (เพราะเราจะดึง 2 ปีมาเทียบกันรายเดือน)
    conditions = []
    params = {'y1': year, 'y2': year - 1}
    
    if team and team != 'All':
        conditions.append("sales_team = :team")
        params['team'] = team
    if rep and rep != 'All':
        conditions.append("sales_rep_name = :rep")
        params['rep'] = rep
        
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
def get_ranking(year: int, month: Optional[str] = 'All', team: Optional[str] = 'All', rep: Optional[str] = 'All'):
    where, params = build_filter(year, month, team, rep)
    
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