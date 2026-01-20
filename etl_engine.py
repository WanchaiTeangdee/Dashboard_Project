import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from io import BytesIO

# --- CONFIG ---
# แก้รหัสผ่านให้ตรงกับของคุณ
db_password = quote_plus("teezaza123") 
DB_CONNECTION_STR = f'postgresql://postgres:{db_password}@localhost:5432/safety_db'

def _process_dataframe(df):
    # CLEAN HEADERS: ตัดช่องว่างหน้า-หลังชื่อคอลัมน์ + ลดช่องว่างซ้ำ
    df.columns = df.columns.str.strip().str.replace(r"\s+", " ", regex=True)

    # รองรับชื่อคอลัมน์ที่สะกด/เว้นวรรคไม่ตรง
    alias_mapping = {
        'วันที่/เดือน/ปี': 'วันที่/เดือน/ปี เอกสาร',
        'ชือพนักงาน': 'ชื่อพนักงาน',
        '% ส่วนลด': '%ส่วนลด',
        '% ลดท้ายบิล': '%ลดท้ายบิล',
        'รายละเอีย ด': 'รายละเอียด'
    }
    df = df.rename(columns=alias_mapping)

    # 2. RENAME: เปลี่ยนชื่อคอลัมน์
    column_mapping = {
        'วันที่/เดือน/ปี เอกสาร': 'document_date',
        'เลขที่บิล': 'invoice_no',
        'รหัสลูกค้า': 'customer_code',
        'ชื่อลูกค้า': 'customer_name',
        'จังหวัด': 'province',
        'รหัสพนักงานขาย': 'sales_rep_code',
        'ชื่อพนักงาน': 'sales_rep_name',
        'ทีม': 'sales_team',
        'รหัสสินค้า': 'product_code',
        'กลุ่มสินค้า': 'product_group',
        'รายละเอียด': 'product_name',
        'จำนวน': 'quantity', 
        'หน่วยนับ': 'unit_of_measure',
        '@': 'unit_price',
        '%ส่วนลด': 'discount_percent',
        '%ลดท้ายบิล': 'bill_discount_percent',
        'หน่วยละ NON VAT': 'unit_price_non_vat',
        'รวมเงิน NON VAT': 'total_amount_non_vat'
    }

    df = df.rename(columns=column_mapping)

    # เลือกเฉพาะคอลัมน์ที่รู้จัก
    valid_cols = list(column_mapping.values())
    final_cols = df.columns.intersection(valid_cols)
    df = df[final_cols]

    # 3. CLEAN DATA (จุดสำคัญ!)
    # แปลงวันที่
    if 'document_date' in df.columns:
        # แปลงเป็นวันที่ ถ้าช่องไหนไม่ใช่ให้เป็น NaT (ว่าง)
        df['document_date'] = pd.to_datetime(df['document_date'], dayfirst=True, errors='coerce')
        
        # --- [จุดฆ่าบั๊ก] ลบบรรทัดที่วันที่เป็นค่าว่างทิ้งไปเลย ---
        print(f"🔎 เจอข้อมูลทั้งหมด: {len(df)} แถว")
        df = df.dropna(subset=['document_date'])
        print(f"🧹 กรองแถวว่างออกเหลือ: {len(df)} แถว")

    # แปลงตัวเลข (เปลี่ยนช่องว่างเป็น 0)
    numeric_cols = ['quantity', 'unit_price', 'total_amount_non_vat', 
                   'discount_percent', 'bill_discount_percent', 'unit_price_non_vat']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 4. LOAD TO DATABASE
    if len(df) > 0:
        engine = create_engine(DB_CONNECTION_STR)
        with engine.connect() as conn:
             df.to_sql('sales_transactions', engine, index=False, if_exists='append')
        
        print(f"✅ Success! นำเข้าข้อมูลสำเร็จจำนวน {len(df)} แถว")
        return True, len(df)
    else:
        print("⚠️ Warning: ไม่เหลือข้อมูลให้นำเข้าเลย (อาจเพราะวันที่ผิด Format หมด)")
        return False, 0

def process_excel_file(file_path):
    print(f"กำลังอ่านไฟล์: {file_path} ...")
    
    try:
        # 1. อ่าน Excel
        df = pd.read_excel(file_path)
        success, _ = _process_dataframe(df)
        return success

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def process_excel_bytes(file_bytes):
    try:
        df = pd.read_excel(BytesIO(file_bytes))
        success, rows = _process_dataframe(df)
        return {"success": success, "rows": rows}
    except Exception as e:
        return {"success": False, "rows": 0, "error": str(e)}