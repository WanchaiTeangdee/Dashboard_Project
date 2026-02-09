import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from io import BytesIO

# --- CONFIG ---
# แก้รหัสผ่านให้ตรงกับของคุณ
db_password = quote_plus("teezaza123") 
DB_CONNECTION_STR = f'postgresql://postgres:{db_password}@localhost:5432/safety_db'

def _process_dataframe(df, batch_id=None):
    # CLEAN HEADERS: ตัดช่องว่างหน้า-หลังชื่อคอลัมน์ + ลดช่องว่างซ้ำ
    df.columns = df.columns.str.strip().str.replace(r"\s+", " ", regex=True)

    # รองรับชื่อคอลัมน์ที่สะกด/เว้นวรรคไม่ตรง
    alias_mapping = {
        'วันที่เอกสาร': 'document_date',
        'วันที่/เดือน/ปี': 'วันที่/เดือน/ปี เอกสาร',
        'ชือพนักงาน': 'ชื่อพนักงาน',
        '% ส่วนลด': '%ส่วนลด',
        '% ลดท้ายบิล': '%ลดท้ายบิล',
        'รายละเอีย ด': 'รายละเอียด',
        'ส่วนลด %': 'ส่วนลด%',
        'ส่วนลด % ': 'ส่วนลด%',
        'ส่วนลด %/': 'ส่วนลด%'
    }
    df = df.rename(columns=alias_mapping)

    # 2. RENAME: เปลี่ยนชื่อคอลัมน์
    column_mapping = {
        'วันที่เอกสาร': 'document_date',
        'วันที่/เดือน/ปี เอกสาร': 'document_date',
        'DATE': 'document_date',
        'DATEDOC': 'document_date',
        'Duc': 'document_date',
        'เลขที่บิล': 'invoice_no',
        'INV': 'invoice_no',
        'DOCNO': 'invoice_no',
        'รหัสลูกค้า/ชื่อลูกค้า': 'customer_code_name',
        'รหัสลูกค้า': 'customer_code',
        'รหัสลูกค้า.1': 'customer_code',
        'ACCID': 'customer_code',
        'ชื่อลูกค้า': 'customer_name',
        'XCOMP': 'customer_name',
        'จังหวัด': 'province',
        'รหัสพนักงานขาย': 'sales_rep_code',
        'รหัสผู้แทน': 'sales_rep_code',
        'ID': 'sales_rep_code',
        'ID_EM': 'sales_rep_code',
        'ชื่อพนักงาน': 'sales_rep_name',
        'ผู้แทน': 'sales_rep_name',
        'SNAME': 'sales_rep_name',
        'ทีม': 'sales_team',
        'TEAM': 'sales_team',
        'TEAMID': 'sales_team',
        'TEAMDESC': 'sales_team',
        'รหัสสินค้า': 'product_code',
        'กลุ่มสินค้า': 'product_group',
        'รายละเอียด': 'product_name',
        'ชื่อสินค้า': 'product_name',
        'XDESC': 'product_name',
        'จำนวน': 'quantity',
        'จน': 'quantity',
        'QUAN': 'quantity',
        'หน่วยนับ': 'unit_of_measure',
        'UNIT': 'unit_of_measure',
        '@': 'unit_price',
        'ราคาต่อหน่วย': 'unit_price',
        'PRICE': 'unit_price',
        '%ส่วนลด': 'discount_percent',
        'ส่วนลด%': 'discount_percent',
        'DISCL': 'discount_percent',
        '%ลดท้ายบิล': 'bill_discount_percent',
        'DISCD': 'bill_discount_percent',
        'หน่วยละ NON VAT': 'unit_price_non_vat',
        'รวมเงิน NON VAT': 'total_amount_non_vat',
        'INVAMT': 'total_amount_non_vat',
        'XNET': 'total_amount_non_vat',
        'ราคารวมvat': 'total_amount_non_vat',
        'VPRICE': 'total_amount_non_vat'
    }

    df = df.rename(columns=column_mapping)

    # กันคอลัมน์ซ้ำหลัง rename
    df = df.loc[:, ~df.columns.duplicated()]

    # ใช้รหัส/ชื่อลูกค้าจากคอลัมน์รวม ถ้าคอลัมน์หลักว่าง
    if 'customer_code_name' in df.columns:
        def _split_customer_code_name(value):
            if pd.isna(value):
                return None, None
            text_value = str(value).strip()
            if not text_value:
                return None, None
            if ':' in text_value:
                code, name = text_value.split(':', 1)
                return code.strip() or None, name.strip() or None
            return None, text_value

        extracted = df['customer_code_name'].apply(_split_customer_code_name)
        df['customer_code_from_name'] = extracted.map(lambda x: x[0])
        df['customer_name_from_name'] = extracted.map(lambda x: x[1])

        if 'customer_code' not in df.columns:
            df['customer_code'] = df['customer_code_from_name']
        else:
            df['customer_code'] = df['customer_code'].fillna(df['customer_code_from_name'])

        if 'customer_name' not in df.columns:
            df['customer_name'] = df['customer_name_from_name']
        else:
            df['customer_name'] = df['customer_name'].fillna(df['customer_name_from_name'])

        df = df.drop(columns=['customer_code_name', 'customer_code_from_name', 'customer_name_from_name'])

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
    if batch_id:
        df["batch_id"] = batch_id
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
        xl = pd.ExcelFile(file_path)
        preferred_sheets = ["DATA ปรับเขต", "DATA FULL", "2025", "2024"]
        sheet = next((s for s in preferred_sheets if s in xl.sheet_names), xl.sheet_names[0])
        df = xl.parse(sheet)
        success, _ = _process_dataframe(df)
        return success

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def process_excel_bytes(file_bytes, batch_id=None):
    try:
        xl = pd.ExcelFile(BytesIO(file_bytes))
        preferred_sheets = ["DATA ปรับเขต", "DATA FULL", "2025", "2024"]
        sheet = next((s for s in preferred_sheets if s in xl.sheet_names), xl.sheet_names[0])
        df = xl.parse(sheet)
        success, rows = _process_dataframe(df, batch_id=batch_id)
        return {"success": success, "rows": rows}
    except Exception as e:
        return {"success": False, "rows": 0, "error": str(e)}