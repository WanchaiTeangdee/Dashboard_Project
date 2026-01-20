import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# --- CONFIG ---
db_password = quote_plus("teezaza123") # <--- อย่าลืมแก้รหัสผ่าน!
DB_CONNECTION_STR = f'postgresql://postgres:{db_password}@localhost:5432/safety_db'

def process_excel_file(file_path):
    print(f"🚀 กำลังเริ่มระบบแก้ไข v2: อ่านไฟล์ {file_path} ...")
    
    try:
        # 1. อ่าน Excel
        df = pd.read_excel(file_path)
        
        # ปริ้นท์ชื่อคอลัมน์ให้ดูหน่อย ว่า Python เห็นเป็นชื่ออะไร
        print(f"👀 รายชื่อคอลัมน์ที่เจอ: {list(df.columns)}")
        
        # CLEAN HEADERS
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

        # --- แก้ปัญหาโลกแตก: ถ้าหาชื่อไม่เจอ ให้บังคับเอาคอลัมน์แรกเป็น document_date เลย ---
        # ชื่อคอลัมน์ที่เราคาดหวัง
        expected_date_col = 'วันที่/เดือน/ปี เอกสาร'
        
        if expected_date_col not in df.columns:
            first_col_name = df.columns[0]
            print(f"⚠️ หาชื่อ '{expected_date_col}' ไม่เจอ! ระบบจะใช้คอลัมน์แรก '{first_col_name}' แทนอัตโนมัติ")
            df = df.rename(columns={first_col_name: 'document_date'})
        
        # 2. RENAME ส่วนที่เหลือ
        column_mapping = {
            'วันที่/เดือน/ปี เอกสาร': 'document_date', # (ถ้ามีชื่อตรงก็ใช้อันนี้)
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
        
        # ตรวจสอบว่ามีคอลัมน์ document_date หรือยัง
        if 'document_date' not in df.columns:
            print("❌ Error: ยังหาคอลัมน์วันที่ไม่เจอ โปรดเช็คไฟล์ Excel ว่าคอลัมน์แรกเป็นวันที่หรือไม่")
            return False
            
        # เลือกคอลัมน์ (ต้องระวังไม่ให้กรอง document_date ออก)
        valid_cols = list(column_mapping.values())
        # เพิ่ม document_date เข้าไปในรายการ valid_cols แน่ๆ
        if 'document_date' not in valid_cols: valid_cols.append('document_date')
            
        final_cols = df.columns.intersection(valid_cols)
        df = df[final_cols]

        # 3. CLEAN DATA (ตัวกรองขยะ)
        df['document_date'] = pd.to_datetime(df['document_date'], dayfirst=True, errors='coerce')
        
        print(f"   🔎 ข้อมูลดิบก่อนกรอง: {len(df)} แถว")
        df = df.dropna(subset=['document_date'])
        print(f"   🧹 กรองแถวว่างทิ้งไปเหลือ: {len(df)} แถว")

        # แปลงตัวเลข
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
            
            print(f"✅ สำเร็จ! นำเข้าข้อมูลจำนวน {len(df)} แถว เข้า Database เรียบร้อย")
            return True
        else:
            print("⚠️ Warning: ไม่เหลือข้อมูลให้นำเข้าเลย")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        return False