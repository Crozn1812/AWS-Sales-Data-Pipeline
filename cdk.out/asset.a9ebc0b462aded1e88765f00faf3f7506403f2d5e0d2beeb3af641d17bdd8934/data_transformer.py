import pandas as pd

def clean_and_transform_sales_data(df: pd.DataFrame):
    """
    Nhận vào DataFrame gốc, phân tách thành dữ liệu sạch (df_clean) và dữ liệu lỗi (df_error)
    dựa trên các quy tắc nghiệp vụ khắt khe.
    """
    
    # 1. Tách Dữ liệu Khuyết thiếu: Lọc bỏ các dòng thiếu quantity hoặc unit_price
    missing_mask = df[['quantity', 'unit_price']].isnull().any(axis=1)
    df_missing = df[missing_mask]
    df_valid = df[~missing_mask]

    # 2. Lọc Dữ liệu Âm / Không hợp lệ: Từ chối các bản ghi có số lượng hoặc đơn giá <= 0
    negative_mask = (df_valid['quantity'] <= 0) | (df_valid['unit_price'] <= 0)
    df_negative = df_valid[negative_mask]
    df_valid = df_valid[~negative_mask]

    # 3. Xóa Trùng lặp: Giữ lại bản ghi đầu tiên, các bản sao bị đẩy vào kho lưu trữ lỗi
    duplicates_mask = df_valid.duplicated(subset=['order_id', 'product_id'], keep='first')
    df_duplicates = df_valid[duplicates_mask]
    
    # 4. Tạo Tập Dữ Liệu Sạch (Good Records)
    df_clean = df_valid[~duplicates_mask].copy()

    # 5. Tính toán Doanh thu dòng: line_revenue = quantity * unit_price
    if not df_clean.empty:
        df_clean['line_revenue'] = df_clean['quantity'] * df_clean['unit_price']
    else:
        df_clean['line_revenue'] = pd.Series(dtype='float64')

    # 6. Hợp nhất Dữ Liệu Lỗi (Bad Records) để phục vụ đối soát
    df_error = pd.concat([df_missing, df_negative, df_duplicates], ignore_index=True)

    return df_clean, df_error