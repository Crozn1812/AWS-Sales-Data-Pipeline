import pandas as pd


REQUIRED_COLUMNS = [
    "order_id",
    "product_id",
    "order_date",
    "quantity",
    "unit_price",
    "payment_status",
]


def clean_and_transform_sales_data(df: pd.DataFrame):
    """
    Trả về:
    - df_clean: dữ liệu sạch
    - df_error: dữ liệu lỗi / bị loại
    """

    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Thiếu cột bắt buộc: {missing_cols}")

    working_df = df.copy()

    # Chuẩn hóa kiểu dữ liệu
    working_df["quantity"] = pd.to_numeric(working_df["quantity"], errors="coerce")
    working_df["unit_price"] = pd.to_numeric(working_df["unit_price"], errors="coerce")
    working_df["payment_status"] = working_df["payment_status"].astype(str).str.strip().str.lower()

    # 1. Missing values
    missing_mask = working_df[["quantity", "unit_price"]].isnull().any(axis=1)
    df_missing = working_df[missing_mask]
    df_valid = working_df[~missing_mask].copy()

    # 2. Invalid numeric values
    invalid_mask = (df_valid["quantity"] <= 0) | (df_valid["unit_price"] <= 0)
    df_invalid = df_valid[invalid_mask]
    df_valid = df_valid[~invalid_mask].copy()

    # 3. Duplicates
    duplicate_mask = df_valid.duplicated(subset=["order_id", "product_id"], keep="first")
    df_duplicates = df_valid[duplicate_mask]
    df_valid = df_valid[~duplicate_mask].copy()

    # 4. Clean dataset
    df_clean = df_valid.copy()
    if not df_clean.empty:
        df_clean["line_revenue"] = df_clean["quantity"] * df_clean["unit_price"]
    else:
        df_clean["line_revenue"] = pd.Series(dtype="float64")

    # 5. Error dataset
    df_error = pd.concat(
        [df_missing, df_invalid, df_duplicates],
        ignore_index=True
    )

    return df_clean, df_error