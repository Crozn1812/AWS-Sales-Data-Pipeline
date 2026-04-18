import pandas as pd
import pytest
import sys
import os

# Thêm thư mục src vào đường dẫn để import được hàm logic
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from data_transformer import clean_and_transform_sales_data


def test_happy_path_and_cleaning_logic():
    """
    Kiểm thử tích hợp các kịch bản: dữ liệu chuẩn, dữ liệu âm, khuyết thiếu và trùng lặp.
    """

    data = {
        "order_id": ["ORD001", "ORD001", "ORD002", "ORD003", "ORD004"],
        "product_id": ["PROD1", "PROD1", "PROD2", "PROD3", "PROD4"],
        "order_date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
        "quantity": [2, -1, None, 5, 1],
        "unit_price": [10.0, 10.0, 20.0, 15.0, 10.0],
        "payment_status": ["paid", "paid", "pending", "paid", "paid"],
    }

    df_raw = pd.DataFrame(data)

    df_clean, df_error = clean_and_transform_sales_data(df_raw)

    # Chỉ ORD001 (bản ghi đầu tiên), ORD003 và ORD004 là hợp lệ
    assert len(df_clean) == 3, "Dữ liệu sạch phải chứa đúng 3 dòng"

    # Kiểm tra line_revenue của ORD003 = 5 * 15 = 75
    assert df_clean[df_clean["order_id"] == "ORD003"]["line_revenue"].iloc[0] == 75.0

    # Dữ liệu lỗi gồm:
    # - ORD001 bản duplicate có quantity = -1 -> invalid
    # - ORD002 có quantity = None -> missing
    assert len(df_error) == 2, "Kho dữ liệu lỗi phải chứa đúng 2 dòng dị thường"


def test_missing_required_columns():
    df = pd.DataFrame({
        "order_id": ["ORD001"],
        "product_id": ["PROD1"],
        "quantity": [1],
        "unit_price": [10.0],
    })

    with pytest.raises(ValueError):
        clean_and_transform_sales_data(df)


def test_payment_status_is_preserved_in_clean_data():
    data = {
        "order_id": ["ORD001", "ORD002"],
        "product_id": ["PROD1", "PROD2"],
        "order_date": ["2024-01-15", "2024-01-15"],
        "quantity": [2, 3],
        "unit_price": [10.0, 20.0],
        "payment_status": ["paid", "pending"],
    }

    df = pd.DataFrame(data)
    df_clean, df_error = clean_and_transform_sales_data(df)

    assert len(df_clean) == 2
    assert len(df_error) == 0
    assert "payment_status" in df_clean.columns
    assert set(df_clean["payment_status"]) == {"paid", "pending"}

def test_duplicate_rows():
    data = {
        "order_id": ["ORD100", "ORD100"],
        "product_id": ["PROD1", "PROD1"],
        "order_date": ["2024-01-15", "2024-01-15"],
        "quantity": [1, 1],
        "unit_price": [10.0, 10.0],
        "payment_status": ["paid", "paid"],
    }

    df = pd.DataFrame(data)
    df_clean, df_error = clean_and_transform_sales_data(df)

    assert len(df_clean) == 1
    assert len(df_error) == 1


def test_negative_quantity():
    data = {
        "order_id": ["ORD200"],
        "product_id": ["PROD2"],
        "order_date": ["2024-01-16"],
        "quantity": [-1],
        "unit_price": [10.0],
        "payment_status": ["paid"],
    }

    df = pd.DataFrame(data)
    df_clean, df_error = clean_and_transform_sales_data(df)

    assert len(df_clean) == 0
    assert len(df_error) == 1