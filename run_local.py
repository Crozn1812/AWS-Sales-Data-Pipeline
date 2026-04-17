import pandas as pd
import sys
import os

# Thêm thư mục src vào hệ thống để gọi hàm của bạn
sys.path.insert(0, os.path.abspath('src'))
from data_transformer import clean_and_transform_sales_data

print("1. Đang bơm file data/sample_sales_data.csv vào hệ thống...")
# Đọc file CSV thực tế mà đề bài cho
df_raw = pd.read_csv('data/sample_sales_data.csv')

print("2. Đang kích hoạt lõi lọc dữ liệu Pandas...\n")
# Chạy qua "cỗ máy" bạn đã viết
df_clean, df_error = clean_and_transform_sales_data(df_raw)

print("==== KẾT QUẢ VẬN HÀNH HỆ THỐNG ====")
print(f"🔹 Tổng số dòng ban đầu: {len(df_raw)}")
print(f"✅ Số dòng SẠCH (Hợp lệ): {len(df_clean)}")
print(f"❌ Số dòng LỖI (Bị loại bỏ): {len(df_error)}")

print("\n==== BÁO CÁO NGHIỆP VỤ (Đáp ứng BR-3) ====")
# 1. Doanh thu theo ngày
daily_revenue = df_clean.groupby('order_date')['line_revenue'].sum()
print("💰 Tổng doanh thu theo ngày:")
print(daily_revenue.to_string())

# 2. Top sản phẩm bán chạy nhất
top_products = df_clean.groupby('product_id')['quantity'].sum().sort_values(ascending=False).head(3)
print("\n🏆 Top 3 Sản phẩm bán chạy nhất:")
print(top_products.to_string())

# 3. Tỷ lệ thanh toán thành công
success_rate = (df_clean['payment_status'] == 'paid').mean() * 100
print(f"\n💳 Tỷ lệ thanh toán thành công: {success_rate:.2f}%")