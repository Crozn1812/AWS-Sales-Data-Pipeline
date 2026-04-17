import os
import boto3
import awswrangler as wr
from datetime import datetime
from data_transformer import clean_and_transform_sales_data

# Khởi tạo kết nối với DynamoDB
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    """
    Điểm vào của AWS Lambda. Sẽ được kích hoạt tự động bởi EventBridge khi có file mới.
    """
    # Lấy tên các tài nguyên từ biến môi trường (sẽ thiết lập ở phần Hạ tầng sau)
    processed_bucket = os.environ.get('PROCESSED_BUCKET', 'shopmart-processed-zone')
    error_bucket = os.environ.get('ERROR_BUCKET', 'shopmart-error-zone')
    table_name = os.environ.get('DYNAMODB_TABLE', 'PipelineMetadataTable')
    
    try:
        # 1. Trích xuất đường dẫn file từ sự kiện EventBridge
        detail = event.get('detail', {})
        source_bucket = detail.get('bucket', {}).get('name')
        file_key = detail.get('object', {}).get('key')
        
        if not source_bucket or not file_key:
            raise ValueError("Không tìm thấy thông tin bucket/key trong event.")
            
        s3_uri = f"s3://{source_bucket}/{file_key}"
        print(f"Đang tiến hành xử lý file: {s3_uri}")

        # 2. Đọc file CSV trực tiếp từ S3 vào RAM bằng awswrangler [cite: 1]
        df_raw = wr.s3.read_csv(path=s3_uri)
        
        # 3. Chạy logic làm sạch dữ liệu cốt lõi
        df_clean, df_error = clean_and_transform_sales_data(df_raw)
        
        # 4. Lưu Dữ Liệu Sạch (Parquet siêu nén, chia thư mục theo ngày) [cite: 1]
        if not df_clean.empty:
            clean_path = f"s3://{processed_bucket}/"
            wr.s3.to_parquet(
                df=df_clean,
                path=clean_path,
                dataset=True,
                partition_cols=['order_date'],
                mode='overwrite_partitions' # Đảm bảo tính lũy đẳng: chạy lại không bị nhân đôi dữ liệu [cite: 1]
            )
            
        # 5. Lưu Dữ Liệu Lỗi (CSV thô để đối soát)
        if not df_error.empty:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            error_path = f"s3://{error_bucket}/errors_{timestamp}_{file_key}"
            wr.s3.to_csv(df=df_error, path=error_path, index=False)

        # 6. Ghi chú trạng thái THÀNH CÔNG vào DynamoDB [cite: 1]
        table = dynamodb.Table(table_name)
        table.put_item(
            Item={
                'file_id': file_key,
                'status': 'COMPLETED',
                'processed_at': datetime.now().isoformat(),
                'clean_records': len(df_clean),
                'error_records': len(df_error)
            }
        )
        
        return {"status": "success", "message": f"Đã hoàn tất xử lý {file_key}"}

    except Exception as e:
        print(f"Lỗi nghiêm trọng: {str(e)}")
        # Cập nhật trạng thái LỖI vào DynamoDB nếu hệ thống sập giữa chừng [cite: 1]
        if 'file_key' in locals() and file_key:
            table = dynamodb.Table(table_name)
            table.put_item(
                Item={
                    'file_id': file_key,
                    'status': 'FAILED',
                    'error_message': str(e),
                    'processed_at': datetime.now().isoformat()
                }
            )
        raise e