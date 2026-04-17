import os
import boto3
import awswrangler as wr
from datetime import datetime
from data_transformer import clean_and_transform_sales_data

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    processed_bucket = os.environ["PROCESSED_BUCKET"]
    error_bucket = os.environ["ERROR_BUCKET"]
    table_name = os.environ["DYNAMODB_TABLE"]

    file_key = None

    try:
        # S3 direct event format
        record = event["Records"][0]
        source_bucket = record["s3"]["bucket"]["name"]
        file_key = record["s3"]["object"]["key"]

        s3_uri = f"s3://{source_bucket}/{file_key}"
        print(f"Đang xử lý file: {s3_uri}")

        df_raw = wr.s3.read_csv(path=[s3_uri])

        df_clean, df_error = clean_and_transform_sales_data(df_raw)

        # parse order_date để partition tốt hơn
        if not df_clean.empty:
            df_clean["order_date"] = df_clean["order_date"].astype(str)
            df_clean["year"] = df_clean["order_date"].str.slice(0, 4)
            df_clean["month"] = df_clean["order_date"].str.slice(5, 7)
            df_clean["day"] = df_clean["order_date"].str.slice(8, 10)

            clean_path = f"s3://{processed_bucket}/sales/"
            wr.s3.to_parquet(
                df=df_clean,
                path=clean_path,
                dataset=True,
                partition_cols=["year", "month", "day"],
                mode="overwrite_partitions"
            )

        if not df_error.empty:
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            error_path = f"s3://{error_bucket}/errors/{timestamp}_{file_key.split('/')[-1]}"
            wr.s3.to_csv(df=df_error, path=error_path, index=False)

        table = dynamodb.Table(table_name)
        table.put_item(
            Item={
                "file_id": file_key,
                "status": "COMPLETED",
                "processed_at": datetime.utcnow().isoformat(),
                "clean_records": int(len(df_clean)),
                "error_records": int(len(df_error)),
                "source_bucket": source_bucket
            }
        )

        return {
            "statusCode": 200,
            "body": f"Đã xử lý thành công {file_key}"
        }

    except Exception as e:
        print(f"Lỗi xử lý: {str(e)}")

        if file_key:
            table = dynamodb.Table(table_name)
            table.put_item(
                Item={
                    "file_id": file_key,
                    "status": "FAILED",
                    "error_message": str(e),
                    "processed_at": datetime.utcnow().isoformat()
                }
            )

        raise