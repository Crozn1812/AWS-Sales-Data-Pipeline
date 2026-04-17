from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_lambda_event_sources as lambda_sources,
    aws_logs as logs,
    aws_iam as iam,
)
from constructs import Construct


class SalesPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # 1. S3 buckets
        raw_bucket = s3.Bucket(
            self,
            "ShopmartRawZone",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        processed_bucket = s3.Bucket(
            self,
            "ShopmartProcessedZone",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        error_bucket = s3.Bucket(
            self,
            "ShopmartErrorZone",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # 2. DynamoDB metadata table
        metadata_table = dynamodb.Table(
            self,
            "PipelineMetadataTable",
            partition_key=dynamodb.Attribute(
                name="file_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # 3. AWS managed layer cho awswrangler (AWS SDK for pandas)
        awswrangler_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "AwsWranglerLayer",
            "arn:aws:lambda:ap-southeast-1:336392948345:layer:AWSSDKPandas-Python312:22",
        )

        # 4. Lambda function
        processing_lambda = _lambda.Function(
            self,
            "DataTransformerFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("src"),
            layers=[awswrangler_layer],
            memory_size=512,
            timeout=Duration.seconds(60),
            environment={
                "PROCESSED_BUCKET": processed_bucket.bucket_name,
                "ERROR_BUCKET": error_bucket.bucket_name,
                "DYNAMODB_TABLE": metadata_table.table_name,
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        # 5. Basic grants
        raw_bucket.grant_read(processing_lambda)
        processed_bucket.grant_write(processing_lambda)
        error_bucket.grant_write(processing_lambda)
        metadata_table.grant_write_data(processing_lambda)

        # 6. Explicit IAM policy để tránh AccessDenied với awswrangler/S3 APIs
        processing_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:ListBucket",
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListBucketMultipartUploads",
                ],
                resources=[
                    raw_bucket.bucket_arn,
                    f"{raw_bucket.bucket_arn}/*",
                    processed_bucket.bucket_arn,
                    f"{processed_bucket.bucket_arn}/*",
                    error_bucket.bucket_arn,
                    f"{error_bucket.bucket_arn}/*",
                ],
            )
        )

        # 7. S3 trigger -> Lambda
        processing_lambda.add_event_source(
            lambda_sources.S3EventSource(
                raw_bucket,
                events=[s3.EventType.OBJECT_CREATED],
            )
        )