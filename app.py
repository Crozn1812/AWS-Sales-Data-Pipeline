import os
import sys
import aws_cdk as cdk

# Kéo thư mục hiện tại vào tầm nhìn của Python
sys.path.append(os.path.dirname(__file__))

# Bây giờ Python đã có thể tìm thấy thư mục infrastructure
from infrastructure.app_stack import SalesPipelineStack

# Khởi tạo ứng dụng CDK
app = cdk.App()

# Khởi tạo Stack hạ tầng của dự án ShopMart
SalesPipelineStack(app, "ShopmartSalesDataPipelineStack")

# Tổng hợp (Synthesize) mã Python thành CloudFormation template
app.synth()