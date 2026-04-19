import os
import sys
import aws_cdk as cdk

# Drag the current directory into Python's view.
sys.path.append(os.path.dirname(__file__))

# Python can now find the directory infrastructure
from infrastructure.app_stack import SalesPipelineStack

# Initialize the CDK application.
app = cdk.App()

# Initialize the infrastructure stack of the ShopMart project.
SalesPipelineStack(app, "ShopmartSalesDataPipelineStack")

# Synthesize the Python code into a CloudFormation template
app.synth()