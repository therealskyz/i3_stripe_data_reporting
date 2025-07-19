FROM public.ecr.aws/lambda/python:3.13

# Install Python packages
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy your pipeline code
COPY i3-stripe-data-reporting-lambda.py .

# Lambda handler
CMD ["i3-stripe-data-reporting-lambda.lambda_handler"]
