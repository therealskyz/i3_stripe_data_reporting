FROM public.ecr.aws/lambda/python:3.13

# Install Python packages into default working directory of the Lambda image: /var/task
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy your pipeline code into default working directory of the Lambda image: /var/task
COPY i3-stripe-data-reporting-lambda.py .

# Lambda handler
CMD ["i3-stripe-data-reporting-lambda.lambda_handler"]
