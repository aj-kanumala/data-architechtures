import pandas as pd
import sqlite3
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError

# Step 1: Create a mock dataset (simulating a source system)
def create_mock_data():
    data = {
        "student_id": [9, 17, 9, 15, 17],
        "student_name": ["Ajay", "Vinay", "Ajay", "Bava", "Vinay"],
        "subject": ["Machine Learning", "Stat & Scientific Comp", "Machine Learning", "Time-series", "Machine Learning"],
        "grade": [99, 90, 78, 30, 88],
        "attendance_days": [30, 15, 25, 1, 10],
        "total_days": [30, 30, 30, 30, 30]
    }
    df = pd.DataFrame(data)
    df.to_csv("student_data.csv", index=False)
    print("Mock data created: student_data.csv")

# Step 2: Store raw data in AWS S3 as the Data Lake
def store_in_s3():
    # Initialize S3 client
    s3_client = boto3.client('s3')
    bucket_name = "datalake-demo-2025"  # AWS S3 bucket name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"raw/student_data_{timestamp}.csv"

    try:
        # Upload the file to S3
        s3_client.upload_file("student_data.csv", bucket_name, s3_key)
        print(f"Raw data stored in S3 Data Lake: s3://{bucket_name}/{s3_key}")
        return bucket_name, s3_key
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
        raise

# Step 3: ETL Pipeline - Extract, Transform, Load
def etl_pipeline(bucket_name, s3_key):
    # Extract: Download from S3
    s3_client = boto3.client('s3')
    local_file = "temp_student_data.csv"
    try:
        s3_client.download_file(bucket_name, s3_key, local_file)
        df = pd.read_csv(local_file)
        print("Extracted data from S3 Data Lake")
    except ClientError as e:
        print(f"Error downloading from S3: {e}")
        raise
    finally:
        if os.path.exists(local_file):
            os.remove(local_file)  # Clean up temporary file

    # Transform: Calculate average grade and attendance rate per student
    transformed_df = df.groupby("student_id").agg({
        "student_name": "first",
        "grade": "mean",
        "attendance_days": "sum",
        "total_days": "sum"
    }).reset_index()
    transformed_df["attendance_rate"] = round((transformed_df["attendance_days"] / transformed_df["total_days"]) * 100,2)
    transformed_df = transformed_df.rename(columns={"grade": "average_grade"})
    print("Transformed data: Calculated averages")

    # Load: Store in SQLite Data Warehouse
    conn = sqlite3.connect("data_warehouse.db")
    transformed_df.to_sql("student_metrics", conn, if_exists="replace", index=False)
    conn.close()
    print("Loaded transformed data into Data Warehouse")

# Step 4: Upload the SQLite database to S3
def upload_db_to_s3():
    s3_client = boto3.client('s3')
    bucket_name = "datalake-demo-2025"
    s3_key = f"structured/data_warehouse.db"

    try:
        s3_client.upload_file("data_warehouse.db", bucket_name, s3_key)
        print(f"Updated Data Warehouse in S3: s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        print(f"Error uploading Data Warehouse to S3: {e}")
        raise
    except FileNotFoundError:
        print("Error: data_warehouse.db not found locally")
        raise

# Step 5: Presentation Layer - Generate a report
def generate_report():
    conn = sqlite3.connect("data_warehouse.db")
    query = "SELECT student_id, student_name, average_grade, attendance_rate FROM student_metrics"
    report_df = pd.read_sql(query, conn)
    conn.close()
    report_df.to_csv("report.csv", index=False)
    print("Generated report: report.csv")
    print(report_df)

# Main function to run the demo
def main():
    print("=== Starting Data Warehouse and S3 Data Lake Integration Demo ===")
    create_mock_data()
    bucket_name, s3_key = store_in_s3()
    etl_pipeline(bucket_name, s3_key)
    upload_db_to_s3() 
    generate_report()
    print("=== Demo Complete ===")

if __name__ == "__main__":
    main()