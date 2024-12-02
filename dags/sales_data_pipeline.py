from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import random
import pandas as pd
from faker import Faker
import io

# Initialize Faker
fake = Faker()

# Constants
KAFKA_TOPIC = 'sales_data'
S3_BUCKET = 'project-sales-retail-data'
AWS_REGION = 'eu-north-1'

# Configuration dictionaries
SHIP_MODES = ['Standard Class', 'Second Class', 'Same Day', 'First Class']
SEGMENTS = ['Consumer', 'Home Office', 'Corporate']
STATES = ['Constantine', 'New South Wales', 'Budapest', 'Karaman', 'Sikasso', 'Atsimo-Andrefana']
COUNTRIES = ['Algeria', 'Australia', 'Hungary', 'Sweden', 'Canada', 'New Zealand', 'Iraq', 
            'Philippines', 'United Kingdom', 'Malaysia', 'United States', 'Japan']
MARKETS = ['Africa', 'APAC', 'EMEA', 'EU', 'Canada', 'LATAM', 'US']
REGIONS = ['Africa', 'APAC', 'EMEA', 'EU', 'Canada', 'LATAM', 'US']
CATEGORIES = ['Office Supplies', 'Furniture', 'Technology']
SUB_CATEGORIES = {
    'Office Supplies': ['Storage', 'Supplies', 'Paper', 'Art', 'Envelopes', 'Fasteners', 'Binders', 'Labels'],
    'Furniture': ['Furnishings', 'Chairs', 'Tables', 'Bookcases'],
    'Technology': ['Machines', 'Appliances', 'Copiers', 'Phones', 'Accessories']
}
PRODUCT_NAMES = {
    ('Office Supplies', 'Storage'): ['File Box', 'Storage Drawer', 'Magazine Rack'],
    ('Office Supplies', 'Paper'): ['Printer Paper', 'Copy Paper', 'Note Pad'],
    ('Furniture', 'Chairs'): ['Executive Chair', 'Task Chair', 'Folding Chair'],
    ('Technology', 'Phones'): ['iPhone', 'Samsung Galaxy', 'Google Pixel'],
}
ORDER_PRIORITIES = ['Medium', 'High', 'Critical', 'Low']

def generate_order_id(year):
    return f"AG-{year}-{random.randint(1000, 9999)}"

def generate_product_id():
    return f"P{random.randint(100000, 999999)}"

def generate_sales_record():
    year = random.randint(2011, 2014)
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    order_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    ship_date = order_date + timedelta(days=random.randint(2, 5))
    
    category = random.choice(CATEGORIES)
    sub_category = random.choice(SUB_CATEGORIES[category])
    
    quantity = random.randint(1, 10)
    unit_price = random.uniform(10, 1000)
    discount = round(random.uniform(0, 0.5), 2)
    sales = round(quantity * unit_price * (1 - discount), 2)
    profit = round(sales * random.uniform(0.1, 0.3), 2)
    shipping_cost = round(random.uniform(5, 50), 2)

    return {
        'order_id': generate_order_id(year),
        'order_date': order_date.strftime('%Y-%m-%d'),
        'ship_date': ship_date.strftime('%Y-%m-%d'),
        'ship_mode': random.choice(SHIP_MODES),
        'segment': random.choice(SEGMENTS),
        'state': random.choice(STATES),
        'country': random.choice(COUNTRIES),
        'market': random.choice(MARKETS),
        'region': random.choice(REGIONS),
        'product_id': generate_product_id(),
        'category': category,
        'sub_category': sub_category,
        'product_name': random.choice(PRODUCT_NAMES.get((category, sub_category), ['Generic Product'])),
        'sales': sales,
        'quantity': quantity,
        'discount': discount,
        'profit': profit,
        'shipping_cost': shipping_cost,
        'order_priority': random.choice(ORDER_PRIORITIES),
        'year': year
    }

def generate_batch_records(num_records=100):
    return [generate_sales_record() for _ in range(num_records)]

def generate_and_save_to_s3(**context):
    records = generate_batch_records(num_records=100)
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Get the base client
    session = s3_hook.get_session()
    s3_client = session.client('s3', region_name=AWS_REGION)
    
    # Convert records to DataFrame
    df = pd.DataFrame(records)
    
    # Convert DataFrame to CSV string
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Generate temporary S3 key
    timestamp = context['ts_nodash']
    temp_s3_key = f'temp/sales_data_{timestamp}.csv'
    
    # Upload CSV to S3
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        bucket_name=S3_BUCKET,
        key=temp_s3_key,
        replace=True
    )
    
    return temp_s3_key

def kafka_producer_from_s3(s3_key):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    data = s3_hook.read_key(key=s3_key, bucket_name=S3_BUCKET)
    # Return data in the format expected by Kafka
    return [(None, data.encode('utf-8'))]  # [(key, value)] format

def save_to_final_s3_location(**context):
    s3_key = context['task_instance'].xcom_pull(task_ids='generate_data')
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Read new data from temporary location
    new_data = s3_hook.read_key(key=s3_key, bucket_name=S3_BUCKET)
    
    # Generate final S3 key (using a fixed name for the consolidated file)
    final_s3_key = 'sales_data/consolidated_sales_data.csv'
    
    # Check if consolidated file exists
    try:
        existing_data = s3_hook.read_key(key=final_s3_key, bucket_name=S3_BUCKET)
        # Combine existing and new data
        # Skip header for new_data to avoid duplicate headers
        combined_data = existing_data.rstrip() + '\n' + '\n'.join(new_data.split('\n')[1:])
    except:
        # If no existing file, use new data as is
        combined_data = new_data
    
    # Upload combined data
    s3_hook.load_string(
        string_data=combined_data,
        bucket_name=S3_BUCKET,
        key=final_s3_key,
        replace=True
    )

def cleanup_temp_file(**context):
    s3_key = context['task_instance'].xcom_pull(task_ids='generate_data')
    if s3_key and s3_key.startswith('temp/'):
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.delete_objects(bucket=S3_BUCKET, keys=[s3_key])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sales_data_generator',
    default_args=default_args,
    description='Generate sales data and push to Kafka and S3 in CSV format',
    schedule_interval='*/30 * * * *',  # Run every 5 minutes
    catchup=False
) as dag:

    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_and_save_to_s3,
        provide_context=True,
    )

    produce_to_kafka = ProduceToTopicOperator(
        task_id='produce_to_kafka',
        kafka_config_id="kafka_default",
        topic=KAFKA_TOPIC,
        producer_function=f"{__name__}.kafka_producer_from_s3",
        producer_function_args=["{{ task_instance.xcom_pull(task_ids='generate_data') }}"],
    )

    save_records_to_s3 = PythonOperator(
        task_id='save_records_to_s3',
        python_callable=save_to_final_s3_location,
        provide_context=True,
    )

    cleanup = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup_temp_file,
        provide_context=True,
    )

    generate_data >> produce_to_kafka >> save_records_to_s3 >> cleanup
