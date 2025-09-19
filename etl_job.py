import os
import psycopg2
import clickhouse_connect
from dotenv import load_dotenv
import logging
import json
from pathlib import Path
import time

# Load env vars first thing
load_dotenv()

# Set up logging - monitoring purposess
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Constants - to makesure checkpoint load data based on datetime
WATERMARK_FILE = "etl_watermark.json"
DEFAULT_BATCH_SIZE = 5000  # Found this works well in practice

def connect_to_postgres():
    """
    Get PostgreSQL connection
    TODO: ----
    """
    try:
        connection = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT", 5432),  # default port just in case
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
        )
        logger.info("PostgreSQL connection established successfully")
        return connection
    except psycopg2.Error as db_error:
        logger.error(f"Failed to connect to PostgreSQL: {db_error}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to PostgreSQL: {e}")
        raise

def connect_to_clickhouse():
    """
    init clickhouse client
    """
    try:
        ch_client = clickhouse_connect.get_client(
            host=os.getenv("CH_HOST"),
            port=int(os.getenv("CH_PORT", 8123)),  # standard HTTP port
            username=os.getenv("CH_USER"),
            password=os.getenv("CH_PASSWORD"),
            database=os.getenv("CH_DB"),
        )
        logger.info("ClickHouse client created successfully")
        return ch_client
    except Exception as error:
        logger.error(f"ClickHouse connection error: {error}")
        raise

def setup_clickhouse_table(clickhouse_client):
    """
    Create the retail_transaction table if it doesn't exist
    Using ReplacingMergeTree for soft deletes (latest updated_at wins)
    """
    table_ddl = """
    CREATE TABLE IF NOT EXISTS retail_transaction (
        id UInt64,
        customer_id UInt64,
        last_status String,
        pos_origin String,
        pos_destination String,
        created_at DateTime,
        updated_at DateTime,
        deleted_at Nullable(DateTime)
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (id)
    """
    
    try:
        clickhouse_client.command(table_ddl)
        logger.info("ClickHouse table setup completed (ReplacingMergeTree for soft deletes)")
    except Exception as e:
        logger.error(f"Failed to create ClickHouse table: {e}")
        raise

def fetch_postgres_data(postgres_conn,last_watermark=None, batch_size=DEFAULT_BATCH_SIZE):
    """
    Extract data from PostgreSQL in batches
    use server-side cursor to avoid loading everything into memory
    """
    cursor = postgres_conn.cursor(name="etl_cursor")  # server-side cursor
    cursor.itersize = batch_size
    
    if last_watermark:
        cursor.execute("""
            SELECT id, customer_id, last_status, pos_origin, pos_destination,
                   created_at, updated_at, deleted_at
            FROM retail_transaction
            WHERE updated_at > %s and deleted_at IS NOT NULL
            ORDER BY updated_at;
        """, (last_watermark,))
    else:
        # First full load
        cursor.execute("""
            SELECT id, customer_id, last_status, pos_origin, pos_destination,
                   created_at, updated_at, deleted_at
            FROM retail_transaction
            WHERE deleted_at IS NOT NULL
            ORDER BY updated_at;
        """)
    
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows
    
    cursor.close()

def insert_to_clickhouse(ch_client, data_rows):
    """
    Load batch of data into ClickHouse
    """
    if not data_rows:
        logger.warning("No data to insert")
        return
    
    try:
        ch_client.insert(
            "retail_transaction",
            data_rows,
            column_names=[
                "id",
                "customer_id", 
                "last_status",
                "pos_origin",
                "pos_destination",
                "created_at",
                "updated_at",
                "deleted_at"
            ]
        )
        logger.info(f"Successfully inserted {len(data_rows)} rows")
    except Exception as insert_error:
        logger.error(f"Insert operation failed: {insert_error}")
        raise

def load_last_watermark():
    """
    Read the last processed timestamp from watermark file
    Returns None if file doesn't exist yet
    """
    watermark_path = Path(WATERMARK_FILE)
    if watermark_path.exists():
        try:
            with open(watermark_path, "r") as file:
                watermark_data = json.load(file)
                return watermark_data.get("last_updated_at")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not read watermark file: {e}")
    return None

def save_current_watermark(timestamp_value):
    """
    Save the current watermark to file for next run
    """
    try:
        with open(WATERMARK_FILE, "w") as file:
            json.dump({"last_updated_at": timestamp_value}, file, indent=2)
        logger.info(f"Watermark saved: {timestamp_value}")
    except IOError as e:
        logger.error(f"Failed to save watermark: {e}")

def execute_etl_pipeline():
    """
    Main ETL process
    Extract from PostgreSQL -> Transform (minimal) -> Load to ClickHouse
    Uses watermarking to only process new/updated records
    """
    start_time = time.time()
    pg_connection = None
    ch_client = None
    
    try:
        logger.info("Starting ETL pipeline...")

        # Load last watermark
        last_watermark = load_last_watermark()
        logger.info(f"Loaded last watermark: {last_watermark}")

        # Initialize connections
        pg_connection = connect_to_postgres()
        ch_client = connect_to_clickhouse()
        
        # Setup destination table
        setup_clickhouse_table(ch_client)
        
        # Process data in batches
        batch_buffer = []
        total_processed = 0
        current_watermark = last_watermark  # track latest timestamp

        for batch_data in fetch_postgres_data(pg_connection, last_watermark, DEFAULT_BATCH_SIZE):
            batch_buffer.extend(batch_data)
            
            # Update watermark candidate (max updated_at in this batch)
            batch_max_ts = max(row[6] for row in batch_data if row[6])  # row[6] = updated_at
            if not current_watermark or batch_max_ts > current_watermark:
                current_watermark = batch_max_ts
            
            # Process when we have enough data
            if len(batch_buffer) >= DEFAULT_BATCH_SIZE:
                insert_to_clickhouse(ch_client, batch_buffer)
                total_processed += len(batch_buffer)
                batch_buffer = []  # Reset for next batch
        
        # Handle remaining data
        if batch_buffer:
            insert_to_clickhouse(ch_client, batch_buffer)
            total_processed += len(batch_buffer)
        
        # Save new watermark if updated
        if current_watermark:
            save_current_watermark(str(current_watermark))  # serialize datetime
            logger.info(f"Updated watermark saved: {current_watermark}")

        elapsed_time = time.time() - start_time
        logger.info(f"ETL completed! Processed {total_processed} records in {elapsed_time:.2f} seconds")
        
    except Exception as pipeline_error:
        logger.error(f"ETL pipeline failed: {pipeline_error}")
        raise
    finally:
        # Clean up connections
        if pg_connection:
            try:
                pg_connection.close()
                logger.info("PostgreSQL connection closed")
            except:
                pass  # Don't worry about close errors

if __name__ == "__main__":
    execute_etl_pipeline()