import os
import json
import pandas as pd
from datetime import datetime
import glob
import schedule
import time
import logging

# Set up basic logging - helps with debugging when things go wrong
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# === CONFIGURATION ===
JSON_INPUT_FOLDER = "data/json_files"  # where we keep all the JSON files
CONSOLIDATED_OUTPUT = "consolidated_table.csv"
BACKUP_FOLDER = "backups"  # might need this later

def process_all_json_files():
    """
    Main function to process JSON files and create consolidated table
    TODO: maybe add error handling for corrupted JSON files?
    """
    records_list = []
    processed_files = 0
    
    logger.info(f"Starting to process JSON files from {JSON_INPUT_FOLDER}")
    
    # Get all JSON files - using glob because it's simple
    json_file_pattern = os.path.join(JSON_INPUT_FOLDER, "*.json")
    json_files = glob.glob(json_file_pattern)
    
    if not json_files:
        logger.warning("No JSON files found in the input directory!")
        return
    
    # Process each file
    for json_file_path in json_files:
        try:
            logger.info(f"Processing file: {os.path.basename(json_file_path)}")
            
            with open(json_file_path, "r", encoding='utf-8') as file:
                file_data = json.load(file)
            
            # Extract the metric data - structure seems consistent across files
            metric_data_results = file_data.get("MetricDataResults", [])
            message_list = file_data.get("Messages", [])
            
            # Process each metric
            for metric_item in metric_data_results:
                metric_identifier = metric_item.get("Id")
                timestamp_list = metric_item.get("Timestamps", [])
                value_list = metric_item.get("Values", [])
                
                # Skip if we don't have both timestamps and values
                if not timestamp_list or not value_list:
                    logger.warning(f"Skipping metric {metric_identifier} - missing data")
                    continue
                
                # Convert load time from milliseconds to minutes
                # Note: dividing by 60000 because that's ms -> minutes conversion
                total_load_time = sum(value_list)
                average_load_minutes = total_load_time / len(value_list) / 60000.0
                
                # Create a record for each timestamp
                for timestamp_str in timestamp_list:
                    try:
                        # Parse the ISO format timestamp
                        parsed_date = datetime.fromisoformat(timestamp_str).date()
                        
                        data_record = {
                            "id": metric_identifier,
                            "runtime_date": parsed_date,
                            "load_time": average_load_minutes,
                            "message": "; ".join(message_list) if message_list else None
                        }
                        records_list.append(data_record)
                        
                    except ValueError as date_error:
                        logger.error(f"Could not parse timestamp {timestamp_str}: {date_error}")
                        continue
            
            processed_files += 1
            
        except json.JSONDecodeError as json_error:
            logger.error(f"Failed to parse JSON file {json_file_path}: {json_error}")
            continue
        except Exception as general_error:
            logger.error(f"Unexpected error processing {json_file_path}: {general_error}")
            continue
    
    if not records_list:
        logger.warning("No records were extracted from the JSON files")
        return
    
    # Create DataFrame and save to CSV
    consolidated_df = pd.DataFrame(records_list)
    
    # Sort by date for better readability
    consolidated_df = consolidated_df.sort_values(['runtime_date', 'id'])
    
    try:
        consolidated_df.to_csv(CONSOLIDATED_OUTPUT, index=False)
        logger.info(f"✅ Successfully saved {len(records_list)} records to {CONSOLIDATED_OUTPUT}")
        logger.info(f"Processed {processed_files} JSON files")
    except Exception as save_error:
        logger.error(f"Failed to save CSV file: {save_error}")
        raise

def run_monthly_etl():
    """
    Wrapper function for the monthly ETL job
    I like to keep this separate in case we need to add pre/post processing
    """
    start_time = time.time()
    logger.info("=== Starting Monthly ETL Process ===")
    
    try:
        process_all_json_files()
        
        execution_time = time.time() - start_time
        logger.info(f"Monthly ETL completed successfully in {execution_time:.2f} seconds")
        
    except Exception as etl_error:
        logger.error(f"Monthly ETL failed: {etl_error}")
        # Could add email notification here later

def create_backup_folder():
    """
    Make sure backup directory exists
    Might use this later for archiving processed files
    """
    if not os.path.exists(BACKUP_FOLDER):
        os.makedirs(BACKUP_FOLDER)
        logger.info(f"Created backup folder: {BACKUP_FOLDER}")

# === JOB SCHEDULING ===
# Run on the 1st of every month at 00:05 (avoiding midnight exactly)
schedule.every().month.at("00:05").do(run_monthly_etl)

def main():
    """
    Main entry point - starts the scheduler
    """
    logger.info("📅 JSON ETL Scheduler is starting up...")
    
    # Create necessary directories
    create_backup_folder()
    
    # Make sure input directory exists
    if not os.path.exists(JSON_INPUT_FOLDER):
        logger.error(f"Input directory {JSON_INPUT_FOLDER} does not exist!")
        return
    
    logger.info("Scheduler is now waiting for the monthly trigger (1st day at 00:05)")
    logger.info("Press Ctrl+C to stop the scheduler")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute - not too aggressive
            
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as scheduler_error:
        logger.error(f"Scheduler error: {scheduler_error}")

if __name__ == "__main__":
    main()