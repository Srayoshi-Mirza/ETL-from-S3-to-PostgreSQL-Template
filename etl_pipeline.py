import os
import gzip
import tempfile
import boto3
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from typing import Optional, Dict, Any
import argparse
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from io import StringIO
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('s3_postgres_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_date_from_filename(filename: str, patterns: Optional[Dict[str, str]] = None, 
                             return_format: str = 'string') -> Optional[Any]:
    """
    Extract date from filename using configurable regex patterns.
    
    Args:
        filename (str): The filename to extract date from
        patterns (dict): Dictionary of pattern names and regex patterns to try
        return_format (str): 'string', 'datetime', or 'dict' (returns all matches)
    
    Returns:
        Extracted date in requested format, or None if no match found
    """
    
    # Default patterns - can be customized for different naming conventions
    if patterns is None:
        patterns = {
            # ISO datetime with T separator: 2025-02-03T030000
            'iso_datetime_compact': r'(\d{4}-\d{2}-\d{2})T(\d{6})',
            
            # ISO datetime with time: 2025-02-03T03:00:00
            'iso_datetime_full': r'(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})',
            
            # ISO date only: 2025-02-03
            'iso_date': r'(\d{4}-\d{2}-\d{2})',
            
            # US format with time: 02-03-2025_030000
            'us_datetime': r'(\d{2}-\d{2}-\d{4})_(\d{6})',
            
            # US format date only: 02-03-2025
            'us_date': r'(\d{2}-\d{2}-\d{4})',
            
            # Compact format: 20250203
            'compact_date': r'(\d{8})',
            
            # Underscore separated: 2025_02_03
            'underscore_date': r'(\d{4}_\d{2}_\d{2})',
            
            # Dot separated: 2025.02.03
            'dot_date': r'(\d{4}\.\d{2}\.\d{2})',
            
            # Year and month only: 2025-02
            'year_month': r'(\d{4}-\d{2})',
            
            # Timestamp in filename: timestamp_1706918400 (Unix timestamp)
            'unix_timestamp': r'timestamp_(\d{10})',
            
            # Date range: 2025-02-03_to_2025-02-10
            'date_range': r'(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})',
        }
    
    results = {}
    
    # Try each pattern
    for pattern_name, pattern in patterns.items():
        match = re.search(pattern, filename)
        if match:
            try:
                # Handle different pattern types
                if pattern_name == 'iso_datetime_compact':
                    date_str = match.group(1)
                    time_str = match.group(2)
                    # Convert time format: 030000 -> 03:00:00
                    formatted_time = f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                    full_datetime = f"{date_str}T{formatted_time}"
                    results[pattern_name] = {
                        'date': date_str,
                        'time': formatted_time,
                        'datetime': full_datetime,
                        'datetime_obj': datetime.fromisoformat(full_datetime)
                    }
                
                elif pattern_name == 'iso_datetime_full':
                    date_str = match.group(1)
                    time_str = match.group(2)
                    full_datetime = f"{date_str}T{time_str}"
                    results[pattern_name] = {
                        'date': date_str,
                        'time': time_str,
                        'datetime': full_datetime,
                        'datetime_obj': datetime.fromisoformat(full_datetime)
                    }
                
                elif pattern_name == 'iso_date':
                    date_str = match.group(1)
                    results[pattern_name] = {
                        'date': date_str,
                        'datetime_obj': datetime.strptime(date_str, '%Y-%m-%d')
                    }
                
                elif pattern_name == 'us_datetime':
                    date_str = match.group(1)  # MM-DD-YYYY
                    time_str = match.group(2)  # HHMMSS
                    # Convert to ISO format
                    month, day, year = date_str.split('-')
                    iso_date = f"{year}-{month}-{day}"
                    formatted_time = f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                    results[pattern_name] = {
                        'date': iso_date,
                        'time': formatted_time,
                        'datetime': f"{iso_date}T{formatted_time}",
                        'datetime_obj': datetime.strptime(f"{iso_date} {formatted_time}", '%Y-%m-%d %H:%M:%S')
                    }
                
                elif pattern_name == 'us_date':
                    date_str = match.group(1)  # MM-DD-YYYY
                    month, day, year = date_str.split('-')
                    iso_date = f"{year}-{month}-{day}"
                    results[pattern_name] = {
                        'date': iso_date,
                        'datetime_obj': datetime.strptime(iso_date, '%Y-%m-%d')
                    }
                
                elif pattern_name == 'compact_date':
                    date_str = match.group(1)  # YYYYMMDD
                    iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    results[pattern_name] = {
                        'date': iso_date,
                        'datetime_obj': datetime.strptime(date_str, '%Y%m%d')
                    }
                
                elif pattern_name == 'underscore_date':
                    date_str = match.group(1)  # YYYY_MM_DD
                    iso_date = date_str.replace('_', '-')
                    results[pattern_name] = {
                        'date': iso_date,
                        'datetime_obj': datetime.strptime(date_str, '%Y_%m_%d')
                    }
                
                elif pattern_name == 'dot_date':
                    date_str = match.group(1)  # YYYY.MM.DD
                    iso_date = date_str.replace('.', '-')
                    results[pattern_name] = {
                        'date': iso_date,
                        'datetime_obj': datetime.strptime(date_str, '%Y.%m.%d')
                    }
                
                elif pattern_name == 'year_month':
                    date_str = match.group(1)  # YYYY-MM
                    results[pattern_name] = {
                        'date': date_str,
                        'datetime_obj': datetime.strptime(date_str, '%Y-%m')
                    }
                
                elif pattern_name == 'unix_timestamp':
                    timestamp = int(match.group(1))
                    dt = datetime.fromtimestamp(timestamp)
                    results[pattern_name] = {
                        'date': dt.strftime('%Y-%m-%d'),
                        'datetime': dt.isoformat(),
                        'datetime_obj': dt,
                        'timestamp': timestamp
                    }
                
                elif pattern_name == 'date_range':
                    start_date = match.group(1)
                    end_date = match.group(2)
                    results[pattern_name] = {
                        'start_date': start_date,
                        'end_date': end_date,
                        'start_datetime_obj': datetime.strptime(start_date, '%Y-%m-%d'),
                        'end_datetime_obj': datetime.strptime(end_date, '%Y-%m-%d')
                    }
                
                logger.debug(f"Pattern '{pattern_name}' matched in filename: {filename}")
                
            except Exception as e:
                logger.warning(f"Error processing pattern '{pattern_name}' for filename '{filename}': {e}")
                continue
    
    # Return based on requested format
    if not results:
        logger.debug(f"No date patterns found in filename: {filename}")
        return None
    
    if return_format == 'dict':
        return results
    
    # Return the first successful match
    first_result = next(iter(results.values()))
    
    if return_format == 'datetime':
        return first_result.get('datetime_obj')
    elif return_format == 'string':
        return first_result.get('date')
    else:
        return first_result.get('date')


# Convenience functions for common use cases
def extract_date_simple(filename: str) -> Optional[str]:
    """Simple date extraction - returns ISO date string or None"""
    return extract_date_from_filename(filename, return_format='string')


def extract_datetime_object(filename: str) -> Optional[datetime]:
    """Extract date as datetime object"""
    return extract_date_from_filename(filename, return_format='datetime')


def extract_all_dates(filename: str) -> Dict[str, Any]:
    """Extract all possible date patterns from filename"""
    result = extract_date_from_filename(filename, return_format='dict')
    return result if result else {}


# Custom pattern examples for specific use cases
def get_custom_patterns():
    """Return custom patterns for specific business needs"""
    return {
        # Sales data patterns
        'sales_daily': r'sales_(\d{4}-\d{2}-\d{2})_daily\.csv',
        'sales_monthly': r'sales_(\d{4}-\d{2})_monthly\.csv',
        
        # Log file patterns
        'log_file': r'app_(\d{4}\d{2}\d{2})_(\d{2}\d{2}\d{2})\.log',
        
        # Backup patterns
        'backup_file': r'backup_(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})\.sql',
        
        # Report patterns
        'report_quarterly': r'report_Q(\d)_(\d{4})\.xlsx',
    }


def process_single_day(day_to_process, db_engine):
    """Process a single day's worth of data from S3, merge all files, and load to PostgreSQL"""
    if not db_engine:
        logger.error("Database engine is required for processing")
        return False
    
    # Format dates for processing
    start_time = day_to_process.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=1)
    
    start_date_str = start_time.strftime('%Y-%m-%d')
    output_date_str = start_time.strftime('%Y-%m-%d')
    
    logger.info(f"{'='*50}")
    logger.info(f"Processing data for {output_date_str}")
    logger.info(f"{'='*50}")

    # Load environment variables from .env file
    load_dotenv()

    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    BUCKET_NAME = os.getenv('BUCKET_NAME')

    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME]):
        raise ValueError("Missing required environment variables. Check your .env file.")

    # Initialize S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # Optional: Use a prefix if you have structured keys
    prefix = ''

    # Paginator for listing objects
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

    all_files = []

    # First, collect all files and show what's available
    logger.info(f"Scanning bucket for files...")
    for page in pages:
        contents = page.get('Contents', [])
        for obj in contents:
            key = obj['Key']
            last_modified = obj['LastModified']
            all_files.append((key, last_modified))

    logger.info(f"Found {len(all_files)} total files in bucket")
    
    # Show some sample files for debugging
    if all_files:
        logger.info("Sample files found:")
        for i, (key, last_modified) in enumerate(all_files[:5]):  # Show first 5
            logger.info(f"  {key} (Modified: {last_modified})")
        if len(all_files) > 5:
            logger.info(f"  ... and {len(all_files) - 5} more files")
    
    # Filter files by extracting date from filename
    logger.info(f"Looking for files with date: {start_date_str}")
    matching_files = []
    
    for key, last_modified in all_files:
        extracted_date = extract_date_from_filename(key)
        if extracted_date == start_date_str:
            matching_files.append((key, last_modified))
            logger.info(f"  Found matching file: {key}")
    
    logger.info(f"Files matching date filter: {len(matching_files)}")

    if len(matching_files) == 0:
        logger.warning(f"No files found for {output_date_str}.")
        logger.info(f"This could mean:")
        logger.info(f"1. Files don't exist for this date")
        logger.info(f"2. Files exist but have different naming pattern")
        
        # Show available dates to help debugging
        logger.info("Available dates found in filenames:")
        available_dates = set()
        for key, _ in all_files[:50]:  # Check first 50 files
            extracted_date = extract_date_from_filename(key)
            if extracted_date:
                available_dates.add(extracted_date)
        
        if available_dates:
            sorted_dates = sorted(list(available_dates))
            logger.info(f"  Found dates: {sorted_dates}")
        else:
            logger.info("  No recognizable date patterns found in filenames")
            
        return False

    # Process and merge all files for the day
    logger.info(f"Processing and merging {len(matching_files)} files for {output_date_str}")
    
    merged_dataframes = []
    total_rows_from_files = 0
    files_processed = 0

    for key, last_modified in matching_files:
        try:
            logger.info(f"Processing file from S3: s3://{BUCKET_NAME}/{key}")
            
            # Create a temporary file for processing
            with tempfile.NamedTemporaryFile(delete=False, suffix='.tmp') as temp_file:
                temp_file_path = temp_file.name
                
            try:
                # Download file to temporary location
                s3.download_file(BUCKET_NAME, key, temp_file_path)
                
                # Process the file based on its type
                if key.endswith('.csv.gz'):
                    # Handle gzipped CSV files
                    with gzip.open(temp_file_path, 'rt', encoding='utf-8') as f:
                        df = pd.read_csv(f, low_memory=False)
                elif key.endswith('.csv'):
                    # Handle regular CSV files
                    df = pd.read_csv(temp_file_path, low_memory=False)
                else:
                    logger.warning(f"Unsupported file format: {key}")
                    continue
                
                logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from {key}")

                # Clean column names (remove { } and strip spaces)
                original_columns = df.columns.tolist()
                df.columns = df.columns.str.replace(r"[\{\}]", "", regex=True).str.strip()
                
                if original_columns != df.columns.tolist():
                    logger.info("Column names cleaned (removed {} characters and stripped spaces)")

                # Add source file information to track which file each row came from
                df['source_file'] = os.path.basename(key)
                
                # Add to list for merging
                merged_dataframes.append(df)
                total_rows_from_files += len(df)
                files_processed += 1
                
                logger.info(f"Successfully processed file {files_processed}/{len(matching_files)}")
                    
            finally:
                # Always clean up the temporary file
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
                    logger.debug(f"Deleted temporary file: {temp_file_path}")
                
        except Exception as e:
            logger.error(f"Error processing {key}: {e}")
            continue

    # Check if we have any data to merge
    if not merged_dataframes:
        logger.error(f"No data successfully loaded from any files for {output_date_str}")
        return False

    # Merge all dataframes
    logger.info(f"Merging {len(merged_dataframes)} dataframes...")
    try:
        # Concatenate all dataframes
        merged_df = pd.concat(merged_dataframes, ignore_index=True, sort=False)
        logger.info(f"Successfully merged data: {len(merged_df)} total rows, {len(merged_df.columns)} columns")
        
        # Show summary of source files in merged data
        if 'source_file' in merged_df.columns:
            file_counts = merged_df['source_file'].value_counts()
            logger.info(f"Rows per source file:")
            for filename, count in file_counts.items():
                logger.info(f"  {filename}: {count:,} rows")
        
    except Exception as e:
        logger.error(f"Error merging dataframes: {e}")
        return False

    # Convert timestamp columns to datetime if they exist
    timestamp_columns = ['installed_at', 'reinstalled_at', 'click_time', 'impression_time']
    for col in timestamp_columns:
        if col in merged_df.columns:
            # Check if the column contains numeric values (Unix timestamps)
            if merged_df[col].dtype in ['int64', 'float64'] or pd.api.types.is_numeric_dtype(merged_df[col]):
                try:
                    merged_df[f'{col}_datetime'] = pd.to_datetime(merged_df[col], unit='s', errors='coerce')
                    logger.info(f"Converted {col} to {col}_datetime")
                except Exception as e:
                    logger.warning(f"Could not convert {col} to datetime: {e}")

    # Add metadata columns
    merged_df['processed_date'] = datetime.now()
    merged_df['source_date'] = start_time
    merged_df['files_merged_count'] = len(merged_dataframes)

    # Print final data summary
    logger.info(f"Final merged data shape: {merged_df.shape}")
    logger.info(f"Total columns: {len(merged_df.columns)}")
    
    # Check for empty columns
    empty_cols = merged_df.columns[merged_df.isnull().all()].tolist()
    if empty_cols:
        logger.warning(f"Found {len(empty_cols)} completely empty columns: {empty_cols}")

    # Check for duplicate rows
    duplicates = merged_df.duplicated().sum()
    if duplicates > 0:
        logger.warning(f"Found {duplicates} duplicate rows in merged data")
        # Optionally remove duplicates
        # merged_df = merged_df.drop_duplicates()
        # logger.info(f"Removed duplicates, final shape: {merged_df.shape}")

    # Load merged data to PostgreSQL database
    try:
        table_name = "table_name"
        
        # Check if the table already exists
        inspector = sqlalchemy.inspect(db_engine)
        table_exists = table_name in inspector.get_table_names()
        
        # Remove completely empty columns before uploading
        if empty_cols:
            logger.info(f"Removing {len(empty_cols)} empty columns before upload")
            merged_df = merged_df.drop(columns=empty_cols)
            logger.info(f"Data shape after removing empty columns: {merged_df.shape}")
        
        # Remove duplicates if they exist
        if duplicates > 0:
            logger.info(f"Removing {duplicates} duplicate rows before upload")
            merged_df = merged_df.drop_duplicates()
            logger.info(f"Data shape after removing duplicates: {merged_df.shape}")
        
        # Split large datasets into chunks for better performance
        chunk_size = 10000  # Adjust this based on your system performance
        total_rows = len(merged_df)
        
        if total_rows > chunk_size:
            logger.info(f"Large dataset detected ({total_rows:,} rows). Uploading in chunks of {chunk_size:,}")
            
            # Upload in chunks
            for i in range(0, total_rows, chunk_size):
                chunk_start = i
                chunk_end = min(i + chunk_size, total_rows)
                chunk_df = merged_df.iloc[chunk_start:chunk_end]
                
                logger.info(f"Uploading chunk {chunk_start+1:,} to {chunk_end:,} ({len(chunk_df):,} rows)")
                
                if table_exists or i > 0:  # Append for existing table or subsequent chunks
                    chunk_df.to_sql(table_name, db_engine, if_exists='append', index=False, method='multi', chunksize=1000)
                else:  # Create table with first chunk
                    logger.info(f"Creating new table: {table_name}")
                    chunk_df.to_sql(table_name, db_engine, if_exists='replace', index=False, method='multi', chunksize=1000)
                    table_exists = True  # Mark as existing for subsequent chunks
                
                logger.info(f"Successfully uploaded chunk {chunk_start+1:,} to {chunk_end:,}")
        else:
            # Small dataset - upload in one go
            if table_exists:
                logger.info(f"Appending merged data to existing table: {table_name}")
                merged_df.to_sql(table_name, db_engine, if_exists='append', index=False, method='multi', chunksize=1000)
            else:
                logger.info(f"Creating new table: {table_name}")
                merged_df.to_sql(table_name, db_engine, if_exists='replace', index=False, method='multi', chunksize=1000)
            
        logger.info(f"Successfully uploaded {len(merged_df):,} merged rows to database")
        
        # Add to processing log
        log_df = pd.DataFrame({
            'date_processed': [datetime.now()],
            'date_of_data': [start_time],
            'files_processed': [files_processed],
            'files_merged': [len(merged_dataframes)],
            'table_name': [table_name],
            'total_row_count': [len(merged_df)],
            'column_count': [len(merged_df.columns)],
            'source_files': [', '.join([os.path.basename(f) for f, _ in matching_files])]
        })
        log_df.to_sql('data_processing_log', db_engine, if_exists='append', index=False)
        
    except Exception as e:
        logger.error(f"Error uploading merged data to database: {e}")
        return False

    # Print final summary
    logger.info(f"{'='*30} PROCESSING SUMMARY {'='*30}")
    logger.info(f"Files found for date: {len(matching_files)}")
    logger.info(f"Files successfully processed: {files_processed}")
    logger.info(f"Total rows from individual files: {total_rows_from_files:,}")
    logger.info(f"Final merged rows loaded to database: {len(merged_df):,}")
    logger.info(f"Date processed: {output_date_str}")
    logger.info(f"Table name: {table_name}")
    
    return True


def create_db_engine():
    """Create SQLAlchemy engine from environment variables"""
    load_dotenv()
    
    DB_TYPE = os.getenv('DB_TYPE', 'postgresql')
    DB_HOST = os.getenv('DW_PG_HOST')
    DB_PORT = os.getenv('DW_PG_PORT', '5432')
    DB_NAME = os.getenv('DW_PG_DATABASE')
    DB_USER = os.getenv('DW_PG_USER')
    DB_PASSWORD = os.getenv('DW_PG_PASSWORD')

    # Check if all required variables are present
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        logger.error("Missing database connection information. Database upload will be skipped.")
        return None
    
    try:
        # Only support PostgreSQL for this version
        if DB_TYPE.lower() == 'postgresql':
            connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        elif DB_TYPE.lower() == 'mysql':
            connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        elif DB_TYPE.lower() == 'mssql':
            connection_string = f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
        else:
            raise ValueError(f"Unsupported database type: {DB_TYPE}")
        
        # Create the engine with optimized settings for large datasets
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            pool_timeout=60,
            pool_recycle=3600,
            echo=False,  # Set to True for SQL debugging
            connect_args={
                "options": "-c statement_timeout=300000"  # 5 minute timeout for PostgreSQL
            } if DB_TYPE.lower() == 'postgresql' else {}
        )
        
        # Test the connection
        with engine.connect() as conn:
            pass
        logger.info("Database connection successful!")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None


def analyze_bucket_dates():
    """Analyze all files in S3 bucket to determine available date ranges"""
    load_dotenv()
    
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    BUCKET_NAME = os.getenv('BUCKET_NAME')

    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    logger.info(f"Analyzing dates in bucket: {BUCKET_NAME}")
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME)

    all_files = []
    extracted_dates = {}
    
    for page in pages:
        contents = page.get('Contents', [])
        for obj in contents:
            key = obj['Key']
            last_modified = obj['LastModified']
            all_files.append((key, last_modified))
            
            # Extract date from filename using the new function
            extracted_date = extract_date_from_filename(key)
            if extracted_date:
                if extracted_date not in extracted_dates:
                    extracted_dates[extracted_date] = 0
                extracted_dates[extracted_date] += 1

    logger.info(f"Found {len(all_files)} total files")
    
    if extracted_dates:
        sorted_dates = sorted(extracted_dates.keys())
        logger.info(f"\nAvailable dates in filenames:")
        logger.info(f"Date range: {min(sorted_dates)} to {max(sorted_dates)}")
        logger.info(f"Total unique dates: {len(sorted_dates)}")
        
        # Show file counts for each date (first 20 dates)
        logger.info(f"\nFile counts by date (showing first 20):")
        for i, date in enumerate(sorted_dates[:20]):
            logger.info(f"  {date}: {extracted_dates[date]} files")
        if len(sorted_dates) > 20:
            logger.info(f"  ... and {len(sorted_dates) - 20} more dates")
            
        # Show last modified date range
        last_modified_dates = sorted([obj[1] for obj in all_files])
        logger.info(f"\nLast modified date range:")
        logger.info(f"  Oldest: {last_modified_dates[0]}")
        logger.info(f"  Newest: {last_modified_dates[-1]}")
    else:
        logger.warning("Could not extract dates from filenames")
        logger.info("Sample filenames:")
        for key, _ in all_files[:10]:
            logger.info(f"  {key}")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process S3 data files, merge by date, and load to PostgreSQL database')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD, default: same as start-date)', default=None)
    parser.add_argument('--analyze-dates', action='store_true', help='Analyze available dates in bucket and exit')
    args = parser.parse_args()
    
    # If analyze-dates flag is used, analyze bucket and exit
    if args.analyze_dates:
        analyze_bucket_dates()
        return
    
    # Require start-date if not analyzing
    if not args.start_date:
        logger.error("--start-date is required unless using --analyze-dates")
        parser.print_help()
        return
    
    # Parse start date
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        logger.error(f"Invalid start date format. Please use YYYY-MM-DD")
        return
    
    # Parse end date (default to same as start date)
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"Invalid end date format. Please use YYYY-MM-DD")
            return
    else:
        end_date = start_date
    
    # Validate date range
    if start_date > end_date:
        logger.error(f"Start date ({args.start_date}) is after end date ({end_date.strftime('%Y-%m-%d')})")
        return
    
    # Create database engine (required for this version)
    db_engine = create_db_engine()
    if not db_engine:
        logger.error("Database connection is required. Please check your database configuration.")
        return
    
    # Process each day in range
    logger.info(f"Processing data from {args.start_date} to {end_date.strftime('%Y-%m-%d')}")
    
    # Count days to process
    days_to_process = (end_date - start_date).days + 1
    logger.info(f"Will process {days_to_process} day(s) of data")
    
    successful_days = 0
    current_date = start_date
    
    while current_date <= end_date:
        success = process_single_day(current_date, db_engine)
        if success:
            successful_days += 1
        current_date += timedelta(days=1)
    
    logger.info(f"{'='*50}")
    logger.info(f"PROCESS COMPLETE")
    logger.info(f"{'='*50}")
    logger.info(f"Successfully processed {successful_days} out of {days_to_process} days.")
    
    if successful_days > 0:
        logger.info(f"All merged data has been loaded to the 'table_name' table in PostgreSQL.")
        logger.info(f"Processing logs are available in the 'data_processing_log' table.")


if __name__ == "__main__":
    main()