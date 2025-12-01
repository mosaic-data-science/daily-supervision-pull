#!/usr/bin/env python3
"""
Phase 1: Data Pull Script

This script pulls supervision hours data and BACB supervision data from the CR database
and saves them as CSV files for downstream processing.

Usage:
    python pull_data.py [--start-date YYYY-MM-DD] [--raw-output PATH] [--bacb-output PATH]
"""

import pandas as pd
import pyodbc
import os
import logging
import re
import argparse
from datetime import datetime, timedelta
from typing import Tuple
from dotenv import load_dotenv
from sql_queries import DIRECT_SERVICES_SQL_TEMPLATE, SUPERVISION_SERVICES_SQL_TEMPLATE, BACB_SUPERVISION_TEMPLATE, EMPLOYEE_LOCATIONS_SQL_TEMPLATE


def setup_logging(log_dir: str = None) -> logging.Logger:
    """Set up logging configuration."""
    # Use root logs directory if not specified
    if log_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up from scripts_notebooks/prod to project root
        project_root = os.path.dirname(os.path.dirname(script_dir))
        log_dir = os.path.join(project_root, 'logs')
    
    # Ensure logs directory exists
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log file path
    log_file = os.path.join(log_dir, 'pull_data.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def get_latest_date_from_files(raw_folder: str) -> str:
    """
    Get the latest date from existing files in the raw_pulls folder.
    
    Args:
        raw_folder (str): Path to raw pulls folder
        
    Returns:
        str: Latest date found in YYYY-MM-DD format, or None if no files found
    """
    try:
        if not os.path.exists(raw_folder):
            return None
        
        # Get all CSV files in the raw_pulls folder
        csv_files = [f for f in os.listdir(raw_folder) if f.endswith('.csv')]
        
        if not csv_files:
            return None
        
        # Extract dates from filenames using regex pattern
        date_pattern = r'(\d{4}-\d{2}-\d{2})'
        dates = []
        
        for filename in csv_files:
            match = re.search(date_pattern, filename)
            if match:
                dates.append(match.group(1))
        
        if not dates:
            return None
        
        # Find the latest date
        return max(dates)
            
    except Exception as e:
        logging.warning(f"Error getting latest date from files: {e}")
        return None


def get_db_connection(server: str, username: str, password: str):
    """
    Create database connection with multiple driver fallback.
    
    Args:
        server (str): Database server
        username (str): Database username
        password (str): Database password
        
    Returns:
        pyodbc.Connection: Database connection
    """
    drivers_to_try = [
        ('ODBC Driver 17 for SQL Server', ''),
        ('ODBC Driver 18 for SQL Server', 'TrustServerCertificate=yes'),
        ('SQL Server', ''),
        ('ODBC Driver 18 for SQL Server', 'TrustServerCertificate=yes;Encrypt=no')
    ]
    
    for driver, extra_params in drivers_to_try:
        try:
            conn_str = f'DRIVER={{{driver}}};SERVER={server};DATABASE=insights;UID={username};PWD={password}'
            if extra_params:
                conn_str += f';{extra_params}'
            
            logging.info(f"Attempting connection with {driver}")
            conn = pyodbc.connect(conn_str)
            logging.info(f"Successfully connected with {driver}")
            return conn
        except Exception as e:
            logging.warning(f"Failed to connect with {driver}: {e}")
            if driver == drivers_to_try[-1][0]:
                raise
            continue
    
    raise Exception("All ODBC drivers failed")


def execute_direct_query(conn, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Execute the direct services SQL query.
    
    Args:
        conn: Database connection
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        
    Returns:
        pd.DataFrame: Query results
    """
    sql_query = DIRECT_SERVICES_SQL_TEMPLATE.format(start_date=start_date, end_date=end_date)
    logging.info(f"Executing direct services query with start_date: {start_date}, end_date: {end_date}")
    df = pd.read_sql(sql_query, conn)
    logging.info(f"Direct services query retrieved {len(df)} rows")
    return df


def execute_supervision_query(conn, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Execute the supervision services SQL query.
    
    Args:
        conn: Database connection
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        
    Returns:
        pd.DataFrame: Query results
    """
    sql_query = SUPERVISION_SERVICES_SQL_TEMPLATE.format(start_date=start_date, end_date=end_date)
    logging.info(f"Executing supervision services query with start_date: {start_date}, end_date: {end_date}")
    df = pd.read_sql(sql_query, conn)
    logging.info(f"Supervision services query retrieved {len(df)} rows")
    return df


def execute_bacb_query(conn, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Execute the BACB supervision SQL query.
    
    Args:
        conn: Database connection
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        
    Returns:
        pd.DataFrame: Query results
    """
    sql_query = BACB_SUPERVISION_TEMPLATE.format(start_date=start_date, end_date=end_date)
    logging.info(f"Executing BACB query with start_date: {start_date}, end_date: {end_date}")
    df = pd.read_sql(sql_query, conn)
    logging.info(f"BACB query retrieved {len(df)} rows")
    return df


def execute_employee_locations_query(conn) -> pd.DataFrame:
    """
    Execute the employee locations SQL query.
    
    Args:
        conn: Database connection
        
    Returns:
        pd.DataFrame: Query results with ProviderContactId, ProviderFirstName, ProviderLastName, WorkLocation (contains ProviderOfficeLocationName)
    """
    sql_query = EMPLOYEE_LOCATIONS_SQL_TEMPLATE
    logging.info("Executing employee locations query...")
    df = pd.read_sql(sql_query, conn)
    logging.info(f"Employee locations query retrieved {len(df)} rows")
    return df


def pull_data_main(start_date: str = None, end_date: str = None, save_files: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Main function to pull data from database.
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format. If None, will determine automatically.
        end_date (str, optional): End date in YYYY-MM-DD format. If None, defaults to tomorrow.
        save_files (bool): Whether to save files to disk. Default True.
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: (direct_df, supervision_df, bacb_df, employee_locations_df)
    """
    # Load environment variables
    load_dotenv()
    
    # Set up logging
    logger = setup_logging()
    
    # Database connection parameters
    server = os.getenv('CR_DWH_SERVER')
    username = os.getenv('CR_UN')
    password = os.getenv('CR_PW')
    
    # Determine start date
    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    
    if start_date:
        logger.info(f"Using provided start date: {start_date}")
    else:
        # Determine date range based on current date
        current_day = now.day
        
        if current_day <= 5:
            # If in first 5 days of month, pull all data from previous month
            # Get first day of previous month
            if now.month == 1:
                # If January, previous month is December of previous year
                prev_month = 12
                prev_year = now.year - 1
            else:
                prev_month = now.month - 1
                prev_year = now.year
            
            start_date = datetime(prev_year, prev_month, 1).strftime('%Y-%m-%d')
            logger.info(f"Current date is in first 5 days of month ({current_day}), pulling previous month data from: {start_date}")
            
            # Set end date to first day of current month (exclusive in SQL, so includes all of previous month)
            if end_date is None:
                end_date = datetime(now.year, now.month, 1).strftime('%Y-%m-%d')
                logger.info(f"End date set to first day of current month (exclusive): {end_date}")
        else:
            # Otherwise, pull month to date (from first day of current month)
            start_date = datetime(now.year, now.month, 1).strftime('%Y-%m-%d')
            logger.info(f"Pulling month-to-date data from: {start_date}")
    
    # Calculate end date (tomorrow to include all of today, unless provided)
    if end_date is None:
        end_date = (now + timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info("="*50)
    logger.info("Phase 1: Data Pulls")
    logger.info("="*50)
    logger.info(f"Start date: {start_date}, End date: {end_date}")
    
    # Connect to database
    conn = get_db_connection(server, username, password)
    
    # Execute direct services query
    logger.info("Pulling direct services data...")
    direct_df = execute_direct_query(conn, start_date, end_date)
    
    # Execute supervision services query
    logger.info("Pulling supervision services data...")
    supervision_df = execute_supervision_query(conn, start_date, end_date)
    
    # Execute BACB query
    logger.info("Pulling BACB supervision data...")
    bacb_df = execute_bacb_query(conn, start_date, end_date)
    
    # Execute employee locations query
    logger.info("Pulling employee locations data...")
    employee_locations_df = execute_employee_locations_query(conn)
    
    # Close connection
    conn.close()
    
    if save_files:
        # Save direct services data
        direct_output = f'../../data/raw_pulls/direct_services_{today_str}.csv'
        os.makedirs(os.path.dirname(direct_output), exist_ok=True)
        direct_df.to_csv(direct_output, index=False)
        logger.info(f"Saved direct services data to: {direct_output}")
        
        # Save supervision services data
        supervision_output = f'../../data/raw_pulls/supervision_services_{today_str}.csv'
        os.makedirs(os.path.dirname(supervision_output), exist_ok=True)
        supervision_df.to_csv(supervision_output, index=False)
        logger.info(f"Saved supervision services data to: {supervision_output}")
        
        # Save BACB data
        bacb_output = f'../../data/raw_pulls/bacb_supervision_hours_{today_str}.csv'
        os.makedirs(os.path.dirname(bacb_output), exist_ok=True)
        bacb_df.to_csv(bacb_output, index=False)
        logger.info(f"Saved BACB data to: {bacb_output}")
        
        # Save employee locations data
        employee_locations_output = f'../../data/raw_pulls/employee_locations_{today_str}.csv'
        os.makedirs(os.path.dirname(employee_locations_output), exist_ok=True)
        employee_locations_df.to_csv(employee_locations_output, index=False)
        logger.info(f"Saved employee locations data to: {employee_locations_output}")
    
    logger.info("="*50)
    logger.info(f"Data pull completed successfully!")
    logger.info(f"Direct: {len(direct_df)} rows, Supervision: {len(supervision_df)} rows, BACB: {len(bacb_df)} rows, Employee Locations: {len(employee_locations_df)} rows")
    logger.info("="*50)
    
    return direct_df, supervision_df, bacb_df, employee_locations_df


def main():
    """CLI entry point for pull_data.py"""
    parser = argparse.ArgumentParser(description='Pull supervision and BACB data from database')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--raw-output', type=str, default='../../data/raw_pulls/daily_supervision_hours_{date}.csv',
                       help='Output path for raw supervision data (use {date} placeholder)')
    parser.add_argument('--bacb-output', type=str, default='../../data/raw_pulls/bacb_supervision_hours_{date}.csv',
                       help='Output path for BACB data (use {date} placeholder)')
    
    args = parser.parse_args()
    
    try:
        pull_data_main(start_date=args.start_date, end_date=None, save_files=True)
        return 0
    except Exception as e:
        logging.error(f"Error in data pull: {e}")
        raise


if __name__ == "__main__":
    exit(main())