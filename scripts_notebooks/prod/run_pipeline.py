#!/usr/bin/env python3
"""
Pipeline Orchestrator Script

This script executes the four-phase data processing pipeline in order:
1. pull_data.py - Pull direct, supervision, and BACB data from database
2. join_supervision_data.py - Join direct and supervision data
3. transform_data.py - Transform raw data
4. merge_data.py - Merge BACB data with transformed data

Usage:
    python run_pipeline.py [--start-date YYYY-MM-DD]
"""

import sys
import os
import logging
import argparse
import subprocess
from datetime import datetime, timedelta
from pull_data import pull_data_main
from join_supervision_data import join_supervision_data_main
from transform_data import transform_data_main
from merge_data import merge_data_main


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
    log_file = os.path.join(log_dir, 'run_pipeline.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)




def get_latest_date_from_files(raw_folder: str = '../../data/raw_pulls') -> str:
    """
    Get the latest date from existing files in the raw_pulls folder.
    
    Args:
        raw_folder (str): Path to raw pulls folder
        
    Returns:
        str: Latest date found in YYYY-MM-DD format, or None if no files found
    """
    import re
    
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


def get_previous_month_last_day() -> str:
    """
    Get the last day of the previous month in YYYY-MM-DD format.
    
    Returns:
        str: Last day of previous month (e.g., "2025-11-30")
    """
    now = datetime.now()
    if now.month == 1:
        # If January, previous month is December of previous year
        prev_month = 12
        prev_year = now.year - 1
    else:
        prev_month = now.month - 1
        prev_year = now.year
    
    # Get last day of previous month
    if prev_month == 12:
        # December - next month is January
        last_day = (datetime(prev_year + 1, 1, 1) - timedelta(days=1)).day
    else:
        # Other months - next month is prev_month + 1
        last_day = (datetime(prev_year, prev_month + 1, 1) - timedelta(days=1)).day
    
    return datetime(prev_year, prev_month, last_day).strftime('%Y-%m-%d')


def find_archived_file_from_date(target_date: str, archive_folder: str = '../../data/transformed_supervision_daily/archived') -> str:
    """
    Find the archived file from a specific date (last day of previous month).
    Looks for files matching patterns:
    - daily_supervision_hours_transformed_{date}.xlsx (old format)
    - daily_supervision_hours_transformed_{date}_FINAL_{month}.xlsx (new format)
    - daily_supervision_hours_transformed_{date}_FINAL_{month}_updated_{date}.xlsx (updated format)
    
    Args:
        target_date (str): Date in YYYY-MM-DD format to search for
        archive_folder (str): Path to archive folder
        
    Returns:
        str: Path to archived file if found, None otherwise
    """
    import re
    
    try:
        if not os.path.exists(archive_folder):
            return None
        
        # Parse target_date to get month name for new format
        try:
            target_dt = datetime.strptime(target_date, '%Y-%m-%d')
            month_name = target_dt.strftime('%B')
        except ValueError:
            month_name = None
        
        # Look for files matching the pattern
        # Try new format first (with FINAL and month name)
        if month_name:
            new_pattern = f'daily_supervision_hours_transformed_{target_date}_FINAL_{month_name}.xlsx'
            updated_pattern_prefix = f'daily_supervision_hours_transformed_{target_date}_FINAL_{month_name}_updated_'
        else:
            new_pattern = None
            updated_pattern_prefix = None
        
        # Old format (without FINAL)
        old_pattern = f'daily_supervision_hours_transformed_{target_date}.xlsx'
        old_updated_prefix = f'daily_supervision_hours_transformed_{target_date}_updated_'
        
        for filename in os.listdir(archive_folder):
            # Check new format first (with FINAL)
            if new_pattern and (filename == new_pattern or filename.startswith(updated_pattern_prefix)):
                return os.path.join(archive_folder, filename)
            # Check old format (without FINAL)
            elif filename == old_pattern or filename.startswith(old_updated_prefix):
                return os.path.join(archive_folder, filename)
        
        return None
            
    except Exception as e:
        logging.warning(f"Error finding archived file: {e}")
        return None


def run_pipeline_phases(start_date: str = None, end_date: str = None, 
                        save_to_archive: bool = False, archive_date: str = None,
                        archive_file_exists: bool = False,
                        logger: logging.Logger = None) -> tuple:
    """
    Run all pipeline phases with given parameters.
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
        save_to_archive (bool): If True, save to archived folder with _updated suffix
        archive_date (str, optional): Date string for _updated_{date} suffix
        archive_file_exists (bool): If True, file exists and will use _updated suffix. If False, creates new file without suffix.
        logger: Logger instance
        
    Returns:
        tuple: (exit_code, error_message, final_df)
    """
    if logger is None:
        logger = setup_logging()
    
    exit_code = 0
    error_message = None
    final_df = None
    
    try:
        # Phase 1: Pull data from database
        logger.info("")
        logger.info("="*70)
        logger.info("PHASE 1: PULLING DATA FROM DATABASE")
        logger.info("="*70)
        logger.info("Executing pull_data.py...")
        direct_df, supervision_df, bacb_df, employee_locations_df = pull_data_main(start_date=start_date, end_date=end_date, save_files=True)
        logger.info("Phase 1 completed successfully")
        
        # Phase 2: Join direct and supervision data
        logger.info("")
        logger.info("="*70)
        logger.info("PHASE 2: JOINING DIRECT AND SUPERVISION DATA")
        logger.info("="*70)
        logger.info("Executing join_supervision_data.py...")
        joined_df = join_supervision_data_main(direct_df=direct_df, supervision_df=supervision_df, save_file=True)
        logger.info("Phase 2 completed successfully")
        
        # Phase 3: Transform data
        logger.info("")
        logger.info("="*70)
        logger.info("PHASE 3: TRANSFORMING DATA")
        logger.info("="*70)
        logger.info("Executing transform_data.py...")
        transformed_df = transform_data_main(df=joined_df, save_file=True)
        logger.info("Phase 3 completed successfully")
        
        # Phase 4: Merge data
        logger.info("")
        logger.info("="*70)
        logger.info("PHASE 4: MERGING DATA")
        logger.info("="*70)
        logger.info("Executing merge_data.py...")
        final_df = merge_data_main(transformed_df=transformed_df, bacb_df=bacb_df, employee_locations_df=employee_locations_df, 
                                  save_file=True, save_to_archive=save_to_archive, archive_date=archive_date,
                                  archive_file_exists=archive_file_exists)
        logger.info("Phase 4 completed successfully")
        
        # Summary
        logger.info("")
        logger.info("="*70)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*70)
        logger.info(f"Final output: {len(final_df)} rows")
        logger.info(f"Columns: {', '.join(final_df.columns.tolist())}")
        logger.info("="*70)
        
    except Exception as e:
        logger.error("="*70)
        logger.error("PIPELINE FAILED!")
        logger.error("="*70)
        logger.error(f"Error: {e}")
        import traceback
        error_traceback = traceback.format_exc()
        logger.error(error_traceback)
        exit_code = 1
        # Capture error message for email (limit length to avoid command line issues)
        error_message = str(e)
        # If error message is too long, truncate it
        if len(error_message) > 500:
            error_message = error_message[:500] + "... (truncated)"
    
    return exit_code, error_message, final_df


def main():
    """
    Main function to orchestrate the pipeline with scheduling logic:
    - Days 1-5: Run both previous month update AND current month-to-date
    - Days 6-31: Run only current month-to-date
    """
    parser = argparse.ArgumentParser(description='Run the data processing pipeline')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format (optional)')
    
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_logging()
    
    logger.info("="*70)
    logger.info("PIPELINE ORCHESTRATOR - Daily Supervision Hours Processing")
    logger.info("="*70)
    
    # Check current date
    now = datetime.now()
    current_day = now.day
    today_str = now.strftime('%Y-%m-%d')
    
    # Initialize variables for error handling
    overall_exit_code = 0
    overall_error_message = None
    
    # Determine if we need to run previous month update (days 1-5)
    # On days 1-5, we update the previous month's archived data
    run_previous_month = (current_day <= 5)
    
    if run_previous_month:
        logger.info(f"Current day is {current_day} (days 1-5) - will update previous month's data AND run current month-to-date")
        
        # Get previous month's last day
        prev_month_last_day = get_previous_month_last_day()
        logger.info(f"Previous month's last day: {prev_month_last_day}")
        
        # Find archived file from previous month
        archived_file = find_archived_file_from_date(prev_month_last_day)
        file_exists = (archived_file is not None)
        if archived_file:
            logger.info(f"Found archived file to update: {archived_file}")
        else:
            logger.info(f"No archived file found for {prev_month_last_day} - will create new one in archive folder")
        
        # Calculate previous month date range
        # Start: first day of previous month
        if now.month == 1:
            prev_month = 12
            prev_year = now.year - 1
        else:
            prev_month = now.month - 1
            prev_year = now.year
        
        prev_month_start = datetime(prev_year, prev_month, 1).strftime('%Y-%m-%d')
        # End: first day of current month (exclusive, so includes all of previous month)
        prev_month_end = datetime(now.year, now.month, 1).strftime('%Y-%m-%d')
        
        logger.info("")
        logger.info("="*70)
        logger.info("RUNNING PREVIOUS MONTH UPDATE")
        logger.info("="*70)
        logger.info(f"Date range: {prev_month_start} to {prev_month_end} (exclusive)")
        
        # Run pipeline for previous month
        exit_code, error_message, final_df = run_pipeline_phases(
            start_date=prev_month_start,
            end_date=prev_month_end,
            save_to_archive=True,
            archive_date=prev_month_last_day,
            archive_file_exists=file_exists,
            logger=logger
        )
        
        if exit_code != 0:
            overall_exit_code = exit_code
            overall_error_message = error_message
            logger.error("Previous month update failed!")
        else:
            logger.info("Previous month update completed successfully!")
    
    # Determine if we need to run current month (days 1-31)
    # Note: Days 1-5 run both previous month update AND current month-to-date
    # Days 6-31 run only current month-to-date
    run_current_month = (current_day >= 1)
    
    if run_current_month:
        logger.info("")
        logger.info("="*70)
        logger.info("RUNNING CURRENT MONTH-TO-DATE")
        logger.info("="*70)
        
        # Determine dates for current month
        if args.start_date:
            # If start date is explicitly provided, use it
            start_date = args.start_date
            end_date = None
            logger.info(f"Using provided start date: {start_date}")
        else:
            # Use smart date logic: month-to-date from first day of current month
            start_date = datetime(now.year, now.month, 1).strftime('%Y-%m-%d')
            end_date = None  # Will default to tomorrow in pull_data_main
            logger.info(f"Pulling month-to-date data from: {start_date}")
        
        # Run pipeline for current month
        exit_code, error_message, final_df = run_pipeline_phases(
            start_date=start_date,
            end_date=end_date,
            save_to_archive=False,
            logger=logger
        )
        
        if exit_code != 0:
            overall_exit_code = exit_code
            if overall_error_message:
                overall_error_message = f"{overall_error_message}; Current month: {error_message}"
            else:
                overall_error_message = error_message
            logger.error("Current month pipeline failed!")
        else:
            logger.info("Current month pipeline completed successfully!")
    
    # Send email notification
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        email_script = os.path.join(script_dir, 'send_email.py')
        logger.info(f"Sending email notification (exit_code: {overall_exit_code})...")
        
        # Build command with optional error message
        email_cmd = [sys.executable, email_script, str(overall_exit_code)]
        if overall_error_message:
            # Escape the error message for command line (replace newlines and quotes)
            escaped_error = overall_error_message.replace('\n', ' ').replace('\r', ' ').replace('"', "'")
            email_cmd.append(escaped_error)
        
        result = subprocess.run(
            email_cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            logger.info("Email notification sent successfully")
        else:
            logger.warning(f"Email script returned error: {result.stderr}")
    except subprocess.TimeoutExpired:
        logger.warning("Email notification timed out")
    except Exception as email_error:
        logger.warning(f"Failed to send email notification: {email_error}")
        # Don't fail the pipeline if email fails
    
    return overall_exit_code


if __name__ == "__main__":
    exit(main())

