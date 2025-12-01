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


def main():
    """Main function to orchestrate the three-phase pipeline."""
    parser = argparse.ArgumentParser(description='Run the three-phase data processing pipeline')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format (optional)')
    
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_logging()
    
    logger.info("="*70)
    logger.info("PIPELINE ORCHESTRATOR - Daily Supervision Hours Processing")
    logger.info("="*70)
    
    # Determine dates
    # Let pull_data_main handle the smart date logic (first 5 days = previous month, otherwise month-to-date)
    if args.start_date:
        # If start date is explicitly provided, use it
        start_date = args.start_date
        end_date = None  # Will default to tomorrow in pull_data_main
        logger.info(f"Using provided start date: {start_date}")
    else:
        # Don't set default dates - let pull_data_main apply smart date logic
        # (first 5 days of month = previous month, otherwise month-to-date)
        start_date = None
        end_date = None
        logger.info("No start date provided - pull_data_main will determine date range based on current date")
    
    # Initialize variables for error handling
    exit_code = 0
    error_message = None
    
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
        final_df = merge_data_main(transformed_df=transformed_df, bacb_df=bacb_df, employee_locations_df=employee_locations_df, save_file=True)
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
    
    finally:
        # Send email notification regardless of success or failure
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            email_script = os.path.join(script_dir, 'send_email.py')
            logger.info(f"Sending email notification (exit_code: {exit_code})...")
            
            # Build command with optional error message
            email_cmd = [sys.executable, email_script, str(exit_code)]
            if error_message:
                # Escape the error message for command line (replace newlines and quotes)
                escaped_error = error_message.replace('\n', ' ').replace('\r', ' ').replace('"', "'")
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
    
    return exit_code


if __name__ == "__main__":
    exit(main())

