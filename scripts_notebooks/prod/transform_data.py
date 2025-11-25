#!/usr/bin/env python3
"""
Phase 2: Data Transformation Script

This script reads raw supervision hours data, transforms it into the required format,
and saves the transformed data as CSV for downstream processing.

Usage:
    python transform_data.py [--input PATH] [--output PATH]
"""

import pandas as pd
import os
import logging
import argparse
from datetime import datetime


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
    log_file = os.path.join(log_dir, 'transform_data.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def clean_clinic_name(name: str) -> str:
    """
    Clean clinic name by removing common suffixes and prefixes.
    
    Args:
        name (str): Raw clinic name
        
    Returns:
        str: Cleaned clinic name
    """
    if pd.isna(name) or name == '':
        return name
    
    name_str = str(name)
    
    # Remove ORGANIZATION prefix if present
    name_str = pd.Series([name_str]).str.replace(
        r'^ORGANIZATION[:_]\s*', '', regex=True, case=False
    ).iloc[0]
    
    # Strip whitespace
    name_str = name_str.strip()
    
    # Remove common suffixes (order matters - try longer matches first)
    name_str = name_str.replace(" 8528 Unive", "")
    name_str = name_str.replace(" 1612 Hi", "")
    name_str = name_str.replace(" Clinic", "")
    name_str = name_str.replace(" Clin", "")
    
    # Remove trailing whitespace
    name_str = name_str.rstrip()
    
    return name_str


def transform_data(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Transform the raw supervision data into the required format.
    
    Args:
        df (pd.DataFrame): Raw data from SQL query
        logger: Logger instance
        
    Returns:
        pd.DataFrame: Transformed data with Clinic, DirectProviderId, DirectProviderName, 
                     DirectHours, SupervisionHours
    """
    logger.info("="*50)
    logger.info("Phase 2: Data Transformation")
    logger.info("="*50)
    
    # Remove exact duplicate rows from SQL query output
    initial_row_count = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    duplicate_count = initial_row_count - len(df)
    if duplicate_count > 0:
        logger.warning(f"Removed {duplicate_count} exact duplicate rows from SQL query output")
    logger.info(f"Processing {len(df)} unique rows")
    
    # Additional deduplication: The SQL query may create duplicates due to Employee table joins
    # Group by key identifying columns to ensure each unique time period is only counted once
    # This handles cases where the same billing entry appears multiple times due to JOINs
    if 'ClientContactId' in df.columns and 'DirectProviderId' in df.columns:
        # For rows with DirectProviderId, group by client, provider, and hours to deduplicate
        # This ensures we don't count the same hours multiple times
        direct_rows = df[df['DirectProviderId'].notna()].copy()
        if len(direct_rows) > 0:
            # Group by the key columns that identify a unique time period
            # Use the minimum of other columns to pick one representative row
            key_cols = ['ClientContactId', 'DirectProviderId', 'ClientOfficeLocationName', 
                       'DirectHours', 'SupervisionHours', 'RowType', 'DirectServiceLocationName']
            # Only include columns that exist
            key_cols = [col for col in key_cols if col in direct_rows.columns]
            
            # Group and take first row (since hours should be the same for duplicates)
            direct_deduped = direct_rows.groupby(key_cols, dropna=False).first().reset_index()
            
            # Count how many duplicates were removed
            direct_dupes = len(direct_rows) - len(direct_deduped)
            if direct_dupes > 0:
                logger.warning(f"Removed {direct_dupes} additional duplicate rows from direct provider data (likely due to Employee table JOIN)")
            
            # Combine with rows that don't have DirectProviderId
            other_rows = df[df['DirectProviderId'].isna()].copy()
            df = pd.concat([direct_deduped, other_rows], ignore_index=True)
            logger.info(f"After deduplication: {len(df)} rows remaining")
    
    # Identify all supervisors (people who appear in Supervisor columns)
    supervisors = set()
    for _, row in df.iterrows():
        if pd.notna(row['SupervisorFirstName']) and pd.notna(row['SupervisorLastName']):
            supervisor_name = (row['SupervisorFirstName'], row['SupervisorLastName'])
            supervisors.add(supervisor_name)
    
    logger.info(f"Found {len(supervisors)} unique supervisors in the data")
    
    # Remove rows where direct provider matches any supervisor
    initial_count = len(df)
    df = df[~(
        df['DirectFirstName'].notna() & 
        df['DirectLastName'].notna() & 
        df.apply(lambda row: (row['DirectFirstName'], row['DirectLastName']) in supervisors, axis=1)
    )].reset_index(drop=True)
    removed_count = initial_count - len(df)
    
    if removed_count > 0:
        logger.info(f"Removed {removed_count} rows where direct provider also appears as supervisor")
    else:
        logger.info("No rows removed (no direct providers matched supervisors)")
    
    # Create direct provider name mapping
    direct_dict = {}
    for _, row in df.iterrows():
        if pd.notna(row['DirectFirstName']) and pd.notna(row['DirectLastName']):
            direct_dict[row['DirectProviderId']] = f"{row['DirectFirstName']} {row['DirectLastName']}"
    
    # Handle overlap rows separately to avoid double-counting DirectHours
    # When the same direct hours overlap with multiple supervisors, we get multiple rows
    # with the same DirectHours value. We need to deduplicate these before summing.
    if 'RowType' in df.columns:
        # Separate rows by type
        overlap_mask = df['RowType'] == 'Direct overlapped with supervision'
        direct_only_mask = df['RowType'] == 'Direct (no supervision overlap)'
        supervision_only_mask = df['RowType'] == 'Supervision without direct overlap'
        
        # Process overlap rows: deduplicate by client first (one row per client, not per supervisor)
        if overlap_mask.any():
            overlap_df = df[overlap_mask].copy()
            logger.info(f"Processing {len(overlap_df)} overlap rows")
            
            # Group by client/provider/clinic/service location and take MAX DirectHours
            # The SQL groups by DirectServiceLocationName, so we need to include it to properly deduplicate
            # But we also need ClientContactId to deduplicate across multiple supervisors for same client
            overlap_deduped = overlap_df.groupby([
                'DirectProviderId',
                'ClientOfficeLocationName',
                'ClientContactId',  # Critical: group by client to deduplicate across supervisors
                'DirectServiceLocationName'  # Also group by service location since SQL groups by it
            ]).agg({
                'DirectHours': 'max',  # All rows for same client/service location have same DirectHours
                'SupervisionHours': 'sum',  # Sum across supervisors for same client/service location
            }).reset_index()
            
            logger.info(f"After deduplication by client: {len(overlap_deduped)} overlap rows")
            
            # Now aggregate across clients (safe to sum now)
            overlap_final = overlap_deduped.groupby([
                'DirectProviderId',
                'ClientOfficeLocationName'
            ]).agg({
                'DirectHours': 'sum',  # Sum across different clients
                'SupervisionHours': 'sum',
                'DirectServiceLocationName': 'first'
            }).reset_index()
        
        # Process direct-only rows (no overlap with supervision)
        if direct_only_mask.any():
            direct_only_df = df[direct_only_mask].copy()
            logger.info(f"Processing {len(direct_only_df)} direct-only rows")
            direct_only_grouped = direct_only_df.groupby([
                'DirectProviderId',
                'ClientOfficeLocationName'
            ]).agg({
                'DirectHours': 'sum',
                'SupervisionHours': 'sum',
                'DirectServiceLocationName': 'first'
            }).reset_index()
        else:
            direct_only_grouped = pd.DataFrame(columns=['DirectProviderId', 'ClientOfficeLocationName', 'DirectHours', 'SupervisionHours', 'DirectServiceLocationName'])
        
        # Process supervision-only rows (no direct overlap)
        if supervision_only_mask.any():
            supervision_only_df = df[supervision_only_mask].copy()
            logger.info(f"Processing {len(supervision_only_df)} supervision-only rows")
            # These have DirectProviderId = NULL, so we skip them for direct provider aggregation
            # They'll be handled separately if needed
        else:
            supervision_only_df = pd.DataFrame()
        
        # Combine overlap and direct-only rows (these are the ones with DirectProviderId)
        if overlap_mask.any() and direct_only_mask.any():
            combined = pd.concat([overlap_final, direct_only_grouped], ignore_index=True)
        elif overlap_mask.any():
            combined = overlap_final
        elif direct_only_mask.any():
            combined = direct_only_grouped
        else:
            combined = pd.DataFrame(columns=['DirectProviderId', 'ClientOfficeLocationName', 'DirectHours', 'SupervisionHours', 'DirectServiceLocationName'])
        
        # Final aggregation to combine any remaining duplicates (shouldn't be any, but just in case)
        if len(combined) > 0:
            transformed_df = combined.groupby([
                'DirectProviderId',
                'ClientOfficeLocationName'
            ]).agg({
                'DirectHours': 'sum',
                'SupervisionHours': 'sum',
                'DirectServiceLocationName': 'first'
            }).reset_index()
        else:
            transformed_df = pd.DataFrame(columns=['DirectProviderId', 'ClientOfficeLocationName', 'DirectHours', 'SupervisionHours', 'DirectServiceLocationName'])
    else:
        # RowType column not available, use normal grouping (fallback)
        logger.warning("RowType column not found, using standard aggregation (may double-count overlap hours)")
        transformed_df = df.groupby([
            'DirectProviderId',
            'ClientOfficeLocationName']).agg({
            'DirectHours': 'sum',
            'SupervisionHours': 'sum',
            'DirectServiceLocationName': 'first'
        }).reset_index()
    
    # Clean clinic names from ClientOfficeLocationName
    # Apply consistent cleaning to ALL clinic names regardless of format
    transformed_df['Clinic'] = transformed_df['ClientOfficeLocationName'].apply(clean_clinic_name)
    
    # Clean DirectServiceLocationName the same way
    # Only process non-null values
    mask = transformed_df['DirectServiceLocationName'].notna()
    if mask.any():
        transformed_df.loc[mask, 'DirectServiceLocationName'] = (
            transformed_df.loc[mask, 'DirectServiceLocationName']
            .apply(clean_clinic_name)
        )
        # Convert empty strings back to NaN
        transformed_df.loc[mask, 'DirectServiceLocationName'] = (
            transformed_df.loc[mask, 'DirectServiceLocationName']
            .replace(['', 'nan'], pd.NA)
        )
    
    # Replace "Diagnostics" in Clinic with DirectServiceLocationName
    # Only replace if DirectServiceLocationName is not null/empty
    diagnostics_mask = (
        transformed_df['Clinic'].str.contains('Diagnostics', na=False, case=False) &
        transformed_df['DirectServiceLocationName'].notna() &
        (transformed_df['DirectServiceLocationName'].astype(str).str.strip() != '')
    )
    if diagnostics_mask.any():
        transformed_df.loc[diagnostics_mask, 'Clinic'] = transformed_df.loc[diagnostics_mask, 'DirectServiceLocationName']
        logger.info(f"Replaced 'Diagnostics' in Clinic column with DirectServiceLocationName for {diagnostics_mask.sum()} rows")
    
    # Add provider names before final aggregation
    transformed_df['DirectProviderName'] = transformed_df['DirectProviderId'].map(direct_dict)
    
    # Final aggregation by provider and cleaned clinic name to eliminate duplicates
    # This handles cases where different ClientOfficeLocationName values cleaned to the same clinic name
    logger.info(f"Before final aggregation: {len(transformed_df)} rows")
    transformed_df = transformed_df.groupby([
        'DirectProviderId',
        'Clinic'
    ]).agg({
        'DirectHours': 'sum',
        'SupervisionHours': 'sum',
        'DirectProviderName': 'first'  # Should be the same for all rows with same provider ID
    }).reset_index()
    logger.info(f"After final aggregation by provider and clinic: {len(transformed_df)} rows")
    
    # Drop the original location columns (no longer needed after aggregation)
    if 'ClientOfficeLocationName' in transformed_df.columns:
        transformed_df.drop(columns=['ClientOfficeLocationName'], inplace=True)
    if 'DirectServiceLocationName' in transformed_df.columns:
        transformed_df.drop(columns=['DirectServiceLocationName'], inplace=True)
    
    # Reorder columns and sort
    transformed_df = transformed_df[[
        'Clinic', 'DirectProviderId', 'DirectProviderName', 
        'DirectHours', 'SupervisionHours'
    ]]
    transformed_df = transformed_df.sort_values(by=['Clinic', 'DirectProviderName'], ascending=True)
    
    logger.info(f"Data transformation completed. {len(transformed_df)} rows in transformed dataset.")
    
    # Diagnostic: Check for any provider with unusually high DirectHours
    if len(transformed_df) > 0 and 'DirectHours' in transformed_df.columns:
        high_hours = transformed_df[transformed_df['DirectHours'] > 200]
        if len(high_hours) > 0:
            logger.warning(f"Found {len(high_hours)} providers with DirectHours > 200:")
            for _, row in high_hours.iterrows():
                logger.warning(f"  Provider {row.get('DirectProviderName', 'Unknown')} (ID: {row.get('DirectProviderId', 'Unknown')}): {row['DirectHours']} hours")
    
    return transformed_df


def transform_data_main(df: pd.DataFrame = None, input_file: str = None, save_file: bool = True) -> pd.DataFrame:
    """
    Main function to transform data.
    
    Args:
        df (pd.DataFrame, optional): Input DataFrame. If None, will read from input_file.
        input_file (str, optional): Input CSV file path. Used if df is None.
        save_file (bool): Whether to save file to disk. Default True.
        
    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    # Set up logging
    logger = setup_logging()
    
    # Get input data
    if df is None:
        if input_file is None:
            # Default to today's file
            today = datetime.now().strftime('%Y-%m-%d')
            input_file = f'../../data/raw_pulls/daily_supervision_hours_{today}.csv'
        
        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        logger.info(f"Reading raw data from: {input_file}")
        df = pd.read_csv(input_file)
        logger.info(f"Loaded {len(df)} rows from input file")
    else:
        logger.info(f"Using provided DataFrame with {len(df)} rows")
    
    # Transform data
    transformed_df = transform_data(df, logger)
    
    if save_file:
        # Save transformed data
        today = datetime.now().strftime('%Y-%m-%d')
        output_file = f'../../data/transformed_supervision_daily/daily_supervision_hours_transformed_{today}.csv'
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        transformed_df.to_csv(output_file, index=False)
        logger.info(f"Saved transformed data to: {output_file}")
    
    logger.info("="*50)
    logger.info("Data transformation completed successfully!")
    logger.info("="*50)
    
    return transformed_df


def main():
    """CLI entry point for transform_data.py"""
    parser = argparse.ArgumentParser(description='Transform raw supervision data')
    parser.add_argument('--input', type=str, 
                       default='../../data/raw_pulls/daily_supervision_hours_{date}.csv',
                       help='Input CSV file path (use {date} placeholder for today)')
    parser.add_argument('--output', type=str,
                       default='../../data/transformed_supervision_daily/daily_supervision_hours_transformed_{date}.csv',
                       help='Output CSV file path (use {date} placeholder for today)')
    
    args = parser.parse_args()
    
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        input_file = args.input.format(date=today)
        transform_data_main(input_file=input_file, save_file=True)
        return 0
    except Exception as e:
        logging.error(f"Error in data transformation: {e}")
        raise


if __name__ == "__main__":
    exit(main())