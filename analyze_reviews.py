import pandas as pd
import glob
import os
import re
import json
import argparse
from collections import defaultdict

# --- Configuration ---
# Default directory if no specific file is provided
DEFAULT_CSV_EXPORT_DIR = "downloads/"
# Set to True if your Ground Truth labels use different values (e.g., 1/0 instead of True/False)
NORMALIZE_GROUND_TRUTH = True
GROUND_TRUTH_POSITIVE_VALUES = ['true', '1', 'yes'] # Lowercase values considered positive ground truth
# -------------------

def normalize_boolean_str(value):
    """Converts various string representations of boolean to standard True/False."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        val_lower = value.strip().lower()
        if val_lower in ['true', '1', 'yes', 'valid']:
            return True
        if val_lower in ['false', '0', 'no', 'invalid']:
            return False
    return None # Cannot determine

def process_csv_exports(files_to_process):
    """Loads, parses, and aggregates validation data from a list of CSV export files."""
    if not files_to_process:
        print(f"Error: No CSV files provided or found to process.")
        return pd.DataFrame() # Return empty DataFrame

    print(f"Processing {len(files_to_process)} CSV file(s)...")
    all_validation_data = []

    # Hardcoded base columns
    id_col = 'Encounter ID'
    date_col = 'Note Date'
    gt_col = 'Ground Truth'

    for i, f in enumerate(files_to_process):
        print(f"Processing file {i+1}/{len(files_to_process)}: {os.path.basename(f)}")
        if not os.path.exists(f):
            print(f"  Warning: File not found - {f}. Skipping.")
            continue
        try:
            # Keep dtype=str for robustness in reading actual values
            df = pd.read_csv(f, dtype=str)
        except Exception as e:
            print(f"  Warning: Could not read file {f}. Error: {e}. Skipping.")
            continue

        # --- Check for essential base columns ---
        cols = df.columns
        if id_col not in cols or date_col not in cols:
            print(f"  Warning: Missing required base column ('{id_col}' or '{date_col}') in {f}. Skipping.")
            continue
            
        # Optional: Check if GT col exists, but don't skip if missing
        gt_col_exists = gt_col in cols
        if not gt_col_exists:
            print(f"  Warning: Ground Truth column ('{gt_col}') not found in {f}. Proceeding without it.")
            
        # --- Extract Version Keys from column names ---
        version_keys_found = set()
        # Pattern to capture the prefix before known suffixes
        version_pattern = re.compile(r"^(.*?)_("
            r"Validation_Result|"
            r"Invalid_Reason|"
            r"Comments|"
            r"Final_Prediction|"
            r"Raw_Prediction|"
            r"Final_Confidence|"
            r"Raw_Confidence|"
            r"Attribution"
            r")$")
        for col in cols:
            match = version_pattern.match(col)
            if match:
                version_keys_found.add(match.group(1)) # Add the prefix

        if not version_keys_found:
            print(f"  Warning: No version-specific columns found in {f}. Skipping file.")
            continue
        
        sorted_versions = sorted(list(version_keys_found))
        print(f"  Found versions: {', '.join(sorted_versions)}")

        # --- Reshape data to long format for this file --- 
        file_data = []
        for index, row in df.iterrows():
            base_info = {
                id_col: row.get(id_col),
                date_col: row.get(date_col),
                gt_col: row.get(gt_col) if gt_col_exists else None, # Use .get safely
                'Source File': os.path.basename(f)
            }
            
            for vk in sorted_versions:
                # Construct expected column names
                val_col = f"{vk}_Validation_Result"
                rea_col = f"{vk}_Invalid_Reason"
                com_col = f"{vk}_Comments"
                pred_col = f"{vk}_Final_Prediction"
                
                # Retrieve values using .get for safety (returns None if col missing)
                validation_result_raw = row.get(val_col)
                invalid_reason = row.get(rea_col)
                comments = row.get(com_col)
                predicted_class = row.get(pred_col)
                
                # Only create a record if validation result exists for this version/row
                # Check for non-None and non-NaN (using pd.notna on raw value before normalization)
                if validation_result_raw is not None and pd.notna(validation_result_raw):
                    record = base_info.copy()
                    record['VersionKey'] = vk
                    record['ValidationResult_Raw'] = validation_result_raw
                    record['ValidationResult'] = normalize_boolean_str(validation_result_raw)
                    record['InvalidReason'] = invalid_reason # Already None if missing
                    record['Comments'] = comments # Already None if missing
                    record['PredictedClass'] = predicted_class # Already None if missing
                    file_data.append(record)

        all_validation_data.extend(file_data)
        print(f"  Processed {len(file_data)} validation records from this file.")

    if not all_validation_data:
        print("Error: No valid validation records found across all files.")
        return pd.DataFrame()

    # Combine all records into a single DataFrame
    final_df = pd.DataFrame(all_validation_data)
    print(f"\nTotal validation records aggregated: {len(final_df)}")
    return final_df

def analyze_validation_data(df):
    """Calculates and prints summary statistics for validation data."""
    if df.empty:
        print("Cannot analyze empty DataFrame.")
        return

    # Ensure 'ValidationResult' column has boolean type where possible
    df['ValidationResult'] = df['ValidationResult_Raw'].apply(normalize_boolean_str)
    
    # Filter out rows where ValidationResult could not be determined
    valid_results_df = df.dropna(subset=['ValidationResult'])
    print(f"Analyzing {len(valid_results_df)} records with clear Valid/Invalid status.")
    if len(valid_results_df) != len(df):
         print(f"  (Excluded {len(df) - len(valid_results_df)} records with ambiguous validation status)")

    if valid_results_df.empty:
        print("No records with clear Valid/Invalid status found for analysis.")
        return
        
    # --- Overall Summary ---
    print("\n--- Overall Validation Summary ---")
    total_reviews = len(valid_results_df)
    total_valid = valid_results_df['ValidationResult'].sum()
    total_invalid = total_reviews - total_valid
    print(f"Total Reviews Analyzed: {total_reviews}")
    print(f"  Valid Attributions: {total_valid} ({total_valid/total_reviews:.1%})")
    print(f"  Invalid Attributions: {total_invalid} ({total_invalid/total_reviews:.1%})")

    # --- Summary by Version Key ---
    print("\n--- Validation Summary by Version Key ---")
    summary = valid_results_df.groupby('VersionKey')['ValidationResult'].agg(['count', 'sum'])
    summary.rename(columns={'count': 'Total Reviews', 'sum': 'Valid Count'}, inplace=True)
    summary['Invalid Count'] = summary['Total Reviews'] - summary['Valid Count']
    summary['Valid Rate'] = (summary['Valid Count'] / summary['Total Reviews'])
    summary['Invalid Rate'] = (summary['Invalid Count'] / summary['Total Reviews'])

    # Format percentages
    summary['Valid Rate'] = summary['Valid Rate'].map('{:.1%}'.format)
    summary['Invalid Rate'] = summary['Invalid Rate'].map('{:.1%}'.format)
    print(summary[['Total Reviews', 'Valid Count', 'Invalid Count', 'Valid Rate', 'Invalid Rate']])

    # --- Invalid Reason Analysis ---
    print("\n--- Invalid Reason Analysis (for Invalid Attributions) ---")
    invalid_df = valid_results_df[valid_results_df['ValidationResult'] == False]
    if not invalid_df.empty:
        reason_summary = invalid_df.groupby(['VersionKey', 'InvalidReason']).size().unstack(fill_value=0)
        # Add row totals
        reason_summary['Total Invalid'] = reason_summary.sum(axis=1)
        # Add column totals
        reason_summary.loc['Total', :] = reason_summary.sum(axis=0)
        print(reason_summary)
    else:
        print("No invalid attributions found to analyze reasons.")
        
    # --- Ground Truth Agreement Analysis ---
    # Add debug prints
    print(f"\nDEBUG: Starting GT Agreement Analysis. Initial records: {len(valid_results_df)}") 
    print("DEBUG: Columns available:", valid_results_df.columns.tolist())
    print("DEBUG: Value counts for initial 'Ground Truth':\n", valid_results_df['Ground Truth'].value_counts(dropna=False).head())
    print("DEBUG: Value counts for initial 'PredictedClass':\n", valid_results_df['PredictedClass'].value_counts(dropna=False).head())
    
    gt_analysis_df = valid_results_df.dropna(subset=['Ground Truth', 'PredictedClass'])
    print(f"DEBUG: Records after dropping NaN in GT or PredictedClass: {len(gt_analysis_df)}")
    
    # Ensure columns are string type for comparison, handle potential non-string values gracefully
    # Add intermediate df to avoid SettingWithCopyWarning if possible
    intermediate_df = gt_analysis_df.copy()
    intermediate_df['Ground Truth Comp'] = intermediate_df['Ground Truth'].astype(str).str.strip().str.lower()
    intermediate_df['PredictedClass Comp'] = intermediate_df['PredictedClass'].astype(str).str.strip().str.lower()
    
    print("DEBUG: Value counts for 'Ground Truth Comp':\n", intermediate_df['Ground Truth Comp'].value_counts(dropna=False).head())
    print("DEBUG: Value counts for 'PredictedClass Comp':\n", intermediate_df['PredictedClass Comp'].value_counts(dropna=False).head())
    
    # Exclude rows where either became empty after cleaning
    final_gt_df = intermediate_df[(intermediate_df['Ground Truth Comp'] != '') & (intermediate_df['PredictedClass Comp'] != '')]
    print(f"DEBUG: Records after dropping empty strings in GT Comp or PredictedClass Comp: {len(final_gt_df)}")

    if not final_gt_df.empty:
        print("\n--- Agreement with Ground Truth (Predicted Class vs Ground Truth Class) ---")
        # Use final_gt_df for the groupby
        gt_summary = final_gt_df.groupby('VersionKey').apply(
            lambda x: pd.Series({
                'Total w/ GT & Pred': len(x),
                'Prediction Matches GT': (x['PredictedClass Comp'] == x['Ground Truth Comp']).sum(),
                'Prediction Mismatches GT': (x['PredictedClass Comp'] != x['Ground Truth Comp']).sum()
            })
        )
        # Handle potential division by zero if 'Total w/ GT & Pred' is 0 for a group
        gt_summary['Agreement Rate'] = gt_summary.apply(lambda row: (row['Prediction Matches GT'] / row['Total w/ GT & Pred']) if row['Total w/ GT & Pred'] > 0 else 0, axis=1).map('{:.1%}'.format)
        gt_summary['Disagreement Rate'] = gt_summary.apply(lambda row: (row['Prediction Mismatches GT'] / row['Total w/ GT & Pred']) if row['Total w/ GT & Pred'] > 0 else 0, axis=1).map('{:.1%}'.format)
        print(gt_summary)
    else:
        print("No records found with non-empty Ground Truth and Predicted Class for agreement analysis.")


# --- Main Execution ---
if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Analyze clinical validation CSV exports.")
    parser.add_argument(
        "--input-file", 
        type=str, 
        help="Path to a specific CSV export file to analyze."
    )
    parser.add_argument(
        "--input-dir", 
        type=str, 
        default=DEFAULT_CSV_EXPORT_DIR,
        help=f"Directory containing CSV exports to analyze if --input-file is not specified (default: {DEFAULT_CSV_EXPORT_DIR})."
    )
    args = parser.parse_args()
    # ------------------------

    files_to_analyze = []

    if args.input_file:
        # Specific file provided
        print(f"Analyzing specific file: {args.input_file}")
        if os.path.isfile(args.input_file):
            files_to_analyze = [args.input_file]
        else:
            print(f"Error: Specified input file not found - {args.input_file}")
    else:
        # No specific file, use directory with specific pattern
        target_pattern = "clinical_validation_denormalized_*.csv" # Specific pattern
        print(f"Analyzing files matching '{target_pattern}' in directory: {os.path.abspath(args.input_dir)}")
        if os.path.isdir(args.input_dir):
            # Use the specific pattern in glob
            files_to_analyze = glob.glob(os.path.join(args.input_dir, target_pattern))
            if not files_to_analyze:
                 print(f"Warning: No files matching '{target_pattern}' found in directory {args.input_dir}")
        else:
            print(f"Error: Specified input directory not found - {args.input_dir}")
            print("Please create the directory or provide a specific file using --input-file.")

    # Proceed only if files were found
    if files_to_analyze:
        aggregated_data = process_csv_exports(files_to_analyze)
    
        if not aggregated_data.empty:
            analyze_validation_data(aggregated_data)
        else:
            print("Analysis finished: No data processed.")
    else:
        print("Analysis finished: No files to analyze.")
    
    print("\nAnalysis script finished.") 