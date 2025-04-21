import os
import pandas as pd
import json
import sqlalchemy
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import dotenv
import io
from tqdm import tqdm
import sys
import pathlib

dotenv.load_dotenv()

# Create a directory to store downloaded files
os.makedirs("downloads", exist_ok=True)

# Path to the prediction data file or SQL query
# You can either:
# 1. Use a pre-generated CSV file
# 2. Run the SQL query directly (preferred method)
USE_SQL_QUERY = True
SQL_QUERY_PATH = "query.sql"
CSV_FILE_PATH = "/Users/zwimpee/data/clinical-note-reviews/2025-04-16/data.csv"

# Model version descriptions for better reporting
MODEL_VERSION_INFO = {
    "0.1.0-prod": {
        "name": "GPT-4o",
        "description": "Production GPT-4o endpoint",
        "variant": "standard"
    },
    "0.2.0-stage": {
        "name": "GPT-4o",
        "description": "Staging GPT-4o endpoint",
        "variant": "standard"
    },
    "0.3.0-stage": {
        "name": "GPT-4.1",
        "description": "GPT-4.1 standard endpoint",
        "variant": "standard"
    },
    "0.4.0-stage": {
        "name": "GPT-4.1-mini",
        "description": "GPT-4.1-mini endpoint (smaller model)",
        "variant": "mini"
    },
    "0.5.0-stage": {
        "name": "GPT-4.1-nano",
        "description": "GPT-4.1-nano endpoint (smallest model)",
        "variant": "nano"
    }
}

# Prompt version descriptions
PROMPT_VERSION_INFO = {
    "1.0.0": "Original production prompt",
    "1.1.0": "Enhanced staging prompt",
    "1.2.0": "Improved staging prompt with more context handling",
    "1.3.0": "Latest prompt with optimized instructions and context"
}

# Azure Storage account details
storage_account_name = "aipredictiveengine"
container_name = "lcn-529"

# Connect to Azure Blob Storage
connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
if not connection_string:
    print("Please set the AZURE_STORAGE_CONNECTION_STRING environment variable")
    connection_string = input("Enter your Azure Storage connection string: ")

# Create Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Load the prediction data
if USE_SQL_QUERY and os.path.exists(SQL_QUERY_PATH):
    # Execute SQL query directly if database connection details are available
    try:
        print(f"Executing SQL query from {SQL_QUERY_PATH}...")
        
        # Database connection details - from environment variables
        # db_host = os.environ.get("METADATA_DB_HOST", "eus2-clinical-notes.postgres.database.azure.com")
        # db_port = os.environ.get("METADATA_DB_PORT", "5432")
        # db_user = os.environ.get("METADATA_DB_USER", "artisight_admin")
        # db_password = os.environ.get("METADATA_DB_PASSWORD")
        # db_name = os.environ.get("METADATA_DB_NAME", "postgres")
        # db_sslmode = os.environ.get("METADATA_DB_SSLMODE", "require")
        db_sslmode = "require"
        # Database connection details - FOR TUNNEL USE
        db_host = "localhost" # Use localhost for the tunnel
        db_port = "15432"     # Use the local port forwarded by SSH
        db_user = os.environ.get("METADATA_DB_USER", "artisight_admin")
        db_password = os.environ.get("METADATA_DB_PASSWORD")
        db_name = os.environ.get("METADATA_DB_NAME", "postgres")
        # SSL mode might need adjustment depending on tunnel/server config
        # Try 'disable' if 'require' causes issues over the tunnel, but 'require' might still work.
        #db_sslmode = os.environ.get("METADATA_DB_SSLMODE", "disable") # Try disable first with tunnel
        
        if not db_password:
            print("Database password not found in environment variables.")
            db_password = input("Enter your database password: ")
        
        # Create SQLAlchemy engine
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={db_sslmode}"
        engine = sqlalchemy.create_engine(connection_string)
        
        # Read the SQL query
        with open(SQL_QUERY_PATH, 'r') as f:
            sql_query = f.read()
        
        # Execute the query
        print("Executing SQL query to fetch prediction data...")
        predictions_df = pd.read_sql(sql_query, engine)
        print(f"Query returned {len(predictions_df)} rows.")
        
    except Exception as e:
        print(f"Error executing SQL query: {str(e)}")
        print("Falling back to CSV file...")
        USE_SQL_QUERY = False
    
if not USE_SQL_QUERY:
    if not os.path.exists(CSV_FILE_PATH):
        print(f"CSV file not found at {CSV_FILE_PATH}")
        sys.exit(1)
        
    print(f"Loading prediction data from: {CSV_FILE_PATH}")
    predictions_df = pd.read_csv(CSV_FILE_PATH)

# Clean up the data (remove quotes from JSON strings)
for col in ['final_prediction', 'raw_prediction', 'attribution', 'ground_truth']:
    if col in predictions_df.columns:
        # Remove surrounding quotes if present
        predictions_df[col] = predictions_df[col].str.replace('^"', '', regex=True).str.replace('"$', '', regex=True)

# Convert boolean columns to proper boolean types if they exist
boolean_columns = ['final_prediction_matches_ground_truth', 'raw_prediction_matches_ground_truth']
for col in boolean_columns:
    if col in predictions_df.columns:
        predictions_df[col] = predictions_df[col].astype(bool)

# Save the predictions file for the CSV viewer
predictions_output_path = "downloads/los_predictions.csv"
predictions_df.to_csv(predictions_output_path, index=False)
print(f"Saved predictions to: {predictions_output_path}")

# Get unique encounters
unique_encounters = predictions_df[['encounter_id', 'patient_id', 'notes_path', 'encounter_start', 'encounter_end']].drop_duplicates()
print(f"Found {len(unique_encounters)} unique encounters in the predictions data")

# Download and process notes
all_notes = []
encounter_metadata = []

for _, encounter in tqdm(unique_encounters.iterrows(), desc="Processing encounters", total=len(unique_encounters)):
    encounter_id = encounter['encounter_id']
    notes_path = encounter['notes_path']
    
    try:
        # Download the notes.csv file directly into a dataframe
        blob_client = container_client.get_blob_client(notes_path)
        download_stream = blob_client.download_blob()
        notes_content = download_stream.readall()
        
        # Read the CSV content directly into pandas from the bytes
        df = pd.read_csv(io.BytesIO(notes_content))
        
        
        # Add encounter information to each note
        df["encounter_id"] = encounter_id
        df["patient_id"] = encounter["patient_id"]
        df["encounter_start"] = encounter["encounter_start"]
        df["encounter_end"] = encounter["encounter_end"]
        
        # Calculate LOS in days
        try:
            start_date = pd.to_datetime(encounter["encounter_start"])
            end_date = pd.to_datetime(encounter["encounter_end"])
            los_days = (end_date - start_date).days
        except:
            los_days = None
        
        df["los_days"] = los_days
        
        # Append to the master list
        all_notes.append(df)
        
        # Add encounter to metadata
        encounter_metadata.append({
            "encounter_id": encounter_id,
            "patient_id": encounter["patient_id"],
            "encounter_start": encounter["encounter_start"],
            "encounter_end": encounter["encounter_end"],
            "los_days": los_days,
            "notes_count": len(df),
            "notes_path": notes_path
        })
        
        tqdm.write(f"Successfully processed {len(df)} notes for encounter {encounter_id}")
        
    except Exception as e:
        tqdm.write(f"Error processing encounter {encounter_id}: {str(e)}")

# Create combined notes dataframe
combined_notes = pd.concat(all_notes, ignore_index=True) if all_notes else pd.DataFrame()

# Create metadata dataframe
metadata_df = pd.DataFrame(encounter_metadata)

# Create version summary dataframe with performance metrics
version_summary = []
if 'version_key' in predictions_df.columns and 'final_prediction_matches_ground_truth' in predictions_df.columns:
    for version in predictions_df['version_key'].unique():
        version_data = predictions_df[predictions_df['version_key'] == version]
        
        # Get model and prompt version if available
        model_version = version_data['model_version'].iloc[0] if 'model_version' in version_data.columns else 'Unknown'
        prompt_version = version_data['prompt_version'].iloc[0] if 'prompt_version' in version_data.columns else 'Unknown'
        
        # Get model and prompt information
        model_info = MODEL_VERSION_INFO.get(model_version, {"name": "Unknown", "description": "Unknown", "variant": "unknown"})
        prompt_info = PROMPT_VERSION_INFO.get(prompt_version, "Unknown prompt version")
        
        # Calculate accuracy metrics
        final_accuracy = version_data['final_prediction_matches_ground_truth'].mean() * 100 if 'final_prediction_matches_ground_truth' in version_data.columns else None
        raw_accuracy = version_data['raw_prediction_matches_ground_truth'].mean() * 100 if 'raw_prediction_matches_ground_truth' in version_data.columns else None
        
        # Count predictions by type
        today_tomorrow_count = (version_data['final_prediction'] == 'today/tomorrow').sum()
        longer_count = (version_data['final_prediction'] == 'longer').sum()
        
        # Count ground truth distribution
        ground_truth_today_tomorrow = (version_data['ground_truth'] == 'today/tomorrow').sum()
        ground_truth_longer = (version_data['ground_truth'] == 'longer').sum()
        
        # Calculate additional metrics - if both classes exist in ground truth
        if ground_truth_today_tomorrow > 0 and ground_truth_longer > 0:
            # For "today/tomorrow" class
            true_positives_tt = ((version_data['final_prediction'] == 'today/tomorrow') & 
                                (version_data['ground_truth'] == 'today/tomorrow')).sum()
            false_positives_tt = ((version_data['final_prediction'] == 'today/tomorrow') & 
                                 (version_data['ground_truth'] == 'longer')).sum()
            false_negatives_tt = ((version_data['final_prediction'] == 'longer') & 
                                 (version_data['ground_truth'] == 'today/tomorrow')).sum()
            
            # Calculate precision, recall, F1 for "today/tomorrow"
            precision_tt = true_positives_tt / (true_positives_tt + false_positives_tt) if (true_positives_tt + false_positives_tt) > 0 else None
            recall_tt = true_positives_tt / (true_positives_tt + false_negatives_tt) if (true_positives_tt + false_negatives_tt) > 0 else None
            f1_tt = 2 * precision_tt * recall_tt / (precision_tt + recall_tt) if (precision_tt and recall_tt) else None
            
            # Convert to percentages
            precision_tt = precision_tt * 100 if precision_tt is not None else None
            recall_tt = recall_tt * 100 if recall_tt is not None else None
            f1_tt = f1_tt * 100 if f1_tt is not None else None
        else:
            precision_tt = None
            recall_tt = None
            f1_tt = None
        
        version_summary.append({
            'version_key': version,
            'model_version': model_version,
            'prompt_version': prompt_version,
            'model_name': model_info['name'],
            'model_variant': model_info['variant'],
            'model_description': model_info['description'],
            'prompt_description': prompt_info,
            'prediction_count': len(version_data),
            'final_accuracy': final_accuracy,
            'raw_accuracy': raw_accuracy,
            'precision_today_tomorrow': precision_tt,
            'recall_today_tomorrow': recall_tt,
            'f1_today_tomorrow': f1_tt,
            'today_tomorrow_predictions': today_tomorrow_count,
            'longer_predictions': longer_count,
            'ground_truth_today_tomorrow': ground_truth_today_tomorrow,
            'ground_truth_longer': ground_truth_longer
        })

    # Create and save version summary
    version_summary_df = pd.DataFrame(version_summary)
    
    # Sort by model and prompt version for a more organized view
    version_summary_df = version_summary_df.sort_values(['model_version', 'prompt_version'])
    
    version_summary_path = "downloads/version_summary.csv"
    version_summary_df.to_csv(version_summary_path, index=False)
    print(f"Saved version summary to: {version_summary_path}")
    
    # Also save a more detailed analysis file with all metrics
    model_analysis_path = "downloads/model_analysis.csv"
    version_summary_df.to_csv(model_analysis_path, index=False)
    print(f"Saved detailed model analysis to: {model_analysis_path}")

# Save the files for use in the CSV viewer
if not combined_notes.empty:
    # Save the full notes file
    notes_output_path = "downloads/all_clinical_notes.csv"
    combined_notes.to_csv(notes_output_path, index=False)
    print(f"Saved complete notes to: {notes_output_path}")
    
    # Save a simplified version with key columns
    if "note_text" in combined_notes.columns:
        key_columns = ["encounter_id", "patient_id", "note_time", "note_type", "note_text"]
    else:
        # Adjust columns based on actual data
        key_columns = [col for col in combined_notes.columns if col in 
                      ["encounter_id", "patient_id", "timestamp", "text", "type", "department"]]
        # Make sure encounter_id and patient_id are included regardless
        if "encounter_id" not in key_columns:
            key_columns.insert(0, "encounter_id")
        if "patient_id" not in key_columns:
            key_columns.insert(1, "patient_id")
    
    # Only keep columns that actually exist
    key_columns = [col for col in key_columns if col in combined_notes.columns]
    
    simplified_notes = combined_notes[key_columns].copy()
    simplified_output_path = "downloads/simplified_notes.csv"
    simplified_notes.to_csv(simplified_output_path, index=False)
    print(f"Saved simplified notes to: {simplified_output_path}")

# Save metadata 
metadata_output_path = "downloads/encounters_metadata.csv"
metadata_df.to_csv(metadata_output_path, index=False)
print(f"Saved encounter metadata to: {metadata_output_path}")

print("\nProcessing complete!")
print(f"Downloaded notes for {len(encounter_metadata)} encounters")
print("Files saved to the 'downloads' directory:")
print(f"- {os.path.basename(predictions_output_path)}: Contains all predictions with version data")
if 'version_key' in predictions_df.columns:
    print(f"- {os.path.basename('downloads/version_summary.csv')}: Contains summary metrics by version")
    print(f"- {os.path.basename('downloads/model_analysis.csv')}: Contains detailed model performance analysis")
print(f"- {os.path.basename(notes_output_path)}: Contains all notes with full details")
print(f"- {os.path.basename(simplified_output_path)}: Contains simplified notes for easier viewing")
print(f"- {os.path.basename(metadata_output_path)}: Contains metadata about the encounters")
print("\nYou can now use the CSV viewer HTML application:")
print("1. Open csv_viewer.html in your browser")
print(f"2. Load '{notes_output_path}' as the Notes CSV")
print(f"3. Load '{predictions_output_path}' as the Predictions CSV")
print("4. The viewer now supports comparing different model/prompt versions side-by-side")

# Generate a summary of model variants for quick review
if 'version_key' in predictions_df.columns:
    print("\nModel Variants Summary:")
    print("=" * 80)
    print(f"{'Model Version':<15} {'Prompt':<10} {'Variant':<10} {'Count':<8} {'Accuracy':<10} {'Precision':<10} {'Recall':<10}")
    print("-" * 80)
    for _, row in version_summary_df.iterrows():
        print(f"{row['model_version']:<15} {row['prompt_version']:<10} {row['model_variant']:<10} "
              f"{row['prediction_count']:<8} {row['final_accuracy']:.2f}% "
              f"{row['precision_today_tomorrow']:.2f}% " if row['precision_today_tomorrow'] else "N/A "
              f"{row['recall_today_tomorrow']:.2f}% " if row['recall_today_tomorrow'] else "N/A")
    print("=" * 80)