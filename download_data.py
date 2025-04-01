import os
import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
from datetime import datetime

# Path to the prediction data file
predictions_file_path = "/Users/zwimpee/data/lcn-529/data-1743526955783.csv"  # Replace with your updated filename when available

# Azure Storage account details
storage_account_name = "aipredictiveengine"
container_name = "lcn-529"

# Create a directory to store downloaded files
os.makedirs("downloads", exist_ok=True)

# Connect to Azure Blob Storage
connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
if not connection_string:
    print("Please set the AZURE_STORAGE_CONNECTION_STRING environment variable")
    connection_string = input("Enter your Azure Storage connection string: ")

# Create Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Load the prediction data
print(f"Loading prediction data from: {predictions_file_path}")
predictions_df = pd.read_csv(predictions_file_path)

# Clean up the data (remove quotes from JSON strings)
for col in ['prediction', 'attribution', 'ground_truth']:
    if col in predictions_df.columns:
        # Remove surrounding quotes if present
        predictions_df[col] = predictions_df[col].str.replace('^"', '', regex=True).str.replace('"$', '', regex=True)

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

for _, encounter in unique_encounters.iterrows():
    encounter_id = encounter['encounter_id']
    notes_path = encounter['notes_path']
    
    try:
        print(f"Downloading notes for encounter {encounter_id}...")
        
        # Download the notes.csv file
        blob_client = container_client.get_blob_client(notes_path)
        download_stream = blob_client.download_blob()
        notes_content = download_stream.readall()
        
        # Save the raw notes file
        local_file_path = f"downloads/{encounter_id}_notes.csv"
        with open(local_file_path, "wb") as local_file:
            local_file.write(notes_content)
        
        # Read the CSV into pandas
        df = pd.read_csv(local_file_path)
        
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
        
        print(f"Successfully processed {len(df)} notes for encounter {encounter_id}")
        
    except Exception as e:
        print(f"Error processing encounter {encounter_id}: {str(e)}")

# Create combined notes dataframe
combined_notes = pd.concat(all_notes, ignore_index=True) if all_notes else pd.DataFrame()

# Create metadata dataframe
metadata_df = pd.DataFrame(encounter_metadata)

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
print(f"- {os.path.basename(predictions_output_path)}: Contains all predictions")
print(f"- {os.path.basename(notes_output_path)}: Contains all notes with full details")
print(f"- {os.path.basename(simplified_output_path)}: Contains simplified notes for easier viewing")
print(f"- {os.path.basename(metadata_output_path)}: Contains metadata about the encounters")
print("\nYou can now use the CSV viewer HTML application:")
print("1. Open csv_viewer.html in your browser")
print(f"2. Load '{notes_output_path}' as the Notes CSV")
print(f"3. Load '{predictions_output_path}' as the Predictions CSV")