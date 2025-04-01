# Clinical Note Review

A tool for reviewing clinical notes alongside model predictions for validation and analysis.

## Overview

The Clinical Note Review tool provides an interactive interface for healthcare professionals and machine learning practitioners to:

- View clinical notes and model predictions side-by-side
- Navigate through large datasets of clinical documents
- Identify attributions and evidence for model predictions
- Validate model performance against ground truth

## Features

- Interactive HTML-based interface for data visualization
- Support for CSV-formatted clinical notes and predictions
- Highlighting of attribution terms in notes
- Chronological ordering of clinical notes
- Navigation between prediction entries

## Getting Started

### Prerequisites

- Modern web browser (Chrome, Firefox, Safari, or Edge)
- Clinical notes in CSV format
- Model predictions in CSV format

### Usage

1. Place your CSV files in the `downloads` directory or prepare them to upload
2. Open `csv_viewer.html` in your web browser
3. Upload your notes CSV and predictions CSV files
4. Review the predictions and corresponding notes

## Data Format

### Notes CSV
Must contain columns with:
- `encounter_id`: Unique identifier for the clinical encounter
- Text content (one of: `note_text`, `text`, or `anonymized_text`)
- Note date (recommended: `note_date` or `note_time`)

### Predictions CSV
Must contain columns with:
- `encounter_id`: Matching the notes encounter ID
- `prediction`: The model's prediction
- Optional: `ground_truth`, `confidence`, `attribution`

## Development

To download data from Azure storage, use the provided download script:

```bash
# Set your Azure connection string
export AZURE_STORAGE_CONNECTION_STRING="your-connection-string"

# Run the download script
python download_data.py
```

## License

[License information to be added] 