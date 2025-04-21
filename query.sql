WITH discharged_encounters AS (
    -- Start with encounters that have been discharged and have notes
    SELECT
        encounter_id,
        patient_id,
        metadata->>'encounter_end' AS encounter_end,
        metadata->>'encounter_start' AS encounter_start,
        metadata->'notes_metadata'->>'blob_path' AS notes_path,
        metadata->'notes_metadata'->>'latest_note_date' AS latest_note_date
    FROM
        encounters
    WHERE
        metadata->>'encounter_end' IS NOT NULL
        AND metadata->'notes_metadata'->>'latest_note_date' IS NOT NULL
        AND metadata->'notes_metadata'->>'latest_note_date' != 'NaT'
        AND metadata->>'encounter_end' != 'NaT'
        -- Ensure the last note is before discharge (valid predictions)
        AND (metadata->'notes_metadata'->>'latest_note_date')::timestamp < (metadata->>'encounter_end')::timestamp
),

-- Define version keys we want to analyze (final predictions only, raw binary handled separately)
version_params AS (
    -- Original GPT-4o model versions
    SELECT 
        '0.1.0-prod' AS model_version, 
        '1.0.0' AS prompt_version,
        'final_prediction' AS prediction_type
    UNION ALL
    SELECT 
        '0.1.0-prod' AS model_version, 
        '1.3.0' AS prompt_version,
        'final_prediction' AS prediction_type
    UNION ALL
    SELECT 
        '0.2.0-stage' AS model_version, 
        '1.1.0' AS prompt_version,
        'final_prediction' AS prediction_type
    UNION ALL
    SELECT 
        '0.2.0-stage' AS model_version, 
        '1.2.0' AS prompt_version,
        'final_prediction' AS prediction_type
    UNION ALL
    SELECT 
        '0.2.0-stage' AS model_version, 
        '1.3.0' AS prompt_version,
        'final_prediction' AS prediction_type
    
    -- GPT-4.1 standard model
    UNION ALL
    SELECT 
        '0.3.0-stage' AS model_version, 
        '1.1.0' AS prompt_version,
        'final_prediction' AS prediction_type
    UNION ALL
    SELECT 
        '0.3.0-stage' AS model_version, 
        '1.2.0' AS prompt_version,
        'final_prediction' AS prediction_type
    UNION ALL
    SELECT 
        '0.3.0-stage' AS model_version, 
        '1.3.0' AS prompt_version,
        'final_prediction' AS prediction_type
        
    -- GPT-4.1-mini model
    UNION ALL
    SELECT 
        '0.4.0-stage' AS model_version, 
        '1.1.0' AS prompt_version,
        'final_prediction' AS prediction_type
    UNION ALL
    SELECT 
        '0.4.0-stage' AS model_version, 
        '1.2.0' AS prompt_version,
        'final_prediction' AS prediction_type
    UNION ALL
    SELECT 
        '0.4.0-stage' AS model_version, 
        '1.3.0' AS prompt_version,
        'final_prediction' AS prediction_type
        
    -- GPT-4.1-nano model
    UNION ALL
    SELECT 
        '0.5.0-stage' AS model_version, 
        '1.1.0' AS prompt_version,
        'final_prediction' AS prediction_type
    UNION ALL
    SELECT 
        '0.5.0-stage' AS model_version, 
        '1.2.0' AS prompt_version,
        'final_prediction' AS prediction_type
    UNION ALL
    SELECT 
        '0.5.0-stage' AS model_version, 
        '1.3.0' AS prompt_version,
        'final_prediction' AS prediction_type
),

-- Get raw binary predictions 
raw_binary_predictions AS (
    -- From encounters table
    SELECT 
        e.encounter_id,
        e.patient_id,
        e.metadata->'predictions'->'los'->'binary_model_results'->>'label' AS raw_prediction,
        e.metadata->'predictions'->'los'->'binary_model_results'->'confidence' AS raw_confidence,
        e.metadata->'notes_metadata'->>'latest_note_date' AS note_date,
        ROW_NUMBER() OVER (PARTITION BY e.encounter_id, e.metadata->'notes_metadata'->>'latest_note_date' 
                           ORDER BY e.metadata->'predictions'->'los'->>'last_processed' DESC) AS rn
    FROM 
        encounters e
    JOIN
        discharged_encounters de ON e.encounter_id = de.encounter_id
    WHERE 
        e.metadata->'predictions'->'los'->>'processing_status' = 'complete'
        AND e.metadata->'predictions'->'los'->'binary_model_results'->>'label' IS NOT NULL
    
    UNION ALL
    
    -- From encounter_history table
    SELECT 
        eh.encounter_id,
        eh.patient_id,
        eh.metadata->'predictions'->'los'->'binary_model_results'->>'label' AS raw_prediction,
        eh.metadata->'predictions'->'los'->'binary_model_results'->'confidence' AS raw_confidence,
        eh.metadata->'notes_metadata'->>'latest_note_date' AS note_date,
        ROW_NUMBER() OVER (PARTITION BY eh.encounter_id, eh.metadata->'notes_metadata'->>'latest_note_date' 
                          ORDER BY eh.metadata->'predictions'->'los'->>'last_processed' DESC) AS rn
    FROM 
        encounter_history eh
    JOIN
        discharged_encounters de ON eh.encounter_id = de.encounter_id
    WHERE 
        eh.metadata->'predictions'->'los'->>'processing_status' = 'complete'
        AND eh.metadata->'predictions'->'los'->'binary_model_results'->>'label' IS NOT NULL
),

-- Get distinct raw binary predictions
distinct_raw_predictions AS (
    SELECT
        encounter_id,
        patient_id,
        raw_prediction,
        raw_confidence,
        note_date
    FROM 
        raw_binary_predictions
    WHERE 
        rn = 1  -- Take only the most recent raw prediction for each encounter/note date
),

final_predictions AS (
    -- UNION both tables to get all final predictions
    -- First from encounters table (current records)
    SELECT 
        e.encounter_id,
        e.patient_id,
        e.metadata->'predictions'->'los'->'final_prediction'->>'final_binary_label' AS prediction,
        e.metadata->'predictions'->'los'->'final_prediction'->'final_binary_confidence' AS confidence,
        e.metadata->'predictions'->'los'->'final_prediction'->>'attribution' AS attribution,
        e.metadata->'predictions'->'los'->>'last_processed' AS prediction_timestamp,
        e.metadata->'notes_metadata'->>'latest_note_date' AS note_date,
        de.notes_path,
        de.encounter_start,
        de.encounter_end,
        e.metadata->'predictions'->'los'->>'model_version' AS model_version,
        e.metadata->'predictions'->'los'->>'prompt_version' AS prompt_version,
        v.model_version || '_' || v.prompt_version AS version_key,
        -- Calculate ground truth based on time between note and discharge
        CASE
            WHEN date_part('day', (de.encounter_end::timestamp - (e.metadata->'notes_metadata'->>'latest_note_date')::timestamp)) <= 2 
            THEN 'today/tomorrow'
            ELSE 'longer'
        END AS ground_truth,
        ROW_NUMBER() OVER (PARTITION BY e.encounter_id, e.metadata->'notes_metadata'->>'latest_note_date', 
            v.model_version || '_' || v.prompt_version
            ORDER BY e.metadata->'predictions'->'los'->>'last_processed' DESC) AS rn
    FROM 
        encounters e
    JOIN
        version_params v ON e.metadata->'predictions'->'los'->>'model_version' = v.model_version 
                       AND e.metadata->'predictions'->'los'->>'prompt_version' = v.prompt_version
    JOIN
        discharged_encounters de ON e.encounter_id = de.encounter_id
    WHERE 
        e.metadata->'predictions'->'los'->>'processing_status' = 'complete'
        AND e.metadata->'predictions'->'los'->'final_prediction'->>'final_binary_label' IS NOT NULL

    UNION ALL

    -- Then from encounter_history table (historical records)
    SELECT 
        eh.encounter_id,
        eh.patient_id,
        eh.metadata->'predictions'->'los'->'final_prediction'->>'final_binary_label' AS prediction,
        eh.metadata->'predictions'->'los'->'final_prediction'->'final_binary_confidence' AS confidence,
        eh.metadata->'predictions'->'los'->'final_prediction'->>'attribution' AS attribution,
        eh.metadata->'predictions'->'los'->>'last_processed' AS prediction_timestamp,
        eh.metadata->'notes_metadata'->>'latest_note_date' AS note_date,
        de.notes_path,
        de.encounter_start,
        de.encounter_end,
        eh.metadata->'predictions'->'los'->>'model_version' AS model_version,
        eh.metadata->'predictions'->'los'->>'prompt_version' AS prompt_version,
        v.model_version || '_' || v.prompt_version AS version_key,
        -- Calculate ground truth based on time between note and discharge
        CASE
            WHEN date_part('day', (de.encounter_end::timestamp - (eh.metadata->'notes_metadata'->>'latest_note_date')::timestamp)) <= 2 
            THEN 'today/tomorrow'
            ELSE 'longer'
        END AS ground_truth,
        ROW_NUMBER() OVER (PARTITION BY eh.encounter_id, eh.metadata->'notes_metadata'->>'latest_note_date', 
            v.model_version || '_' || v.prompt_version
            ORDER BY eh.metadata->'predictions'->'los'->>'last_processed' DESC) AS rn
    FROM 
        encounter_history eh
    JOIN
        version_params v ON eh.metadata->'predictions'->'los'->>'model_version' = v.model_version 
                       AND eh.metadata->'predictions'->'los'->>'prompt_version' = v.prompt_version
    JOIN
        discharged_encounters de ON eh.encounter_id = de.encounter_id
    WHERE 
        eh.metadata->'predictions'->'los'->>'processing_status' = 'complete'
        AND eh.metadata->'predictions'->'los'->'final_prediction'->>'final_binary_label' IS NOT NULL
),

-- Get distinct final predictions
distinct_final_predictions AS (
    SELECT
        encounter_id,
        patient_id,
        prediction AS final_prediction,
        confidence AS final_confidence,
        attribution,
        prediction_timestamp,
        note_date,
        notes_path,
        encounter_start,
        encounter_end,
        model_version,
        prompt_version,
        version_key,
        ground_truth
    FROM 
        final_predictions
    WHERE 
        rn = 1  -- Take only the most recent prediction for each unique combo
        AND prediction IS NOT NULL  -- Ensure we have a valid prediction
)

-- Main query - combine final predictions with raw binary predictions
SELECT
    fp.encounter_id,
    fp.patient_id,
    fp.final_prediction,
    fp.final_confidence,
    rp.raw_prediction,
    rp.raw_confidence,
    fp.attribution,
    fp.prediction_timestamp,
    fp.note_date,
    fp.notes_path,
    fp.encounter_start,
    fp.encounter_end,
    fp.model_version,
    fp.prompt_version,
    fp.version_key,
    fp.ground_truth,
    -- Add columns indicating if predictions match ground truth
    (fp.final_prediction = fp.ground_truth) AS final_prediction_matches_ground_truth,
    (rp.raw_prediction = fp.ground_truth) AS raw_prediction_matches_ground_truth
FROM
    distinct_final_predictions fp
LEFT JOIN
    distinct_raw_predictions rp ON fp.encounter_id = rp.encounter_id AND fp.note_date = rp.note_date
ORDER BY
    fp.encounter_id,
    fp.note_date,
    fp.version_key;