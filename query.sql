WITH discharged_encounters AS (
    -- Start with encounters that have been discharged and have a final prediction
    SELECT
        encounter_id,
        patient_id,
        metadata->>'encounter_end' AS encounter_end,
        metadata->>'encounter_start' AS encounter_start,
        metadata->'notes_metadata'->>'blob_path' AS notes_path
    FROM
        encounters
    WHERE
        metadata->>'encounter_end' IS NOT NULL
        AND metadata->'predictions'->'los'->'results'->'label' IS NOT NULL
),

prediction_points AS (
    -- Get all prediction points for these encounters based on unique note dates
    SELECT DISTINCT ON (eh.encounter_id, eh.metadata->'notes_metadata'->>'latest_note_date')
        de.encounter_id,
        de.patient_id,
        eh.metadata->'predictions'->'los'->'results'->'label' AS prediction,
        eh.metadata->'predictions'->'los'->'results'->'confidence' AS confidence,
        eh.metadata->'predictions'->'los'->'results'->'verification'->'attribution' AS attribution,
        eh.metadata->'predictions'->'los'->'last_processed' AS prediction_timestamp,
        eh.metadata->'notes_metadata'->>'latest_note_date' AS note_date,
        de.notes_path,
        de.encounter_start,
        de.encounter_end,
        -- Calculate ground truth based on time between note and discharge
        CASE
            WHEN date_part('day', (de.encounter_end::timestamp - (eh.metadata->'notes_metadata'->>'latest_note_date')::timestamp)) <= 2 
            THEN '"today/tomorrow"'
            ELSE '"longer"'
        END AS ground_truth,
        ROW_NUMBER() OVER (PARTITION BY eh.encounter_id ORDER BY eh.metadata->'notes_metadata'->>'latest_note_date') AS sequence_position
    FROM
        encounter_history eh
    JOIN
        discharged_encounters de ON eh.encounter_id = de.encounter_id
    WHERE
        eh.metadata->'predictions'->'los'->'results'->'label' IS NOT NULL
        AND eh.metadata->'predictions'->'los'->'processing_status' = '"complete"'
        AND (eh.metadata->'predictions'->'los'->'results'->'verification'->'attribution')::text NOT ILIKE '%newborn%' -- Filter out newborn mentions
    ORDER BY
        eh.encounter_id,
        eh.metadata->'notes_metadata'->>'latest_note_date',
        eh.add_date DESC
),

-- Find correct predictions (where prediction matches ground truth)
correct_predictions AS (
    SELECT *
    FROM prediction_points
    WHERE prediction::text = ground_truth  -- Convert prediction to text for comparison
),

-- Get examples for both categories ensuring a balanced dataset
balanced_examples AS (
    (SELECT * FROM correct_predictions WHERE ground_truth = '"today/tomorrow"' LIMIT 50)
    UNION ALL
    (SELECT * FROM correct_predictions WHERE ground_truth = '"longer"' LIMIT 50)
)

-- Main query - get complete prediction histories for these selected examples
SELECT
    pp.*
FROM
    prediction_points pp
JOIN
    balanced_examples be ON pp.encounter_id = be.encounter_id
ORDER BY
    pp.encounter_id,
    pp.sequence_position;