WITH ihave_times AS (
    SELECT
        message_id,
        peer_id_unique_key AS peer_id,
        COUNT(message_id) AS ihave_count,
        MIN(event_date_time) AS first_ihave_datetime
    FROM default.libp2p_rpc_meta_control_ihave FINAL
    WHERE event_date_time BETWEEN 
            toDateTime('2025-07-09 12:00:00') AND toDateTime('2025-07-09 12:30:00')
        AND meta_network_name = 'mainnet'
        AND startsWith(topic_name, 'beacon_attestation')
    GROUP BY message_id, peer_id_unique_key
),
attes_msg AS (
    SELECT TOP 1 WITH TIES
        message_id,
        peer_id_unique_key AS first_attesting_peer,
        event_date_time AS first_attestation_datetime,
        slot,
        slot_start_date_time AS slot_start_datetime,
        attesting_validator_index AS attesting_validator,
        attesting_validator_committee_index AS attesting_validator_committee,
        message_size
    FROM default.libp2p_gossipsub_beacon_attestation FINAL
    WHERE slot_start_date_time BETWEEN 
            toDateTime('2025-07-09 12:00:00') AND toDateTime('2025-07-09 12:30:00')
        AND meta_network_name = 'mainnet'
    ORDER BY row_number() OVER (PARTITION BY message_id ORDER BY event_date_time)
)
SELECT
    ihave_times.message_id,
    ihave_times.peer_id,
    ihave_count,
    first_ihave_datetime,
    slot, 
    slot_start_datetime,
    DATEDIFF(MILLISECOND, slot_start_datetime, first_ihave_datetime) AS first_ihave_time_ms,
    first_attesting_peer,
    first_attestation_datetime,
    attesting_validator,
    attesting_validator_committee,
    message_size
FROM ihave_times INNER JOIN attes_msg ON 
    ihave_times.message_id = attes_msg.message_id