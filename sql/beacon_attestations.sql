WITH beacon_api_attestations AS (
    SELECT 
        slot,
        any(slot_start_date_time) AS slot_start_time,
        meta_client_name,
        any(meta_client_geo_country) AS meta_client_geo_country,
        any(meta_consensus_implementation) AS meta_consensus_implementation,
        attesting_validator_index,
        MIN(propagation_slot_start_diff) AS propagation_slot_start_diff
    FROM default.beacon_api_eth_v1_events_attestation
    WHERE
        slot_start_date_time BETWEEN 
            toDateTime('2025-07-09 12:00:00') AND toDateTime('2025-07-09 13:00:00')
        AND meta_network_name = 'mainnet'
        AND attesting_validator_index Is NOT NULL
    GROUP BY slot, meta_client_name, attesting_validator_index
)
SELECT
    slot,
    any(slot_start_time) AS slot_start_time,
    meta_client_name AS node_name,
    any(meta_client_geo_country) AS node_country,
    any(meta_consensus_implementation) AS node_consensus_client,
    count(distinct attesting_validator_index) AS num_obs_attes,
    avg(propagation_slot_start_diff) AS attes_time_avg,
    stddevSamp(propagation_slot_start_diff) AS attes_time_std,
    min(propagation_slot_start_diff) AS p0,
    quantileExact(0.05)(propagation_slot_start_diff) AS p05,
    quantileExact(0.1)(propagation_slot_start_diff) AS p10,
    quantileExact(0.15)(propagation_slot_start_diff) AS p15,
    quantileExact(0.2)(propagation_slot_start_diff) AS p20,
    quantileExact(0.25)(propagation_slot_start_diff) AS p25,
    quantileExact(0.3)(propagation_slot_start_diff) AS p30,
    quantileExact(0.35)(propagation_slot_start_diff) AS p35,
    quantileExact(0.4)(propagation_slot_start_diff) AS p40,
    quantileExact(0.45)(propagation_slot_start_diff) AS p45,
    quantileExact(0.5)(propagation_slot_start_diff) AS p50,
    quantileExact(0.55)(propagation_slot_start_diff) AS p55,
    quantileExact(0.6)(propagation_slot_start_diff) AS p60,
    quantileExact(0.65)(propagation_slot_start_diff) AS p65,
    quantileExact(0.7)(propagation_slot_start_diff) AS p70,
    quantileExact(0.75)(propagation_slot_start_diff) AS p75,
    quantileExact(0.8)(propagation_slot_start_diff) AS p80,
    quantileExact(0.85)(propagation_slot_start_diff) AS p85,
    quantileExact(0.9)(propagation_slot_start_diff) AS p90,
    quantileExact(0.95)(propagation_slot_start_diff) AS p95,
    quantileExact(0.99)(propagation_slot_start_diff) AS p99,
    max(propagation_slot_start_diff) AS p100
FROM beacon_api_attestations
GROUP BY slot, meta_client_name
ORDER BY slot;