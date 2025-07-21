WITH first_seen_atts AS (
    SELECT TOP 1 WITH TIES slot,
        slot_start_date_time,
        meta_client_name,
        meta_client_geo_country,
        propagation_slot_start_diff,
        attesting_validator_index,
        attesting_validator_committee_index
    FROM default.libp2p_gossipsub_beacon_attestation FINAL
    WHERE slot_start_date_time BETWEEN toDateTime('2025-07-21 08:00:00') AND toDateTime('2025-07-21 09:00:00')
        AND meta_network_name = 'mainnet'
        AND startsWith(
            meta_client_name,
            'ethpandaops/mainnet/xatu-tysm'
        )
    ORDER BY row_number() OVER (
            PARTITION BY slot,
            meta_client_name,
            attesting_validator_index
            ORDER BY event_date_time
        )
)
SELECT slot,
    any(slot_start_date_time) AS slot_start_datetime,
    meta_client_name AS node_name,
    any(meta_client_geo_country) AS node_country,
    attesting_validator_committee_index AS attesting_committee,
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
FROM first_seen_atts
GROUP BY slot,
    meta_client_name,
    attesting_validator_committee_index
ORDER BY slot;