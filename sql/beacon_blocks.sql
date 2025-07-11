SELECT
    slot,
    meta_client_name AS node_name,
    meta_client_geo_country AS node_country,
    meta_consensus_implementation AS node_consensus_client,
    slot_start_date_time AS slot_start_time,
    event_date_time AS block_arrival_time,
    propagation_slot_start_diff AS block_arrival_ms
FROM default.beacon_api_eth_v1_events_block FINAL
WHERE slot_start_date_time BETWEEN 
        toDateTime('2025-07-09 12:00:00') AND toDateTime('2025-07-09 13:00:00')
    AND meta_network_name = 'mainnet'