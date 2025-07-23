SELECT TOP 1 WITH TIES slot,
    slot_start_date_time AS slot_start_datetime,
    meta_client_name AS node_name,
    meta_client_geo_country AS node_country,
    propagation_slot_start_diff AS block_arrival_ms
FROM default.libp2p_gossipsub_beacon_block FINAL
WHERE slot_start_date_time BETWEEN toDateTime('2025-07-21 08:30:00') AND toDateTime('2025-07-21 10:00:00')
    AND meta_network_name = 'mainnet'
    AND startsWith(
        meta_client_name,
        'ethpandaops/mainnet/xatu-tysm'
    )
ORDER BY row_number() OVER (
        PARTITION BY slot,
        meta_client_name
        ORDER BY event_date_time
    )