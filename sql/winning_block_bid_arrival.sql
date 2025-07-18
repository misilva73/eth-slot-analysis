WITH blocks AS (
    SELECT
        slot,
        proposer_index,
        execution_payload_block_hash AS block_hash,
        block_total_bytes,
        block_total_bytes_compressed,
        execution_payload_gas_used AS gas_used,
        execution_payload_transactions_count AS num_txs
    FROM default.canonical_beacon_block FINAL
    WHERE
        slot_start_date_time BETWEEN 
            toDateTime('2025-07-17 12:00:00') AND toDateTime('2025-07-17 13:00:00')
        AND meta_network_name = 'mainnet'
),
first_bids AS (
    SELECT
        slot,
        builder_pubkey,
        block_hash,
        value,
        MIN(response_at_slot_time) AS response_time
    FROM default.mev_relay_bid_trace FINAL
    WHERE
        slot_start_date_time BETWEEN 
            toDateTime('2025-07-17 12:00:00') AND toDateTime('2025-07-17 13:00:00')
        AND meta_network_name = 'mainnet'
    GROUP BY slot, block_hash, builder_pubkey, value
),
max_first_bids AS (
    SELECT TOP 1 WITH TIES
        block_hash,
        value AS max_bid_value,
        response_time as first_bid_response
    FROM first_bids
    ORDER BY row_number() OVER (PARTITION BY slot, block_hash ORDER BY value)
)
SELECT
    slot,
    proposer_index,
    blocks.block_hash,
    block_total_bytes,
    block_total_bytes_compressed,
    gas_used,
    num_txs,
    max_bid_value,
    first_bid_response
FROM blocks LEFT JOIN max_first_bids ON blocks.block_hash=max_first_bids.block_hash
ORDER BY slot