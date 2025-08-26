import pytz
import logging
import numpy as np
import pandas as pd
from typing import Set
from datetime import datetime
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_relay_slots(db_url: str, slot_start: int, slot_end: int) -> Set[int]:
    # Define dates for queries
    start_date_str = convert_slot_number_to_date_string(slot_start)
    end_date_str = convert_slot_number_to_date_string(slot_end)
    # Define and run query
    query_str = f"""
    SELECT DISTINCT
        slot
    FROM default.mev_relay_proposer_payload_delivered FINAL
    WHERE
        slot_start_date_time BETWEEN toDateTime('{start_date_str}') AND toDateTime('{end_date_str}')
        AND meta_network_name = 'mainnet'
    """
    engine = create_engine(db_url)
    relay_slots = set(pd.read_sql(query_str, con=engine)["slot"].values.tolist())
    return relay_slots


def get_missed_slots(db_url: str, slot_start: int, slot_end: int) -> Set[int]:
    # Define dates for queries
    start_date_str = convert_slot_number_to_date_string(slot_start)
    end_date_str = convert_slot_number_to_date_string(slot_end)
    # Define and run query
    query_str = f"""
        WITH beacon_unique_slots AS (
            SELECT slot
            FROM default.canonical_beacon_block FINAL
            WHERE slot_start_date_time BETWEEN toDateTime('{start_date_str}') AND toDateTime('{end_date_str}')
                AND meta_network_name = 'mainnet'
            GROUP BY slot
            ORDER BY slot
        ),
        beacon_slot_diffs AS (
            SELECT slot -1 AS slot,
                slot - lagInFrame(slot, 1, slot) OVER (
                    ORDER BY slot ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS beacon_slot_diff
            FROM beacon_unique_slots
            ORDER BY slot
        ),
        libp2p_unique_slots AS (
            SELECT slot
            FROM default.libp2p_gossipsub_beacon_block FINAL
            WHERE slot_start_date_time BETWEEN toDateTime('{start_date_str}') AND toDateTime('{end_date_str}')
                AND meta_network_name = 'mainnet'
            GROUP BY slot
            ORDER BY slot
        ),
        libp2p_slot_diffs AS (
            SELECT slot -1 AS slot,
                slot - lagInFrame(slot, 1, slot) OVER (
                    ORDER BY slot ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS libp2p_slot_diff
            FROM libp2p_unique_slots
            ORDER BY slot
        )
        SELECT beacon_slot_diffs.slot
        FROM beacon_slot_diffs
            OUTER JOIN libp2p_slot_diffs ON beacon_slot_diffs.slot = libp2p_slot_diffs.slot
        WHERE beacon_slot_diffs.beacon_slot_diff > 1
            AND libp2p_slot_diffs.libp2p_slot_diff > 1
    """
    engine = create_engine(db_url)
    missed_slots = set(pd.read_sql(query_str, con=engine)["slot"].values.tolist())
    return missed_slots


def get_slot_start_times(db_url: str, slot_start: int, slot_end: int) -> pd.DataFrame:
    # Define dates for queries
    start_date_str = convert_slot_number_to_date_string(slot_start)
    end_date_str = convert_slot_number_to_date_string(slot_end)
    # Define and run query
    query_str = f"""
        SELECT DISTINCT slot, slot_start_date_time AS slot_start_datetime
        FROM default.canonical_beacon_block FINAL
        WHERE slot_start_date_time BETWEEN toDateTime('{start_date_str}') AND toDateTime('{end_date_str}')
            AND meta_network_name = 'mainnet'
    """
    engine = create_engine(db_url)
    slot_start_df = pd.read_sql(query_str, con=engine)
    slot_start_df["slot_start_datetime"] = pd.to_datetime(
        slot_start_df["slot_start_datetime"]
    )
    return slot_start_df


def get_attestations_for_slot(slot: int, db_url: str) -> pd.DataFrame:
    # Define dates for queries (with some buffer)
    start_date_str = convert_slot_number_to_date_string(slot - 5)
    end_date_str = convert_slot_number_to_date_string(slot + 5)
    # Define and run query
    query_str = f"""
        SELECT slot,
            attesting_validator_index AS atts_validator,
            attesting_validator_committee_index AS atts_subnet,
            MIN(propagation_slot_start_diff) AS atts_arrival_time_ms
        FROM default.libp2p_gossipsub_beacon_attestation FINAL
        WHERE slot_start_date_time BETWEEN toDateTime('{start_date_str}') AND toDateTime('{end_date_str}')
            AND meta_network_name = 'mainnet'
            AND slot = {slot}
            AND attesting_validator_index IS NOT NULL
            AND propagation_slot_start_diff >= 0
            AND propagation_slot_start_diff <= 12000
            AND startsWith(
                meta_client_name,
                'ethpandaops/mainnet/xatu-tysm'
            )
            AND endsWith(
                meta_client_name,
                '0-1'
            )
        GROUP BY slot,
            attesting_validator_index,
            attesting_validator_committee_index
    """
    engine = create_engine(db_url)
    atts_df = pd.read_sql(query_str, con=engine)
    return atts_df


def get_committee_attestations_for_slot(slot: int, db_url: str) -> pd.DataFrame:
    # Define dates for queries (with some buffer)
    start_date_str = convert_slot_number_to_date_string(slot - 5)
    end_date_str = convert_slot_number_to_date_string(slot + 5)
    # Define and run query
    query_str = f"""
        SELECT slot,
            meta_client_name AS node_name,
            any(meta_client_geo_country) AS node_country,
            attesting_validator_index AS atts_validator,
            attesting_validator_committee_index AS atts_subnet,
            MIN(propagation_slot_start_diff) AS atts_arrival_time_ms
        FROM default.libp2p_gossipsub_beacon_attestation FINAL
        WHERE slot_start_date_time BETWEEN toDateTime('{start_date_str}') AND toDateTime('{end_date_str}')
            AND meta_network_name = 'mainnet'
            AND slot = {slot}
            AND attesting_validator_index IS NOT NULL
            AND propagation_slot_start_diff >= 0
            AND propagation_slot_start_diff <= 12000
            AND startsWith(
                meta_client_name,
                'ethpandaops/mainnet/xatu-tysm'
            )
            AND endsWith(
                meta_client_name,
                '0-1'
            )
        GROUP BY slot,
            meta_client_name,
            attesting_validator_index,
            attesting_validator_committee_index
    """
    engine = create_engine(db_url)
    atts_df = pd.read_sql(query_str, con=engine)
    return atts_df


def get_validator_ethseer_info(db_url: str) -> pd.DataFrame:
    # ignoring pubkey for now; may be useful later
    query_str = f"""
        SELECT TOP 1 WITH TIES 
            index, 
            --pubkey,
            entity
        FROM ethseer_validator_entity FINAL
        WHERE meta_network_name = 'mainnet'
        ORDER BY row_number() OVER (
            PARTITION BY index, pubkey
            ORDER BY updated_date_time DESC
        )
    """
    engine = create_engine(db_url)
    info_df = pd.read_sql(query_str, con=engine)
    return info_df


def get_block_propagation_stats(
    db_url: str, slot_start: int, slot_end: int
) -> pd.DataFrame:
    # Define dates for queries
    start_date_str = convert_slot_number_to_date_string(slot_start)
    end_date_str = convert_slot_number_to_date_string(slot_end)
    # Define and run query
    query_str = f"""
    SELECT slot,
        quantileExact(0.95)(propagation_slot_start_diff) AS p95_block_arrival_ms,
        quantileExact(0.66)(propagation_slot_start_diff) AS p66_block_arrival_ms,
        avg(propagation_slot_start_diff) AS avg_block_arrival_ms
    FROM default.beacon_api_eth_v1_events_block_gossip FINAL
    WHERE slot_start_date_time BETWEEN toDateTime('{start_date_str}') AND toDateTime('{end_date_str}')
        AND meta_network_name = 'mainnet'
    GROUP BY slot
    """
    engine = create_engine(db_url)
    block_df = pd.read_sql(query_str, con=engine)
    return block_df


def get_block_content_info(db_url: str, slot_start: int, slot_end: int) -> pd.DataFrame:
    # Define dates for queries
    start_date_str = convert_slot_number_to_date_string(slot_start)
    end_date_str = convert_slot_number_to_date_string(slot_end)
    # Define and run query
    query_str = f"""
    SELECT slot,
        any(block_total_bytes_compressed) AS block_total_bytes_compressed,
        any(proposer_index) AS block_proposer_index,
        any(execution_payload_gas_used) AS block_gas_used,
        any(execution_payload_transactions_count) AS block_tx_count,
        any(execution_payload_blob_gas_used)/131072 AS block_blob_count
    FROM default.canonical_beacon_block FINAL
    WHERE slot_start_date_time BETWEEN toDateTime('{start_date_str}') AND toDateTime('{end_date_str}')
        AND meta_network_name = 'mainnet'
    GROUP BY slot
    """
    engine = create_engine(db_url)
    block_df = pd.read_sql(query_str, con=engine)
    return block_df


def convert_slot_number_to_date_string(slot: int) -> str:
    slot_timestamp = slot * 12 + 1606824023
    slot_date = datetime.fromtimestamp(slot_timestamp, tz=pytz.timezone("UTC"))
    slot_date_str = slot_date.strftime("%Y-%m-%d %H:%M:%S")
    return slot_date_str
