import os
import sys
import json
import logging
import argparse
import numpy as np
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from typing import List, Tuple


sys.path.append(str(Path(__file__).parent))
import query

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_slot_sample_df(
    slot_start: int, slot_end: int, db_url: str, sample_size: int, data_dir: str
) -> pd.DataFrame:
    relay_slots_list, self_build_list = get_slot_lists(slot_start, slot_end, db_url)
    # Get relay data
    raw_relay_df = load_relay_data(data_dir)
    if len(raw_relay_df) == 0:
        relay_df = pd.DataFrame(
            {
                "slot": relay_slots_list,
                "request_datetime": None,
                "publish_datetime": None,
                "header_from": "no_relay_info",
            }
        )
    else:
        relay_df = raw_relay_df[raw_relay_df["slot"].isin(relay_slots_list)]
    # Get self build data
    self_build_df = pd.DataFrame(
        {
            "slot": self_build_list,
            "request_datetime": None,
            "publish_datetime": None,
            "header_from": "self_build",
        }
    )
    # Add slot start times
    slot_start_df = query.get_slot_start_times(db_url, slot_start - 2, slot_end + 2)
    relay_df = relay_df.merge(
        slot_start_df,
        on="slot",
        how="inner",
    )
    self_build_df = self_build_df.merge(
        slot_start_df,
        on="slot",
        how="inner",
    )
    # Filter rows with data issues on relay df
    relay_df = relay_df[
        (df["publish_datetime"] - df["slot_start_datetime"]).dt.total_seconds().between(0,4)
        ]
    relay_df = relay_df[
        (df["request_datetime"] - df["slot_start_datetime"]).dt.total_seconds()>0
        ]
    relay_df = relay_df[
        (df["publish_datetime"] - df["request_datetime"]).dt.total_seconds()>0
        ]
    # Sample slots by category and combine
    self_build_n = len(self_build_df)
    relay_n = len(relay_df)
    if sample_size is None or self_build_n + relay_n <= sample_size:
        df = pd.concat([relay_df, self_build_df], ignore_index=True)
    elif self_build_n <= int(sample_size / 2):
        relay_sample_df = relay_df.sample(n=sample_size - self_build_n, random_state=42)
        df = pd.concat([relay_sample_df, self_build_df], ignore_index=True)
    elif relay_n <= int(sample_size / 2):
        self_build_sample_df = self_build_df.sample(
            n=sample_size - relay_n, random_state=42
        )
        df = pd.concat([relay_df, self_build_sample_df], ignore_index=True)
    else:
        relay_sample_df = relay_df.sample(n=int(sample_size / 2))
        self_build_sample_df = self_build_df.sample(n=int(sample_size / 2))
        df = pd.concat([relay_sample_df, self_build_sample_df], ignore_index=True)
    # Compute extra columns
    df["publish_datetime"] = pd.to_datetime(df["publish_datetime"])
    df["request_datetime"] = np.where(
        df["header_from"] == "self_build",
        df["slot_start_datetime"],
        df["request_datetime"],
    )
    df["publish_datetime"] = np.where(
        df["header_from"] == "self_build",
        df["slot_start_datetime"],
        df["publish_datetime"],
    )
    df["request_time_ms"] = (
        df["request_datetime"] - df["slot_start_datetime"]
    ).dt.total_seconds() * 1000
    df["publish_time_ms"] = (
        df["publish_datetime"] - df["slot_start_datetime"]
    ).dt.total_seconds() * 1000
    return df


def get_slot_lists(
    slot_start: int,
    slot_end: int,
    db_url: str,
) -> Tuple[List[int], List[int]]:
    # Get slots sets
    all_slots = set(range(slot_start, slot_end + 1))
    relay_slots_with_buffer = query.get_relay_slots(
        db_url, slot_start - 2, slot_end + 2
    )
    missed_slots_with_buffer = query.get_missed_slots(
        db_url, slot_start - 2, slot_end + 2
    )
    missed_or_relay = relay_slots_with_buffer.union(missed_slots_with_buffer)
    self_build_slots = all_slots.difference(missed_or_relay)
    # Build slot lists
    relay_slots_list = list(relay_slots_with_buffer.intersection(all_slots))
    self_build_list = list(self_build_slots)
    return relay_slots_list, self_build_list


def load_relay_data(data_dir: str) -> pd.DataFrame:
    relay_data_folder = os.path.join(data_dir, "relay")
    # Titan relay data
    titan_file = os.path.join(relay_data_folder, "block_times_since_12187616.csv")
    try:
        titan_df = pd.read_csv(titan_file)
        titan_df = titan_df.rename(columns={"slot_number": "slot"})
        titan_df["request_datetime"] = pd.to_datetime(
            titan_df["header_request_received"]
        )
        titan_df["publish_datetime"] = pd.to_datetime(titan_df["block_published"])
        titan_df = titan_df[["slot", "request_datetime", "publish_datetime"]]
        titan_df["header_from"] = "titan"
    except FileNotFoundError:
        logging.warning(f"Relay data file {titan_file} not found.")
        titan_df = pd.DataFrame(
            columns=["slot", "request_datetime", "publish_datetime"]
        )
    # Ultrasounds relay data
    ultra_file = os.path.join(
        relay_data_folder, "ultrasoud_payload_publish_time_07_21_17_32.csv"
    )
    try:
        ultra_df = pd.read_csv(ultra_file)
        ultra_df["request_datetime"] = pd.to_datetime(
            ultra_df["received_at"]
        ).dt.tz_localize(None)
        ultra_df["publish_datetime"] = pd.to_datetime(
            ultra_df["time_before_publish"]
        ).dt.tz_localize(None)
        ultra_df = ultra_df[ultra_df["is_ultra_sound_header"]]
        ultra_df = ultra_df[["slot", "request_datetime", "publish_datetime"]]
        ultra_df["header_from"] = "ultrasound"
    except FileNotFoundError:
        logging.warning(f"Relay data file {ultra_file} not found.")
        ultra_df = pd.DataFrame(
            columns=["slot", "request_datetime", "publish_datetime"]
        )
    # Join datasets
    relay_df = pd.concat([ultra_df, titan_df], ignore_index=True)
    return relay_df


def get_and_save_ethseer_validator_info(
    db_url: str, data_dir: str, reprocess: bool = False
) -> None:
    file_path = os.path.join(data_dir, "ethseer_val_info.parquet")
    if os.path.exists(file_path) and reprocess == False:
        logging.info(f"Loading validator info already in {file_path}")
    else:
        logging.info(f"Querying validator info from ethseer and saving in {file_path}")
        val_entities_df = query.get_validator_ethseer_info(db_url)
        val_entities_df.to_parquet(file_path, index=False)


def get_and_save_attestations(
    slot_sample_df: pd.DataFrame,
    db_url: str,
    out_dir: str,
) -> None:
    atts_df = pd.DataFrame()
    for slot in tqdm(slot_sample_df["slot"].unique()):
        temp_df = query.get_attestations_for_slot(slot, db_url)
        atts_df = pd.concat([atts_df, temp_df], ignore_index=True)
    # Save attestations
    atts_file_path = os.path.join(out_dir, "sample_atts.parquet")
    atts_df.to_parquet(atts_file_path, index=False)
    logging.info(f"Attestations saved to {atts_file_path}")


def get_and_save_slot_info(
    slot_sample_df: pd.DataFrame, db_url: str, out_dir: str
) -> None:
    slot_start = slot_sample_df["slot"].min()
    slot_end = slot_sample_df["slot"].max()
    # Query info
    block_prop_df = query.get_block_propagation_stats(
        db_url, slot_start - 2, slot_end + 2
    )
    block_cont_df = query.get_block_content_info(db_url, slot_start - 2, slot_end + 2)
    # Merge dataframes
    slot_df = slot_sample_df.merge(block_prop_df, on="slot", how="inner").merge(
        block_cont_df, on="slot", how="inner"
    )
    # Save block info
    slot_file_path = os.path.join(out_dir, "slot_info.parquet")
    slot_df.to_parquet(slot_file_path, index=False)
    logging.info(f"Slot info saved to {slot_file_path}")


def parse_configuration():
    file_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Makes a random sample of Ethereum slots and gathers "
        "attestation data for those slots"
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default=os.path.abspath(os.path.join(file_dir, "..", "data")),
        help="Data directory (default: ./data). Used for both input and output.",
    )
    parser.add_argument(
        "--run_id",
        type=str,
        default=None,
        help="Run ID for the analysis. Used to create a subdirectory in data_dir "
        "for run-specific outputs. Defaults to `sample_slot_start_slot_end`.",
    )
    parser.add_argument(
        "--slot_start",
        type=int,
        default=12187616,
        help="Earliest slot number for sample. Default is 12187616, i.e., "
        "the earliest slot with correct libp2p data.",
    )
    parser.add_argument(
        "--slot_range",
        type=int,
        default=14400,
        help="Max slot range for sample. Default is 2 days after starting slot.",
    )
    parser.add_argument(
        "--sample_size",
        type=int,
        default=None,
        help="Number of slots to sample, if any. Default None, i.e., no sampling.",
    )
    parser.add_argument(
        "--secrets_path",
        type=str,
        default=os.path.abspath(os.path.join(file_dir, "..", "secrets.json")),
        help="Path to secrets.json file (default: ./secrets.json)",
    )
    parser.add_argument(
        "--xatu_username",
        type=str,
        help="Xatu Clickhouse username (can be provided in secrets.json)",
    )
    parser.add_argument(
        "--xatu_password",
        type=str,
        help="Xatu Clickhouse password (can be provided in secrets.json)",
    )
    args = parser.parse_args()
    config = {
        "data_dir": args.data_dir,
        "run_id": (
            args.run_id
            if args.run_id
            else f"sample_{args.slot_start}_{args.slot_start+args.slot_range}"
        ),
        "slot_start": args.slot_start,
        "slot_range": args.slot_range,
        "sample_size": args.sample_size,
        "xatu_username": args.xatu_username,
        "xatu_password": args.xatu_password,
    }
    try:
        with open(args.secrets_path, "r") as file:
            secrets_dict = json.load(file)
        if not config["xatu_username"]:
            config["xatu_username"] = secrets_dict.get("xatu_username")
        if not config["xatu_password"]:
            config["xatu_password"] = secrets_dict.get("xatu_password")

    except FileNotFoundError:
        logging.warning(
            f"Secrets file not found at {config['secrets_path']}. Secrets might"
            "be missing if not provided via command line."
        )
    return config


def main():
    # Config
    config = parse_configuration()
    data_dir = config["data_dir"]
    run_id = config["run_id"]
    out_dir = os.path.join(data_dir, run_id)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    slot_start = config["slot_start"]
    slot_range = config["slot_range"]
    slot_end = slot_start + slot_range
    sample_size = config["sample_size"]
    xatu_username = config["xatu_username"]
    xatu_password = config["xatu_password"]
    db_url = (
        f"clickhouse+http://{xatu_username}:{xatu_password}@"
        "clickhouse.xatu.ethpandaops.io:443/default?protocol=https"
    )
    # Collect validators info from ethseer
    get_and_save_ethseer_validator_info(db_url, data_dir)
    # Get sample of slots
    slot_sample_df = get_slot_sample_df(
        slot_start, slot_end, db_url, sample_size, data_dir
    )
    # Collect slot info
    get_and_save_slot_info(slot_sample_df, db_url, out_dir)
    # Collect attestations for sampled slots
    get_and_save_attestations(
        slot_sample_df,
        db_url,
        out_dir,
    )


if __name__ == "__main__":
    main()
