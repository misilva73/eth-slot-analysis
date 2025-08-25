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
from atts_sample_runner import get_and_save_attestations
import query

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_configuration():
    file_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Gathers attestation data for missed slots"
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
        "for run-specific outputs. Defaults to `missed_slot_start_slot_end`.",
    )
    parser.add_argument(
        "--slot_start",
        type=int,
        default=12243620,
        help="Earliest slot number for sample. Default is 12243620, i.e., "
        "the start of July 29th 2025",
    )
    parser.add_argument(
        "--slot_range",
        type=int,
        default=50400,
        help="Max slot range for sample. Default is 7 days after starting slot.",
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
            else f"missed_{args.slot_start}_{args.slot_start+args.slot_range}"
        ),
        "slot_start": args.slot_start,
        "slot_range": args.slot_range,
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
    xatu_username = config["xatu_username"]
    xatu_password = config["xatu_password"]
    db_url = (
        f"clickhouse+http://{xatu_username}:{xatu_password}@"
        "clickhouse.xatu.ethpandaops.io:443/default?protocol=https"
    )
    # Get list of missed slots (add 2 slot buffer)
    missed_slots = query.get_missed_slots(db_url, slot_start - 2, slot_end + 2)
    logging.info(f"Gathering attestations for {len(missed_slots)} missed slots.")
    missed_slot_df = pd.DataFrame({"slot": list(missed_slots)})
    # Collect attestations for sampled slots
    get_and_save_attestations(
        missed_slot_df,
        db_url,
        out_dir,
    )


if __name__ == "__main__":
    main()
