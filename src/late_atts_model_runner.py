import os
import pickle
import logging
import argparse
import numpy as np
import pandas as pd
from typing import Tuple, Union

import shap
from lightgbm import LGBMClassifier
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler, OneHotEncoder

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


FEATURES = [
    "header_from",
    "p95_block_arrival_ms",
    "p66_block_arrival_ms",
    "avg_block_arrival_ms",
    "block_total_bytes_compressed",
    "block_proposer_index",
    "block_gas_used",
    "block_tx_count",
    "entity",
]
NUM_FEATURES_INDICES = [1, 2, 3, 4, 5, 6, 7]
CAT_FEATURES_INDICES = [0, 8]


def load_data(data_dir: str, run_id: str) -> pd.DataFrame:
    # Validator info
    val_df = pd.read_parquet(os.path.join(data_dir, "ethseer_val_info.parquet"))
    # Slot info
    slot_df = pd.read_parquet(os.path.join(data_dir, run_id, "slot_info.parquet"))
    # Attestations info
    atts_df = pd.read_parquet(os.path.join(data_dir, run_id, "sample_atts.parquet"))
    # Join datasets
    df = (
        atts_df.merge(slot_df, on="slot", how="left")
        .merge(val_df, right_on="index", left_on="atts_validator", how="left")
        .drop(columns=["index"])
        .fillna({"entity": "Unknown"})
    )
    # Compute attestation arrival time after pubishing
    df["net_atts_arrival_time_ms"] = df["atts_arrival_time_ms"] - df["publish_time_ms"]
    return df


def prep_data_for_training_and_save(
    df: pd.DataFrame,
    out_dir: str,
) -> Tuple[np.ndarray, np.ndarray, ColumnTransformer]:
    # Reduce entity dimensions
    entity_counts = df["entity"].value_counts() / len(df)
    model_df = df.merge(entity_counts, left_on="entity", right_index=True)
    model_df["entity"] = np.where(model_df["count"] > 0.01, model_df["entity"], "other")
    # Filter rows with negative arrival times
    model_df = model_df[model_df["net_atts_arrival_time_ms"] >= 0]
    # Select features
    X_raw = model_df[FEATURES].values
    # Build the column transformer
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), NUM_FEATURES_INDICES),
            ("cat", OneHotEncoder(handle_unknown="ignore"), CAT_FEATURES_INDICES),
        ]
    )
    # Fit and transform X
    X = preprocessor.fit_transform(X_raw)
    # Define the target: binary classification: late (1) or not (0)
    y = np.where(model_df["net_atts_arrival_time_ms"] > 4500, True, False)
    # Save processed data
    out_data_dir = os.path.join(out_dir, "train_data")
    os.makedirs(out_data_dir, exist_ok=True)
    np.save(os.path.join(out_data_dir, "X.npy"), X)
    np.save(os.path.join(out_data_dir, "y.npy"), y)
    logging.info(f"Sucessfully saved training data to {out_data_dir}.")
    return X, y, preprocessor


def train_tree_model_and_save(
    X: np.ndarray,
    y: np.ndarray,
    model: Union[LGBMClassifier, RandomForestClassifier],
    preprocessor: ColumnTransformer,
    out_dir: str,
) -> None:
    # Create output directory
    model_name = type(model).__name__
    os.makedirs(os.path.join(out_dir, model_name), exist_ok=True)
    logging.info(f"Training {model_name} model")
    # Train model
    model.fit(X, y)
    # Save model
    out_file = os.path.join(out_dir, model_name, "model.pkl")
    with open(out_file, "wb") as outp:
        pickle.dump(model, outp)
    logging.info(f"Sucessfully saved trained {model_name} model")
    # Compute SHAP values
    feature_names = get_feature_names(preprocessor)
    explainer = shap.TreeExplainer(model=model, data=X, feature_names=feature_names)
    explanation = explainer(X)
    # Save shap values
    out_file = os.path.join(out_dir, model_name, "shap.pkl")
    with open(out_file, "wb") as outp:
        pickle.dump(explanation, outp)
    logging.info(f"Sucessfully saved shap values for {model_name} model")


def get_feature_names(preprocessor: ColumnTransformer) -> list:
    # Get feature names
    num_feature_names = np.array(FEATURES)[NUM_FEATURES_INDICES].tolist()
    cat_feature_names = preprocessor.named_transformers_["cat"].get_feature_names_out(
        np.array(FEATURES)[CAT_FEATURES_INDICES].tolist()
    )
    feature_names = np.concatenate([num_feature_names, cat_feature_names]).tolist()
    return feature_names


def parse_configuration():
    file_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Trains a set of classifers to predict whether an attestation "
        "was late and computes SHAP values for feature exaploration"
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
        help="Run ID of the data sample. Used to read the data to train the models. "
        "No defaults. Must be provded by user",
    )
    args = parser.parse_args()
    config = {
        "data_dir": args.data_dir,
        "run_id": args.run_id,
    }
    if not config["run_id"]:
        raise ValueError("Run ID must be provided. Use --run_id argument.")
    return config


def main():
    # Config
    config = parse_configuration()
    data_dir = config["data_dir"]
    run_id = config["run_id"]
    out_dir = os.path.join(data_dir, "model_outputs")
    # Data prep
    df = load_data(data_dir, run_id)
    X, y, preprocessor = prep_data_for_training_and_save(df, out_dir)
    # Train LighGBM model
    lgbm_model = LGBMClassifier(max_depth=3)
    train_tree_model_and_save(X, y, lgbm_model, preprocessor, out_dir)
    # Train Random forest model
    rf_model = RandomForestClassifier(max_depth=3)
    train_tree_model_and_save(X, y, rf_model, preprocessor, out_dir)


if __name__ == "__main__":
    main()
