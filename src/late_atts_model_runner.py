import os
import pickle
import logging
import argparse
import numpy as np
import pandas as pd
from typing import Tuple, List, Union

import shap
from lightgbm import LGBMClassifier
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, OneHotEncoder

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


FEATURES = [
    "header_from",
    "p66_block_arrival_ms",
    "block_total_bytes_compressed",
    "block_proposer_index",
    "block_gas_used",
    "block_tx_count",
    "entity",
]
CAT_FEATURES = ["header_from", "entity"]
PREDICTOR = "net_atts_arrival_time_ms"


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
    features: List[str],
    cat_features: List[str],
    predictor: str,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    # Reduce entity dimensions
    entity_counts = df["entity"].value_counts() / len(df)
    model_df = df.merge(entity_counts, left_on="entity", right_index=True)
    model_df["entity"] = np.where(model_df["count"] > 0.01, model_df["entity"], "other")
    # Filter rows with negative arrival times
    model_df = model_df[model_df[predictor] >= 0]
    # Select features
    X_raw = model_df[features].values
    # Build the column transformer
    cat_indices = [features.index(x) for x in features if x in cat_features]
    num_indices = [features.index(x) for x in features if x not in cat_features]
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), num_indices),
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_indices),
        ],
        sparse_threshold=0
    )
    # Fit and transform X
    X = preprocessor.fit_transform(X_raw)
    # Define the target: binary classification: late (1) or not (0)
    y = np.where(model_df[predictor] > 4500, True, False)
    # Save processed data
    out_data_dir = os.path.join(out_dir, "train_data")
    os.makedirs(out_data_dir, exist_ok=True)
    np.save(os.path.join(out_data_dir, "X.npy"), X)
    np.save(os.path.join(out_data_dir, "y.npy"), y)
    # Save feature names
    feature_names = get_feature_names(preprocessor, features, cat_features)
    np.save(os.path.join(out_data_dir, "feature_names.npy"), feature_names)
    logging.info(f"Sucessfully saved training data to {out_data_dir}")
    return X, y, feature_names


def train_classifier_and_save(
    X: np.ndarray,
    y: np.ndarray,
    model: Union[LGBMClassifier, RandomForestClassifier, LogisticRegression],
    feature_names: np.ndarray,
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
    if model_name == "LogisticRegression":
        explainer = shap.LinearExplainer(model, X, feature_names=feature_names)
    else:
        explainer = shap.TreeExplainer(model, X, feature_names=feature_names)
    explanation = explainer(X)
    # Save shap values
    out_file = os.path.join(out_dir, model_name, "shap.pkl")
    with open(out_file, "wb") as outp:
        pickle.dump(explanation, outp)
    logging.info(f"Sucessfully saved shap values for {model_name} model")


def do_model_run(
    df: pd.DataFrame,
    out_dir: str,
    features: List[str] = FEATURES,
    cat_features: List[str] = CAT_FEATURES,
    predictor: str = PREDICTOR,
) -> None:
    # Data prep
    X, y, feature_names = prep_data_for_training_and_save(
        df, out_dir, features, cat_features, predictor
    )
    # Train LighGBM model
    lgbm_model = LGBMClassifier(max_depth=3, verbose=-1)
    train_classifier_and_save(X, y, lgbm_model, feature_names, out_dir)
    # Train Random forest model
    rf_model = RandomForestClassifier(max_depth=3)
    train_classifier_and_save(X, y, rf_model, feature_names, out_dir)
    # Train Logistic regression
    lr_model = LogisticRegression()
    train_classifier_and_save(X, y, lr_model, feature_names, out_dir)


def get_feature_names(
    preprocessor: ColumnTransformer,
    features: List[str],
    cat_features: List[str],
) -> np.ndarray:
    # Get feature names
    num_feature_names = np.array([x for x in features if x not in cat_features])
    cat_feature_names = preprocessor.named_transformers_["cat"].get_feature_names_out(
        cat_features
    )
    feature_names = np.concatenate([num_feature_names, cat_feature_names])
    return feature_names


def parse_configuration():
    file_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Trains a set of classifers to predict whether an attestation "
        "was late and computes SHAP values for feature exaploration."
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
        "No defaults. Must be provded by user.",
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
    # Load data
    df = load_data(data_dir, run_id)
    # Model run - original data
    logging.info("Starting training for original sample")
    run_out_dir = os.path.join(out_dir, "1_original")
    do_model_run(df, run_out_dir)
    # Model run - balanced sample
    logging.info("Starting training for balanced sample")
    late_df = df[df["net_atts_arrival_time_ms"] > 4500]
    sample_size = len(late_df)
    early_df = df[df["net_atts_arrival_time_ms"] <= 4500].sample(n=sample_size)
    balanced_df = pd.concat([late_df, early_df]).sort_index()
    run_out_dir = os.path.join(out_dir, "2_balanced")
    do_model_run(balanced_df, run_out_dir)
    # Model run - self_build
    logging.info("Starting training for self-builder sample")
    self_build_df = df[df["header_from"] == "self_build"]
    run_out_dir = os.path.join(out_dir, "3_self_build")
    do_model_run(self_build_df, run_out_dir)
    # Model run - relay
    logging.info("Starting training for relay sample")
    relay_df = df[df["header_from"] != "self_build"]
    run_out_dir = os.path.join(out_dir, "4_relay")
    do_model_run(relay_df, run_out_dir)
    # Model run - no entities
    logging.info("Starting training for no entity feature sample")
    no_entity_features = [f for f in FEATURES if f != "entity"]
    no_entity_cat_features = [f for f in CAT_FEATURES if f != "entity"]
    run_out_dir = os.path.join(out_dir, "5_no_entity")
    do_model_run(df, run_out_dir, no_entity_features, no_entity_cat_features)
    # Model run - slot start times
    logging.info("Starting training for timings since slot start")
    run_out_dir = os.path.join(out_dir, "6_full_time_pred")
    do_model_run(df, run_out_dir, predictor="atts_arrival_time_ms")


if __name__ == "__main__":
    main()
