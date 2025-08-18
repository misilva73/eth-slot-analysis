import os
import pickle
import logging
import datetime
import argparse
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from mdutils.mdutils import MdUtils
from typing import Tuple, List, Union

import shap
import statsmodels.api as sm
from lightgbm import LGBMClassifier
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import (
    precision_recall_curve,
    average_precision_score,
    classification_report,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

sns.set_theme(
    style="whitegrid", palette="Set2", rc={"figure.dpi": 500, "axes.titlesize": 15}
)


FEATURES = [
    "header_from",
    "p66_block_arrival_ms",
    "block_total_bytes_compressed",
    "block_proposer_index",
    "block_gas_used",
    "block_blob_count",
    "block_tx_count",
    "entity",
]
CAT_FEATURES = ["header_from", "entity"]
PREDICTOR = "net_atts_arrival_time_ms"


def load_data(data_dir: str, sample_id: str) -> pd.DataFrame:
    # Validator info
    val_df = pd.read_parquet(os.path.join(data_dir, "ethseer_val_info.parquet"))
    # Slot info
    slot_df = pd.read_parquet(os.path.join(data_dir, sample_id, "slot_info.parquet"))
    # Attestations info
    atts_df = pd.read_parquet(os.path.join(data_dir, sample_id, "sample_atts.parquet"))
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
        sparse_threshold=0,
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


def save_pr_curve_for_model(
    X: np.ndarray,
    y: np.ndarray,
    model: Union[LGBMClassifier, RandomForestClassifier, LogisticRegression],
    sample_dir: str,
) -> None:
    model_name = type(model).__name__
    y_scores = model.predict_proba(X)[:, 1]
    precision_train, recall_train, _ = precision_recall_curve(y, y_scores)
    ap_train = average_precision_score(y, y_scores)
    # Plot
    fig = plt.figure(figsize=(4, 4))
    plt.plot(recall_train, precision_train, label=f"Train (AP={ap_train:.2f})")
    plt.xlabel("Recall = TP/actual lates")
    plt.ylabel("Precision = TP/predicted lates")
    plt.title(f"Precision-Recall Curve for {model_name}")
    plt.grid(True)
    fig.savefig(
        os.path.join(sample_dir, model_name, "pr_curve.png"),
        dpi=144 * 3,
        bbox_inches="tight",
    )
    plt.close()


def compute_feature_importance_for_model(
    model: Union[LGBMClassifier, RandomForestClassifier, LogisticRegression],
    feature_names: List[str],
) -> pd.DataFrame:
    model_name = type(model).__name__
    # Get feature importance from model
    if model_name == "LogisticRegression":
        importances = model.coef_.reshape(-1)
    else:
        importances = model.feature_importances_
    # Build Dataframe
    feat_imp_df = pd.DataFrame(
        {"feature": feature_names, "importance": importances}
    ).sort_values("importance", ascending=False)
    # Filter features with 0 importance
    feat_imp_df = feat_imp_df[feat_imp_df["importance"] > 0]
    return feat_imp_df


def save_shap_beeswarm_for_model(
    model: Union[LGBMClassifier, RandomForestClassifier, LogisticRegression],
    sample_dir: str,
) -> None:
    model_name = type(model).__name__
    # Load shap values
    shap_path = os.path.join(sample_dir, model_name, "shap.pkl")
    with open(shap_path, "rb") as f:
        explanation = pickle.load(f)
    if model_name == "RandomForestClassifier":
        explanation = explanation[:, :, 1]
    # Plot summary
    ax = shap.plots.beeswarm(explanation, max_display=20, show=False)
    plt.title(f"SHAP Beeswarm Plot for Late Arrivals ({model_name})")
    fig = ax.get_figure()
    fig.savefig(
        os.path.join(sample_dir, model_name, "shap_beeswarm.png"),
        dpi=144 * 3,
        bbox_inches="tight",
    )
    plt.close()


def update_model_report_with_sample_outputs(
    md_file: MdUtils, sample_name: str, sample_dir: str
) -> MdUtils:
    sample_folder = sample_dir.split("/")[-1]
    # Load data
    X = np.load(os.path.join(sample_dir, "train_data", "X.npy"))
    y = np.load(os.path.join(sample_dir, "train_data", "y.npy"))
    feature_names = np.load(
        os.path.join(sample_dir, "train_data", "feature_names.npy"), allow_pickle=True
    ).tolist()
    # Add section title
    md_file.new_header(level=1, title=sample_name)
    # Add Statsmodel sub-section
    try:
        X_df = pd.DataFrame(X, columns=feature_names)
        X_with_intercept_df = sm.add_constant(X_df)  # adds intercept
        logit_model = sm.Logit(y, X_with_intercept_df)
        result = logit_model.fit()
        result_str = str(result.summary())
    except:
        result_str = "logit model did not run..."
    md_file.new_header(level=2, title="Logit regression - statsmodel report")
    md_file.new_paragraph("```python")
    md_file.new_line(result_str)
    md_file.new_line("```")
    # Add saved models subsections
    for model_name in [
        "RandomForestClassifier",
        "LGBMClassifier",
        "LogisticRegression",
    ]:
        ## Load model outputs
        with open(os.path.join(sample_dir, model_name, "model.pkl"), "rb") as f:
            model = pickle.load(f)
        ## Add subsection titles
        md_file.new_header(level=2, title=f"{model_name} outputs")
        md_file.new_header(
            level=3, title="Model performance", add_table_of_contents="n"
        )
        ## Add Confusion matrix component
        report = classification_report(
            y, model.predict(X), target_names=["on-time", "late"], digits=3
        )
        md_file.new_paragraph("```python")
        md_file.new_paragraph(report)
        md_file.new_line("```")
        ## Add PR curve image
        save_pr_curve_for_model(X, y, model, sample_dir)
        md_file.new_paragraph(
            f'<img src="./{sample_folder}/{model_name}/pr_curve.png" alt="pr_curve" width="400"/>'
        )
        md_file.new_line()
        ## Add Feature importances
        md_file.new_header(
            level=3, title=f"Feature importance", add_table_of_contents="n"
        )
        feat_imp_df = compute_feature_importance_for_model(model, feature_names)
        headers = list(feat_imp_df.columns)
        rows = feat_imp_df.values.tolist()
        flat_data = headers + [str(item) for row in rows for item in row]
        md_file.new_table(
            columns=len(headers),
            rows=len(rows) + 1,  # +1 for header row
            text=flat_data,
            text_align="center",
        )
        ## Add Shap beeswarm plot
        save_shap_beeswarm_for_model(model, sample_dir)
        md_file.new_paragraph(
            f'<img src="./{sample_folder}/{model_name}/shap_beeswarm.png" alt="shap_beeswarm" width="800"/>'
        )
        md_file.new_paragraph("")
    return md_file


def generate_and_save_model_report(
    out_dir: str, run_time: datetime.datetime, sample_id: str
) -> None:
    md_file = MdUtils(
        file_name=os.path.join(out_dir, "model_report"),
        title=f"Late arrivals model report",
    )
    md_file.new_paragraph("***")
    md_file.new_header(level=2, title="Introduction", add_table_of_contents="n")
    md_file.new_paragraph(
        f"""
This report summarises the main outputs from the model trained in the script 
[`late_atts_model_runner.py`](https://github.com/misilva73/eth-slot-analysis/blob/main/src/late_atts_model_runner.py).

We collected data on the attestation arrival times for two types of slots - slots from 
relays (titan and ultrasound) and slots from self-builders. For the relays, we compute the 
attestation arrival since the block was published by the relay (to account for timing games), 
while for self-builders, we compute the attestation arrival since the start of the slot 
(i.e., assuming no timing games or other delays). 

We gathered additional data on the slot and the attester and trained different models 
that predict whether an attestation was late, which is defined as taking more than 4.5s. We 
use these models to compute feature importance and SHAP values to surface which properties of the 
slot or the attester explain late arrivals.

This report was generated from the data in `{sample_id}` at {run_time.strftime("%d-%m-%Y %H:%M:%S")}.
"""
    )
    sample_tuple_list = [
        ("1_original", "Original sample"),
        ("2_balanced", "Balanced sample (50% of each predictor class)"),
        ("3_self_build", "Self-builder balanced sample"),
        ("4_relay", "Relay balanced sample"),
        ("5_no_entity", "No entity feature balanced sample"),
        ("6_full_time_pred", "Timings since slot start balanced predictor"),
    ]
    for sample_folder, sample_name in sample_tuple_list:
        sample_dir = os.path.join(out_dir, sample_folder)
        update_model_report_with_sample_outputs(md_file, sample_name, sample_dir)
    md_file.new_table_of_contents(depth=2, table_title="")
    md_file.create_md_file()


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
        "--sample_id",
        type=str,
        default=None,
        help="ID of the data sample. Used to read the data to train the models. "
        "No defaults. Must be provided by user if train_models is set to True.",
    )
    parser.add_argument(
        "--train_models",
        action="store_true",
        help="Runs model training and generates the corresponding report."
    )
    parser.add_argument(
        "--only_report",
        dest="train_models",
        action="store_false",
        help="Only genertes model report susing the latest model run."
    )
    parser.set_defaults(train_models=True) 
    args = parser.parse_args()
    config = {
        "data_dir": args.data_dir,
        "sample_id": args.sample_id,
        "train_models": args.train_models,
    }
    if not config["sample_id"] and config["train_models"]:
        raise ValueError(
            "Sample ID must be provided when train_models=True. Use --sample_id argument."
        )
    return config


def make_balanced_sample(df: pd.DataFrame, predictor: str = PREDICTOR) -> pd.DataFrame:
    late_df = df[df[predictor] > 4500]
    sample_size = len(late_df)
    early_df = df[df[predictor] <= 4500].sample(n=sample_size)
    balanced_df = pd.concat([late_df, early_df]).sort_index()
    return balanced_df


def get_latest_model_run_time(data_dir: str) -> datetime.datetime:
    models_dir = os.path.join(data_dir, "model_outputs")
    run_dirs = []
    for run_dir in os.listdir(models_dir):
        try:
            run_time = datetime.datetime.strptime(run_dir, "%d-%m-%Y_%H:%M:%S")
            run_dirs.append(run_time)
        except:
            pass
    run_dirs.sort()
    return run_dirs[-1]


def main():
    # Config
    config = parse_configuration()
    data_dir = config["data_dir"]
    sample_id = config["sample_id"]
    train_models = config["train_models"]
    if train_models:
        run_time = datetime.datetime.now()
        out_dir = os.path.join(
            data_dir, "model_outputs", run_time.strftime("%d-%m-%Y_%H:%M:%S")
        )
        # Load data
        df = load_data(data_dir, sample_id)
        # Model run - original data
        logging.info("Starting training for original sample")
        run_out_dir = os.path.join(out_dir, "1_original")
        do_model_run(df, run_out_dir)
        # Model run - balanced sample
        balanced_df = make_balanced_sample(df)
        run_out_dir = os.path.join(out_dir, "2_balanced")
        do_model_run(balanced_df, run_out_dir)
        # Model run - self_build
        logging.info("Starting training for self-builder sample")
        self_build_df = df[df["header_from"] == "self_build"]
        self_build_df = make_balanced_sample(self_build_df)
        run_out_dir = os.path.join(out_dir, "3_self_build")
        do_model_run(self_build_df, run_out_dir)
        # Model run - relay
        logging.info("Starting training for relay sample")
        relay_df = df[df["header_from"] != "self_build"]
        relay_df = make_balanced_sample(relay_df)
        run_out_dir = os.path.join(out_dir, "4_relay")
        do_model_run(relay_df, run_out_dir)
        # Model run - no entities
        logging.info("Starting training for no entity feature sample")
        no_entity_features = [f for f in FEATURES if f != "entity"]
        no_entity_cat_features = [f for f in CAT_FEATURES if f != "entity"]
        run_out_dir = os.path.join(out_dir, "5_no_entity")
        do_model_run(
            balanced_df, run_out_dir, no_entity_features, no_entity_cat_features
        )
        # Model run - slot start times
        logging.info("Starting training for timings since slot start")
        run_out_dir = os.path.join(out_dir, "6_full_time_pred")
        slot_start_df = make_balanced_sample(df, predictor="atts_arrival_time_ms")
        do_model_run(slot_start_df, run_out_dir, predictor="atts_arrival_time_ms")
    else:
        run_time = get_latest_model_run_time(data_dir)
        out_dir = os.path.join(
            data_dir, "model_outputs", run_time.strftime("%d-%m-%Y_%H:%M:%S")
        )
    # Create markdown report
    logging.info("Generating model report")
    generate_and_save_model_report(out_dir, run_time, sample_id)


if __name__ == "__main__":
    main()
