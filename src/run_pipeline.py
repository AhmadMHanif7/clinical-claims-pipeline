from pathlib import Path
import pandas as pd
from parse_ccda import parse_ccda_file
from merge_claims import load_claims, merge_with_claims
from write_output import save_outputs
import logging

def setup_logger():
    log_dir = Path(__file__).resolve().parents[1] / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / "pipeline.log"

    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s",
        filemode="w"
    )

def main():
    setup_logger()
    logger = logging.getLogger()

    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "data"
    dx_claims_path = project_root / "source files/data_engineer_exam_claims_final.csv"
    rx_claims_path = project_root / "source files/data_engineer_exam_rx_final.csv"
    xml_files = list(data_dir.glob("*.xml"))

    all_meds, all_probs = [], []

    for xml_file in xml_files:
        logger.info(f"Parsing {xml_file.name}")
        meds_df, probs_df = parse_ccda_file(xml_file)
        logger.info(f"  Meds rows: {len(meds_df)}, Problems rows: {len(probs_df)}")
        if not meds_df.empty:
            all_meds.append(meds_df)
        if not probs_df.empty:
            all_probs.append(probs_df)

    if not all_meds and not all_probs:
        logger.warning("No clinical data found.")
        print("No clinical data found.")
        return

    combined_meds = pd.concat(all_meds, ignore_index=True)
    combined_probs = pd.concat(all_probs, ignore_index=True)

    # 🔍 Filter out rows with null patient_id
    combined_meds = combined_meds.dropna(subset=["patient_id"])
    combined_probs = combined_probs.dropna(subset=["patient_id"])
    logger.info(f"After filtering — Meds: {len(combined_meds)}, Problems: {len(combined_probs)}")

    dx_claims, rx_claims = load_claims(dx_claims_path, rx_claims_path)
    merged_meds, merged_probs = merge_with_claims(combined_meds, combined_probs, dx_claims, rx_claims)

    save_outputs(merged_meds, "merged_medications")
    save_outputs(merged_probs, "merged_problems")

    logger.info(f"Merged Medications shape: {merged_meds.shape}")
    logger.info(f"Merged Problems shape: {merged_probs.shape}")
    logger.info("Pipeline run completed successfully.")

    # 🧪 Output sample
    print("\nSample — Merged Medications:")
    print(merged_meds.head())

    print("\nSample — Merged Problems:")
    print(merged_probs.head())

    print("\nFull pipeline test complete.")

if __name__ == "__main__":
    main()
