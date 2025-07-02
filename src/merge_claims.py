import pandas as pd
from pathlib import Path

def load_claims(dx_claims_path: Path, rx_claims_path: Path):
    #Load diagnosis and rx claims data into DataFrames
    dx_claims = pd.read_csv(dx_claims_path)
    rx_claims = pd.read_csv(rx_claims_path)
    return dx_claims, rx_claims

def merge_with_claims(medications_df, problems_df, dx_claims_df, rx_claims_df):
    #Merge medications and problems with claims data
    meds_merged = medications_df.merge(rx_claims_df, left_on="patient_id", right_on="MemberID", how="left")
    probs_merged = problems_df.merge(dx_claims_df, left_on="patient_id", right_on="MemberID", how="left")
    return meds_merged, probs_merged
