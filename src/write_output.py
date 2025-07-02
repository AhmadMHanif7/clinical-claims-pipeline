from pathlib import Path

# Always reference the root directory
project_root = Path(__file__).resolve().parents[1]
csv_dir = project_root / "output" / "CSV Data"
parquet_dir = project_root / "output" / "Parquet Data"

def save_outputs(df, base_filename):
    #Save given DataFrame to both CSV and Parquet formats
    csv_path = csv_dir / f"{base_filename}.csv"
    parquet_path = parquet_dir / f"{base_filename}.parquet"

    csv_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
