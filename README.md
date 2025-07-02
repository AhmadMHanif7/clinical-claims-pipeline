# Clinical + Claims Data Pipeline – Milliman Data Engineer Assignment

---
## Background

This project builds a data pipeline that extracts clinical information from unstructured C-CDA XML documents (specifically medications and problems) and combines it with structured claims and prescription data provided in CSV format.

The goal is to simulate a real-world healthcare data engineering task where:

- Each XML file represents a patient’s clinical summary.
- Extracted clinical data (like Lisinopril, Hypertension, etc.) is normalized.
- This data is then joined with claims to provide real-world service/payment context.
- The final output is written to CSV and Parquet, making it ready for ingestion into platforms like Databricks or a FHIR-aligned Common Data Model.


---

## Objectives

- Parse clinical documents (C-CDA XMLs) to extract:
  - `Medications` (`code: 10160-0`)
  - `Problems` (`code: 11450-4`)
-  Combine clinical data with diagnosis and prescription claims using `MemberID`
-  Store outputs in both **CSV** and **Parquet** format
-  Log pipeline steps for reproducibility and auditability
-  Prepare data structure for a FHIR/CDM pipeline
-  Make it easy to run and review on **Databricks**

---

##  How to Run the Pipeline

### Step 1: Install dependencies
```bash
   pip install -r requirements.txt
```


### Step 2: Download the XML files (if needed)

```bash
   python src/extract_data.py
```

### Step 3: Classify src and test directories (Pycharm)

- Right click and mark src as source root and tests as test


### Step 4: Run the pipeline

```bash
   python src/run_pipeline.py
```

#
### This will:

- Parse XML files from data folder
- Load claims CSVs from the project root
- Merge clinical and claims data
- Write outputs to output folder as both CSV(human) and Parquet(Databricks)
- Log all steps to log folder

---

## File Responsibilities


**extract_data.py**  
Downloads clinical XML documents using pre-signed AWS S3 URLs listed in `ccda_pre_signed_urls.csv`.  
Saves them to the `data/` directory and optionally logs downloads for traceability.

Note: There are two versions of this file extract_data.py and extract_data_v2.py. The data was only available for 6 hours so the first attempt was just to get the files before access is revoked.

Changes: 
- V2 uses pathlib.Path to match the format of the other files and fits with best practices.
- Logs to a log file instead of a .csv
- Encapsulation of logging function
- Able to dynamically find root folder for Root Resolution
- Cleaner looping and logging

**parse_ccda.py**  
Parses each downloaded C-CDA XML file to extract:
- Medications (from LOINC section `10160-0`)
- Problems (from LOINC section `11450-4`)  
Returns a pandas DataFrames with standardized column names (e.g., `med_name`, `rxnorm_code`, `problem_name`, `code`, `status`, `start_date`, etc.).  
Handles missing data, multiple entries per patient, and merges multiple documents per patient.

**merge_claims.py**  
Loads the structured claims datasets (`data_engineer_exam_claims_final.csv` and `data_engineer_exam_rx_final.csv`).  
Performs patient-level joins between the clinical data (from `parse_ccda.py`) and claims data using `patient_id` and `MemberID`.  
Supports left joins to preserve clinical data, flags matched records, and resolves duplicates if needed.

**write_output.py**  
Writes the final merged DataFrames to both CSV and Parquet formats.  
Outputs are saved into `output/CSV Data/` and `output/Parquet Data/` directories.  
Includes logging for successful writes and row counts.

**run_pipeline.py**  
Main orchestration script to run the full pipeline:
1. Load and parse all XML files
2. Merge with claims data
3. Save the final merged datasets
4. Log progress and basic stats  
Also prints sample rows from the final DataFrames for quick verification.

**tests/tests.py**   
You can use this file to validate:
- Correct parsing of XML documents
- Schema consistency in outputs
- Merging logic correctness

---

## Logging, Validation, and Filtering

- All pipeline steps and row counts are logged to `logs/pipeline.log`
- Records with missing `patient_id` are filtered out
- Sample rows (head) of output DataFrames are printed to console
- You can extend the pipeline to save metrics like row counts and patient IDs

---

## Potential Upgrades & Enhancements

These are improvements you could propose during your technical interview to demonstrate engineering maturity and awareness of production needs.

---

### 1. Unit & Integration Testing

- **Already done**: Unit tests for XML parsing and merging logic  
- **Suggested Upgrade**: Add an end-to-end integration test (e.g., `parse → merge → write` for a known sample input)

---

### 2. Configuration Support

- Add a `config.yaml` file to manage:
  - File paths (input, output, logs)
  - Output toggles (CSV vs Parquet)
  - Logging verbosity  
- **Benefit**: Makes the pipeline environment-agnostic, easier to maintain, and scalable

---

### 3. Schema & Data Validation

- Add validation logic for:
  - Code formats (RxNorm, ICD)
  - Business rules (e.g., `start_date < end_date`, no future dates)
  - Nulls and missing columns  
- **Tools**: `pandera`, `pydantic`, or custom rule sets  
- **Benefit**: Ensures data quality and reduces ingestion/analytics failures

---

### 4. Dockerize the Project

- Add a `Dockerfile` and optional `docker-compose.yml`
- Bundle dependencies and pipeline runner  
- **Benefit**: Enables portable, reproducible environments for local use, review, CI/CD, or cloud deployment

---

### 5. Parallel XML Parsing

- Use `concurrent.futures.ThreadPoolExecutor` to parse multiple XMLs in parallel  
- **Benefit**: Boosts performance for large datasets (100s–1000s of files)

---

### 6. Output Metrics & Validation

- Export a `metrics.json` with:
  - Record counts (per domain)
  - Unique patients
  - Date ranges, null counts  
- **Benefit**: Supports QA, monitoring, and proving pipeline correctness

---

### 7. FHIR/HL7 Export Format

- Emit data in:
  - FHIR NDJSON bundles
  - Or structure outputs to match FHIR resource types (e.g., `Condition`, `MedicationStatement`)  
- **Tools**: `fhir.resources`, `Firely`, or `FHIRKit`  
- **Benefit**: Aligns with Lakehouse ingestion and Common Data Model needs

---

### 8. Exploratory Data Reporting

- Generate sample charts or summaries:
  - Medications per patient
  - Most common diagnoses  
- Optional: Include a notebook or script for exploratory data analysis  
- **Benefit**: Demonstrates ability to **surface insights**, not just transform data

---

### 9. Configurable Logging

- Add support for:
  - Log rotation
  - Toggle console vs. file output
  - Dynamic log levels from config  
- **Benefit**: Enables production-grade observability

---

## Structuring for FHIR/CDM in a Lakehouse

### Goal: Align with FHIR Resources, store data in Delta Lake or similar
#### Mapping Concepts:

- Medications: map to FHIR MedicationStatement or MedicationRequest
- Problems: map to FHIR Condition
- patient_id: becomes subject → reference
- rxnorm: becomes medicationCodeableConcept → coding.code

### CDM Strategy:
#### Design a normalized schema with:
- patients
- conditions
- medications
- claims_dx
-claims_rx

#### Use Delta Lake tables for ACID compliance, schema evolution, and time travel.
#### Partition tables by year or patient_id for scalability.

---

## Justifications & Design Decisions


### Used CSV and Parquet	
CSV is universally portable; Parquet is optimal for Spark/Databricks. Exporting both gives flexibility.

### XML parsing with ElementTree	
Lightweight, built-in, fast. Could swap with lxml for stricter validation if needed.

### Separate meds and problems	
Keeps domains clean; makes joins and future FHIR mapping easier.

### Merge on patient_id = MemberID	
MemberID is the consistent join key across claims and clinical.

### Left joins in merge	
Clinical data should not be dropped even if no claim exists — useful for audit or research.

### Logging to file	
Enables reproducible debugging across environments.

### Pathlib over os.path	
More modern, readable, and platform-agnostic.