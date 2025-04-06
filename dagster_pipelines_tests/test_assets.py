import pandas as pd
import dagster as dg
from dagster_pipelines.etl.extract import read_excel
from dagster_pipelines.assets import kpi_fy_raw

# Expected data types
expected_dtypes = {
    "Fiscal_Year": "int64",
    "Center_ID": "object",
    "Kpi Number": "object",
    "Kpi_Name": "object",
    "Unit": "object",
    "Plan_Total": "float64",
    "Plan_Q1": "float64",
    "Plan_Q2": "float64",
    "Plan_Q3": "float64",
    "Plan_Q4": "float64",
    "Actual_Total": "float64",
    "Actual_Q1": "float64",
    "Actual_Q2": "float64",
    "Actual_Q3": "float64",
    "Actual_Q4": "float64",
}

@dg.asset_check(asset=kpi_fy_raw)
def kpi_fy_raw_dtypes_check():
    df = read_excel()
    all_types_match = True
    column_checks = {}

    for col, expected_dtype in expected_dtypes.items():
        if col in df.columns:
            actual_dtype = str(df[col].dtype)
            if actual_dtype == expected_dtype:
                column_checks[col] = f"[OK] {actual_dtype} matches expected {expected_dtype}"
            else:
                all_types_match = False
                column_checks[col] = f"[FAIL] {actual_dtype} does not match expected {expected_dtype}"
        else:
            all_types_match = False
            column_checks[col] = "[NOT FOUND] Column not found"

    return dg.AssetCheckResult(
        passed=all_types_match,
        metadata={"Column Checks": dg.MetadataValue.md("\n".join(f"- {col}: {result}" for col, result in column_checks.items()))}
    )