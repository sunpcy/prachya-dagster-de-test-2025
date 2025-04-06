import pandas as pd
from datetime import datetime

# 2.1.1 Read KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file
def read_excel() -> pd.DataFrame:
    excel_path = "/opt/dagster/app/dagster_pipelines/data/KPI_FY.xlsm"
    # excel_path = "/workspaces/prachya-dagster-de-test-2025/dagster_pipelines/data/KPI_FY.xlsm"
    sheet_name = "Data to DB"
    print(f"Loading data from {excel_path}")
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    df["Center_ID"] = df["Center_ID"].astype(str)
    return df

# 2.1.2 Read center master data from the "M_Center.csv" CSV file
def read_csv() -> pd.DataFrame:
    csv_path = "/opt/dagster/app/dagster_pipelines/data/M_Center.csv"
    # csv_path = "/workspaces/prachya-dagster-de-test-2025/dagster_pipelines/data/M_Center.csv"
    print(f"Loading data from {csv_path}")
    df = pd.read_csv(csv_path)
    df["Center_ID"] = df["Center_ID"].astype(str)
    df["Center_Name"] = df["Center_Name"].astype(str)
    return df