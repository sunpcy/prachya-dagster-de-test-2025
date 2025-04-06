import dagster as dg
from dagster_pipelines.etl.extract import read_excel, read_csv
from dagster_pipelines.etl.transform import pivot_data
from dagster_pipelines.etl.load import load_to_duckdb
import pandas as pd
from datetime import datetime, timedelta

# 2.3.1.1 Load pivoted KPI_FY.xlsm into KPI_FY
@dg.asset(compute_kind="duckdb", group_name="plan")
def kpi_fy(context: dg.AssetExecutionContext):
    
    # Extract
    df = read_excel()

    # Transform
    df_pivoted = pivot_data()
    
    # Load to DuckDB
    load_to_duckdb(df_pivoted, "KPI_FY")

    return df_pivoted

# 2.3.1.2 Load M_Center.csv into M_Center
@dg.asset(compute_kind="duckdb", group_name="plan")
def m_center(context: dg.AssetExecutionContext):
    # Extract
    df = read_csv()
    
    # Load
    load_to_duckdb(df, "M_Center")

    return df

# 2.3.2 Create asset kpi_fy_final_asset()
@dg.asset(
    compute_kind="duckdb",
    group_name="plan",
    deps=[kpi_fy, m_center]  # Declare dependencies explicitly
)
def kpi_fy_final_asset(
    context: dg.AssetExecutionContext,
    kpi_fy: pd.DataFrame,
    m_center: pd.DataFrame
):
    # Merge two datasets
    df_final = kpi_fy.merge(m_center[["Center_ID", "Center_Name"]], on="Center_ID", how="left")
    
    # BKK timezone
    thai_offset = timedelta(hours=7)

    # Add updated_at and change to UTC+7
    df_final["updated_at"] = datetime.now() + thai_offset

    # Load the final result into DuckDB (KPI_FY_Final)
    load_to_duckdb(df_final, "KPI_FY_Final")
    
    return df_final
