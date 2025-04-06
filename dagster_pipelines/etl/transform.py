import pandas as pd
from dagster_pipelines.etl.extract import read_excel

# 2.2.1 Pivot data in the "KPI_FY.xlsm" file
def pivot_data() -> pd.DataFrame:
    df = read_excel()

    df_melted = pd.melt(
        df,
        id_vars=['Fiscal_Year', 'Center_ID', 'Kpi Number', 'Kpi_Name', 'Unit'],
        value_vars=['Plan_Q1', 'Plan_Q2', 'Plan_Q3', 'Plan_Q4', 'Actual_Q1', 'Actual_Q2', 'Actual_Q3', 'Actual_Q4'],
        var_name='Amount_Name',
        value_name='Amount'
    )
    df_melted['Amount_Type'] = df_melted['Amount_Name'].apply(lambda x: 'Plan' if 'Plan' in x else 'Actual')

    return df_melted