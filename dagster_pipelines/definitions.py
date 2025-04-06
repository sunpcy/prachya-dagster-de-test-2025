from dagster import Definitions, load_assets_from_modules, asset_check
from dagster_pipelines import assets
from dagster_pipelines.schedules import kpi_fy_monthly_job_schedule
from dagster_pipelines_tests.test_assets import kpi_fy_raw_dtypes_check


defs = Definitions(
    assets=load_assets_from_modules([assets]),
    schedules=[kpi_fy_monthly_job_schedule],
    asset_checks=[kpi_fy_raw_dtypes_check],
)