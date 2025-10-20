"""Project pipelines."""

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from coding_challenge.pipelines.bronze.pipeline import create_pipeline as create_bronze
from coding_challenge.pipelines.silver.pipeline import create_pipeline as create_silver
from coding_challenge.pipelines.gold.pipeline import create_pipeline as create_gold


def register_pipelines() -> dict[str, Pipeline]:
    bronze = create_bronze()
    silver = create_silver()
    gold = create_gold()
    default = bronze + silver + gold
    return {
        "__default__": default,
        "01_bronze": bronze,
        "02_silver": silver,
        "03_gold": gold,
    }
