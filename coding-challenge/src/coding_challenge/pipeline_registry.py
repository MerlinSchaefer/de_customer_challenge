"""Project pipelines."""

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from coding_challenge.pipelines.bronze.pipeline import create_pipeline as bronze_create 
from coding_challenge.pipelines.silver.pipeline import create_pipeline as silver_create

# def register_pipelines() -> dict[str, Pipeline]:
#     """Register the project's pipelines.

#     Returns:
#         A mapping from pipeline names to ``Pipeline`` objects.
#     """
#     pipelines = find_pipelines()
#     pipelines["__default__"] = sum(pipelines.values())
#     pipelines["bronze"] = bronze_create()
#     return pipelines
def register_pipelines() -> dict[str, Pipeline]:
    bronze = bronze_create()
    silver = silver_create()
    default = bronze + silver
    return {
        "__default__": default,
        "01_bronze": bronze,
        "02_silver": silver,
    }