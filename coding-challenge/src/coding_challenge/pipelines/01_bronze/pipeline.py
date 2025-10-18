from kedro.pipeline import Pipeline, node
from .nodes import check_sales_not_empty
    
def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=check_sales_not_empty,
                inputs=[
                    "01_sales",
                    "parameters",
                ],
                outputs="99_EmptySalesLog",
                name="check_for_empty_sales",
                confirms="01_sales",
            ),
        ]
    )