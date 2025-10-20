from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    build_dim_product,
    build_dim_store,
    build_fact_daily_store_product,
    build_view_features_ml_daily,
    build_view_app_daily,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        # DIMs zuerst (benötigt für Preis & View)
        node(build_dim_product,
             inputs="silver.products",
             outputs="gold.dim_product",
             name="gold_build_dim_product"),

        node(build_dim_store,
             inputs="silver.stores",
             outputs="gold.dim_store",
             name="gold_build_dim_store"),

        # FACT
        node(
            build_fact_daily_store_product,
            inputs=[
            "silver.sales_daily",
            "gold.dim_product",
            "gold.dim_store",
            "bronze.deliveries_all",
            "mapping_product_union",
            "mapping_store_union",
            "params:delivery_to_sales_map",
            ],
            outputs="gold.fact_daily_store_product",
            name="gold_build_fact",
        ),

        # VIEWS
        node(build_view_features_ml_daily,
             inputs="gold.fact_daily_store_product",
             outputs="views.features_ml_daily",
             name="gold_build_view_ml"),

        node(build_view_app_daily,
             inputs=["gold.fact_daily_store_product","gold.dim_product","gold.dim_store"],
             outputs="views.app_view_daily",
             name="gold_build_view_app"),
    ])
