from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    union_mapping_product,
    union_mapping_store,
    build_silver_products,
    build_silver_stores,
    build_silver_sales_daily,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        # 1) Mappings union (je Entität)
        node(
            union_mapping_product,
            inputs=["raw_1001_mapping_product", "raw_1002_mapping_product", "raw_1003_mapping_product"],
            outputs="mapping_product_union",
            name="silver_union_mapping_product",
        ),
        node(
            union_mapping_store,
            inputs=["raw_1001_mapping_store", "raw_1002_mapping_store", "raw_1003_mapping_store"],
            outputs="mapping_store_union",
            name="silver_union_mapping_store",
        ),

        # 2) Dimensions
        node(
            build_silver_products,
            inputs=["bronze.products_all", "mapping_product_union"],
            outputs="silver.products",
            name="silver_build_products",
        ),
        node(
            build_silver_stores,
            inputs=["bronze.stores_all", "mapping_store_union"],
            outputs="silver.stores",
            name="silver_build_stores",
        ),

        # 3) Daily fact with stockout
        node(
            build_silver_sales_daily,
            inputs=[
                "bronze.sales_all",
                "bronze.deliveries_all",
                "raw_1001_mapping_delivery2sales", # neededed for stockout logic
                "mapping_product_union",   # <-- pass mappings, not dims
                "mapping_store_union",
            ],
            outputs="silver.sales_daily",
            name="silver_build_sales_daily",
        ),
    ])