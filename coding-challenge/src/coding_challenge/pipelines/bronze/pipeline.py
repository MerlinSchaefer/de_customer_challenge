from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    check_sales_not_empty,
    join_logs,
    normalize_cosmos_sales_bronze,
    normalize_cosmos_deliveries_bronze,
    normalize_cosmos_products_bronze,
    normalize_cosmos_stores_bronze,
    flatten_galaxy_deliveries_sales_bronze,
    normalize_galaxy_prices_bronze_daily,
    normalize_galaxy_products_bronze,
    parse_galaxy_stores_bronze,
    enrich_galaxy_products_with_prices_bronze,
    concat_frames_with_meta,
)


def create_pipeline(**kwargs) -> Pipeline:

    # Checks
    checks = [
        node(
            func=check_sales_not_empty,
            inputs=dict(raw_sales="raw_1001_sales", parameters="parameters"),
            outputs="empty_sales_log_1001",
            name="check_empty_sales_1001",
        ),
        node(
            func=check_sales_not_empty,
            inputs=dict(raw_sales="raw_1002_sales", parameters="parameters"),
            outputs="empty_sales_log_1002",
            name="check_empty_sales_1002",
        ),
        node(
            func=join_logs,
            inputs=["empty_sales_log_1001", "empty_sales_log_1002"],
            outputs="99_EmptySalesLog",
            name="write_empty_sales_log",
        ),
    ]

    # COSMOS 1001
    cosmos_1001 = [
        node(
            normalize_cosmos_sales_bronze,
            inputs=dict(
                raw_sales="raw_1001_sales",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1001_id",
            ),
            outputs="bronze.sales_1001",
            name="bronze_cosmos_1001_sales",
        ),
        node(
            normalize_cosmos_deliveries_bronze,
            inputs=dict(
                raw_deliveries="raw_1001_deliveries",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1001_id",
            ),
            outputs="bronze.deliveries_1001",
            name="bronze_cosmos_1001_deliveries",
        ),
        node(
            normalize_cosmos_products_bronze,
            inputs=dict(
                raw_products="raw_1001_products",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1001_id",
            ),
            outputs="bronze.products_1001",
            name="bronze_cosmos_1001_products",
        ),
        node(
            normalize_cosmos_stores_bronze,
            inputs=dict(
                raw_stores="raw_1001_stores",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1001_id",
            ),
            outputs="bronze.stores_1001",
            name="bronze_cosmos_1001_stores",
        ),
    ]

    # COSMOS 1002
    cosmos_1002 = [
        node(
            normalize_cosmos_sales_bronze,
            inputs=dict(
                raw_sales="raw_1002_sales",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1002_id",
            ),
            outputs="bronze.sales_1002",
            name="bronze_cosmos_1002_sales",
        ),
        node(
            normalize_cosmos_deliveries_bronze,
            inputs=dict(
                raw_deliveries="raw_1002_deliveries",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1002_id",
            ),
            outputs="bronze.deliveries_1002",
            name="bronze_cosmos_1002_deliveries",
        ),
        node(
            normalize_cosmos_products_bronze,
            inputs=dict(
                raw_products="raw_1002_products",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1002_id",
            ),
            outputs="bronze.products_1002",
            name="bronze_cosmos_1002_products",
        ),
        node(
            normalize_cosmos_stores_bronze,
            inputs=dict(
                raw_stores="raw_1002_stores",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1002_id",
            ),
            outputs="bronze.stores_1002",
            name="bronze_cosmos_1002_stores",
        ),
    ]

    # GALAXY 1003
    galaxy_1003 = [
        node(
            flatten_galaxy_deliveries_sales_bronze,
            inputs=dict(
                raw_deliv_sales="raw_1003_deliveries_sales",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1003_id",
            ),
            outputs="bronze.deliveries_sales_1003",
            name="bronze_galaxy_1003_deliveries_sales",
        ),
        node(
            normalize_galaxy_prices_bronze_daily,
            inputs=dict(
                raw_prices="raw_1003_prices",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1003_id",
            ),
            outputs="bronze.prices_1003",
            name="bronze_galaxy_1003_prices",
        ),
        node(
            normalize_galaxy_products_bronze,
            inputs=dict(
                raw_products="raw_1003_products",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1003_id",
            ),
            outputs="bronze.products_1003",
            name="bronze_galaxy_1003_products",
        ),
        node(
            parse_galaxy_stores_bronze,
            inputs=dict(
                raw_stores="raw_1003_stores",
                ingestion_config="ingestion_config",
                customer_id="params:customer_1003_id",
            ),
            outputs="bronze.stores_1003",
            name="bronze_galaxy_1003_stores",
        ),
        # enrich 1003 products with its prices
        node(
            enrich_galaxy_products_with_prices_bronze,
            inputs=["bronze.products_1003", "bronze.prices_1003"],
            outputs="bronze.products_1003_enriched",
            name="bronze_enrich_1003_products_with_prices",
        ),
    ]

    # merge kundenübergreifend (Bronze-All)
    merges = [
        node(
            concat_frames_with_meta,
            inputs=[
                "bronze.sales_1001",
                "bronze.sales_1002",
                "bronze.deliveries_sales_1003",
            ],  #  Note: deliveries_sales_1003 contains sales data too
            outputs="bronze.sales_all",
            name="merge_bronze_sales_all",
        ),
        node(
            concat_frames_with_meta,
            inputs=[
                "bronze.deliveries_1001",
                "bronze.deliveries_1002",
                "bronze.deliveries_sales_1003",
            ],
            outputs="bronze.deliveries_all",
            name="merge_bronze_deliveries_all",
        ),
        node(
            concat_frames_with_meta,
            inputs=[
                "bronze.products_1001",
                "bronze.products_1002",
                "bronze.products_1003_enriched",
            ],
            outputs="bronze.products_all",
            name="merge_bronze_products_all",
        ),
        node(
            concat_frames_with_meta,
            inputs=["bronze.stores_1001", "bronze.stores_1002", "bronze.stores_1003"],
            outputs="bronze.stores_all",
            name="merge_bronze_stores_all",
        ),
    ]

    return pipeline(checks + cosmos_1001 + cosmos_1002 + galaxy_1003 + merges)
