import pandas as pd
from pandas.testing import assert_frame_equal
from coding_challenge.pipelines.gold.nodes import (
    build_dim_product,
    build_dim_store,
    build_fact_daily_store_product,
    build_view_features_ml_daily,
    build_view_app_daily,
)


def test_gold_fact_dense_calendar_and_price_and_extras():
    # Silver sales_daily (only for sales SKU)
    sales_daily = pd.DataFrame(
        {
            "id_product": [10010003, 10010003],
            "id_store": [100190001, 100190001],
            "target_date": pd.to_datetime(["2025-08-10", "2025-08-12"]),
            "sales_qty": [173, 48],
            "return_qty": [0, 0],
            "delivery_qty": [0, 0],
            "stockout": [False, False],
        }
    )
    # Dims (price on product)
    dim_product = pd.DataFrame(
        {
            "id_product": [10010003, 10010002],
            "number_product": ["72", "29"],
            "product_name": ["Kürbisbrötchen", "Kürbisbrötchen Teigling"],
            "product_group": ["", ""],
            "moq": [0, 0],
            "price_current": [1.35, 16.20],
        }
    )
    dim_store = pd.DataFrame(
        {
            "id_store": [100190001],
            "number_store": ["2"],
            "store_name": ["Filiale Hochbetrieb"],
            "street": ["Kosmonautengasse 1"],
            "postal_code": ["13353"],
            "city": ["Glitzerstadt"],
            "country": ["Deutschland"],
            "state": ["Bayern"],
            "store_address": ["Kosmonautengasse 1 – 13353 – Glitzerstadt"],
        }
    )
    # Bronze deliveries (for pack SKU '29')
    bronze_deliveries_all = pd.DataFrame(
        {
            "_customer_id": ["1001", "1001"],
            "number_store": ["2", "2"],
            "number_product": ["29", "29"],
            "target_date": pd.to_datetime(["2025-08-11", "2025-08-12"]),
            "delivery_qty": [4, 4],
        }
    )
    # Mappings
    mapping_product_union = pd.DataFrame(
        {
            "id_product": [10010003, 10010002],
            "number_product": ["72", "29"],
            "_customer_id": ["1001", "1001"],
        }
    )
    mapping_store_union = pd.DataFrame(
        {
            "id_store": [100190001],
            "number_store": ["2"],
            "_customer_id": ["1001"],
        }
    )
    delivery_to_sales_map = [
        {
            "_customer_id": "1001",
            "number_product_delivery": "29",
            "number_product_sales": "72",
            "factor": 12,
        }
    ]

    fact = build_fact_daily_store_product(
        sales_daily,
        dim_product,
        dim_store,
        bronze_deliveries_all,
        mapping_product_union,
        mapping_store_union,
        delivery_to_sales_map,
    )

    # 1) Dense calendar: 2025-08-10..2025-08-12 must be present
    dates = fact.query("id_store==100190001 & id_product==10010003")[
        "target_date"
    ].tolist()
    assert [str(d) for d in dates] == ["2025-08-10", "2025-08-11", "2025-08-12"]

    # 2) Pack-SKU rows exist (delivery-only):
    pack = fact.query("id_product==10010002 & id_store==100190001").sort_values(
        "target_date"
    )
    assert not pack.empty
    assert (pack["sales_qty"] == 0).all()
    assert (pack["return_qty"] == 0).all()
    assert (pack["delivery_qty"] > 0).any()
    # price for pack sku should be its own (16.20)
    assert set(pack["price"].dropna().unique()) == {16.20}

    # 3) ML view minimal
    ml = build_view_features_ml_daily(fact)
    assert list(ml.columns) == [
        "id_product",
        "id_store",
        "target_date",
        "sales_qty",
        "stockout",
    ]

    # 4) App view contains names and address
    app = build_view_app_daily(fact, dim_product, dim_store)
    assert {"product_name", "number_product", "store_name", "store_address"} <= set(
        app.columns
    )
