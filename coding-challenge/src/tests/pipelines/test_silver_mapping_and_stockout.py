import pandas as pd
from coding_challenge.pipelines.silver.nodes import (
    union_mapping_product,
    union_mapping_store,
    build_silver_sales_daily,
)


def test_union_mapping_product_and_store_casts():
    mp1 = pd.DataFrame(
        {"id_product": [10010001, 10010002], "number_product": [405, 29]}
    )
    ms1 = pd.DataFrame({"id_store": [100190001, 100190002], "number_store": [2, 11]})

    mp = union_mapping_product(mp1, pd.DataFrame(), pd.DataFrame())
    ms = union_mapping_store(ms1, pd.DataFrame(), pd.DataFrame())

    assert set(mp.columns) == {"id_product", "number_product", "_customer_id"}
    assert set(ms.columns) == {"id_store", "number_store", "_customer_id"}
    # first 4 digits as customer id
    assert mp["_customer_id"].unique().tolist() == ["1001"]
    assert ms["_customer_id"].unique().tolist() == ["1001"]
    assert mp["number_product"].dtype.name == "string"
    assert ms["number_store"].dtype.name == "string"
