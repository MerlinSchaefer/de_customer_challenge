import pandas as pd
from coding_challenge.pipelines.bronze.nodes import concat_frames_with_meta

def test_concat_frames_with_meta_types_and_meta():
    a = pd.DataFrame({
        "target_date": ["2025-08-10"],
        "number_store": [2],  # int
        "number_product": ["72"],  # str
        "sales_qty": [5.0],
        "_customer_id": ["1001"]
    })
    b = pd.DataFrame({
        "target_date": ["2025-08-10"],
        "number_store": ["2"],  # str
        "number_product": ["72"],
        "delivery_qty": [3.0],
        "_customer_id": ["1001"]
    })
    out = concat_frames_with_meta(a, b)
    assert out["number_store"].dtype.name == "string"
    assert "_ingest_ts" in out.columns and "_row_hash" in out.columns
    # both rows present (different measures)
    assert len(out) == 2
