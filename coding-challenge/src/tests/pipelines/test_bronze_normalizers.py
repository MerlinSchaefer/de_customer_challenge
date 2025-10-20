import pandas as pd
from pandas.testing import assert_frame_equal


from coding_challenge.pipelines.bronze.nodes import (
    normalize_cosmos_sales_bronze,
    normalize_galaxy_prices_bronze_daily,
)


def test_normalize_cosmos_sales_bronze_iso():
    raw = {
        "sales_2025-08-10.csv": pd.DataFrame(
            {
                "Datum": ["2025-08-10", "2025-08-11"],
                "Kunde": [2, 2],
                "Artikel": [72, 72],
                "VK-Menge": [173, 47],
                "VK-Betrag": [245.25, 63.65],
            }
        )
    }
    ingestion_config = {
        "erps": {
            "cosmos": {
                "columns": {
                    "sales": {
                        "date": "Datum",
                        "store": "Kunde",
                        "product": "Artikel",
                        "qty": "VK-Menge",
                    }
                }
            }
        }
    }
    out = normalize_cosmos_sales_bronze(raw, ingestion_config, customer_id="1001")
    assert list(out.columns) == [
        "target_date",
        "number_store",
        "number_product",
        "sales_qty",
        "_customer_id",
        "_source_file",
    ]
    assert out["_customer_id"].unique().tolist() == ["1001"]
    assert out["target_date"].dtype.kind == "M"  # datetime64
    assert out["sales_qty"].sum() == 220  # 173+47


def test_normalize_galaxy_prices_bronze_daily_wrapper():
    raw = {
        "prices.json": pd.DataFrame(
            {
                # loader returned a single dict in the first cell:
                "payload": [
                    {
                        "Verkaufspreise": [
                            {"ArtikelNummer": "1070", "ArtikelPreis": "2.99"},
                            {"ArtikelNummer": "1088", "ArtikelPreis": "3.49"},
                        ]
                    }
                ]
            }
        )
    }
    ingestion_config = {
        "erps": {
            "galaxy": {
                "prices": {
                    "wrapper": "Verkaufspreise",
                    "product": "ArtikelNummer",
                    "price": "ArtikelPreis",
                }
            }
        }
    }
    out = normalize_galaxy_prices_bronze_daily(
        raw, ingestion_config, customer_id="1003"
    )
    assert list(out.columns) == [
        "target_date",
        "number_product",
        "price",
        "_customer_id",
    ]
    assert out["_customer_id"].unique().tolist() == ["1003"]
    assert out["number_product"].tolist() == ["1070", "1088"]
    assert out["price"].tolist() == [2.99, 3.49]
