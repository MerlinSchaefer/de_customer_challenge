import pandas as pd
from typing import List

# -------- helpers --------


def _batch_ts(df: pd.DataFrame) -> pd.Timestamp:
    """Generate a uniform batch timestamp for the node output."""
    return pd.Timestamp.utcnow()


def _derive_customer_from_id(series: pd.Series) -> pd.Series:
    """Derive customer ID from the first 4 digits of the given series.

    Args:
        series (pd.Series): Series containing IDs.

    Returns:
        pd.Series: Series with derived customer IDs.
    """
    return series.astype(str).str.slice(0, 4)


def _hash_rows(df: pd.DataFrame, key_cols: List[str]) -> pd.Series:
    """Hash rows of the DataFrame based on specified key columns.

    Args:
        df (pd.DataFrame): DataFrame to hash.
        key_cols (List[str]): List of column names to use as keys.

    Returns:
        pd.Series: Series of hashed values.
    """
    keys = df[key_cols].astype(str).fillna("")
    return pd.util.hash_pandas_object(keys, index=False).astype("uint64")


def union_mapping_product(m1, m2, m3):
    """Union multiple product mappings into a single DataFrame.

    Args:
        m1, m2, m3: DataFrames containing product mappings.

    Returns:
        pd.DataFrame: Combined DataFrame of product mappings.
    """
    maps = [m for m in [m1, m2, m3] if m is not None and not m.empty]
    if not maps:
        return pd.DataFrame(columns=["id_product", "number_product", "_customer_id"])
    df = pd.concat(maps, ignore_index=True).drop_duplicates()
    df["id_product"] = pd.to_numeric(df["id_product"], errors="raise").astype("int64")
    df["number_product"] = (
        pd.to_numeric(df["number_product"], errors="raise")
        .astype("int64")
        .astype("string")
    )
    df["_customer_id"] = df["id_product"].astype(str).str.slice(0, 4)
    return df[["id_product", "number_product", "_customer_id"]].drop_duplicates()


def union_mapping_store(m1, m2, m3):
    """Union multiple store mappings into a single DataFrame.

    Args:
        m1, m2, m3: DataFrames containing store mappings.

    Returns:
        pd.DataFrame: Combined DataFrame of store mappings.
    """
    maps = [m for m in [m1, m2, m3] if m is not None and not m.empty]
    if not maps:
        return pd.DataFrame(columns=["id_store", "number_store", "_customer_id"])
    df = pd.concat(maps, ignore_index=True).drop_duplicates()
    df["id_store"] = pd.to_numeric(df["id_store"], errors="raise").astype("int64")
    df["number_store"] = (
        pd.to_numeric(df["number_store"], errors="raise")
        .astype("int64")
        .astype("string")
    )
    df["_customer_id"] = df["id_store"].astype(str).str.slice(0, 4)
    return df[["id_store", "number_store", "_customer_id"]].drop_duplicates()


# -------- dimensions --------


def build_silver_products(
    bronze_products_all: pd.DataFrame,
    mapping_product_union: pd.DataFrame,
    bronze_prices_1003: pd.DataFrame = None,
) -> pd.DataFrame:
    """Build silver products DataFrame from bronze products and mapping.

    Args:
        bronze_products_all (pd.DataFrame): DataFrame of all bronze products.
        mapping_product_union (pd.DataFrame): DataFrame of product mappings.
        bronze_prices_1003 (pd.DataFrame, optional): DataFrame of bronze prices.

    Returns:
        pd.DataFrame: Silver products DataFrame.
    """
    if bronze_products_all is None or bronze_products_all.empty:
        return pd.DataFrame(
            columns=[
                "id_product",
                "number_product",
                "product_name",
                "product_group",
                "price",
                "moq",
                "_customer_id",
                "_ingest_ts",
                "_row_hash",
            ]
        )

    bp = bronze_products_all.copy()
    mp = mapping_product_union.copy()

    # align types for join
    bp["number_product"] = bp["number_product"].astype("string").str.strip()
    bp["_customer_id"] = bp["_customer_id"].astype("string").str.strip()
    mp["number_product"] = mp["number_product"].astype("string").str.strip()
    mp["_customer_id"] = mp["_customer_id"].astype("string").str.strip()

    df = bp.merge(mp, on=["number_product", "_customer_id"], how="left", validate="m:1")

    # (could be moved) if you enrich prices in Silver; if you enriched in Bronze, remove this block
    if bronze_prices_1003 is not None and not bronze_prices_1003.empty:
        p3 = bronze_prices_1003.copy()
        p3["number_product"] = p3["number_product"].astype("string")
        p3["_customer_id"] = p3["_customer_id"].astype("string")
        p3 = p3.drop_duplicates(["number_product", "_customer_id"], keep="last")
        df = df.merge(
            p3[["number_product", "_customer_id", "price"]],
            on=["number_product", "_customer_id"],
            how="left",
            suffixes=("", "_from_prices"),
            validate="m:1",
        )
        if "price_from_prices" in df.columns:
            df["price"] = df["price"].where(
                df["price"].notna(), df["price_from_prices"]
            )
            df.drop(columns=["price_from_prices"], inplace=True)

    df["_ingest_ts"] = pd.Timestamp.utcnow()
    df["_row_hash"] = pd.util.hash_pandas_object(
        df[["id_product", "_customer_id", "number_product"]].astype(str).fillna(""),
        index=False,
    ).astype("uint64")

    keep = [
        "id_product",
        "number_product",
        "product_name",
        "product_group",
        "price",
        "moq",
        "_customer_id",
        "_ingest_ts",
        "_row_hash",
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA
    return df[keep].drop_duplicates().reset_index(drop=True)


def build_silver_stores(
    bronze_stores_all: pd.DataFrame,
    mapping_store_union: pd.DataFrame,
) -> pd.DataFrame:
    """Build silver stores DataFrame from bronze stores and mapping.

    Args:
        bronze_stores_all (pd.DataFrame): DataFrame of all bronze stores.
        mapping_store_union (pd.DataFrame): DataFrame of store mappings.

    Returns:
        pd.DataFrame: Silver stores DataFrame.
    """
    if bronze_stores_all is None or bronze_stores_all.empty:
        return pd.DataFrame(
            columns=[
                "id_store",
                "number_store",
                "store_name",
                "street",
                "postal_code",
                "city",
                "country",
                "state",
                "store_address",
                "_customer_id",
                "_ingest_ts",
                "_row_hash",
            ]
        )

    bs = bronze_stores_all.copy()
    ms = mapping_store_union.copy()

    # align types for join
    bs["number_store"] = bs["number_store"].astype("string").str.strip()
    bs["_customer_id"] = bs["_customer_id"].astype("string").str.strip()
    ms["number_store"] = ms["number_store"].astype("string").str.strip()
    ms["_customer_id"] = ms["_customer_id"].astype("string").str.strip()

    df = bs.merge(ms, on=["number_store", "_customer_id"], how="left", validate="m:1")

    df["_ingest_ts"] = pd.Timestamp.utcnow()
    df["_row_hash"] = pd.util.hash_pandas_object(
        df[["id_store", "_customer_id", "number_store"]].astype(str).fillna(""),
        index=False,
    ).astype("uint64")

    keep = [
        "id_store",
        "number_store",
        "store_name",
        "street",
        "postal_code",
        "city",
        "country",
        "state",
        "store_address",
        "_customer_id",
        "_ingest_ts",
        "_row_hash",
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA
    return df[keep].drop_duplicates().reset_index(drop=True)


# facts (daily)


def build_silver_sales_daily(
    bronze_sales_all: pd.DataFrame,
    bronze_deliveries_all: pd.DataFrame,
    map_delivery2sales_1001: pd.DataFrame,
    mapping_product_union: pd.DataFrame,
    mapping_store_union: pd.DataFrame,
) -> pd.DataFrame:
    """Build daily silver sales DataFrame from bronze sales and deliveries.

    Args:
        bronze_sales_all (pd.DataFrame): DataFrame of all bronze sales.
        bronze_deliveries_all (pd.DataFrame): DataFrame of all bronze deliveries.
        map_delivery2sales_1001 (pd.DataFrame): Mapping for delivery to sales adjustments.
        mapping_product_union (pd.DataFrame): DataFrame of product mappings.
        mapping_store_union (pd.DataFrame): DataFrame of store mappings.

    Returns:
        pd.DataFrame: Daily silver sales DataFrame.
    """
    #  aggregate sales
    if bronze_sales_all is None or bronze_sales_all.empty:
        s_agg = pd.DataFrame(
            columns=[
                "_customer_id",
                "number_store",
                "number_product",
                "target_date",
                "sales_qty",
            ]
        )
    else:
        s = bronze_sales_all.copy()
        s["sales_qty"] = pd.to_numeric(s.get("sales_qty", 0.0), errors="coerce").fillna(
            0.0
        )
        s["target_date"] = pd.to_datetime(
            s["target_date"], errors="coerce"
        ).dt.normalize()
        s_agg = (
            s.groupby(
                ["_customer_id", "number_store", "number_product", "target_date"],
                dropna=False,
            )["sales_qty"]
            .sum()
            .reset_index()
        )

    # aggregate deliveries
    if bronze_deliveries_all is None or bronze_deliveries_all.empty:
        d_pre = pd.DataFrame(
            columns=[
                "_customer_id",
                "number_store",
                "number_product",
                "target_date",
                "delivery_qty",
            ]
        )
    else:
        d_pre = bronze_deliveries_all.copy()
        d_pre["delivery_qty"] = pd.to_numeric(d_pre.get("delivery_qty"), errors="raise")
        d_pre["target_date"] = pd.to_datetime(
            d_pre["target_date"], errors="raise"
        ).dt.normalize()
        d_pre["_customer_id"] = d_pre["_customer_id"].astype("string")
        d_pre["number_product"] = d_pre["number_product"].astype("string")
        d_pre["number_store"] = d_pre["number_store"].astype("string")

        # load Mapping, if present
        if map_delivery2sales_1001 is not None and not map_delivery2sales_1001.empty:
            m = map_delivery2sales_1001.copy()
            m["_customer_id"] = m["_customer_id"].astype("string")
            m["number_product_delivery"] = m["number_product_delivery"].astype("string")
            m["number_product_sales"] = m["number_product_sales"].astype("string")
            m["factor"] = pd.to_numeric(m["factor"], errors="raise")

            # Join (_customer_id, number_product)
            d_pre = d_pre.merge(
                m,
                left_on=["_customer_id", "number_product"],
                right_on=["_customer_id", "number_product_delivery"],
                how="left",
                validate="m:1",
            )

            # Ziel-Produkt & Menge berechnen
            d_pre["number_product_adj"] = d_pre["number_product"].where(
                d_pre["number_product_sales"].isna(), d_pre["number_product_sales"]
            )
            d_pre["delivery_qty_adj"] = d_pre["delivery_qty"] * d_pre["factor"].fillna(
                1.0
            )
        else:
            d_pre["number_product_adj"] = d_pre["number_product"]
            d_pre["delivery_qty_adj"] = d_pre["delivery_qty"]

        # agg on *adjusted* product num
        d_agg = (
            d_pre.groupby(
                ["_customer_id", "number_store", "number_product_adj", "target_date"],
                dropna=False,
            )["delivery_qty_adj"]
            .sum()
            .reset_index()
            .rename(
                columns={
                    "number_product_adj": "number_product",
                    "delivery_qty_adj": "delivery_qty",
                }
            )
        )

    # merge to fact on *raw keys*
    keys_raw = ["_customer_id", "number_store", "number_product", "target_date"]
    fact = s_agg.merge(d_agg, on=keys_raw, how="outer")
    if fact.empty:
        # return empty with columns present
        cols = [
            "id_product",
            "id_store",
            "target_date",
            "sales_qty",
            "return_qty",
            "delivery_qty",
            "stockout",
            "_customer_id",
            "_ingest_ts",
            "_row_hash",
            "number_product",
            "number_store",
        ]
        return pd.DataFrame(columns=cols)

    fact["sales_qty"] = pd.to_numeric(fact["sales_qty"], errors="coerce").fillna(0.0)
    fact["delivery_qty"] = pd.to_numeric(fact["delivery_qty"], errors="coerce").fillna(
        0.0
    )
    fact["return_qty"] = 0.0

    # compute stockout on raw keys
    fact = fact.sort_values(keys_raw)

    def _compute_stockout(g: pd.DataFrame) -> pd.DataFrame:
        """Compute stockout status for a group of sales data.

        Args:
            g (pd.DataFrame): Grouped DataFrame of sales data.

        Returns:
            pd.DataFrame: DataFrame with stockout status.
        """
        net = (
            g["delivery_qty"].fillna(0)
            + g["return_qty"].fillna(0)
            - g["sales_qty"].fillna(0)
        )
        rb = net.cumsum()
        rb = rb.where(rb > 0, 0.0)
        prev_rb = rb.shift(1).fillna(0.0)
        st = (prev_rb == 0.0) & (g["sales_qty"] > 0.0)
        if len(st) > 0:
            st.iloc[0] = False  # first day never stockout (ASSUMPTION)
        out = g.copy()
        out["stockout"] = st.astype(bool)
        return out

    fact = fact.groupby(
        ["_customer_id", "number_store", "number_product"], group_keys=False
    ).apply(_compute_stockout)

    #  map IDs AFTER stockout
    fact["number_product"] = fact["number_product"].astype("string").str.strip()
    fact["number_store"] = fact["number_store"].astype("string").str.strip()
    fact["_customer_id"] = fact["_customer_id"].astype("string").str.strip()

    # Build unique maps from mapping unions (they should already be unique, but be safe)
    pmap = (
        mapping_product_union[["id_product", "number_product", "_customer_id"]]
        .astype({"number_product": "string", "_customer_id": "string"})
        .drop_duplicates(subset=["_customer_id", "number_product"], keep="last")
    )
    smap = (
        mapping_store_union[["id_store", "number_store", "_customer_id"]]
        .astype({"number_store": "string", "_customer_id": "string"})
        .drop_duplicates(subset=["_customer_id", "number_store"], keep="last")
    )

    # Map IDs (now truly many-to-one)
    fact = fact.merge(
        pmap, on=["_customer_id", "number_product"], how="left", validate="m:1"
    )
    fact = fact.merge(
        smap, on=["_customer_id", "number_store"], how="left", validate="m:1"
    )

    # lineage + final cols
    fact["_ingest_ts"] = pd.Timestamp.utcnow()
    fact["_row_hash"] = pd.util.hash_pandas_object(
        fact[["_customer_id", "number_store", "number_product", "target_date"]]
        .astype(str)
        .fillna(""),
        index=False,
    ).astype("uint64")

    cols = [
        "id_product",
        "id_store",
        "target_date",
        "sales_qty",
        "return_qty",
        "delivery_qty",
        "stockout",
        "_customer_id",
        "_ingest_ts",
        "_row_hash",
        "number_product",
        "number_store",
    ]
    for c in cols:
        if c not in fact.columns:
            fact[c] = pd.NA

    return (
        fact[cols]
        .sort_values(["id_store", "id_product", "target_date"])
        .reset_index(drop=True)
    )
