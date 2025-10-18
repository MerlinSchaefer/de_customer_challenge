import pandas as pd
from typing import  Any, List, Optional


# helpers TODO: move to utils.py?

def _concat_incremental_with_source(raw: Any, filename_col: str = "_source_file") -> pd.DataFrame:
    """Accept dict[filename->df] from IncrementalDataSet or a single df; keep a _source_file column."""
    if raw is None:
        return pd.DataFrame()
    if isinstance(raw, dict):
        if not raw:
            return pd.DataFrame()
        parts = []
        for fname, df in raw.items():
            if df is None or df.empty:
                continue
            df = df.copy()
            df[filename_col] = fname
            parts.append(df)
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    df = raw.copy()
    if filename_col not in df.columns:
        df[filename_col] = pd.NA
    return df


# old check (returns str for text log)
def check_sales_not_empty(raw_sales: dict[str, pd.DataFrame], parameters: dict) -> str:
    """
    Checks each new sales file for emptiness and returns filenames (newline-separated)
    for appending to a text log dataset.
    """
    if not parameters.get("quality", {}).get("check_empty_sales_log", True):
        return ""
    empty_files = []
    for filename, df in (raw_sales or {}).items():
        if hasattr(df, "empty") and df.empty:
            empty_files.append(filename)
    return ("\n".join(empty_files) + "\n") if empty_files else ""


def join_logs(*msgs: str) -> str:
    """Concatenate multiple log strings into one (for single text dataset output)."""
    return "".join(m for m in msgs if m)


# COSMOS (1001/1002)

def normalize_cosmos_sales_bronze(raw_sales: Any, ingestion_config: dict, customer_id: str) -> pd.DataFrame:
    df = _concat_incremental_with_source(raw_sales)
    if df.empty:
        return pd.DataFrame(columns=["target_date", "number_store", "number_product", "sales_qty", "_customer_id"])
    cols = ingestion_config["erps"]["cosmos"]["columns"]["sales"]
    df = df.rename(columns={
        cols["date"]: "target_date",
        cols["store"]: "number_store",
        cols["product"]: "number_product",
        cols["qty"]: "sales_qty",
    })
    df["sales_qty"] = pd.to_numeric(df["sales_qty"], errors="coerce").fillna(0.0)
    df["target_date"] = pd.to_datetime(df["target_date"], errors="coerce").dt.date
    df["_customer_id"] = customer_id
    return df[["target_date", "number_store", "number_product", "sales_qty", "_customer_id"]]


def normalize_cosmos_deliveries_bronze(raw_deliveries: Any, ingestion_config: dict, customer_id: str) -> pd.DataFrame:
    df = _concat_incremental_with_source(raw_deliveries)
    if df.empty:
        return pd.DataFrame(columns=["target_date", "number_store", "number_product", "delivery_qty", "delivery_batch", "_customer_id"])
    cols = ingestion_config["erps"]["cosmos"]["columns"]["deliveries"]
    df = df.rename(columns={
        cols["date"]: "target_date",
        cols["store"]: "number_store",
        cols["product"]: "number_product",
        cols["qty"]: "delivery_qty",
        cols["batch"]: "delivery_batch",
    })
    df["delivery_qty"] = pd.to_numeric(df["delivery_qty"], errors="coerce").fillna(0.0)
    df["target_date"] = pd.to_datetime(df["target_date"], errors="coerce").dt.date
    df["_customer_id"] = customer_id
    return df[["target_date", "number_store", "number_product", "delivery_qty", "delivery_batch", "_customer_id"]]


def normalize_cosmos_products_bronze(raw_products: Any, ingestion_config: dict, customer_id: str) -> pd.DataFrame:
    df = _concat_incremental_with_source(raw_products)
    if df.empty:
        return pd.DataFrame(columns=["number_product", "product_name", "product_group", "price", "moq", "_customer_id"])
    cols = ingestion_config["erps"]["cosmos"]["columns"]["products"]
    df = df.rename(columns={
        cols["product"]: "number_product",
        cols["name"]: "product_name",
        cols["group"]: "product_group",
        cols["price"]: "price",
        cols["moq"]: "moq",
    })
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["moq"] = pd.to_numeric(df["moq"], errors="coerce").fillna(0).astype("Int64")
    df["_customer_id"] = customer_id
    return df[["number_product", "product_name", "product_group", "price", "moq", "_customer_id"]]


def normalize_cosmos_stores_bronze(raw_stores: Any, ingestion_config: dict, customer_id: str) -> pd.DataFrame:
    df = _concat_incremental_with_source(raw_stores)
    if df.empty:
        return pd.DataFrame(columns=["number_store", "store_name", "street", "postal_code", "city", "country", "state", "store_address", "_customer_id"])
    cols = ingestion_config["erps"]["cosmos"]["columns"]["stores"]
    df = df.rename(columns={
        cols["store"]: "number_store",
        cols["name"]: "store_name",
        cols["street"]: "street",
        cols["postal_code"]: "postal_code",
        cols["city"]: "city",
        cols["country"]: "country",
        cols["state"]: "state",
    })
    df["_customer_id"] = customer_id
    df["store_address"] = df[["street", "postal_code", "city"]].fillna("").agg(" – ".join, axis=1)
    return df[["number_store", "store_name", "street", "postal_code", "city", "country", "state", "store_address", "_customer_id"]]



# GALAXY (1003)

def flatten_galaxy_deliveries_sales_bronze(raw_deliv_sales: Any, ingestion_config: dict, customer_id: str) -> pd.DataFrame:
    df_raw = _concat_incremental_with_source(raw_deliv_sales)
    if df_raw.empty:
        return pd.DataFrame(columns=["target_date", "number_store", "number_product", "sales_qty", "delivery_qty", "delivery_batch", "_customer_id"])

    cfg = ingestion_config["erps"]["galaxy"]["deliveries_sales"]
    root_date, root_store, hist_key = cfg["root_date"], cfg["root_store"], cfg["history_array"]
    f = cfg["fields"]

    # explode ArtikelHistory
    df = df_raw.explode(hist_key, ignore_index=True)
    hist = pd.json_normalize(df[hist_key])

    out = pd.DataFrame({
        "target_date": pd.to_datetime(df[root_date], errors="coerce").dt.date,
        "number_store": df[root_store],
        "number_product": hist.get(f["product"]),
        "sales_qty": pd.to_numeric(hist.get(f["sales_qty"]), errors="coerce").fillna(0.0),
        "delivery_qty": pd.to_numeric(hist.get(f["delivery_qty"]), errors="coerce").fillna(0.0),
        "delivery_batch": hist.get(f.get("delivery_batch", ""), pd.Series([None] * len(hist))),
    })
    out["_customer_id"] = customer_id
    return out[["target_date", "number_store", "number_product", "sales_qty", "delivery_qty", "delivery_batch", "_customer_id"]]


def normalize_galaxy_prices_bronze_daily(raw_prices: Any, ingestion_config: dict, customer_id: str) -> pd.DataFrame:
    df = _concat_incremental_with_source(raw_prices)
    if df.empty:
        return pd.DataFrame(columns=["target_date", "number_product", "price", "_customer_id"])

    f = ingestion_config["erps"]["galaxy"]["prices"]
    df = df.rename(columns={f["product"]: "number_product", f["price"]: "price"}).copy()

    # Wenn target_date in JSON vorhanden ist, übernehmen; sonst NaT (wird später ggf. behandelt)
    if "target_date" in df.columns:
        df["target_date"] = pd.to_datetime(df["target_date"], errors="coerce").dt.date
    else:
        df["target_date"] = pd.NaT  # bleibt leer, bis im Silver ggf. anders gelöst

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["_customer_id"] = customer_id
    return df[["target_date", "number_product", "price", "_customer_id"]]


def normalize_galaxy_products_bronze(raw_products: Any, ingestion_config: dict, customer_id: str) -> pd.DataFrame:
    df = _concat_incremental_with_source(raw_products)
    if df.empty:
        return pd.DataFrame(columns=["number_product", "product_name", "product_group", "moq", "_customer_id"])
    f = ingestion_config["erps"]["galaxy"]["products"]
    df = df.rename(columns={
        f["product"]: "number_product",
        f["name"]: "product_name",
        f["group"]: "product_group",
        f["moq"]: "moq",
    })
    df["moq"] = pd.to_numeric(df["moq"], errors="coerce").fillna(0).astype("Int64")
    df["_customer_id"] = customer_id
    return df[["number_product", "product_name", "product_group", "moq", "_customer_id"]]


def parse_galaxy_stores_bronze(raw_stores: Any, ingestion_config: dict, customer_id: str) -> pd.DataFrame:
    df = _concat_incremental_with_source(raw_stores)
    if df.empty:
        return pd.DataFrame(columns=["number_store", "store_name", "street", "postal_code", "city", "country", "state", "store_address", "_customer_id"])

    s = ingestion_config["erps"]["galaxy"]["stores"]
    df = df.rename(columns={s["store"]: "number_store", s["name"]: "store_name", s["address_multiline"]: "address_ml"}).copy()

    # simple address parsing
    streets, postals, cities, countries, states = [], [], [], [], []
    for addr in df["address_ml"].fillna(""):
        lines = [ln.strip() for ln in str(addr).split("\n") if ln.strip()]
        streets.append(lines[0] if len(lines) > 0 else None)

        pc, ct = None, None
        for ln in lines:
            # einfache Heuristik: "PLZ Stadt"
            m = pd.Series(ln).str.extract(r"(?P<postal>\d{4,5})\s+(?P<city>.+)")
            if not m.isna().all(axis=None):
                pc = m.iloc[0]["postal"]
                ct = m.iloc[0]["city"]
                break
        postals.append(pc); cities.append(ct)
        countries.append(lines[-1] if len(lines) >= 2 else None)
        states.append(None)

    out = pd.DataFrame({
        "number_store": df["number_store"],
        "store_name": df["store_name"],
        "street": streets,
        "postal_code": postals,
        "city": cities,
        "country": countries,
        "state": states,
    })
    out["_customer_id"] = customer_id
    out["store_address"] = out[["street", "postal_code", "city"]].fillna("").agg(" – ".join, axis=1)
    return out[["number_store", "store_name", "street", "postal_code", "city", "country", "state", "store_address", "_customer_id"]]



# merge (kundenübergreifend)
def concat_frames_with_meta(*dfs: pd.DataFrame) -> pd.DataFrame:
    frames = [d for d in dfs if d is not None and not d.empty]
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True).drop_duplicates()

    # single batch timestamp for the merge
    df["_ingest_ts"] = pd.Timestamp.utcnow()

    # compute row hash if we can identify keys
    def _pick_key_cols(df: pd.DataFrame) -> list[str]:
        rules = [
            ["target_date", "number_store", "number_product", "_customer_id"],  # daily fact-like
            ["number_product", "_customer_id"],  # products
            ["number_store", "_customer_id"],    # stores
        ]
        cols = set(map(str.lower, df.columns))
        for rule in rules:
            if all(c.lower() in cols for c in rule):
                return rule
        for rule in rules:
            subset = [c for c in rule if c.lower() in cols]
            if subset:
                return subset
        return []

    key_cols = _pick_key_cols(df)
    if key_cols:
        keys = df[key_cols].astype(str).fillna("")
        df["_row_hash"] = pd.util.hash_pandas_object(keys, index=False).astype("uint64")

    return df
