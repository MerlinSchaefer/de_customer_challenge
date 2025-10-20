import pandas as pd
from typing import Any


def _concat_incremental_with_source(
    raw: Any, filename_col: str = "_source_file"
) -> pd.DataFrame:
    """Concatenate incremental inputs and attach source filename.

    Args:
        raw: None, mapping filename->DataFrame, or a single DataFrame.
        filename_col: Column name to store source filename.

    Returns:
        DataFrame: concatenated copy with filename_col present (may be empty).
    """
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


def check_sales_not_empty(raw_sales: dict[str, pd.DataFrame], parameters: dict) -> str:
    """Return newline-separated filenames for any empty sales DataFrames.

    Args:
        raw_sales: mapping filename->DataFrame (may be None).
        parameters: config; respects parameters["quality"]["check_empty_sales_log"].

    Returns:
        str: newline-separated filenames with trailing newline if non-empty, else "".
    """
    if not parameters.get("quality", {}).get("check_empty_sales_log", True):
        return ""
    empty_files = []
    for filename, df in (raw_sales or {}).items():
        if hasattr(df, "empty") and df.empty:
            empty_files.append(filename)
    return ("\n".join(empty_files) + "\n") if empty_files else ""


def join_logs(*msgs: str) -> str:
    """Concatenate multiple log strings, skipping empty ones.

    Args:
        *msgs: log message strings.

    Returns:
        str: concatenated log.
    """
    return "".join(m for m in msgs if m)


# COSMOS (1001/1002)


def normalize_cosmos_sales_bronze(
    raw_sales: Any, ingestion_config: dict, customer_id: str
) -> pd.DataFrame:
    """Normalize Cosmos sales input to the bronze sales schema.

    Args:
        raw_sales: raw DataFrame or mapping filename->DataFrame.
        ingestion_config: ingestion configuration with column mappings.
        customer_id: customer identifier to set in _customer_id.

    Returns:
        DataFrame: normalized sales with canonical columns and dtypes.
    """
    # keep filenames when IncrementalDataSet returns dict{filename: df}
    df = _concat_incremental_with_source(raw_sales)
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "target_date",
                "number_store",
                "number_product",
                "sales_qty",
                "_customer_id",
                "_source_file",
            ]
        )

    cols = ingestion_config["erps"]["cosmos"]["columns"]["sales"]
    df = df.rename(
        columns={
            cols["date"]: "target_date",
            cols["store"]: "number_store",
            cols["product"]: "number_product",
            cols["qty"]: "sales_qty",
        }
    ).copy()

    # normalize types
    df["target_date"] = pd.to_datetime(
        df["target_date"], format="%Y-%m-%d", errors="raise"
    ).dt.normalize()
    df["number_store"] = df["number_store"].astype("string")
    df["number_product"] = df["number_product"].astype("string")
    df["sales_qty"] = pd.to_numeric(df["sales_qty"], errors="raise").fillna(0.0)

    df["_customer_id"] = customer_id
    # ensure column exists even if raw didn’t provide dict filenames
    if "_source_file" not in df.columns:
        df["_source_file"] = pd.NA

    return df[
        [
            "target_date",
            "number_store",
            "number_product",
            "sales_qty",
            "_customer_id",
            "_source_file",
        ]
    ]


def normalize_cosmos_deliveries_bronze(
    raw_deliveries: Any, ingestion_config: dict, customer_id: str
) -> pd.DataFrame:
    """Normalize Cosmos deliveries to bronze deliveries schema.

    Args:
        raw_deliveries: raw DataFrame or mapping filename->DataFrame.
        ingestion_config: ingestion configuration with column mappings.
        customer_id: customer identifier.

    Returns:
        DataFrame: normalized deliveries with expected columns and types.
    """
    df = _concat_incremental_with_source(raw_deliveries)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "target_date",
                "number_store",
                "number_product",
                "delivery_qty",
                "delivery_batch",
                "_customer_id",
            ]
        )
    cols = ingestion_config["erps"]["cosmos"]["columns"]["deliveries"]
    df = df.rename(
        columns={
            cols["date"]: "target_date",
            cols["store"]: "number_store",
            cols["product"]: "number_product",
            cols["qty"]: "delivery_qty",
            cols["batch"]: "delivery_batch",
        }
    )
    df["delivery_qty"] = pd.to_numeric(df["delivery_qty"], errors="raise").fillna(0.0)
    df["target_date"] = pd.to_datetime(
        df["target_date"], format="%Y-%m-%d", errors="raise"
    ).dt.normalize()
    df["_customer_id"] = customer_id
    return df[
        [
            "target_date",
            "number_store",
            "number_product",
            "delivery_qty",
            "delivery_batch",
            "_customer_id",
        ]
    ]


def normalize_cosmos_products_bronze(
    raw_products: Any, ingestion_config: dict, customer_id: str
) -> pd.DataFrame:
    """Normalize Cosmos product master to bronze schema.

    Args:
        raw_products: raw DataFrame or mapping filename->DataFrame.
        ingestion_config: ingestion configuration with column mappings.
        customer_id: customer identifier.

    Returns:
        DataFrame: normalized products with numeric price/moq and string ids.
    """
    df = _concat_incremental_with_source(raw_products)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "number_product",
                "product_name",
                "product_group",
                "price",
                "moq",
                "_customer_id",
            ]
        )
    cols = ingestion_config["erps"]["cosmos"]["columns"]["products"]
    df = df.rename(
        columns={
            cols["product"]: "number_product",
            cols["name"]: "product_name",
            cols["group"]: "product_group",
            cols["price"]: "price",
            cols["moq"]: "moq",
        }
    )
    df["price"] = pd.to_numeric(df["price"], errors="raise")
    df["moq"] = pd.to_numeric(df["moq"], errors="raise").fillna(0).astype("Int64")
    df["_customer_id"] = customer_id
    return df[
        [
            "number_product",
            "product_name",
            "product_group",
            "price",
            "moq",
            "_customer_id",
        ]
    ]


def normalize_cosmos_stores_bronze(
    raw_stores: Any, ingestion_config: dict, customer_id: str
) -> pd.DataFrame:
    """Normalize Cosmos stores to bronze schema and build address.

    Args:
        raw_stores: raw DataFrame or mapping filename->DataFrame.
        ingestion_config: ingestion configuration with column mappings.
        customer_id: customer identifier.

    Returns:
        DataFrame: normalized stores with address fields and store_address.
    """
    df = _concat_incremental_with_source(raw_stores)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "number_store",
                "store_name",
                "street",
                "postal_code",
                "city",
                "country",
                "state",
                "store_address",
                "_customer_id",
            ]
        )
    cols = ingestion_config["erps"]["cosmos"]["columns"]["stores"]
    df = df.rename(
        columns={
            cols["store"]: "number_store",
            cols["name"]: "store_name",
            cols["street"]: "street",
            cols["postal_code"]: "postal_code",
            cols["city"]: "city",
            cols["country"]: "country",
            cols["state"]: "state",
        }
    )
    df["_customer_id"] = customer_id
    # make address parts strings (preserve leading zeros)
    for col in ["street", "postal_code", "city"]:
        df[col] = df[col].astype("string").fillna("")

    df["store_address"] = df[["street", "postal_code", "city"]].agg(" – ".join, axis=1)
    return df[
        [
            "number_store",
            "store_name",
            "street",
            "postal_code",
            "city",
            "country",
            "state",
            "store_address",
            "_customer_id",
        ]
    ]


# GALAXY (1003)


def flatten_galaxy_deliveries_sales_bronze(
    raw_deliv_sales: Any, ingestion_config: dict, customer_id: str
) -> pd.DataFrame:
    """Flatten Galaxy deliveries+sales nested JSON into bronze rows.

    Args:
        raw_deliv_sales: raw DataFrame or mapping filename->DataFrame.
        ingestion_config: ingestion configuration describing nested keys.
        customer_id: customer identifier.

    Returns:
        DataFrame: flattened rows with sales and delivery fields.
    """
    # Accept IncrementalDataSet dict or a DataFrame
    if isinstance(raw_deliv_sales, dict):
        parts = []
        for fname, d in raw_deliv_sales.items():
            if d is None or d.empty:
                continue
            tmp = d.copy()
            tmp["_source_file"] = fname
            parts.append(tmp)
        df0 = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    else:
        df0 = raw_deliv_sales.copy()

    if df0 is None or df0.empty:
        return pd.DataFrame(
            columns=[
                "target_date",
                "number_store",
                "number_product",
                "sales_qty",
                "delivery_qty",
                "delivery_batch",
                "_customer_id",
                "_source_file",
            ]
        )

    cfg = ingestion_config["erps"]["galaxy"]["deliveries_sales"]
    filiale_key = cfg.get("filiale_array", "Filiale")  # assume present
    root_date = cfg["root_date"]  # "Datum"
    root_store = cfg["root_store"]  # "FilialNummer"
    hist_key = cfg["history_array"]  # "ArtikelHistory"
    f = cfg["fields"]  # product/sales_qty/delivery_qty/delivery_batch

    # 1) explode Filiale
    df0 = df0.explode(filiale_key, ignore_index=True)
    fil = pd.json_normalize(df0[filiale_key])
    if "_source_file" in df0.columns:
        fil["_source_file"] = df0["_source_file"].values

    # 2) explode ArtikelHistory
    fil = fil.explode(hist_key, ignore_index=True)
    hist = pd.json_normalize(fil[hist_key])

    # 3) map fields → bronze schema
    out = pd.DataFrame(
        {
            "target_date": pd.to_datetime(
                fil[root_date], errors="raise", dayfirst=True
            ).dt.date,
            "number_store": fil[root_store].astype("string"),
            "number_product": hist[f["product"]].astype("string"),
            "sales_qty": pd.to_numeric(hist[f["sales_qty"]], errors="raise").fillna(
                0.0
            ),
            "delivery_qty": pd.to_numeric(
                hist[f["delivery_qty"]], errors="raise"
            ).fillna(0.0),
            "delivery_batch": hist.get(
                f.get("delivery_batch", ""), pd.Series([None] * len(hist))
            ),
            "_source_file": fil.get("_source_file", pd.Series([pd.NA] * len(fil))),
        }
    )

    out["_customer_id"] = customer_id

    return out[
        [
            "target_date",
            "number_store",
            "number_product",
            "sales_qty",
            "delivery_qty",
            "delivery_batch",
            "_customer_id",
            "_source_file",
        ]
    ]


def normalize_galaxy_prices_bronze_daily(
    raw_prices: Any, ingestion_config: dict, customer_id: str
) -> pd.DataFrame:
    """Extract and normalize Galaxy daily prices into bronze schema.

    Args:
        raw_prices: raw DataFrame or mapping filename->DataFrame.
        ingestion_config: ingestion configuration with price extraction keys.
        customer_id: customer identifier.

    Returns:
        DataFrame: rows with target_date (NaT), number_product, price, _customer_id.
    """
    df0 = _concat_incremental_with_source(raw_prices)
    if df0 is None or df0.empty:
        return pd.DataFrame(
            columns=["target_date", "number_product", "price", "_customer_id"]
        )

    f = ingestion_config["erps"]["galaxy"][
        "prices"
    ]  # {"wrapper":"Verkaufspreise","product":"ArtikelNummer","price":"ArtikelPreis"}
    wrapper = f.get("wrapper", "Verkaufspreise")
    prod_key, price_key = f["product"], f["price"]

    items = []

    if wrapper in df0.columns:
        # NEW: robust per-cell extraction
        for v in df0[wrapper].dropna():
            if isinstance(v, list):
                items.extend(v)
            elif isinstance(v, dict):
                # case: directly a price object
                if prod_key in v and price_key in v:
                    items.append(v)
                # case: nested again under wrapper
                elif wrapper in v and isinstance(v[wrapper], list):
                    items.extend(v[wrapper])
                # else: ignore
            elif isinstance(v, str):
                # try to parse JSON string
                try:
                    import json

                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        items.extend(parsed)
                    elif isinstance(parsed, dict):
                        if prod_key in parsed and price_key in parsed:
                            items.append(parsed)
                        elif wrapper in parsed and isinstance(parsed[wrapper], list):
                            items.extend(parsed[wrapper])
                except Exception:
                    pass

    # fallback: look in any column for a dict with wrapper
    if not items:
        for col in df0.columns:
            for v in df0[col].dropna():
                if (
                    isinstance(v, dict)
                    and wrapper in v
                    and isinstance(v[wrapper], list)
                ):
                    items.extend(v[wrapper])

    if not items:
        raise KeyError(
            f"Galaxy prices: could not extract items from '{wrapper}'. "
            f"Columns: {list(df0.columns)}; first_row_type={type(df0.iloc[0,0]).__name__}"
        )

    inner = pd.DataFrame(items)

    if prod_key not in inner.columns or price_key not in inner.columns:
        raise KeyError(
            f"Galaxy prices: missing '{prod_key}'/'{price_key}' in extracted items. Got: {list(inner.columns)}"
        )

    out = pd.DataFrame(
        {
            "number_product": inner[prod_key].astype("string"),
            "price": pd.to_numeric(
                inner[price_key].astype(str).str.replace(",", ".", regex=False),
                errors="raise",
            ),
            "target_date": pd.NaT,
            "_customer_id": customer_id,
        }
    )
    return out[["target_date", "number_product", "price", "_customer_id"]]


def normalize_galaxy_products_bronze(
    raw_products: Any, ingestion_config: dict, customer_id: str
) -> pd.DataFrame:
    """Normalize Galaxy product master to bronze schema.

    Args:
        raw_products: raw DataFrame or mapping filename->DataFrame.
        ingestion_config: ingestion configuration with product keys.
        customer_id: customer identifier.

    Returns:
        DataFrame: normalized product rows with moq parsed as Int64.
    """
    df0 = _concat_incremental_with_source(raw_products)
    if df0 is None or df0.empty:
        return pd.DataFrame(
            columns=[
                "number_product",
                "product_name",
                "product_group",
                "moq",
                "_customer_id",
            ]
        )

    f = ingestion_config["erps"]["galaxy"]["products"]
    wrapper = "Artikel"  # from your JSON sample

    #  extract inner list under "Artikel" from whatever shape we got
    items = []

    # case A: "Artikel" is a column
    if wrapper in df0.columns:
        for v in df0[wrapper].dropna():
            if isinstance(v, list):
                items.extend(v)
            elif isinstance(v, dict):
                # already a single product dict → append
                items.append(v)
            elif isinstance(v, str):
                # try parse JSON string
                try:
                    import json

                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        items.extend(parsed)
                    elif isinstance(parsed, dict):
                        items.append(parsed)
                except Exception:
                    pass

    # case B: loader put the dict in a single cell: {"Artikel": [ ... ]}
    if not items:
        for col in df0.columns:
            for v in df0[col].dropna():
                if (
                    isinstance(v, dict)
                    and wrapper in v
                    and isinstance(v[wrapper], list)
                ):
                    items.extend(v[wrapper])

    if not items:
        raise KeyError(
            f"Galaxy products: could not extract items from '{wrapper}'. "
            f"Columns: {list(df0.columns)}; first_row_type={type(df0.iloc[0,0]).__name__}"
        )

    inner = pd.DataFrame(items)

    #  build output explicitly using config keys
    prod_key = f["product"]  # "ArtikelNummer"
    name_key = f["name"]  # "ArtikelName"
    group_key = f.get("group")  # "Artikelgruppe" (optional)
    moq_key = f.get("moq")  # "BestellMindestEinheit"

    # required columns
    missing = [k for k in [prod_key, name_key] if k not in inner.columns]
    if missing:
        raise KeyError(
            f"Galaxy products: missing required columns {missing}. Got: {list(inner.columns)}"
        )

    out = pd.DataFrame(
        {
            "number_product": inner[prod_key].astype("string"),
            "product_name": inner[name_key].astype("string"),
            "product_group": (
                inner[group_key].astype("string")
                if group_key and group_key in inner.columns
                else pd.Series([pd.NA] * len(inner), dtype="string")
            ),
        }
    )

    # MOQ: strings like "1.000" → 1000 or 1?
    # Typical ERP pattern here is thousand-separators with dot. MOQ is an integer.
    # Strategy: remove all non-digits and parse as Int64. If empty → 0.
    if moq_key and moq_key in inner.columns:
        out["moq"] = (
            pd.to_numeric(inner[moq_key], errors="raise").fillna(0).astype("Int64")
        )
    else:
        out["moq"] = pd.Series([0] * len(inner), dtype="Int64")

    out["_customer_id"] = customer_id

    return out[
        ["number_product", "product_name", "product_group", "moq", "_customer_id"]
    ]


def parse_galaxy_stores_bronze(
    raw_stores: Any, ingestion_config: dict, customer_id: str
) -> pd.DataFrame:
    """Parse Galaxy store listings into bronze stores schema.

    Args:
        raw_stores: raw DataFrame or mapping filename->DataFrame.
        ingestion_config: ingestion configuration with store keys.
        customer_id: customer identifier.

    Returns:
        DataFrame: parsed stores with address fields and store_address.
    """
    df0 = _concat_incremental_with_source(raw_stores)
    if df0 is None or df0.empty:
        return pd.DataFrame(
            columns=[
                "number_store",
                "store_name",
                "street",
                "postal_code",
                "city",
                "country",
                "state",
                "store_address",
                "_customer_id",
            ]
        )

    s_cfg = ingestion_config["erps"]["galaxy"]["stores"]
    wrapper = "Filialliste"  # from your JSON sample
    store_key = s_cfg["store"]  # "FilialNummer"
    name_key = s_cfg["name"]  # "FilialName"
    addr_key = s_cfg["address_multiline"]  # "FilialAnschrift"

    #  unwrap Filialliste robustly
    items = []
    if wrapper in df0.columns:
        for v in df0[wrapper].dropna():
            if isinstance(v, list):
                items.extend(v)
            elif isinstance(v, dict):
                items.append(v)
            elif isinstance(v, str):
                try:
                    import json

                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        items.extend(parsed)
                    elif isinstance(parsed, dict):
                        items.append(parsed)
                except Exception:
                    pass
    if not items:
        # fallback: dict in a single cell with {"Filialliste":[...]}
        for col in df0.columns:
            for v in df0[col].dropna():
                if (
                    isinstance(v, dict)
                    and wrapper in v
                    and isinstance(v[wrapper], list)
                ):
                    items.extend(v[wrapper])
    if not items:
        raise KeyError(
            f"Galaxy stores: could not extract items from '{wrapper}'. "
            f"Columns: {list(df0.columns)}; first_row_type={type(df0.iloc[0,0]).__name__}"
        )

    inner = pd.DataFrame(items)

    #  select required fields
    missing = [k for k in [store_key, name_key, addr_key] if k not in inner.columns]
    if missing:
        raise KeyError(
            f"Galaxy stores: missing required columns {missing}. Got: {list(inner.columns)}"
        )

    tmp = pd.DataFrame(
        {
            "number_store": inner[store_key].astype("string"),
            "store_name": inner[name_key].astype("string"),
            "address_ml": inner[addr_key].astype("string"),
        }
    )

    # parse multiline address
    import re

    streets, postals, cities, countries, states = [], [], [], [], []
    for addr in tmp["address_ml"].fillna(""):
        lines = [ln.strip() for ln in str(addr).split("\n") if ln.strip()]

        # naive defaults by position if available
        street = lines[0] if len(lines) > 0 else None
        postal = None
        city = None
        country = (
            lines[3] if len(lines) > 3 else (lines[-1] if len(lines) >= 2 else None)
        )
        state = lines[4] if len(lines) > 4 else (lines[2] if len(lines) > 2 else None)

        # try to detect a postal code line (4–5 digits)
        for ln in lines:
            m = re.fullmatch(r"(\d{4,5})", ln)
            if m:
                postal = m.group(1)
                # city likely the line after postal if exists
                idx = lines.index(ln)
                if idx + 1 < len(lines):
                    city = lines[idx + 1]
                break

        streets.append(street)
        postals.append(postal)
        cities.append(city)
        countries.append(country)
        states.append(state)

    out = pd.DataFrame(
        {
            "number_store": tmp["number_store"],
            "store_name": tmp["store_name"],
            "street": pd.Series(streets, dtype="string"),
            "postal_code": pd.Series(postals, dtype="string"),
            "city": pd.Series(cities, dtype="string"),
            "country": pd.Series(countries, dtype="string"),
            "state": pd.Series(states, dtype="string"),
        }
    )

    # build address string (safe string types)
    for c in ["street", "postal_code", "city"]:
        out[c] = out[c].astype("string").fillna("")
    out["store_address"] = out[["street", "postal_code", "city"]].agg(
        " – ".join, axis=1
    )

    out["_customer_id"] = customer_id
    return out[
        [
            "number_store",
            "store_name",
            "street",
            "postal_code",
            "city",
            "country",
            "state",
            "store_address",
            "_customer_id",
        ]
    ]


def enrich_galaxy_products_with_prices_bronze(
    products_1003: pd.DataFrame,
    prices_1003: pd.DataFrame,
) -> pd.DataFrame:
    """Enrich Galaxy product master with prices from the prices table.

    Args:
        products_1003: products DataFrame.
        prices_1003: prices DataFrame.

    Returns:
        DataFrame: products with price column filled from prices where missing.
    """
    if products_1003 is None or products_1003.empty:
        return pd.DataFrame(
            columns=[
                "number_product",
                "product_name",
                "product_group",
                "price",
                "moq",
                "_customer_id",
            ]
        )

    p = products_1003.copy()
    p["number_product"] = p["number_product"].astype("string")

    if prices_1003 is None or prices_1003.empty:
        # nothing to enrich, return as-is (ensure price column exists)
        if "price" not in p.columns:
            p["price"] = pd.NA
        return p[
            [
                "number_product",
                "product_name",
                "product_group",
                "price",
                "moq",
                "_customer_id",
            ]
        ]

    pr = prices_1003.copy()
    pr["number_product"] = pr["number_product"].astype("string")
    # prices dataset already parsed numeric; if not, enforce here:
    pr["price"] = pd.to_numeric(pr["price"], errors="raise")

    out = p.merge(
        pr[["number_product", "_customer_id", "price"]],
        on=["number_product", "_customer_id"],
        how="left",
        suffixes=("", "_from_prices"),
        validate="m:1",
    )

    # coalesce: keep product.price if present, otherwise take price_from_prices
    if "price" not in out.columns:
        out["price"] = pd.NA
    if "price_from_prices" in out.columns:
        out["price"] = out["price"].where(
            out["price"].notna(), out["price_from_prices"]
        )
        out.drop(columns=["price_from_prices"], inplace=True)

    return out[
        [
            "number_product",
            "product_name",
            "product_group",
            "price",
            "moq",
            "_customer_id",
        ]
    ]


# merge (kundenübergreifend)
def concat_frames_with_meta(*dfs: pd.DataFrame) -> pd.DataFrame:
    """Concatenate multiple bronze frames, normalize dtypes and add metadata.

    Args:
        *dfs: DataFrames to concatenate.

    Returns:
        DataFrame: unified frame with canonical dtypes, _ingest_ts and optional _row_hash.
    """
    frames = [d for d in dfs if d is not None and not d.empty]
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True).drop_duplicates()

    # Canonical dtypes for Bronze
    # string-like ids / labels
    for col in [
        "number_store",
        "number_product",
        "delivery_batch",
        "_customer_id",
        "_source_file",
    ]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # dates to pandas datetime64[ns] (Parquet-friendly)
    if "target_date" in df.columns:
        df["target_date"] = pd.to_datetime(
            df["target_date"], errors="raise"
        ).dt.normalize()

    # numeric measures
    for col in ["sales_qty", "delivery_qty", "return_qty", "price", "moq"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="raise")

    # single batch timestamp for the merge
    df["_ingest_ts"] = pd.Timestamp.utcnow()

    # compute row hash if we can identify keys
    def _pick_key_cols(df_: pd.DataFrame) -> list[str]:
        rules = [
            [
                "target_date",
                "number_store",
                "number_product",
                "_customer_id",
            ],  # daily fact-like
            ["number_product", "_customer_id"],  # products
            ["number_store", "_customer_id"],  # stores
        ]
        cols = set(map(str.lower, df_.columns))
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
