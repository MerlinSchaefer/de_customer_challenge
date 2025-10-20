
import pandas as pd

# DIMs 

def build_dim_product(silver_products: pd.DataFrame) -> pd.DataFrame:
    """
    Nehme die jüngste Zeile je id_product (nach _ingest_ts) und liefere:
    id_product, number_product, product_name, product_group, moq, price_current.
    """
    if silver_products is None or silver_products.empty:
        return pd.DataFrame(columns=["id_product","number_product","product_name","product_group","moq","price_current"])

    df = silver_products.copy()
    # jüngste Version je id_product
    df = (df.sort_values(["id_product","_ingest_ts"])
            .drop_duplicates(subset=["id_product"], keep="last"))

    out = pd.DataFrame({
        "id_product": df["id_product"].astype("int64", errors="ignore"),
        "number_product": df["number_product"].astype("string"),
        "product_name": df["product_name"].astype("string"),
        "product_group": df.get("product_group").astype("string") if "product_group" in df else pd.Series([pd.NA]*len(df), dtype="string"),
        "moq": pd.to_numeric(df.get("moq"), errors="ignore"),
        "price_current": pd.to_numeric(df.get("price"), errors="ignore"),
    })
    return out.drop_duplicates().reset_index(drop=True)


def build_dim_store(silver_stores: pd.DataFrame) -> pd.DataFrame:
    """
    Jüngste Zeile je id_store. Adresse in beiden Formen: gesplittet + concat.
    """
    if silver_stores is None or silver_stores.empty:
        return pd.DataFrame(columns=[
            "id_store","number_store","store_name",
            "street","postal_code","city","country","state","store_address"
        ])

    df = (silver_stores.copy()
            .sort_values(["id_store","_ingest_ts"])
            .drop_duplicates(subset=["id_store"], keep="last"))

    out = pd.DataFrame({
        "id_store": df["id_store"].astype("int64", errors="ignore"),
        "number_store": df["number_store"].astype("string"),
        "store_name": df["store_name"].astype("string"),
        "street": df.get("street").astype("string"),
        "postal_code": df.get("postal_code").astype("string"),
        "city": df.get("city").astype("string"),
        "country": df.get("country").astype("string"),
        "state": df.get("state").astype("string"),
        "store_address": df.get("store_address").astype("string"),
    })
    return out.drop_duplicates().reset_index(drop=True)

# FACT

def _dense_calendar(df: pd.DataFrame) -> pd.DataFrame:
    # Erwartet Spalten: id_store, id_product, target_date, sales_qty, return_qty, delivery_qty, stockout, price
    if df.empty:
        return df
    df["target_date"] = pd.to_datetime(df["target_date"], errors="raise").dt.date
    # Vollständiger Index je Paar
    out_parts = []
    for (s, p), g in df.groupby(["id_store", "id_product"], dropna=False):
        if g.empty:
            continue
        start = g["target_date"].min()
        end   = g["target_date"].max()
        full = pd.DataFrame({"target_date": pd.date_range(start, end, freq="D").date})
        full = full.merge(g, on="target_date", how="left")
        # fehlende Mengen mit 0 auffüllen, stockout fehlend -> False, price forward-fill (statisch ok)
        for c in ["sales_qty","return_qty","delivery_qty"]:
            full[c] = pd.to_numeric(full[c], errors="coerce").fillna(0.0)
        full["stockout"] = full["stockout"].fillna(False).astype(bool)
        # Preis aus vorhandenen Zeilen übernehmen (stationär)
        if "price" in full.columns and full["price"].notna().any():
            full["price"] = full["price"].ffill().bfill()
        full["id_store"] = s
        full["id_product"] = p
        out_parts.append(full)
    return pd.concat(out_parts, ignore_index=True) if out_parts else df

def build_fact_daily_store_product(
    silver_sales_daily: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_store: pd.DataFrame,
    bronze_deliveries_all: pd.DataFrame,     
    mapping_product_union: pd.DataFrame,     
    mapping_store_union: pd.DataFrame,      
    delivery_to_sales_map: list[dict] = None # patch for pack-SKUs
) -> pd.DataFrame:

    # --- Basis: Fact aus Silver + Preis aus Dim ---
    if silver_sales_daily is None or silver_sales_daily.empty:
        base = pd.DataFrame(columns=[
            "id_product","id_store","target_date","sales_qty","return_qty","delivery_qty","stockout","price"
        ])
    else:
        fact = silver_sales_daily.copy()
        p = dim_product[["id_product","price_current"]].drop_duplicates()
        fact = fact.merge(p, on="id_product", how="left", validate="m:1")
        fact.rename(columns={"price_current":"price"}, inplace=True)
        base = fact[[
            "id_product","id_store","target_date",
            "sales_qty","return_qty","delivery_qty","stockout","price"
        ]].copy()

    # --- Zusatz: Pack-SKU-Reihe(n) (z.B. 29) aus Bronze-Lieferungen materialisieren ---
    # --- Zusatz: Pack-SKU-Zeilen aus Bronze-Lieferungen materialisieren (z.B. 1001: 29) ---
    extras = []
    if bronze_deliveries_all is not None and not bronze_deliveries_all.empty and delivery_to_sales_map:
        b = bronze_deliveries_all.copy()

        # strict casting
        b["target_date"]     = pd.to_datetime(b["target_date"], errors="raise").dt.date
        b["_customer_id"]    = b["_customer_id"].astype("string").str.strip()
        b["number_product"]  = b["number_product"].astype("string").str.strip()
        b["number_store"]    = b["number_store"].astype("string").str.strip()

        # param map -> DataFrame, ensure STRING types
        m = pd.DataFrame(delivery_to_sales_map)
        if not m.empty:
            m["_customer_id"]             = m["_customer_id"].astype("string").str.strip()
            m["number_product_delivery"]  = m["number_product_delivery"].astype("string").str.strip()
            m["number_product_sales"]     = m["number_product_sales"].astype("string").str.strip()
            m["factor"] = pd.to_numeric(m["factor"], errors="raise")

            # join deliveries ↔ param map on (_customer_id, number_product == number_product_delivery)
            dlv = b.merge(
                m,
                left_on=["_customer_id", "number_product"],
                right_on=["_customer_id", "number_product_delivery"],
                how="inner",   # only mapped pack SKUs
                validate="m:1",
            )

            if not dlv.empty:
                # resolve IDs for delivery product (pack SKU) + store
                pmap = (mapping_product_union[["id_product","number_product","_customer_id"]]
                        .astype({"number_product":"string","_customer_id":"string"})
                        .drop_duplicates(["_customer_id","number_product"], keep="last"))
                smap = (mapping_store_union[["id_store","number_store","_customer_id"]]
                        .astype({"number_store":"string","_customer_id":"string"})
                        .drop_duplicates(["_customer_id","number_store"], keep="last"))

                dlv = dlv.merge(pmap, left_on=["_customer_id","number_product"], right_on=["_customer_id","number_product"], how="left", validate="m:1")
                dlv = dlv.merge(smap, left_on=["_customer_id","number_store"],  right_on=["_customer_id","number_store"],  how="left", validate="m:1")

                # aggregate per day/store/pack-id
                g = (dlv.groupby(["id_store","id_product","target_date"], dropna=False)["delivery_qty"]
                        .sum().reset_index())

                # attach pack price from dim_product
                price_map = dim_product[["id_product","price_current"]].drop_duplicates()
                g = g.merge(price_map, on="id_product", how="left", validate="m:1")

                g["sales_qty"]  = 0.0
                g["return_qty"] = 0.0
                g["stockout"]   = False
                g.rename(columns={"price_current":"price"}, inplace=True)

                extras.append(g[["id_product","id_store","target_date","sales_qty","return_qty","delivery_qty","stockout","price"]])


    # --- Union Basis + Extras ---
    fact_all = pd.concat([base] + extras, ignore_index=True) if extras else base

    # --- Kalendertage auffüllen (pro Paar) ---
    fact_all = _dense_calendar(fact_all)

    # Sortierung
    fact_all = fact_all.sort_values(["id_store","id_product","target_date"]).reset_index(drop=True)
    return fact_all

# VIEWS 

def build_view_features_ml_daily(fact: pd.DataFrame) -> pd.DataFrame:
    """
    ML-View: nur die geforderten Features (id_product, id_store, target_date, sales_qty, stockout).
    """
    if fact is None or fact.empty:
        return pd.DataFrame(columns=["id_product","id_store","target_date","sales_qty","stockout"])

    df = fact[["id_product","id_store","target_date","sales_qty","stockout"]].copy()
    df["target_date"] = pd.to_datetime(df["target_date"], errors="raise").dt.date
    df["stockout"] = df["stockout"].astype(bool)
    return df.sort_values(["id_store","id_product","target_date"]).reset_index(drop=True)


def build_view_app_daily(fact: pd.DataFrame, dim_product: pd.DataFrame, dim_store: pd.DataFrame) -> pd.DataFrame:
    """
    App-View: Fact + Produkt-/Store-Stammdaten.
    """
    if fact is None or fact.empty:
        return pd.DataFrame(columns=[
            "id_product","id_store","target_date","sales_qty","return_qty","delivery_qty","stockout","price",
            "product_name","number_product","moq",
            "number_store","store_name","store_address",
        ])

    df = fact.copy()
    p = dim_product[["id_product","product_name","number_product","moq"]]
    s = dim_store[["id_store","number_store","store_name","store_address"]]

    df = df.merge(p, on="id_product", how="left", validate="m:1")
    df = df.merge(s, on="id_store",   how="left", validate="m:1")

    cols = [
        "id_product","id_store","target_date",
        "sales_qty","return_qty","delivery_qty","stockout","price",
        "product_name","number_product","moq",
        "number_store","store_name","store_address",
    ]
    # Sicherstellen, dass alle Spalten existieren
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    df["target_date"] = pd.to_datetime(df["target_date"], errors="raise").dt.date
    df["stockout"] = df["stockout"].astype(bool)

    return df[cols].sort_values(["id_store","id_product","target_date"]).reset_index(drop=True)
