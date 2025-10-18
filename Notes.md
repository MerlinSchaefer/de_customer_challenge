# Task 1
## Ziele

Einheitlicher Tages-Eintrag (fact) je Produkt × Filiale × Datum mit sales_qty, return_qty, delivery_qty, stockout, price.

2 Dim Tabellen: dim_product, dim_store (inkl. Name, Nummern, Adresse, Mindestbestellmenge, ggf. Preis).

2 VIEWs:

- ML-Features (nur features, num/bool)
  - ML braucht: id_product, id_store, target_date, sales_qty, stockout (+ optionale Features)


- App-View (mehr Infos, mit Namen/Adressen etc.)
  - App braucht: id_product, id_store, target_date, sales_qty, return_qty, delivery_qty, stockout, price, product_name, number_product, moq, number_store, store_name, store_address

## How&Why

Laden und Standardisierung: 
- Kundenformate landen roh in Bronze
- Silber vereinheitlicht (Spaltennamen, Typen, Dezimalkomma, Datumsformat, Kodierung)

Keys: 
- id_product, id_store (global) via Mappings 
- kundenspezifische Nummern (number_product, number_store) bleiben 

Granularität auf Tagesebene:
- Kennzahlen agg. auf (Produkt, Filiale, Datum) für einfache Joins + klare Struktur

Gold & VIEWs: 
- Star-Schema für flexibilität wenn neue Dims enstehen und gutem Normalisierungs/"Lesbarkeits"-Grad 
- je ein VIEW pro Use-Case (ML & Plots).

## Data Flow & Transformationen

### 1) Bronze (Laden & minimales Cleaning/Renaming)

Pro Kunde leicht unterschiedlich (z. B. 1001 csv mit Semikolons):

Input: 
- CSV,JSON, später ggf. weitere
Erstes Cleaning: 
- Dezimalkomma -> Punkt, Datumsstrings -> DATE

Spaltenmapping (z.B. für 1001):

Sales: 
- Datum->target_date, Kunde->number_store, Artikel->number_product, VK-Menge->sales_qty, VK-Betrag->sales_amount

Deliveries: 
- LI-Menge->delivery_qty (leere Zellen = 0), Kunde_Nummer->number_store

Products: 
- ArtNr->number_product, Bezeichnung->product_name, Preis->price, Mindestbestellmenge->moq

Stores: 
- Nummer->number_store, Name->store_name, Straße/PLZ/Ort->Adresse *Wieso nicht als getrennte Einträge?*

Metadata (kommt dazu): 
- _customer_id, _ingest_ts, _source_file, _row_hash.

Andere Steps: 
- Typ-Casts, Encoding-Fix (K�rbis… -> UTF-8)

### 2) Silver

JOINS mit mapping tabellen:

- silver_products = bronze_products JOIN mapping_product -> id_product.
- silver_stores = bronze_stores JOIN mapping_store -> id_store.

Tagesaggregation :

- silver_daily_sales (id_product, id_store, target_date, sales_qty, sales_amount)

- silver_daily_deliveries (… , delivery_qty)

- silver_daily_returns (… , return_qty) (falls vorhanden)

Ggf. weitere Transformationen wenn nötig


### 3) Gold, finale Tabellen + VIEWS

#### a) fact_daily_store_product

Keys: id_product, id_store, target_date

Spalten: sales_qty, return_qty, delivery_qty, stockout, price

Inhalt: Links-Joins der drei Silver-Tagesquellen; fehlende Werte -> 0; Preis nach Möglichkeit täglich, *sonst "last known" ?*

#### b) dim_product, dim_store

product: id_product, product_name (NOT NULL), number_product, moq, optional product_group, price_current

store: id_store, store_name, number_store, store_address, optional state, country

#### c) VIEWs

app_view_daily: die im Task geforderten Spalten, mit Namen/Adresse.

features_ml_daily: nume/boolean Features; leicht erweiterbar um Lags, Rolling Avg etc., Kalendermerkmale usw.

### Stockout (ausverkauft) Logik

Da keine Bestands-Snapshots vorliegen, zwei Stufen:

Direkt aus Quelle: Falls eine echte „Ausverkauft“-Flag geliefert wird -> übernehmen (scheint erstmal nicht der Fall)

Heuristik via theoretischem Bestand (pro Store/Produkt):

stock_t = stock_{t-1} + delivery_qty_t - sales_qty_t + return_qty_t

Warm-Up (erste Tage) ohne Flag -> **WIR GEHEN DAVON AUS, DASS PRODUKTE VORLIEGEN (?)**


### pot. Probleme
Nicht-parsbares Datum -> Text Logs

fehlende Mappings -> Text Logs („unbekannte Nummern“)

Duplicaets (per _row_hash) erkennen -> Text Logs

Idempotency:
- daily UPSERT auf Partitionsschlüssel target_date; nur betroffene Partition überschreiben