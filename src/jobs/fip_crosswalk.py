# src/jobs/fip_crosswalk.py

import requests
import pandas as pd
import os

# ── Config ─────────────────────────────────────────────────────────────────────
SDA_URL    = "https://sdmdataaccess.usda.gov/tabular/post.rest"
OUTPUT_CSV = "data/fip_crosswalk.csv"

# ── SDA Query Helper ───────────────────────────────────────────────────────────
def query_sda(sql: str) -> pd.DataFrame:
    payload = {"query": sql, "format": "JSON+COLUMNNAME"}

    print("[SDA] Sending query...")
    r = requests.post(SDA_URL, data=payload, timeout=60)

    print(f"[SDA] Status code : {r.status_code}")

    if r.status_code != 200:
        raise RuntimeError(f"SDA API error: HTTP {r.status_code}\n{r.text[:500]}")

    if not r.text.strip():
        raise ValueError("[SDA] Empty response — query may be invalid or too large")

    print(f"[SDA] Response preview: {r.text[:300]}")

    data = r.json()

    if "Table" not in data:
        raise ValueError(f"[SDA] Unexpected response structure: {data}")

    cols = data["Table"][0]
    rows = data["Table"][1:]
    print(f"[SDA] Received {len(rows)} rows, columns: {cols}")
    return pd.DataFrame(rows, columns=cols)


# ── SQL ────────────────────────────────────────────────────────────────────────
SQL = """
SELECT
    l.areasymbol,
    l.areaname                  AS survey_area_name,
    la.areaname                 AS county_name,
    la.areatypename             AS area_type,
    la.areakey                  AS fips
FROM legend l
INNER JOIN laoverlap la
    ON la.lkey = l.lkey
WHERE la.areatypename = 'County or Parish'
  AND l.areasymbol != 'US'
ORDER BY l.areasymbol
"""

SQL_FALLBACK = """
SELECT
    l.areasymbol,
    l.areaname  AS survey_area_name,
    l.areasymbol AS fips
FROM legend l
WHERE l.areasymbol != 'US'
ORDER BY l.areasymbol
"""


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    # 1. Fetch
    try:
        print("[STEP 1] Querying USDA Soil Data Access...")
        df = query_sda(SQL)
    except Exception as e:
        print(f"[WARN] Primary query failed: {e}")
        print("[STEP 1] Retrying with fallback query...")
        df = query_sda(SQL_FALLBACK)

    print(f"\n[STEP 1] Sample:\n{df.head(5)}")

    # 2. Clean
    print("\n[STEP 2] Cleaning...")
    df["areasymbol"]       = df["areasymbol"].str.strip().str.upper()
    df["survey_area_name"] = df["survey_area_name"].str.strip()
    df["county_name"]      = df["county_name"].str.strip()
    df["fips"]             = df["fips"].str.strip().str.zfill(5)
    df = df.dropna(subset=["fips"])

    print(f"[STEP 2] Total rows after cleaning: {len(df)}")

    # 3. Save
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n[STEP 3] Saved to {OUTPUT_CSV}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()