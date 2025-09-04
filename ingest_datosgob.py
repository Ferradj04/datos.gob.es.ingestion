import os
import json
import sqlite3
import csv
import requests


BASE = "https://datos.gob.es/apidata/catalog/dataset"
PAGE_SIZE = 5

def mk_conn(db_path="datosgob.db"):
    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE IF EXISTS datasets")
    conn.execute("DROP TABLE IF EXISTS distributions")
    conn.execute("DROP TABLE IF EXISTS geo_taxonomy")  # <-- AJOUT


    #DataSet Query
    conn.execute("""
        CREATE TABLE datasets(
            id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            issued TEXT,
            modified TEXT,
            publisher TEXT,
            raw_json TEXT
        )
    """)
    #Distributions Query
    conn.execute("""
        CREATE TABLE distributions(
            id TEXT PRIMARY KEY,
            dataset_id TEXT,
            title TEXT,
            format TEXT,
            access_url TEXT,
            download_url TEXT,
            raw_json TEXT,
            FOREIGN KEY(dataset_id) REFERENCES datasets(id)
        )
    """)
    # Table de la taxonomie géographique NTI
    conn.execute("""
        CREATE TABLE geo_taxonomy(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT NOT NULL,     -- Country, Autonomous-region, Province
            name TEXT NOT NULL,
            uri TEXT NOT NULL UNIQUE
        )
    """)
    conn.commit()
    return conn

def fetch_page(page=0):
    url = BASE
    params = {"_pageSize": PAGE_SIZE, "_page": page, "_sort": "-modified"}
    r = requests.get(url, params=params, headers={"Accept": "application/json"}, timeout=60)
    print(params, r.url, r.status_code)
    r.raise_for_status()
    return r.json()

def normalize_text(value):
    """Unifie string / dict / list en une seule chaîne lisible"""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return value.get("_value") or value.get("text") or json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return " | ".join(filter(None, [normalize_text(v) for v in value]))
    return str(value)

def extract_dataset(item):
    return {
        "id": item.get("identifier"),
        "title": normalize_text(item.get("title")),
        "description": normalize_text(item.get("description")),
        "issued": item.get("issued"),
        "modified": item.get("modified"),
        "publisher": normalize_text(item.get("publisher")),
        "raw_json": json.dumps(item, ensure_ascii=False)
    }

def extract_distributions(item, dataset_id):
    dists = []
    dist_data = item.get("distribution")
    if isinstance(dist_data, dict):  # parfois objet unique
        dist_data = [dist_data]
    if isinstance(dist_data, list):
        for dist in dist_data:
            dists.append({
                "id": dist.get("identifier"),
                "dataset_id": dataset_id,
                "title": normalize_text(dist.get("title")),
                "format": normalize_text(dist.get("format")),
                "access_url": dist.get("accessURL"),
                "download_url": dist.get("downloadURL"),
                "raw_json": json.dumps(dist, ensure_ascii=False)
            })
    return dists

def insert_geo_entities(conn):
    entities = [
        ("Country", "España", "http://datos.gob.es/apidata/nti/territory/Country/España"),
        ("Autonomous-region", "Comunidad de Madrid", "http://datos.gob.es/apidata/nti/territory/Autonomous-region/Comunidad-Madrid"),
        ("Autonomous-region", "Andalucía", "http://datos.gob.es/apidata/nti/territory/Autonomous-region/Andalucia"),
        ("Province", "Madrid", "http://datos.gob.es/apidata/nti/territory/Province/Madrid"),
        ("Province", "Sevilla", "http://datos.gob.es/apidata/nti/territory/Province/Sevilla"),
    ]
    conn.executemany("INSERT OR IGNORE INTO geo_taxonomy(level,name,uri) VALUES (?,?,?)", entities)
    conn.commit()

def save_to_db(conn, dataset, distributions):
    conn.execute("""INSERT OR REPLACE INTO datasets VALUES (?,?,?,?,?,?,?)""",
                 (dataset["id"], dataset["title"], dataset["description"],
                  dataset["issued"], dataset["modified"], dataset["publisher"],
                  dataset["raw_json"]))
    for d in distributions:
        conn.execute("""INSERT OR REPLACE INTO distributions VALUES (?,?,?,?,?,?,?)""",
                     (d["id"], d["dataset_id"], d["title"], d["format"],
                      d["access_url"], d["download_url"], d["raw_json"]))
    conn.commit()

def export_csv(conn, out_dir="data"):
    os.makedirs(out_dir, exist_ok=True)

    with open(os.path.join(out_dir, "datasets.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id","title","description","issued","modified","publisher"])
        for row in conn.execute("SELECT id,title,description,issued,modified,publisher FROM datasets"):
            writer.writerow(row)

    with open(os.path.join(out_dir, "distributions.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id","dataset_id","title","format","access_url","download_url"])
        for row in conn.execute("SELECT id,dataset_id,title,format,access_url,download_url FROM distributions"):
            writer.writerow(row)

    with open(os.path.join(out_dir, "geo_taxonomy.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "level", "name", "uri"])
        for row in conn.execute("SELECT id, level, name, uri FROM geo_taxonomy"):
            writer.writerow(row)



def main(max_pages=100):  # <-- plafond par défaut = 10 pages
    conn = mk_conn()

    # Injection des entités NTI
    insert_geo_entities(conn)
    print("✅ Taxonomie NTI insérée dans geo_taxonomy.")

    total_datasets = 0
    total_pages = 0
    page = 0

    while True:
        # stop si on a atteint la limite
        if max_pages and page >= max_pages:
            print(f"[STOP] Limite de {max_pages} pages atteinte.")
            break

        data = fetch_page(page)
        items = data.get("result", {}).get("items", [])
        if not items:
            print(f"[PAGE {page}] Aucun dataset trouvé, arrêt.")
            break

        print(f"[PAGE {page}] {len(items)} datasets trouvés.")
        for i, it in enumerate(items[:3]):
            print("   →", normalize_text(it.get("title")))

        for item in items:
            ds = extract_dataset(item)
            if not ds["id"]:
                continue
            dists = extract_distributions(item, ds["id"])
            save_to_db(conn, ds, dists)
            total_datasets += 1

        total_pages += 1
        page += 1

    print(f"\n✅ Ingestion terminée.")
    print(f"   - Nombre total de pages parcourues : {total_pages}")
    print(f"   - Nombre total de datasets stockés : {total_datasets}")

    export_csv(conn)
    print("📂 Export CSV terminé dans ./data/")


if __name__ == "__main__":
    main()
