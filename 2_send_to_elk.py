from elasticsearch import Elasticsearch, helpers
import pandas as pd

es = Elasticsearch("http://localhost:9200")
df = pd.read_parquet("airbnb_clean.parquet")

def actions(df):
    for _, row in df.iterrows():
        try:
            yield {
                "_index": "airbnb-listings",
                "_source": row.to_dict()
            }
        except Exception:
            continue

helpers.bulk(es, actions(df), chunk_size=500, raise_on_error=False)

print("✅ Envoi terminé sans blocage")
