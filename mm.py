import pandas as pd
import numpy as np
import re
import os
from elasticsearch import Elasticsearch, helpers
# =========================================================
# 1️⃣ Configuration et Connexion
# =========================================================
ES_URL = "http://localhost:9200"
INDEX_NAME = "airbnb_listings"

es = Elasticsearch(ES_URL)

# =========================================================
# 2️⃣ Chargement et Fusion
# =========================================================
print("📂 Chargement des fichiers CSV...")
paris_file = "listings_paris.csv"
lyon_file = "listings_lyon.csv"

# Lecture sécurisée
df_paris = pd.read_csv(paris_file, low_memory=False)
df_lyon = pd.read_csv(lyon_file, low_memory=False)

df_paris['city'] = 'Paris'
df_lyon['city'] = 'Lyon'
df = pd.concat([df_paris, df_lyon], ignore_index=True)

# =========================================================
# 3️⃣ Sélection et Nettoyage Strict
# =========================================================
print("🧹 Nettoyage des données...")

colonnes_conservees = [
    'id', 'city', 'name', 'host_id', 'host_name',
    'latitude', 'longitude', 'property_type', 'accommodates',
    'bedrooms', 'bathrooms_text', 'amenities', 'price',
    'availability_365', 'number_of_reviews', 'review_scores_rating'
]

# On ne garde que ce qui existe
df = df[[c for c in colonnes_conservees if c in df.columns]].copy()

# --- Nettoyage du Prix ---
def clean_price(p):
    if pd.isna(p): return 0.0
    res = re.sub(r'[€$£, ]', '', str(p))
    try: return float(res)
    except: return 0.0

df['price'] = df['price'].apply(clean_price)

# --- Géolocalisation (Crucial pour éviter les BulkIndexError) ---
df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

# On supprime les lignes où les coordonnées sont manquantes (sinon ES rejette)
initial_count = len(df)
df = df.dropna(subset=['latitude', 'longitude'])
print(f"🗑️ {initial_count - len(df)} lignes supprimées (GPS invalide).")

# Création du champ location pour le mapping geo_point
df['location'] = df.apply(lambda r: {"lat": float(r['latitude']), "lon": float(r['longitude'])}, axis=1)

# --- Extraction Bathrooms ---
def extract_bath(t):
    if pd.isna(t): return 0.0
    match = re.search(r'(\d+\.?\d*)', str(t))
    return float(match.group(1)) if match else (0.5 if 'half' in str(t).lower() else 0.0)

df['bathrooms'] = df['bathrooms_text'].apply(extract_bath) if 'bathrooms_text' in df.columns else 0.0

# --- Conversion des types pour Elasticsearch ---
df['id'] = df['id'].astype(str)
df['bedrooms'] = pd.to_numeric(df['bedrooms'], errors='coerce').fillna(0).astype(int)
df['accommodates'] = pd.to_numeric(df['accommodates'], errors='coerce').fillna(0).astype(int)
df['review_scores_rating'] = pd.to_numeric(df['review_scores_rating'], errors='coerce').fillna(0).astype(float)

# Suppression des doublons
df = df.drop_duplicates(subset='id')

print(f"✅ Nettoyage terminé : {len(df):,} lignes prêtes.")

# =========================================================
# 4️⃣ Création de l'Index avec Mapping
# =========================================================
if es.indices.exists(index=INDEX_NAME):
    print(f"🗑️ Suppression de l'ancien index '{INDEX_NAME}'...")
    es.indices.delete(index=INDEX_NAME)

mapping = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "city": {"type": "keyword"},
            "name": {"type": "text", "analyzer": "french"},
            "location": {"type": "geo_point"},
            "price": {"type": "float"},
            "bedrooms": {"type": "integer"},
            "bathrooms": {"type": "float"},
            "accommodates": {"type": "integer"},
            "review_scores_rating": {"type": "float"},
            "amenities": {"type": "text"}
        }
    }
}

es.indices.create(index=INDEX_NAME, body=mapping)
print(f"✅ Nouvel index '{INDEX_NAME}' créé avec mapping GeoPoint.")

# =========================================================
# 5️⃣ Envoi des données (Bulk Ingest)
# =========================================================
def generate_data(dataframe):
    for _, row in dataframe.iterrows():
        doc = row.to_dict()
        # Supprimer les colonnes temporaires pour ne pas encombrer ES
        if 'bathrooms_text' in doc: del doc['bathrooms_text']
        if 'latitude' in doc: del doc['latitude']
        if 'longitude' in doc: del doc['longitude']

        yield {
            "_index": INDEX_NAME,
            "_id": doc['id'],
            "_source": doc
        }

print("🚀 Envoi vers Elasticsearch (cela peut prendre quelques secondes)...")
try:
    # raise_on_error=False permet de ne pas stopper le script si 1 ou 2 docs posent problème
    success, errors = helpers.bulk(es, generate_data(df), raise_on_error=False, chunk_size=1000)
    print(f"✅ Terminé ! {success:,} documents indexés.")
    if errors:
        print(f"⚠️ {len(errors)} documents n'ont pas pu être indexés.")
except Exception as e:
    print(f"❌ Erreur critique lors de l'envoi : {e}")

print(f"\n💡 Vous pouvez maintenant tester la requête dans Kibana Dev Tools.")
