# clean_data_tp1.py
import pandas as pd
import numpy as np
import re
import os

# =========================
# 1️⃣ Charger et Fusionner les fichiers
# =========================
print("=" * 60)
print("TP1 : AUDIT ET NETTOYAGE DES DONNÉES AIRBNB")
print("=" * 60)

paris_file = "listings_paris.csv"
lyon_file = "listings_lyon.csv"

# Vérification de l'existence des fichiers
for f in [paris_file, lyon_file]:
    if not os.path.exists(f):
        print(f"❌ Erreur : Le fichier {f} est introuvable.")

print("📂 Chargement des fichiers...")
df_paris = pd.read_csv(paris_file, low_memory=False)
df_lyon = pd.read_csv(lyon_file, low_memory=False)

# Identification de la ville et fusion
df_paris['city'] = 'Paris'
df_lyon['city'] = 'Lyon'
df = pd.concat([df_paris, df_lyon], ignore_index=True)

print(f"✅ Paris : {df_paris.shape[0]:,} lignes")
print(f"✅ Lyon  : {df_lyon.shape[0]:,} lignes")
print(f"✅ Dataset fusionné : {df.shape[0]:,} lignes")

# =========================
# 2️⃣ Sélection des colonnes
# =========================
# Note : on garde bathrooms_text pour extraire la colonne numérique bathrooms
colonnes_conservees = [
    'id',
    'city',
    'name',
    'host_id',
    'host_name',
    'latitude',
    'longitude',
    'property_type',
    'accommodates',
    'bedrooms',
    'bathrooms_text',
    'amenities',
    'price',
    'availability_365',
    'number_of_reviews',
    'review_scores_rating'
]

# Filtrer uniquement les colonnes présentes dans le CSV
colonnes_disponibles = [c for c in colonnes_conservees if c in df.columns]
df = df[colonnes_disponibles].copy()

print(f"📋 Colonnes conservées : {list(df.columns)}")

# =========================
# 3️⃣ NETTOYAGE DES DONNÉES
# =========================
print("\n" + "=" * 60)
print("🧹 NETTOYAGE DES DONNÉES")
print("=" * 60)

# ---- 1. NETTOYAGE DU PRIX ----
def clean_price(price):
    if pd.isna(price): return np.nan
    price_str = re.sub(r'[€$£, ]', '', str(price))
    try:
        return float(price_str)
    except:
        return np.nan

if 'price' in df.columns:
    df['price'] = df['price'].apply(clean_price)
    print("✅ Prix nettoyés.")

# ---- 2. GÉOLOCALISATION ----
if 'latitude' in df.columns and 'longitude' in df.columns:
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    # Création du champ location pour Elasticsearch (GeoPoint)
    df['location'] = df['latitude'].astype(str) + ',' + df['longitude'].astype(str)
    print("✅ Coordonnées converties et champ 'location' créé.")

# ---- 3. EXTRACTION BATHROOMS ----
if 'bathrooms_text' in df.columns:
    def extract_bathrooms(text):
        if pd.isna(text): return np.nan
        match = re.search(r'(\d+\.?\d*)', str(text))
        if match: return float(match.group(1))
        if 'half' in str(text).lower(): return 0.5
        return np.nan

    df['bathrooms'] = df['bathrooms_text'].apply(extract_bathrooms)
    print("✅ Colonne 'bathrooms' extraite à partir du texte.")

# ---- 4. BEDROOMS & AUTRES NUMÉRIQUES ----
if 'bedrooms' in df.columns:
    df['bedrooms'] = pd.to_numeric(df['bedrooms'], errors='coerce').fillna(df['bedrooms'].median())

cols_to_zero = ['accommodates', 'availability_365', 'number_of_reviews', 'review_scores_rating']
for col in cols_to_zero:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

# =========================
# 4️⃣ SUPPRESSION DOUBLONS ET FINITION
# =========================
initial_count = len(df)
df = df.drop_duplicates(subset='id', keep='first')
print(f"✅ Doublons supprimés : {initial_count - len(df)}")

# =========================
# 5️⃣ GÉNÉRATION DU RAPPORT MARKDOWN
# =========================
md_content = f"""# Analyse des données Airbnb - Paris & Lyon
*Généré automatiquement par le script de nettoyage*

## 1. Métriques générales
- **Total annonces** : {len(df):,}
- **Paris** : {len(df[df['city'] == 'Paris']):,}
- **Lyon** : {len(df[df['city'] == 'Lyon']):,}

## 2. Audit de qualité après nettoyage

| Colonne | Type | Valeurs nulles | Exemple |
|---------|------|---------------|---------|
"""

for col in df.columns:
    null_pct = (df[col].isnull().sum() / len(df)) * 100
    sample = str(df[col].dropna().iloc[0])[:50] if not df[col].dropna().empty else "N/A"
    md_content += f"| `{col}` | {df[col].dtype} | {null_pct:.1f}% | `{sample}` |\n"

with open("analyse_donnees.md", "w", encoding="utf-8") as f:
    f.write(md_content)


print(f"\n🚀 TERMINÉ !")
print(f"📊 Dataset final : {df.shape[0]} lignes x {df.shape[1]} colonnes")
print(f"📄 Rapport créé : {os.path.abspath('analyse_donnees.md')}")
