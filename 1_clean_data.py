import pandas as pd

paris = pd.read_csv("listings_paris.csv")
lyon = pd.read_csv("listings_lyon.csv")

paris["target_city"] = "Paris"
lyon["target_city"] = "Lyon"

df = pd.concat([paris, lyon], ignore_index=True)

cols = [
    "id", "name", "host_id", "host_name",
    "latitude", "longitude", "property_type",
    "accommodates", "bathrooms_text", "bedrooms",
    "amenities", "price", "availability_365",
    "number_of_reviews", "review_scores_rating",
    "target_city"
]
df = df[cols]

# Nettoyage price
df["price"] = (
    df["price"]
    .astype(str)
    .str.replace("$", "", regex=False)
    .str.replace(",", "", regex=False)
)
df["price"] = pd.to_numeric(df["price"], errors="coerce")

# Bathrooms
df["bathrooms"] = (
    df["bathrooms_text"]
    .astype(str)
    .str.extract(r"([\d\.]+)")
)
df["bathrooms"] = pd.to_numeric(df["bathrooms"], errors="coerce")

df.drop(columns=["bathrooms_text"], inplace=True)

# Geo point PROPRE
df["location"] = df.apply(
    lambda row: {"lat": float(row["latitude"]), "lon": float(row["longitude"])},
    axis=1
)

df.drop(columns=["latitude", "longitude"], inplace=True)

# ⚠️ SUPPRESSION DES NaN (CRUCIAL)
df = df.dropna(subset=[
    "price", "bathrooms", "bedrooms",
    "review_scores_rating", "location"
])

# Sécuriser amenities
df["amenities"] = df["amenities"].fillna("")

# Export
df.to_parquet("airbnb_clean.parquet", index=False)

print("✅ Nettoyage robuste terminé")
