# Analyse des données Airbnb - Paris & Lyon
*Généré automatiquement par le script de nettoyage*

## 1. Métriques générales
- **Total annonces** : 91,500
- **Paris** : 81,853
- **Lyon** : 9,647

## 2. Audit de qualité après nettoyage

| Colonne | Type | Valeurs nulles | Exemple |
|---------|------|---------------|---------|
| `id` | int64 | 0.0% | `2719440` |
| `city` | object | 0.0% | `Paris` |
| `name` | object | 0.0% | `Nice flat close to Montparnasse` |
| `host_id` | int64 | 0.0% | `13915159` |
| `host_name` | object | 0.0% | `Thibaut` |
| `latitude` | float64 | 0.0% | `48.84997` |
| `longitude` | float64 | 0.0% | `2.31974` |
| `property_type` | object | 0.0% | `Entire rental unit` |
| `accommodates` | int64 | 0.0% | `2` |
| `bedrooms` | float64 | 0.0% | `1.0` |
| `bathrooms_text` | object | 0.1% | `1 bath` |
| `amenities` | object | 0.0% | `["Wifi", "Hot water", "Heating", "Coffee maker", "` |
| `price` | float64 | 94.3% | `118.0` |
| `availability_365` | int64 | 0.0% | `0` |
| `number_of_reviews` | int64 | 0.0% | `79` |
| `review_scores_rating` | float64 | 0.0% | `4.78` |
| `location` | object | 0.0% | `48.84997,2.31974` |
| `bathrooms` | float64 | 0.1% | `1.0` |
