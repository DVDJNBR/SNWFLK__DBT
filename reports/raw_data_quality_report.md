# Rapport de Qualité des Données NYC Taxi

## 📊 Résumé Général
- **Total lignes analysées** : 76,977,173
- **Lignes propres estimées** : 59,487,932 (77.28%)

## ❌ Problèmes Identifiés

### 1. Valeurs Manquantes
- **Nombre** : 12,502,017
- **Pourcentage** : 16.24%

### 2. Montants Négatifs
- **Nombre** : 2,821,289
- **Pourcentage** : 3.67%

### 3. Trajets Distance Zéro
- **Nombre** : 1,791,588
- **Pourcentage** : 2.33%

### 4. Distances Extrêmes (>1000 miles)
- **Nombre** : 2,642
- **Pourcentage** : 0.0%

### 5. Dates Incohérentes
- **Nombre** : 371,705
- **Pourcentage** : 0.48%

## ✅ Actions de Nettoyage Appliquées
- Filtrage des montants négatifs
- Exclusion des trajets avec dates incohérentes  
- Gestion des valeurs manquantes
- Suppression des outliers extrêmes
- Filtrage des distances entre 0.1 et 100 miles

## 📈 Résultat Final
Après nettoyage, environ **77.28%** des données sont utilisables pour l'analyse.
