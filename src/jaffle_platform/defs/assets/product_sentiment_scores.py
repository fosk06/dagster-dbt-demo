from typing import Dict, List
import os

import dagster as dg
import nltk
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer
from pathlib import Path

# Télécharger le lexique de sentiment VADER
nltk.download('vader_lexicon', quiet=True)

@dg.asset(
    ins={"products": dg.AssetIn(key_prefix=["target", "main"])},
    description="Analyse le sentiment des tweets sur les produits Jaffle",
    compute_kind="python",
    group_name="datamarts",
)
def product_sentiment_scores(context: dg.AssetExecutionContext, products: pd.DataFrame) -> pd.DataFrame:
    """
    Analyse le sentiment des tweets mentionnant des produits Jaffle et calcule un score moyen par produit.
    Le score est calculé avec VADER (Valence Aware Dictionary and sEntiment Reasoner) et va de -1 (très négatif) à 1 (très positif).
    """
    # Obtenir le chemin de base du projet
    project_root = Path(context.run_config.get("project_root", os.getcwd()))

    # Charger les tweets
    tweets_path = project_root / "jaffle-data" / "raw_tweets.csv"
    context.log.info(f"Loading tweets from {tweets_path}")
    tweets_df = pd.read_csv(tweets_path)

    # Utiliser le DataFrame products fourni par l'IO manager
    product_names = products['product_name'].tolist()

    # Initialiser l'analyseur de sentiment
    sia = SentimentIntensityAnalyzer()

    # Fonction pour extraire les produits mentionnés dans un tweet
    def extract_products(text: str, product_list: List[str]) -> List[str]:
        return [p for p in product_list if p.lower() in text.lower()]

    # Fonction pour calculer le score de sentiment
    def get_sentiment_score(text: str) -> float:
        scores = sia.polarity_scores(text)
        return scores['compound']  # Le score composé va de -1 (négatif) à 1 (positif)

    # Ajouter les colonnes de sentiment et produits mentionnés
    context.log.info(f"Analyzing sentiment for {len(tweets_df)} tweets")
    tweets_df['sentiment_score'] = tweets_df['content'].apply(get_sentiment_score)
    tweets_df['mentioned_products'] = tweets_df['content'].apply(
        lambda x: extract_products(x, product_names)
    )

    # Exploser la colonne des produits mentionnés pour avoir une ligne par produit
    tweets_with_products = tweets_df.explode('mentioned_products')

    # Calculer les statistiques de sentiment par produit
    product_sentiment = tweets_with_products.groupby('mentioned_products').agg({
        'sentiment_score': ['mean', 'count', 'std'],
        'tweeted_at': ['min', 'max']
    }).reset_index()

    # Nettoyer les noms de colonnes
    product_sentiment.columns = [
        'product_name',
        'avg_sentiment',
        'mention_count',
        'sentiment_std',
        'first_mention',
        'last_mention'
    ]

    # Ajouter des colonnes dérivées utiles
    product_sentiment['sentiment_category'] = pd.cut(
        product_sentiment['avg_sentiment'],
        bins=[-1, -0.5, -0.1, 0.1, 0.5, 1],
        labels=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']
    )

    context.log.info(f"Analyzed sentiment for {len(product_sentiment)} products")
    return product_sentiment 