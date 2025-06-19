import os
from pathlib import Path

import dagster as dg
import duckdb
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Télécharger le lexique de sentiment VADER
nltk.download('vader_lexicon', quiet=True)

@dg.asset(
    description="Analyse le sentiment des tweets sur les produits Jaffle",
    compute_kind="python",
    group_name="default",
    deps=["raw_tweets", "products"],
    required_resource_keys={"duckdb"}  # On déclare qu'on a besoin de la ressource duckdb
)
def product_sentiment_scores(context: dg.AssetExecutionContext):
    """
    Analyse le sentiment des tweets mentionnant des produits Jaffle.
    """
    # Initialiser l'analyseur de sentiment
    sia = SentimentIntensityAnalyzer()
    
    # Créer une fonction SQL pour calculer le sentiment
    def get_sentiment_score(text: str) -> float:
        if not text:
            return 0.0
        scores = sia.polarity_scores(text)
        return scores['compound']  # Le score composé va de -1 (négatif) à 1 (positif)
    
    # Utiliser la ressource DuckDB
    con = context.resources.duckdb
    
    # Enregistrer la fonction de sentiment
    con.create_function('sentiment_score', get_sentiment_score)
    
    context.log.info("Analyzing tweets sentiment")
    
    result = con.execute("""
        WITH product_mentions AS (
            SELECT 
                t.*,
                p.product_name,
                sentiment_score(t.content) as sentiment_score
            FROM main.raw_tweets t
            CROSS JOIN (SELECT DISTINCT product_name FROM main.products) p
            WHERE LOWER(t.content) LIKE '%' || LOWER(p.product_name) || '%'
        )
        SELECT 
            product_name,
            AVG(sentiment_score) as avg_sentiment,
            COUNT(*) as mention_count,
            STDDEV(sentiment_score) as sentiment_std,
            MIN(tweeted_at) as first_mention,
            MAX(tweeted_at) as last_mention,
            CASE 
                WHEN AVG(sentiment_score) <= -0.5 THEN 'Very Negative'
                WHEN AVG(sentiment_score) <= -0.1 THEN 'Negative'
                WHEN AVG(sentiment_score) <= 0.1 THEN 'Neutral'
                WHEN AVG(sentiment_score) <= 0.5 THEN 'Positive'
                ELSE 'Very Positive'
            END as sentiment_category
        FROM product_mentions
        GROUP BY product_name
        ORDER BY avg_sentiment DESC
    """)
    
    context.log.info("Analysis complete")
    return result.df()