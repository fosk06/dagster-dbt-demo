import dagster as dg
import nltk
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer

# Télécharger le lexique de sentiment VADER
nltk.download('vader_lexicon', quiet=True)

@dg.asset(
    description="Analyse le sentiment des tweets sur les produits Jaffle",
    compute_kind="python",
    group_name="default",
    ins={
        "raw_tweets": dg.AssetIn(key=["target", "main", "raw_tweets"]),
        "products": dg.AssetIn(key=["target", "main", "products"]),
    },
    io_manager_key="duckdb_io_manager",
    metadata={"schema": "main"}
)
def product_sentiment_scores(context: dg.AssetExecutionContext, raw_tweets: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """
    Analyse le sentiment des tweets mentionnant des produits Jaffle.
    """
    # Initialiser l'analyseur de sentiment
    sia = SentimentIntensityAnalyzer()
    
    # Fonction pour calculer le sentiment
    def get_sentiment_score(text: str) -> float:
        if not text:
            return 0.0
        scores = sia.polarity_scores(text)
        return scores['compound']  # Le score composé va de -1 (négatif) à 1 (positif)
    
    context.log.info("Analyzing tweets sentiment")
    
    # Calculer les scores de sentiment pour chaque tweet
    raw_tweets['sentiment_score'] = raw_tweets['content'].apply(get_sentiment_score)
    
    # Obtenir la liste unique des produits
    product_names = products['product_name'].unique()
    
    # Préparer la liste des mentions de produits
    product_mentions = []
    for tweet in raw_tweets.to_dict('records'):
        for product in product_names:
            if product.lower() in tweet['content'].lower():
                product_mentions.append({
                    'product_name': product,
                    'sentiment_score': tweet['sentiment_score'],
                    'tweeted_at': tweet['tweeted_at']
                })
    
    # Convertir en DataFrame
    mentions_df = pd.DataFrame(product_mentions)
    
    if len(mentions_df) == 0:
        context.log.warning("No product mentions found in tweets")
        return pd.DataFrame(columns=[
            'product_name', 'avg_sentiment', 'mention_count', 
            'sentiment_std', 'first_mention', 'last_mention', 
            'sentiment_category'
        ])
    
    # Calculer les statistiques par produit
    result = mentions_df.groupby('product_name').agg({
        'sentiment_score': ['mean', 'count', 'std'],
        'tweeted_at': ['min', 'max']
    }).reset_index()
    
    # Nettoyer les noms de colonnes
    result.columns = [
        'product_name',
        'avg_sentiment',
        'mention_count',
        'sentiment_std',
        'first_mention',
        'last_mention'
    ]
    
    # Ajouter la catégorie de sentiment
    result['sentiment_category'] = pd.cut(
        result['avg_sentiment'],
        bins=[-1, -0.5, -0.1, 0.1, 0.5, 1],
        labels=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']
    )
    
    context.log.info(f"Analyzed sentiment for {len(result)} products")
    return result