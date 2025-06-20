import dagster as dg
import pandas as pd
import duckdb
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

nltk.download('vader_lexicon', quiet=True)

@dg.asset(
    description="Analyse le sentiment des tweets sur les produits Jaffle (connexion directe DuckDB)",
    compute_kind="python",
    group_name="default",
    deps=[dg.AssetKey(["target", "main", "raw_tweets"]), dg.AssetKey(["target", "main", "products"])],
)
def product_sentiment_scores(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Analyse le sentiment des tweets mentionnant des produits Jaffle et calcule un score moyen par produit.
    """
    # Connexion à DuckDB
    conn = duckdb.connect('/tmp/jaffle_platform.duckdb')
    raw_tweets = conn.execute("SELECT * FROM main.raw_tweets").df()
    products = conn.execute("SELECT * FROM main.products").df()
    conn.close()

    sia = SentimentIntensityAnalyzer()
    product_names = products['product_name'].unique()

    def extract_products(text: str, product_list):
        return [p for p in product_list if p.lower() in text.lower()]

    def get_sentiment_score(text: str) -> float:
        scores = sia.polarity_scores(text)
        return scores['compound']

    raw_tweets['sentiment_score'] = raw_tweets['content'].apply(get_sentiment_score)
    raw_tweets['mentioned_products'] = raw_tweets['content'].apply(lambda x: extract_products(x, product_names))
    tweets_with_products = raw_tweets.explode('mentioned_products')

    product_sentiment = tweets_with_products.groupby('mentioned_products').agg({
        'sentiment_score': ['mean', 'count', 'std'],
        'tweeted_at': ['min', 'max']
    }).reset_index()

    product_sentiment.columns = [
        'product_name',
        'avg_sentiment',
        'mention_count',
        'sentiment_std',
        'first_mention',
        'last_mention'
    ]

    product_sentiment['sentiment_category'] = pd.cut(
        product_sentiment['avg_sentiment'],
        bins=[-1, -0.5, -0.1, 0.1, 0.5, 1],
        labels=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']
    )

    context.log.info(f"Sentiment calculé pour {len(product_sentiment)} produits.")
    return product_sentiment 