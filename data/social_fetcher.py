import os
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv
import yfinance as yf

load_dotenv()

class SocialFetcher:
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()
        self.cryptopanic_key = os.getenv('CRYPTOPANIC_API_KEY')
        self.newsapi_key = os.getenv('NEWSAPI_KEY')
        
    def fetch_reddit_sentiment(self, subreddit="CryptoCurrency", limit=5):
        """Fetches recent posts from a subreddit and calculates average sentiment"""
        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={limit}"
        headers = {'User-agent': 'CryptoRiskBot 0.1'}
        
        try:
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                return None, []
                
            data = response.json()
            posts = data['data']['children']
            
            sentiment_score = 0
            news_items = []
            
            for post in posts:
                title = post['data']['title']
                vs = self.analyzer.polarity_scores(title)
                sentiment_score += vs['compound']
                news_items.append({"title": title, "score": vs['compound']})
                
            avg_sentiment = sentiment_score / len(posts) if posts else 0
            return avg_sentiment, news_items
            
        except Exception as e:
            return None, []

    def fetch_yfinance_news(self, symbol="BTC-USD"):
        """Yahoo Finance uzerinden basliklari ceker (API Key GEREKTIRMEZ)"""
        try:
            yf_symbol = symbol.replace('/', '-').replace('USDT', 'USD')
            ticker = yf.Ticker(yf_symbol)
            news = ticker.news
            if not news:
                return None, []
                
            sentiment_score = 0
            news_items = []
            
            for item in news[:5]:
                # YFinance v1.5+ uses nested 'content'
                if 'content' in item:
                    title = item['content'].get('title', '')
                else:
                    title = item.get('title', '')
                    
                if title:
                    vs = self.analyzer.polarity_scores(title)
                    sentiment_score += vs['compound']
                    news_items.append({"text": title, "score": vs['compound']})
                    
            avg_sentiment = sentiment_score / len(news_items) if news_items else 0
            return avg_sentiment, news_items
        except Exception as e:
            return None, [{"text": f"YFinance Hatasi: {str(e)}", "score": 0}]

    def fetch_cryptopanic_news(self, limit=5):
        """CryptoPanic uzerinden kripto haberlerini ceker (.env icinde CRYPTOPANIC_API_KEY gerektirir)"""
        if not self.cryptopanic_key:
            return None, [{"text": "CryptoPanic API Key bulunamadi (.env dosyasina CRYPTOPANIC_API_KEY ekleyin).", "score": 0}]
            
        url = f"https://cryptopanic.com/api/v1/posts/?auth_token={self.cryptopanic_key}&kind=news"
        try:
            response = requests.get(url)
            if response.status_code != 200:
                return None, [{"text": "CryptoPanic verisi alinamadi", "score": 0}]
                
            data = response.json()
            posts = data.get('results', [])
            
            sentiment_score = 0
            news_items = []
            
            for post in posts[:limit]:
                title = post.get('title', '')
                if title:
                    vs = self.analyzer.polarity_scores(title)
                    sentiment_score += vs['compound']
                    news_items.append({"text": title, "score": vs['compound']})
                    
            avg_sentiment = sentiment_score / len(news_items) if news_items else 0
            return avg_sentiment, news_items
        except Exception as e:
            return None, [{"text": f"CryptoPanic Hatasi: {str(e)}", "score": 0}]
            
    def fetch_newsapi_news(self, query="bitcoin OR crypto OR economy", limit=5):
        """NewsAPI uzerinden dunya basini haberlerini ceker (.env icinde NEWSAPI_KEY gerektirir)"""
        if not self.newsapi_key:
            return None, [{"text": "NewsAPI Key bulunamadi (.env dosyasina NEWSAPI_KEY ekleyin).", "score": 0}]
            
        url = f"https://newsapi.org/v2/everything?q={query}&sortBy=publishedAt&apiKey={self.newsapi_key}"
        try:
            response = requests.get(url)
            if response.status_code != 200:
                return None, [{"text": "NewsAPI verisi alinamadi", "score": 0}]
                
            data = response.json()
            articles = data.get('articles', [])
            
            sentiment_score = 0
            news_items = []
            
            for article in articles[:limit]:
                title = article.get('title', '')
                if title:
                    vs = self.analyzer.polarity_scores(title)
                    sentiment_score += vs['compound']
                    news_items.append({"text": title, "score": vs['compound']})
                    
            avg_sentiment = sentiment_score / len(news_items) if news_items else 0
            return avg_sentiment, news_items
        except Exception as e:
            return None, [{"text": f"NewsAPI Hatasi: {str(e)}", "score": 0}]
