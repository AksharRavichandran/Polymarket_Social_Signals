"""
Twitter/X Data Collection Pipeline

Collects tweets from public datasets or APIs
Supports multiple data sources:
- Academic datasets (Twitter Archive, HuggingFace)
- Public datasets (Twitter has made available)
- Note: Twitter API v2 requires authentication
"""

import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
import os
from pathlib import Path
import gzip

try:
    import tweepy
    TWEEPY_AVAILABLE = True
except ImportError:
    TWEEPY_AVAILABLE = False
    print("Warning: tweepy not available. Install with: pip install tweepy")

class TwitterCollector:
    """
    Collects Twitter/X data from public datasets or APIs
    """
    
    def __init__(self, 
                 output_dir: str = "data/twitter",
                 dataset_dir: Optional[str] = None,
                 api_config: Optional[Dict] = None):
        """
        Args:
            output_dir: Directory to save collected data
            dataset_dir: Path to Twitter dataset files
            api_config: Dict with API credentials if using Twitter API
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.dataset_dir = dataset_dir
        
        # Initialize API if config provided
        self.api = None
        if api_config and TWEEPY_AVAILABLE:
            auth = tweepy.OAuth1UserHandler(
                api_config.get('consumer_key', ''),
                api_config.get('consumer_secret', ''),
                api_config.get('access_token', ''),
                api_config.get('access_token_secret', '')
            )
            self.api = tweepy.API(auth, wait_on_rate_limit=True)
    
    def load_from_dataset(self, dataset_file: str) -> pd.DataFrame:
        """
        Load tweets from a dataset file (JSON, JSONL, CSV)
        
        Supports common formats:
        - JSONL (one JSON per line)
        - JSON array
        - CSV with standard columns
        """
        dataset_path = Path(dataset_file)
        
        # Determine compression
        opener = open
        if dataset_path.suffix == '.gz':
            opener = gzip.open
        
        tweets = []
        
        print(f"Loading dataset: {dataset_file}")
        
        # Try JSONL first
        if dataset_path.suffix in ['.jsonl', '.json']:
            with opener(dataset_path, 'rt', encoding='utf-8') as f:
                for line_num, line in enumerate(f):
                    if line_num % 100000 == 0:
                        print(f"  Processed {line_num} lines...")
                    
                    try:
                        tweet = json.loads(line.strip())
                        tweets.append(tweet)
                    except json.JSONDecodeError:
                        continue
        elif dataset_path.suffix == '.csv':
            # Load CSV directly
            df = pd.read_csv(dataset_path)
            return df
        
        return pd.DataFrame(tweets)
    
    def normalize_tweet_data(self, tweet: Dict) -> Dict:
        """
        Normalize tweet data from different sources
        
        Handles variations in field names across datasets
        """
        normalized = {}
        
        # Handle different tweet structures
        if 'full_text' in tweet:
            normalized['text'] = tweet['full_text']
        elif 'text' in tweet:
            normalized['text'] = tweet['text']
        elif 'tweet' in tweet:
            normalized['text'] = tweet['tweet']
        
        # Handle timestamps
        if 'created_at' in tweet:
            normalized['created_at'] = tweet['created_at']
        elif 'timestamp' in tweet:
            normalized['created_at'] = tweet['timestamp']
        elif 'date' in tweet:
            normalized['created_at'] = tweet['date']
        
        # Handle IDs
        if 'id_str' in tweet:
            normalized['tweet_id'] = tweet['id_str']
        elif 'id' in tweet:
            normalized['tweet_id'] = str(tweet['id'])
        
        # Handle user info
        if 'user' in tweet and isinstance(tweet['user'], dict):
            normalized['username'] = tweet['user'].get('screen_name', tweet['user'].get('username', ''))
            normalized['user_id'] = tweet['user'].get('id_str', tweet['user'].get('id', ''))
            normalized['verified'] = tweet['user'].get('verified', False)
            normalized['followers_count'] = tweet['user'].get('followers_count', 0)
        
        # Handle engagement metrics
        normalized['retweet_count'] = tweet.get('retweet_count', tweet.get('retweets', 0))
        normalized['like_count'] = tweet.get('favorite_count', tweet.get('likes', tweet.get('like_count', 0)))
        normalized['reply_count'] = tweet.get('reply_count', 0)
        normalized['quote_count'] = tweet.get('quote_count', 0)
        
        # Handle hashtags
        if 'entities' in tweet and 'hashtags' in tweet['entities']:
            normalized['hashtags'] = [h.get('text', '') for h in tweet['entities']['hashtags']]
        elif 'hashtags' in tweet:
            normalized['hashtags'] = tweet['hashtags']
        
        # Handle mentions
        if 'entities' in tweet and 'user_mentions' in tweet['entities']:
            normalized['mentions'] = [m.get('screen_name', '') for m in tweet['entities']['user_mentions']]
        
        # Preserve original fields
        normalized['raw_data'] = json.dumps(tweet)
        
        return normalized
    
    def filter_by_keywords(self, df: pd.DataFrame, keywords: List[str]) -> pd.DataFrame:
        """Filter dataframe by keywords in text"""
        if 'text' not in df.columns:
            return df
        
        keyword_mask = pd.Series([False] * len(df))
        
        for keyword in keywords:
            keyword_mask |= df['text'].str.contains(keyword, case=False, na=False)
        
        return df[keyword_mask]
    
    def filter_by_date_range(self, df: pd.DataFrame,
                            start_date: datetime,
                            end_date: datetime) -> pd.DataFrame:
        """Filter dataframe by date range"""
        if 'created_at' not in df.columns:
            return df
        
        # Try to parse dates
        try:
            df['created_dt'] = pd.to_datetime(df['created_at'])
        except:
            return df
        
        mask = (df['created_dt'] >= start_date) & (df['created_dt'] <= end_date)
        return df[mask].drop('created_dt', axis=1)
    
    def filter_by_hashtags(self, df: pd.DataFrame, hashtags: List[str]) -> pd.DataFrame:
        """Filter dataframe by hashtags"""
        if 'hashtags' not in df.columns:
            return df
        
        # Handle list of hashtags vs string
        def contains_hashtag(hashtag_list, target_hashtags):
            if isinstance(hashtag_list, list):
                return any(any(t.lower() in str(h).lower() for h in hashtag_list) for t in target_hashtags)
            return False
        
        hashtag_mask = df['hashtags'].apply(
            lambda x: contains_hashtag(x, [h.lower().lstrip('#') for h in hashtags])
        )
        
        return df[hashtag_mask]
    
    def collect_from_api(self,
                        query: str,
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None,
                        max_tweets: int = 1000) -> pd.DataFrame:
        """
        Collect tweets using Twitter API (requires authentication)
        
        Args:
            query: Search query
            start_date: Start date for search
            end_date: End date for search
            max_tweets: Maximum tweets to collect
        """
        if not self.api:
            raise ValueError("Twitter API not initialized. Provide api_config.")
        
        tweets = []
        
        # Format date for API
        since = start_date.strftime('%Y-%m-%d') if start_date else None
        until = end_date.strftime('%Y-%m-%d') if end_date else None
        
        try:
            for tweet in tweepy.Cursor(
                self.api.search_tweets,
                q=query,
                lang='en',
                tweet_mode='extended',
                since_id=None
            ).items(max_tweets):
                
                normalized = self.normalize_tweet_data(tweet._json)
                tweets.append(normalized)
                
        except Exception as e:
            print(f"Error collecting tweets: {e}")
        
        return pd.DataFrame(tweets)
    
    def collect_for_market(self,
                          market_id: str,
                          query_set: Dict,
                          start_date: datetime,
                          end_date: datetime,
                          dataset_files: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Collect Twitter posts for a specific Polymarket market
        
        Args:
            market_id: Polymarket market ID
            query_set: Dict with 'primary_queries', 'hashtags', 'key_phrases'
            start_date: Start of collection window
            end_date: End of collection window
            dataset_files: List of dataset files to search
        """
        print(f"Collecting Twitter data for market {market_id}")
        
        # Build keyword list
        keywords = []
        if 'primary_queries' in query_set:
            keywords.extend(query_set['primary_queries'])
        if 'key_phrases' in query_set:
            keywords.extend(query_set['key_phrases'][:5])
        
        hashtags = query_set.get('hashtags', [])
        
        all_tweets = []
        
        # Method 1: Load from dataset files (preferred)
        if dataset_files:
            for dataset_file in dataset_files:
                # Load dataset
                df = self.load_from_dataset(dataset_file)
                
                # Normalize data
                if not df.empty:
                    normalized_tweets = df.apply(
                        lambda row: self.normalize_tweet_data(row.to_dict()),
                        axis=1
                    )
                    df = pd.DataFrame(list(normalized_tweets))
                    
                    # Filter by date
                    df = self.filter_by_date_range(df, start_date, end_date)
                    
                    # Filter by keywords
                    if keywords:
                        df = self.filter_by_keywords(df, keywords)
                    
                    # Filter by hashtags
                    if hashtags:
                        df = self.filter_by_hashtags(df, hashtags)
                    
                    all_tweets.append(df)
        
        # Method 2: Use Twitter API (requires auth, limited)
        elif self.api and keywords:
            for keyword in keywords[:3]:  # Limit to top 3
                try:
                    df = self.collect_from_api(
                        query=keyword,
                        start_date=start_date,
                        end_date=end_date,
                        max_tweets=100
                    )
                    all_tweets.append(df)
                except Exception as e:
                    print(f"Error collecting via API for '{keyword}': {e}")
        
        # Combine all tweets
        if all_tweets:
            combined_df = pd.concat(all_tweets, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['tweet_id'])
            
            # Save to file
            output_path = self.output_dir / f"market_{market_id}_twitter.csv"
            combined_df.to_csv(output_path, index=False)
            
            print(f"Collected {len(combined_df)} tweets, saved to {output_path}")
            return combined_df
        
        return pd.DataFrame()

def main():
    """Example usage"""
    collector = TwitterCollector(
        dataset_dir="path/to/twitter/datasets"
    )
    
    print("Twitter collector ready. Configure with dataset files or API credentials.")
    print("\nRecommended public datasets:")
    print("  - Twitter Academic Research datasets")
    print("  - HuggingFace Twitter datasets")
    print("  - Public Twitter archives (various formats)")

if __name__ == "__main__":
    main()
