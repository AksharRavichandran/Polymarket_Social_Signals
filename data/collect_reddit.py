"""
Reddit Data Collection Pipeline using Pushshift Dataset

Collects Reddit posts from Pushshift dumps or API
Designed to work with:
- Pushshift dumps (torrent-based distribution)
- Pushshift API (if available)
- PRAW as fallback (limited rate)
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import os
from pathlib import Path
import gzip
import bz2

try:
    import praw
    PRAW_AVAILABLE = True
except ImportError:
    PRAW_AVAILABLE = False
    print("Warning: PRAW not available. Install with: pip install praw")

class RedditCollector:
    """
    Collects Reddit posts from Pushshift dumps or API
    """
    
    def __init__(self, 
                 output_dir: str = "data/reddit",
                 pushshift_dump_dir: Optional[str] = None,
                 praw_config: Optional[Dict] = None):
        """
        Args:
            output_dir: Directory to save collected data
            pushshift_dump_dir: Path to Pushshift dump files (if using dumps)
            praw_config: Dict with 'client_id', 'client_secret', 'user_agent' for PRAW
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.pushshift_dump_dir = pushshift_dump_dir
        
        # Initialize PRAW if config provided
        self.reddit = None
        if praw_config and PRAW_AVAILABLE:
            self.reddit = praw.Reddit(
                client_id=praw_config['client_id'],
                client_secret=praw_config['client_secret'],
                user_agent=praw_config.get('user_agent', 'research_tool')
            )
    
    def load_pushshift_dump(self, dump_file: str, filter_func=None) -> pd.DataFrame:
        """
        Load and parse Pushshift dump file
        
        Supports .json, .json.gz, .json.bz2 formats
        
        Args:
            dump_file: Path to dump file
            filter_func: Optional function to filter posts (returns bool)
        """
        dump_path = Path(dump_file)
        
        # Determine compression
        if dump_path.suffix == '.bz2':
            opener = bz2.open
        elif dump_path.suffix == '.gz':
            opener = gzip.open
        else:
            opener = open
        
        posts = []
        
        print(f"Loading dump file: {dump_file}")
        with opener(dump_path, 'rt', encoding='utf-8') as f:
            for line_num, line in enumerate(f):
                if line_num % 100000 == 0:
                    print(f"  Processed {line_num} lines...")
                
                try:
                    post = json.loads(line)
                    if filter_func is None or filter_func(post):
                        posts.append(post)
                except json.JSONDecodeError:
                    continue
        
        print(f"Loaded {len(posts)} posts")
        return pd.DataFrame(posts)
    
    def filter_by_subreddits(self, df: pd.DataFrame, subreddits: List[str]) -> pd.DataFrame:
        """Filter dataframe by subreddit names"""
        if 'subreddit' in df.columns:
            return df[df['subreddit'].str.lower().isin([s.lower() for s in subreddits])]
        return df
    
    def filter_by_keywords(self, df: pd.DataFrame, keywords: List[str]) -> pd.DataFrame:
        """Filter dataframe by keywords in title/body"""
        if 'title' not in df.columns and 'selftext' not in df.columns:
            return df
        
        keyword_mask = pd.Series([False] * len(df))
        
        for keyword in keywords:
            if 'title' in df.columns:
                keyword_mask |= df['title'].str.contains(keyword, case=False, na=False)
            if 'selftext' in df.columns:
                keyword_mask |= df['selftext'].str.contains(keyword, case=False, na=False)
        
        return df[keyword_mask]
    
    def filter_by_date_range(self, df: pd.DataFrame, 
                            start_date: datetime, 
                            end_date: datetime) -> pd.DataFrame:
        """Filter dataframe by date range"""
        if 'created_utc' not in df.columns:
            return df
        
        # Convert unix timestamp to datetime
        df['created_dt'] = pd.to_datetime(df['created_utc'], unit='s')
        
        mask = (df['created_dt'] >= start_date) & (df['created_dt'] <= end_date)
        return df[mask].drop('created_dt', axis=1)
    
    def collect_from_praw(self, 
                         subreddit: str,
                         query: str,
                         limit: int = 1000,
                         time_filter: str = 'all') -> pd.DataFrame:
        """
        Collect posts using PRAW (fallback, limited rate)
        
        Args:
            subreddit: Subreddit name
            query: Search query
            limit: Max posts to collect
            time_filter: 'all', 'year', 'month', 'week', 'day', 'hour'
        """
        if not self.reddit:
            raise ValueError("PRAW not initialized. Provide praw_config.")
        
        posts = []
        subreddit_obj = self.reddit.subreddit(subreddit)
        
        # Search within subreddit
        for submission in subreddit_obj.search(query, limit=limit, time_filter=time_filter):
            posts.append({
                'id': submission.id,
                'title': submission.title,
                'selftext': submission.selftext,
                'subreddit': submission.subreddit.display_name,
                'created_utc': submission.created_utc,
                'score': submission.score,
                'num_comments': submission.num_comments,
                'url': submission.url,
                'permalink': submission.permalink,
            })
        
        return pd.DataFrame(posts)
    
    def collect_for_market(self,
                          market_id: str,
                          query_set: Dict,
                          start_date: datetime,
                          end_date: datetime,
                          subreddits: Optional[List[str]] = None,
                          pushshift_files: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Collect Reddit posts for a specific Polymarket market
        
        Args:
            market_id: Polymarket market ID
            query_set: Dict with 'primary_queries', 'hashtags', 'key_phrases'
            start_date: Start of collection window
            end_date: End of collection window
            subreddits: List of subreddits to filter (None = all)
            pushshift_files: List of Pushshift dump files to search
        """
        print(f"Collecting Reddit data for market {market_id}")
        
        # Build keyword list
        keywords = []
        if 'primary_queries' in query_set:
            keywords.extend(query_set['primary_queries'])
        if 'key_phrases' in query_set:
            keywords.extend(query_set['key_phrases'][:5])  # Limit to top 5
        
        all_posts = []
        
        # Method 1: Pushshift dumps (preferred)
        if pushshift_files:
            for dump_file in pushshift_files:
                # Load dump
                df = self.load_pushshift_dump(dump_file)
                
                # Filter by date
                df = self.filter_by_date_range(df, start_date, end_date)
                
                # Filter by keywords
                if keywords:
                    df = self.filter_by_keywords(df, keywords)
                
                # Filter by subreddits
                if subreddits:
                    df = self.filter_by_subreddits(df, subreddits)
                
                all_posts.append(df)
        
        # Method 2: PRAW (fallback, limited)
        elif self.reddit and subreddits:
            for subreddit in subreddits:
                for keyword in keywords[:3]:  # Limit to top 3 keywords
                    try:
                        df = self.collect_from_praw(subreddit, keyword, limit=100)
                        all_posts.append(df)
                    except Exception as e:
                        print(f"Error collecting from r/{subreddit}: {e}")
        
        # Combine all posts
        if all_posts:
            combined_df = pd.concat(all_posts, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['id'])
            
            # Save to file
            output_path = self.output_dir / f"market_{market_id}_reddit.csv"
            combined_df.to_csv(output_path, index=False)
            
            print(f"Collected {len(combined_df)} posts, saved to {output_path}")
            return combined_df
        
        return pd.DataFrame()

def main():
    """Example usage"""
    # Example: Collect from Pushshift dump
    collector = RedditCollector(
        pushshift_dump_dir="path/to/pushshift/dumps"
    )
    
    # Example query set from Polymarket market
    query_set = {
        'primary_queries': ['Bitcoin ETF approval'],
        'hashtags': ['BTC', 'crypto'],
        'key_phrases': ['bitcoin etf', 'sec approval']
    }
    
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 31)
    
    # If using Pushshift dumps
    # df = collector.collect_for_market(
    #     market_id="example_market",
    #     query_set=query_set,
    #     start_date=start_date,
    #     end_date=end_date,
    #     subreddits=['wallstreetbets', 'cryptocurrency'],
    #     pushshift_files=["path/to/RS_2024-01.json.bz2"]
    # )
    
    print("Reddit collector ready. Configure with Pushshift dumps or PRAW.")

if __name__ == "__main__":
    main()
