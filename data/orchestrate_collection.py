"""
Main Orchestrator for Data Collection Pipeline

Coordinates collection across Polymarket, Reddit, and Twitter
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import argparse

# Import collectors - use relative imports if running from data/ directory
# or absolute imports if running from project root
import sys
from pathlib import Path

# Add data directory to path for imports
data_dir = Path(__file__).parent
if str(data_dir) not in sys.path:
    sys.path.insert(0, str(data_dir))

from collect_polymarket import PolymarketCollector
from collect_reddit import RedditCollector
from collect_twitter import TwitterCollector

class DataCollectionOrchestrator:
    """
    Orchestrates data collection across all sources
    """
    
    def __init__(self, config_path: str = "data/config.json"):
        self.config_path = Path(config_path)
        self.load_config()
        
        # Initialize collectors
        self.polymarket_collector = PolymarketCollector(
            output_dir=self.config['polymarket']['output_dir']
        )
        
        self.reddit_collector = RedditCollector(
            output_dir=self.config['reddit']['output_dir'],
            pushshift_dump_dir=self.config['reddit'].get('pushshift_dump_dir'),
            praw_config=self.config['reddit'].get('praw') if self.config['reddit'].get('praw', {}).get('client_id') else None
        )
        
        self.twitter_collector = TwitterCollector(
            output_dir=self.config['twitter']['output_dir'],
            dataset_dir=self.config['twitter'].get('dataset_dir'),
            api_config=self.config['twitter'].get('api') if self.config['twitter'].get('api', {}).get('consumer_key') else None
        )
    
    def load_config(self):
        """Load configuration from JSON file"""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {self.config_path}\n"
                f"Copy data/config_example.json to data/config.json and fill in your settings."
            )
        
        with open(self.config_path, 'r') as f:
            self.config = json.load(f)
    
    def step1_collect_polymarket_markets(self, max_markets: Optional[int] = None) -> pd.DataFrame:
        """
        Step 1: Collect Polymarket markets
        
        Returns:
            DataFrame with market information and query sets
        """
        print("=" * 60)
        print("STEP 1: Collecting Polymarket Markets")
        print("=" * 60)
        
        max_markets = max_markets or self.config['polymarket'].get('max_markets')
        markets_df = self.polymarket_collector.collect_all_markets(
            max_markets=max_markets,
            save_raw=True,
            save_processed=True
        )
        
        print(f"\n✓ Collected {len(markets_df)} markets")
        return markets_df
    
    def step2_load_query_sets(self) -> List[Dict]:
        """Load query sets from Polymarket collection"""
        query_sets_path = Path(self.config['polymarket']['output_dir']) / "query_sets.json"
        
        if not query_sets_path.exists():
            raise FileNotFoundError(
                f"Query sets not found: {query_sets_path}\n"
                "Run step1_collect_polymarket_markets() first."
            )
        
        with open(query_sets_path, 'r') as f:
            query_sets = json.load(f)
        
        return query_sets
    
    def step3_collect_social_media(self,
                                   markets_df: pd.DataFrame,
                                   query_sets: List[Dict],
                                   markets_to_process: Optional[List[str]] = None,
                                   reddit_enabled: bool = True,
                                   twitter_enabled: bool = True) -> Dict:
        """
        Step 3: Collect social media data for each market
        
        Args:
            markets_df: DataFrame with market information
            query_sets: List of query sets from step 2
            markets_to_process: List of market IDs to process (None = all)
            reddit_enabled: Whether to collect Reddit data
            twitter_enabled: Whether to collect Twitter data
        """
        print("\n" + "=" * 60)
        print("STEP 3: Collecting Social Media Data")
        print("=" * 60)
        
        # Create query set lookup
        query_set_lookup = {qs['market_id']: qs for qs in query_sets}
        
        # Filter markets if specified
        if markets_to_process:
            markets_df = markets_df[markets_df['market_id'].isin(markets_to_process)]
        
        collection_config = self.config['collection']
        results = {'markets_processed': 0, 'reddit_posts': 0, 'twitter_tweets': 0}
        
        for idx, market_row in markets_df.iterrows():
            market_id = market_row['market_id']
            query_set = query_set_lookup.get(market_id, {})
            
            if not query_set:
                print(f"⚠ Skipping market {market_id}: no query set found")
                continue
            
            print(f"\nProcessing market: {market_id}")
            print(f"  Title: {market_row.get('title', 'N/A')}")
            
            # Determine time window
            end_date_str = market_row.get('end_date', '')
            created_at_str = market_row.get('created_at', '')
            
            try:
                if end_date_str:
                    end_date = pd.to_datetime(end_date_str)
                else:
                    end_date = datetime.now()
                
                if created_at_str:
                    created_at = pd.to_datetime(created_at_str)
                else:
                    created_at = end_date - timedelta(days=30)
                
                # Collection window
                window_before = collection_config.get('time_window_days_before', 30)
                window_after = collection_config.get('time_window_days_after', 7)
                
                start_date = created_at - timedelta(days=window_before)
                collection_end_date = end_date + timedelta(days=window_after)
                
                print(f"  Date range: {start_date.date()} to {collection_end_date.date()}")
                
            except Exception as e:
                print(f"  ⚠ Error parsing dates: {e}")
                continue
            
            # Collect Reddit data
            if reddit_enabled:
                try:
                    subreddits = self.config['reddit'].get('preferred_subreddits', [])
                    pushshift_files = None  # User should specify these in config
                    
                    reddit_df = self.reddit_collector.collect_for_market(
                        market_id=market_id,
                        query_set=query_set,
                        start_date=start_date,
                        end_date=collection_end_date,
                        subreddits=subreddits,
                        pushshift_files=pushshift_files
                    )
                    
                    if not reddit_df.empty:
                        results['reddit_posts'] += len(reddit_df)
                        print(f"  ✓ Collected {len(reddit_df)} Reddit posts")
                
                except Exception as e:
                    print(f"  ⚠ Error collecting Reddit data: {e}")
            
            # Collect Twitter data
            if twitter_enabled:
                try:
                    dataset_files = None  # User should specify these in config
                    
                    twitter_df = self.twitter_collector.collect_for_market(
                        market_id=market_id,
                        query_set=query_set,
                        start_date=start_date,
                        end_date=collection_end_date,
                        dataset_files=dataset_files
                    )
                    
                    if not twitter_df.empty:
                        results['twitter_tweets'] += len(twitter_df)
                        print(f"  ✓ Collected {len(twitter_df)} Twitter tweets")
                
                except Exception as e:
                    print(f"  ⚠ Error collecting Twitter data: {e}")
            
            results['markets_processed'] += 1
        
        print("\n" + "=" * 60)
        print("Collection Summary:")
        print(f"  Markets processed: {results['markets_processed']}")
        print(f"  Reddit posts: {results['reddit_posts']}")
        print(f"  Twitter tweets: {results['twitter_tweets']}")
        print("=" * 60)
        
        return results
    
    def run_full_pipeline(self,
                         max_markets: Optional[int] = None,
                         markets_to_process: Optional[List[str]] = None,
                         reddit_enabled: bool = True,
                         twitter_enabled: bool = True):
        """
        Run the complete data collection pipeline
        """
        print("\n" + "=" * 60)
        print("POLYMARKET SOCIAL SIGNALS - DATA COLLECTION PIPELINE")
        print("=" * 60)
        
        # Step 1: Collect Polymarket markets
        markets_df = self.step1_collect_polymarket_markets(max_markets=max_markets)
        
        # Step 2: Load query sets
        query_sets = self.step2_load_query_sets()
        
        # Step 3: Collect social media data
        results = self.step3_collect_social_media(
            markets_df=markets_df,
            query_sets=query_sets,
            markets_to_process=markets_to_process,
            reddit_enabled=reddit_enabled,
            twitter_enabled=twitter_enabled
        )
        
        print("\n✓ Pipeline complete!")
        return results

def main():
    parser = argparse.ArgumentParser(description='Data Collection Pipeline')
    parser.add_argument('--config', type=str, default='data/config.json',
                        help='Path to config file')
    parser.add_argument('--max-markets', type=int, default=None,
                        help='Maximum number of markets to collect')
    parser.add_argument('--no-reddit', action='store_true',
                        help='Skip Reddit collection')
    parser.add_argument('--no-twitter', action='store_true',
                        help='Skip Twitter collection')
    
    args = parser.parse_args()
    
    orchestrator = DataCollectionOrchestrator(config_path=args.config)
    
    orchestrator.run_full_pipeline(
        max_markets=args.max_markets,
        reddit_enabled=not args.no_reddit,
        twitter_enabled=not args.no_twitter
    )

if __name__ == "__main__":
    main()
