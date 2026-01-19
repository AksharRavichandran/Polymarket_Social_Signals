"""
Polymarket Data Collection Pipeline

Collects market data from Polymarket API including:
- market title, description, tags, end date, resolution criteria
- Builds query sets for social media extraction
"""

import requests
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
import time
import os
from pathlib import Path

class PolymarketCollector:
    """
    Collects market data from Polymarket API
    """
    
    BASE_URL = "https://clob.polymarket.com"
    
    def __init__(self, output_dir: str = "data/polymarket"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Research Tool)'
        })
    
    def get_markets(self, 
                   limit: int = 100,
                   active: Optional[bool] = None,
                   closed: Optional[bool] = None) -> List[Dict]:
        """
        Fetch markets from Polymarket API
        
        Args:
            limit: Number of markets to fetch
            active: Filter for active markets (None = all)
            closed: Filter for closed markets (None = all)
        """
        url = f"{self.BASE_URL}/markets"
        params = {
            'limit': limit
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, list) else data.get('data', [])
        except requests.exceptions.RequestException as e:
            print(f"Error fetching markets: {e}")
            return []
    
    def get_market_details(self, market_id: str) -> Optional[Dict]:
        """
        Get detailed information for a specific market
        """
        url = f"{self.BASE_URL}/markets/{market_id}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching market {market_id}: {e}")
            return None
    
    def extract_market_fields(self, market: Dict) -> Dict:
        """
        Extract key fields from market data
        
        Returns dict with:
        - market_id
        - title
        - description
        - tags
        - end_date
        - resolution_criteria
        - created_at
        - market_type
        - liquidity
        - volume
        """
        extracted = {
            'market_id': market.get('id', market.get('slug', '')),
            'title': market.get('question', market.get('title', '')),
            'description': market.get('description', market.get('subtitle', '')),
            'tags': market.get('tags', []),
            'end_date': market.get('endDate', market.get('end_date_iso', '')),
            'resolution_criteria': market.get('resolutionRules', market.get('resolution_criteria', '')),
            'created_at': market.get('createdAt', market.get('created_at_iso', '')),
            'market_type': market.get('type', market.get('market_type', '')),
            'liquidity': market.get('liquidity', 0),
            'volume': market.get('volume', 0),
            'url': market.get('url', market.get('market_url', '')),
            'outcomes': market.get('outcomes', market.get('tokens', [])),
        }
        
        return extracted
    
    def build_query_set(self, market: Dict) -> Dict:
        """
        Build query set for social media extraction from market data
        
        Extracts:
        - Entity names
        - Key phrases
        - Hashtags
        - Aliases
        """
        title = market.get('title', '')
        description = market.get('description', '')
        tags = market.get('tags', [])
        
        # Basic extraction (can be enhanced with LLM later)
        query_set = {
            'market_id': market.get('market_id', ''),
            'primary_queries': [title],
            'hashtags': tags,
            'key_phrases': self._extract_key_phrases(title, description),
            'aliases': self._extract_aliases(title, description),
        }
        
        return query_set
    
    def _extract_key_phrases(self, title: str, description: str) -> List[str]:
        """Extract key phrases from text (basic implementation)"""
        # This can be enhanced with NLP/LLM
        text = f"{title} {description}".lower()
        # Simple extraction - can be improved
        phrases = []
        # Split into meaningful chunks (simplified)
        words = text.split()
        for i in range(len(words) - 1):
            phrases.append(f"{words[i]} {words[i+1]}")
        return phrases[:10]  # Limit to top 10
    
    def _extract_aliases(self, title: str, description: str) -> List[str]:
        """Extract potential aliases/entity names"""
        # Basic implementation - can use NER or LLM for better results
        aliases = []
        # Look for capitalized words (potential entity names)
        words = title.split()
        aliases.extend([w for w in words if w and w[0].isupper()])
        return list(set(aliases))
    
    def collect_all_markets(self, 
                           max_markets: Optional[int] = None,
                           save_raw: bool = True,
                           save_processed: bool = True) -> pd.DataFrame:
        """
        Collect all markets and process them
        
        Args:
            max_markets: Maximum number of markets to collect (None = all)
            save_raw: Save raw API responses
            save_processed: Save processed dataframe
        """
        print("Fetching markets from Polymarket API...")
        markets = self.get_markets(limit=1000)  # Adjust based on API limits
        
        if max_markets:
            markets = markets[:max_markets]
        
        print(f"Found {len(markets)} markets")
        
        # Save raw data if requested
        if save_raw:
            raw_path = self.output_dir / f"raw_markets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(raw_path, 'w') as f:
                json.dump(markets, f, indent=2)
            print(f"Saved raw data to {raw_path}")
        
        # Process markets
        processed_markets = []
        query_sets = []
        
        for i, market in enumerate(markets):
            if i % 10 == 0:
                print(f"Processing market {i+1}/{len(markets)}")
            
            extracted = self.extract_market_fields(market)
            processed_markets.append(extracted)
            
            query_set = self.build_query_set(extracted)
            query_sets.append(query_set)
            
            # Rate limiting
            time.sleep(0.1)
        
        # Create dataframes
        markets_df = pd.DataFrame(processed_markets)
        queries_df = pd.DataFrame(query_sets)
        
        if save_processed:
            markets_path = self.output_dir / "markets_processed.csv"
            queries_path = self.output_dir / "query_sets.json"
            
            markets_df.to_csv(markets_path, index=False)
            with open(queries_path, 'w') as f:
                json.dump(query_sets, f, indent=2)
            
            print(f"Saved processed data:")
            print(f"  - Markets: {markets_path}")
            print(f"  - Query sets: {queries_path}")
        
        return markets_df

def main():
    """Main collection script"""
    collector = PolymarketCollector()
    
    # Collect markets (adjust max_markets as needed)
    df = collector.collect_all_markets(
        max_markets=None,  # Set to None for all, or a number to limit
        save_raw=True,
        save_processed=True
    )
    
    print(f"\nCollection complete!")
    print(f"Total markets collected: {len(df)}")
    print(f"\nSample markets:")
    print(df[['market_id', 'title', 'end_date']].head())

if __name__ == "__main__":
    main()
