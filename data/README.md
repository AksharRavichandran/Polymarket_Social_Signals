# Data Collection Pipeline

This directory contains the data collection pipeline for the Polymarket Social Signals research project.

## Overview

The pipeline collects data from three sources:
1. **Polymarket** - Market data via API
2. **Reddit** - Posts from Pushshift dumps or PRAW
3. **Twitter/X** - Tweets from public datasets or API

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure settings:**
   ```bash
   cp data/config_example.json data/config.json
   # Edit data/config.json with your settings
   ```

3. **Set up data sources:**
   - **Pushshift dumps**: Download Reddit dumps and specify path in config
   - **Twitter datasets**: Download public Twitter datasets and specify path in config
   - **API credentials**: (Optional) Add API keys if using PRAW or Twitter API

## Usage

### Full Pipeline

Run the complete collection pipeline:

```bash
python data/orchestrate_collection.py
```

### Individual Collectors

#### Polymarket

```python
from data.collect_polymarket import PolymarketCollector

collector = PolymarketCollector()
markets_df = collector.collect_all_markets(max_markets=100)
```

#### Reddit

```python
from data.collect_reddit import RedditCollector

collector = RedditCollector(pushshift_dump_dir="path/to/dumps")
# Or with PRAW
collector = RedditCollector(praw_config={
    'client_id': '...',
    'client_secret': '...',
    'user_agent': '...'
})
```

#### Twitter

```python
from data.collect_twitter import TwitterCollector

collector = TwitterCollector(dataset_dir="path/to/twitter/datasets")
# Or with API
collector = TwitterCollector(api_config={
    'consumer_key': '...',
    'consumer_secret': '...',
    'access_token': '...',
    'access_token_secret': '...'
})
```

## Data Sources

### Polymarket
- **API**: `https://clob.polymarket.com`
- **Fields collected**: market_id, title, description, tags, end_date, resolution_criteria

### Reddit (Pushshift)
- **Source**: Pushshift Reddit dumps
- **Formats**: JSON, JSON.gz, JSON.bz2
- **Recommended**: Download dumps from Pushshift torrents
- **Subreddits**: wallstreetbets, cryptocurrency, politics, stocks, etc.

### Twitter/X
- **Sources**: Public academic datasets, HuggingFace datasets
- **Formats**: JSONL, JSON, CSV
- **Note**: Twitter API v2 requires authentication (paid/research access)

## Output Structure

```
data/
├── polymarket/
│   ├── raw_markets_YYYYMMDD_HHMMSS.json
│   ├── markets_processed.csv
│   └── query_sets.json
├── reddit/
│   └── market_{market_id}_reddit.csv
└── twitter/
    └── market_{market_id}_twitter.csv
```

## Configuration

Edit `data/config.json` to specify:
- Output directories
- API credentials (optional)
- Dataset paths
- Collection time windows
- Preferred subreddits

## Notes

- **Rate Limiting**: The pipeline includes rate limiting for API calls
- **Data Volume**: Pushshift dumps can be large; ensure sufficient disk space
- **Privacy**: Ensure compliance with data usage policies for all sources
