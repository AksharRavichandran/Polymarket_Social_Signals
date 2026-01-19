# Polymarket Social Signals

Research project investigating whether social media narratives (X, Reddit) contain predictive signal for prediction market dynamics (Polymarket), and whether that signal transfers to traditional financial markets.

## Research Questions

**Primary Question:**
Do social media narratives (X, Reddit) contain predictive signal for prediction market dynamics (Polymarket), and does that signal transfer to traditional financial markets?

**Sub-questions:**
1. Do changes in social media sentiment precede Polymarket price movements?
2. Are topics/narratives more predictive than raw sentiment?
3. Does Polymarket incorporate information faster than equity markets?
4. Can Polymarket probabilities predict abnormal returns or volatility in related equities?

## Conceptual Framework

Three-layer causal pipeline:

```
Social Media Discourse (X, Reddit)
  ↓
LLM-based Signal Extraction (sentiment, stance, narratives, uncertainty)
  ↓
Prediction Market Response (Polymarket prices, volume)
  ↓
Traditional Market Reaction (stocks, options, volatility)
```

## Repository Structure

```
polymarket-social-signals/
│
├── data/
│   ├── polymarket/      # Polymarket market data
│   ├── twitter/         # Twitter/X collected data
│   ├── reddit/          # Reddit collected data
│   └── collect_*.py     # Data collection scripts
│
├── labeling/
│   └── prompts/         # LLM prompts for signal extraction
│
├── features/
│   ├── sentiment.py     # Sentiment extraction
│   └── narratives.py    # Narrative clustering
│
├── models/
│   ├── lead_lag.py      # Lead-lag analysis
│   └── event_study.py   # Event study models
│
├── backtests/           # Trading strategy backtests
├── notebooks/           # Analysis notebooks
├── figures/             # Generated figures
└── paper/               # Paper materials
```

## Quick Start

### 1. Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure settings
cp data/config_example.json data/config.json
# Edit data/config.json with your API keys and paths
```

### 2. Data Collection

Run the full data collection pipeline:

```bash
python data/orchestrate_collection.py
```

Or run individual collectors:

```bash
# Polymarket only
python data/collect_polymarket.py

# See data/README.md for more details
```

### 3. Data Sources

- **Polymarket**: API at `https://clob.polymarket.com`
- **Reddit**: Pushshift dumps (recommended) or PRAW API
- **Twitter/X**: Public datasets or Twitter API

## Data Collection Strategy

### Polymarket (Anchor Dataset)

For each Polymarket market, we extract:
- Market title, description, tags, end date, resolution criteria
- Build query sets: entity names, hashtags, aliases, key phrases

### Social Media Collection

Posts are collected in time windows:
- **Market window**: Market creation → resolution
- **Shock window**: Around big probability moves

This aligns social media data with Polymarket time series for causal/lead-lag analysis.

## Status

🚧 **In Development** - Data collection pipeline is set up. Next steps:
- [x] Data collection infrastructure
- [ ] LLM-based signal extraction
- [ ] Lead-lag analysis
- [ ] Cross-market spillover analysis
- [ ] Paper implementation

## License

Research project - see individual data source licenses for usage restrictions.

## Contributing

This is a research project. For questions or collaborations, please contact the repository maintainer.
