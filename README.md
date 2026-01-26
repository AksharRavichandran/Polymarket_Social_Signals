# Polymarket Momentum Analysis

Research project analyzing belief momentum and price dynamics on Polymarket prediction markets. This project investigates how market beliefs evolve, which markets exhibit informational vs trivial dynamics, and how momentum patterns relate to market outcomes.

## Research Questions

**Primary Questions:**
1. Which markets are informational vs trivial (one-sided)?
2. Where does belief change precede resolution?
3. Who moves prices first - informational traders or herd behavior?
4. When does momentum become noise vs predictive signal?

**Key Analyses:**
- Directional dominance (one-sided markets)
- Outcome predictability vs market belief
- Momentum persistence (price, volume, trader count)
- Early vs late trader behavior
- Correlated/chained bets across markets
- YES/NO belief dynamics

## Analysis Framework

The momentum analysis focuses on belief dynamics rather than simple price movements:

```
Market Creation
  ↓
Price Evolution (entropy, momentum, flips)
  ↓
Trader Behavior (early vs late, PnL analysis)
  ↓
Market Resolution
  ↓
Outcome Predictability (calibration, Brier scores)
```

**Key Metrics:**
- **Price Entropy**: Measures uncertainty in price path (low = one-sided, high = contested)
- **Momentum**: Price/volume/trader count persistence over time
- **Belief Velocity**: Rate of belief change before resolution
- **Calibration**: How well market probabilities predict actual outcomes

## Repository Structure

```
polymarket-social-signals/
│
├── data/
│   ├── polymarket/           # Polymarket market data
│   │   ├── markets_processed.csv
│   │   ├── query_sets.json
│   │   ├── positions/        # User position data (optional)
│   │   └── price_history/    # Historical price data
│   ├── reddit/               # Reddit data (disabled)
│   ├── twitter/              # Twitter data (disabled)
│   └── collect_*.py          # Data collection scripts
│
├── notebooks/
│   ├── momentum_analysis/    # Momentum analysis module
│   │   ├── momentum_analysis.ipynb  # Main analysis notebook
│   │   ├── momentum_analysis.py    # Analysis functions
│   │   └── README.md               # Analysis documentation
│   └── momentum_analysis.ipynb     # (legacy, use subdirectory)
│
├── features/                 # Feature extraction (future)
├── models/                   # Predictive models (future)
├── backtests/                # Trading strategy backtests (future)
├── figures/                  # Generated figures
├── paper/                   # Paper materials
├── sources/                  # Research papers and sources
├── labeling/                 # Labeling tools and prompts
├── requirements.txt          # Python dependencies
└── README.md                 # This file
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

Collect Polymarket market data:

```bash
# Collect market metadata
python data/collect_polymarket.py

# Collect user positions (optional, requires API key)
python data/collect_polymarket.py positions <user_address>
```

**Data Requirements:**
- **Required**: Market metadata (`markets_processed.csv`)
- **Optional**: User positions (`positions/positions_processed.csv`)
- **Required for full analysis**: Historical price data (to be collected from Polymarket API)

### 3. Run Momentum Analysis

Open and run the Jupyter notebook:

```bash
jupyter notebook notebooks/momentum_analysis/momentum_analysis.ipynb
```

Or use the Python module directly:

```python
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from notebooks.momentum_analysis.momentum_analysis import (
    analyze_directional_dominance,
    analyze_momentum_persistence,
    analyze_early_vs_late_traders,
    # ... other functions
)
```

See `notebooks/momentum_analysis/README.md` for detailed documentation.

## Analysis Components

### 1. Directional Dominance
Identifies one-sided markets and calculates price entropy to distinguish trivial (near-certain) vs informational markets.

### 2. Outcome Predictability
- Calibration curves showing how well market probabilities predict outcomes
- Brier scores for forecast accuracy
- Identifies "obvious early" markets where outcome was clear from the start

### 3. Momentum Persistence
- Price momentum analysis (autocorrelation, mean reversion)
- Volume momentum
- Trader count momentum
- Tests whether momentum continues or reverts

### 4. Early vs Late Traders
- Compares PnL across trading phases
- Identifies informational traders (early, profitable) vs herd behavior (late, following)

### 5. Correlated/Chained Bets
- Finds semantically similar markets using text similarity
- Tracks belief migration across related markets

### 6. YES/NO Belief Dynamics
- Analyzes which side (YES/NO) is favored over time
- Measures belief strength and tracks belief flips
- Calculates belief velocity (rate of change)

## Key Insights

The analysis answers:

1. **Which markets are informational vs trivial?**
   - Low entropy = trivial (near-certain outcomes from start)
   - High entropy + high flip frequency = informational (contested, dynamic)

2. **Where does belief change precede resolution?**
   - Markets with high belief velocity before resolution
   - Early price movements that predict outcomes

3. **Who moves prices first?**
   - Compare early vs late trader PnL
   - Higher early trader PnL = informational traders leading
   - Higher late trader PnL = herd behavior following

4. **When does momentum become noise?**
   - High autocorrelation + low predictive power = noise
   - Mean-reverting markets = momentum is noise, not signal

## Status

🚧 **In Development** - Momentum analysis framework is set up. Current status:
- [x] Market data collection infrastructure
- [x] Momentum analysis functions
- [x] Analysis notebook with sample data
- [ ] Historical price data collection from API
- [ ] Full analysis on real price history
- [ ] Trading strategy backtests
- [ ] Paper implementation

## License

Research project - see individual data source licenses for usage restrictions.

## Contributing

This is a research project. For questions or collaborations, please contact akshar.ravichandran@gmail.com.
