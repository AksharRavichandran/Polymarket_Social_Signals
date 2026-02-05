# Momentum Analysis Notebook

This notebook provides comprehensive momentum analysis for Polymarket markets, focusing on belief dynamics rather than simple price movements.

## Features

### 1. Directional Dominance
- Identifies one-sided markets
- Calculates price entropy (low entropy = near-certain outcomes)
- Measures % time in high certainty ranges

### 2. Outcome Predictability
- Calibration curves
- Brier scores
- Identifies "obvious early" markets

### 3. Momentum Persistence
- Price momentum analysis
- Volume momentum
- Trader count momentum
- Tests for mean reversion vs momentum continuation

### 4. Early vs Late Traders
- Compares PnL across trading phases
- Identifies informational vs herd behavior

### 5. Correlated/Chained Bets
- Finds semantically similar markets
- Tracks belief migration across related markets

### 6. YES/NO Belief Dynamics
- Analyzes which side is favored
- Measures belief strength
- Tracks belief flips
- Calculates belief velocity

## Usage

### Prerequisites

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Collect data:
   - Market metadata: `data/polymarket_data/markets.csv`
   - Trades: `data/polymarket_data/processed/trades.csv`
   - Raw fills: `data/polymarket_data/goldsky/orderFilled.csv`

### Running the Notebook

1. Open `notebooks/momentum_analysis/momentum_analysis.ipynb` in Jupyter
2. Run cells sequentially
3. The notebook includes sample data generation for demonstration
4. Load the top markets and related data:
```python
from notebooks.momentum_analysis.momentum_analysis import load_top_market_bundle

bundle = load_top_market_bundle(n=500, chunksize=500_000)
bundle.markets.head()
bundle.trades.head()
bundle.order_filled.head()
```

### Data Requirements

- **Required**: `data/polymarket_data/markets.csv`
- **Required**: `data/polymarket_data/processed/trades.csv`
- **Required**: `data/polymarket_data/goldsky/orderFilled.csv`

## Key Insights

The notebook answers:

1. **Which markets are informational vs trivial?**
   - Low entropy = trivial (near-certain outcomes)
   - High entropy + high flip frequency = informational

2. **Where does belief change precede resolution?**
   - Markets with high belief velocity before resolution
   - Early price movements that predict outcomes

3. **Who moves prices first?**
   - Compare early vs late trader PnL
   - Higher early trader PnL = informational traders

4. **When does momentum become noise?**
   - High autocorrelation + low predictive power = noise
   - Mean-reverting markets = momentum is noise

## Notes

- The notebook expects the new CSV scheme described above
- Some analyses still require price history or resolved outcomes (not yet wired to the new scheme)
- All functions are available in `notebooks/momentum_analysis/momentum_analysis.py` for reuse
