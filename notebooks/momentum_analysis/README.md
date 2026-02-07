# Momentum Analysis Notebook

This notebook provides momentum analysis for Polymarket markets, focusing on belief dynamics derived from price history.

## Features

### 1. Informational vs Trivial Markets
- Price entropy (low entropy = near-certain outcomes)
- Flip frequency across 0.5

### 2. Belief Change Before Resolution
- Belief velocity in the final window

### 3. Early vs Late Price Movers
- PnL proxy from early vs late price change

### 4. Momentum vs Noise
- Return autocorrelation and predictive power
- Momentum persistence vs mean reversion

## Usage

### Prerequisites

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Collect data:
   - Run `python data/collect_polymarket.py`
   - Outputs:
     - `data/polymarket/markets.jsonl` (market metadata + `clobTokenIds`)
     - `data/polymarket/prices_history.jsonl` (price history per token)

### Running the Notebook

1. Open `notebooks/momentum_analysis/momentum_analysis.ipynb` in Jupyter
2. Run cells sequentially
3. Run cells sequentially; outputs are saved to `data/polymarket/analysis/`.

### Data Requirements

- **Required**: `data/polymarket/markets.jsonl`
- **Required**: `data/polymarket/prices_history.jsonl`
- **Optional fallback**: `data/polymarket_data/markets.csv` (metadata only)

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

- The notebook filters price history to the first `clobTokenIds` entry per market (assumed YES token).
- Outputs are written to:
  - `data/polymarket/analysis/momentum_metrics.csv`
  - `data/polymarket/analysis/momentum_persistence.csv`
