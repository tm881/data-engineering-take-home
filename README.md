# Submission Contents

This repo now includes the take-home deliverables in the repository root:

- `ANSWERS.md`: final answers, assumptions, data quality notes, clarifying questions, and reproduction steps
- `total_usage_per_day.png`: line chart of total usage per day
- `daily_usage_mb.csv`: chart source data
- `linked_usage_events.csv`: event-level linked/rated output
- `redesign.sql`: proposed schema redesign with keys, constraints, indices, and tradeoffs
- `analyze.py`: reproducible analysis script
- `requirements.txt`: Python dependencies

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 analyze.py
```

The script reads the parquet files in `data/`, applies the documented cleanup logic, regenerates the chart and CSV outputs, and rewrites `ANSWERS.md`.
