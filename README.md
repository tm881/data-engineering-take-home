# How To Read This Repo

This file is a quick guide to the main deliverables in the take-home and what each one is for.

## Start Here

- [`ASSIGNMENT.md`](ASSIGNMENT.md)
  This is the original company prompt. It lists the required questions and deliverables.

## Direct Answers

- [`ANSWERS.md`](ANSWERS.md)
  This is the main submission write-up. It answers the README questions in order, summarizes the cleaning approach, documents assumptions and data-quality issues, and includes reproduction steps.

## Redesign Materials

- [`REDESIGN.md`](REDESIGN.md)
  This is the narrative redesign walkthrough. It explains the high-level architecture decisions, the step-by-step thought process, and why the proposed model is better than the original.

- [`redesign.sql`](redesign.sql)
  This is the concrete SQL version of the proposed redesigned data model, with comments explaining why the structure was chosen.

- [`ERD_updated.png`](ERD_updated.png)
  This is the updated ERD for the redesigned model.

- [`ERD_updated.dot`](ERD_updated.dot)
  This is the Graphviz source used to generate the updated ERD image.

## Analysis Walkthrough

- [`telecom_usage_analysis.ipynb`](telecom_usage_analysis.ipynb)
  This is the notebook walkthrough. It is organized in README order and shows the data exploration, cleaning logic, and reasoning behind each answer.

- [`telecom_usage_analysis.py`](telecom_usage_analysis.py)
  This is the sequential script version of the analysis. It reproduces the main outputs without needing the notebook.

## Output Files

- [`total_usage_per_day.png`](total_usage_per_day.png)
  Final line chart of total usage per day.

- [`outputs/daily_usage_mb.csv`](outputs/daily_usage_mb.csv)
  Day-level aggregated data behind the chart.

- [`outputs/linked_usage_events.csv`](outputs/linked_usage_events.csv)
  Event-level linked and rated dataset used for the cost answer.

## Supporting Code

- [`utils/table_cleaning.py`](utils/table_cleaning.py)
  Table-specific cleaning rules for the four source datasets.

- [`utils/dataframe_cleaner.py`](utils/dataframe_cleaner.py)
  Reusable cleaning utility that applies checks and returns both cleaned data and a check summary.

- [`utils/analysis_helpers.py`](utils/analysis_helpers.py)
  Helper functions for linking usage to SIMs, plans, and rates.

## Suggested Reading Order

1. [`ASSIGNMENT.md`](ASSIGNMENT.md)
2. [`ANSWERS.md`](ANSWERS.md)
3. [`telecom_usage_analysis.ipynb`](telecom_usage_analysis.ipynb)
4. [`REDESIGN.md`](REDESIGN.md)
5. [`redesign.sql`](redesign.sql)
