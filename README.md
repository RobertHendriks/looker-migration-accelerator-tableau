# **Looker Enterprise Migration Accelerator (LEMA) - Tableau Edition**

A Python tool that automates the migration of multiple Tableau workbooks (`.twb`) into a unified, production-ready LookML project for Looker.

---

## Overview

Migrating from Tableau to Looker at enterprise scale typically involves manually re-creating dozens of data sources, calculated fields, and dashboards — often with significant duplication across workbooks. This tool automates that process by:

1. **Parsing** Tableau workbook XML to extract data source metadata, columns, and custom SQL
2. **Consolidating** duplicate views and calculated fields across all workbooks
3. **Generating** a unified LookML project (models, views, dashboards)
4. **Flagging** conflicts that require human review before deployment

---

## Features

- **Multi-workbook analysis** — processes any number of `.twb` files in a single run
- **View deduplication** — detects identical and near-identical data source definitions and merges them
- **Smart merge strategies** — identical views are merged automatically; divergent views are flagged for manual review
- **LookML generation** — produces valid `.view.lkml`, `.model.lkml`, and `.dashboard.lookml` files
- **Datatype mapping** — translates Tableau column types (`string`, `integer`, `real`, `boolean`, `date`, `datetime`) to their LookML equivalents
- **Custom SQL support** — preserves derived table logic from Tableau custom SQL relations
- **Visualization mapping** — maps Tableau chart types to corresponding Looker visualization types
- **Governance report** — outputs a `GOVERNANCE_REVIEW.md` with a full list of conflicts and recommended resolutions
- **Consolidation report** — outputs a `consolidation_report.json` with before/after metrics

---

## Project Structure

```
.
├── orchestrator.py             # Main module (all classes and logic)
└── enterprise_migration_output/
    ├── consolidation_report.json
    └── lookml/
        ├── models/
        │   └── enterprise.model.lkml
        ├── views/
        │   └── <view_name>.view.lkml
        ├── dashboards/
        │   └── <dashboard_name>.dashboard.lookml
        └── GOVERNANCE_REVIEW.md
```

---

## Requirements

- Python 3.8+
- No third-party dependencies — uses only Python standard library (`xml.etree.ElementTree`, `hashlib`, `json`, `pathlib`, `dataclasses`, `collections`)

---

## Usage

### Standalone (CLI / Script)

```python
from orchestrator import run_orchestrator

result = run_orchestrator(
    workbook_paths=[
        "workbooks/sales_dashboard.twb",
        "workbooks/marketing_trends.twb",
        "workbooks/ops_overview.twb"
    ],
    output_dir="./enterprise_migration_output"
)

print(result['summary'])
```

### Flask Integration

The `run_orchestrator()` function is designed as a clean entry point for a Flask API:

```python
from flask import Flask, request, jsonify
from orchestrator import run_orchestrator

app = Flask(__name__)

@app.route('/migrate', methods=['POST'])
def migrate():
    data = request.json
    result = run_orchestrator(data['workbook_paths'], data.get('output_dir', './output'))
    return jsonify(result)
```

---

## Output

### `consolidation_report.json`

Summary of what was found and merged across all workbooks:

```json
{
  "summary": {
    "workbooks_analyzed": 3,
    "views_before_consolidation": 9,
    "views_after_consolidation": 4,
    "views_eliminated": 5,
    "views_requiring_manual_review": 1,
    "duplicate_calculated_fields": 0
  },
  "unified_views": { ... },
  "views_requiring_review": [ ... ]
}
```

### `GOVERNANCE_REVIEW.md`

Human-readable document listing every view with conflicting definitions, showing which columns or calculated fields differ between workbooks and what resolution is recommended.

---

## Key Classes

| Class | Responsibility |
|---|---|
| `EnterpriseMigrationOrchestrator` | Top-level coordinator — runs the full pipeline |
| `TableauParserWrapper` | Parses `.twb` XML and extracts data source metadata |
| `ViewConsolidator` | Deduplicates and merges view definitions across workbooks |
| `CalculatedFieldConsolidator` | Identifies calculated fields with the same name but different formulas |
| `UnifiedView` | Data class representing a merged view and its provenance |

---

## Merge Strategies

| Strategy | When Applied |
|---|---|
| `identical` | All workbooks define the view the same way (hash match) |
| `most_complete` | One workbook has a superset of columns — that definition wins |
| `manual_review` | Definitions diverge in ways that can't be resolved automatically |

---

## Limitations & Notes

- **Calculated fields**: The current parser extracts column metadata from `<metadata-records>` in the TWB XML. Tableau calculated fields embedded in `<column>` elements with formulas are noted for future enhancement.
- **Dashboard layout**: Migrated dashboards use a simplified `newspaper` layout. Complex Tableau floating layouts require manual adjustment in Looker.
- **Connection string**: The generated model defaults to `connection: "enterprise_database"` — update this to match your Looker database connection name before deploying.
- **Review before deploying**: Always inspect `GOVERNANCE_REVIEW.md` and validate generated LookML in the Looker IDE before pushing to production.
