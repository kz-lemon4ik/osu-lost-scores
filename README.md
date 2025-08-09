# osu! Lost Scores Analyzer

PySide6 application that scans local osu! replays, highlights potential lost scores, and generates summary images plus a JSON report. The current implementation still relies on the original monolithic modules (`analyzer.py`, `generate_image.py`, `gui.py`) and produces the legacy wide JSON export.

## Currently implemented

- Scans the `Songs` directory and matches `.osr` files from `Data/r`
- Calculates current vs potential PP, produces side-by-side summary images
- Exports a JSON report with full statistics (still the "thick" format)
- Lets you toggle ranked/approved/loved/unranked filters inside the UI

## Requirements

- Python 3.11 or newer
- Dependencies from `requirements.txt` (PySide6, PIL, numpy, etc.)
- osu! client installed locally (paths in the UI expect the default layout)

## Setup and run

```bash
git clone https://github.com/kz-lemon4ik/osu-lost-scores.git
cd osu-lost-scores

python -m venv .venv
.\.venv\Scripts\activate    # or source .venv/bin/activate on Unix
pip install -r requirements.txt

python src/main.py
```

At first launch the app will generate a `.env` file with default settings (no secrets). Values can be edited manually if you need to adjust paths or API limits.

## Repository layout (today)

```
src/
  analyzer.py        # main scanning pipeline (still monolithic)
  generate_image.py  # image generation helpers
  gui.py             # PySide6 windows and widgets
  app_config.py      # loads `.env` and default paths
  osu_api.py         # direct osu! API helpers
  database.py        # legacy SQLite helpers
  utils.py
  path_utils.py
  file_parser.py
  oauth_browser.py
assets/              # icons, fonts, styles
cache/               # avatars and beatmap metadata cached by the app
data/                # JSON reports and generated screenshots
log/                 # application logs
```

Issues and feature requests are welcome. Please reference related tasks when opening issues so desktop and backend changes stay aligned.
