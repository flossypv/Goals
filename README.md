
# Goals Tracker (JFM) – Streamlit

Enter goals using the same structure as the provided Excel template.

## Run
```bash
cd goals_streamlit_app
pip install -r requirements.txt
streamlit run app.py
```

## Storage
- SQLite DB: `goals_jfm.db`
- Override with env var `GOALS_DB_PATH`

## Seed
Upload the template in the sidebar and click **Seed DB from template** (optional).
