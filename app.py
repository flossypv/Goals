
from __future__ import annotations

import io
import os
import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st

DB_PATH = os.getenv("GOALS_DB_PATH", "goals_jfm.db")

# ---- Schema inferred from your uploaded workbook ----
JFM_OBJECTIVE_COL = 'Objectives'
JFM_TEAM_COLS = ['MoHI', 'Canyon', 'Toyota', 'Ares QA']

MONTHLY_SHEETS = ['MOHI', 'CANYON', 'TOYOTA', 'ARES QA']
MONTHLY_SCHEMA = {'MOHI': ['MOHI', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3'], 'CANYON': ['CANYON', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3'], 'TOYOTA': ['TOYOTA', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3'], 'ARES QA': ['ARES QA', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3']}

PERSONAL_SHEET_NAME = 'Flossy - JFM'


def conn():
    return sqlite3.connect(DB_PATH)


def utc_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def init_db():
    with conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS jfm_goal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                objective TEXT NOT NULL,
                team TEXT NOT NULL,
                value TEXT,
                updated_by TEXT,
                updated_at_utc TEXT NOT NULL,
                UNIQUE(objective, team)
            );
            """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_goal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                team_sheet TEXT NOT NULL,
                metric TEXT NOT NULL,
                month TEXT NOT NULL,
                value TEXT,
                updated_by TEXT,
                updated_at_utc TEXT NOT NULL,
                UNIQUE(team_sheet, metric, month)
            );
            """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS personal_goal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                goal TEXT NOT NULL,
                updated_by TEXT,
                updated_at_utc TEXT NOT NULL
            );
            """
        )


def seed_from_template_bytes(xlsx_bytes: bytes):
    """Seed DB from the Excel template if tables are empty."""
    import tempfile
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        tmp.write(xlsx_bytes)
        tmp_path = tmp.name

    xls = pd.ExcelFile(tmp_path, engine='openpyxl')

    with conn() as c:
        jfm_cnt = c.execute("SELECT COUNT(1) FROM jfm_goal").fetchone()[0]
        mon_cnt = c.execute("SELECT COUNT(1) FROM monthly_goal").fetchone()[0]
        per_cnt = c.execute("SELECT COUNT(1) FROM personal_goal").fetchone()[0]

    if jfm_cnt == 0 and 'JFM GOAL' in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name='JFM GOAL').dropna(how='all')
        if JFM_OBJECTIVE_COL in df.columns:
            with conn() as c:
                for _, r in df.iterrows():
                    obj = str(r.get(JFM_OBJECTIVE_COL, '')).strip()
                    if not obj:
                        continue
                    for t in JFM_TEAM_COLS:
                        val = r.get(t, '')
                        val = '' if pd.isna(val) else str(val)
                        c.execute(
                            "INSERT OR IGNORE INTO jfm_goal(objective, team, value, updated_by, updated_at_utc) VALUES (?,?,?,?,?)",
                            (obj, str(t), val, None, utc_now())
                        )
                c.commit()

    if mon_cnt == 0:
        for sheet in MONTHLY_SHEETS:
            if sheet in xls.sheet_names:
                dfm = pd.read_excel(xls, sheet_name=sheet).dropna(how='all')
                cols = [str(c).strip() for c in dfm.columns if str(c).strip()!='']
                if len(cols) < 3:
                    continue
                metric_col = cols[1]
                month_cols = cols[2:]
                with conn() as c:
                    for _, r in dfm.iterrows():
                        metric = str(r.get(metric_col, '')).strip()
                        if not metric:
                            continue
                        for m in month_cols:
                            val = r.get(m, '')
                            val = '' if pd.isna(val) else str(val)
                            c.execute(
                                "INSERT OR IGNORE INTO monthly_goal(team_sheet, metric, month, value, updated_by, updated_at_utc) VALUES (?,?,?,?,?,?)",
                                (sheet, metric, str(m).strip(), val, None, utc_now())
                            )
                    c.commit()

    if per_cnt == 0 and PERSONAL_SHEET_NAME and PERSONAL_SHEET_NAME in xls.sheet_names:
        dfp = pd.read_excel(xls, sheet_name=PERSONAL_SHEET_NAME, header=None)
        current_cat = None
        with conn() as c:
            for v in dfp.iloc[:,0].tolist():
                if pd.isna(v):
                    continue
                s = str(v).strip()
                if not s:
                    continue
                if s.endswith(':') or s.isupper():
                    current_cat = s.rstrip(':')
                    continue
                c.execute(
                    "INSERT INTO personal_goal(category, goal, updated_by, updated_at_utc) VALUES (?,?,?,?)",
                    (current_cat, s, None, utc_now())
                )
            c.commit()


def get_jfm_grid() -> pd.DataFrame:
    with conn() as c:
        rows = c.execute("SELECT objective, team, value FROM jfm_goal").fetchall()
    if not rows:
        return pd.DataFrame(columns=[JFM_OBJECTIVE_COL] + list(JFM_TEAM_COLS))
    df = pd.DataFrame(rows, columns=['objective','team','value'])
    pivot = df.pivot(index='objective', columns='team', values='value').reset_index()
    pivot = pivot.rename(columns={'objective': JFM_OBJECTIVE_COL})
    for t in JFM_TEAM_COLS:
        if t not in pivot.columns:
            pivot[t] = ''
    return pivot[[JFM_OBJECTIVE_COL] + list(JFM_TEAM_COLS)]


def save_jfm_grid(grid: pd.DataFrame, updated_by: str | None):
    now = utc_now()
    with conn() as c:
        for _, r in grid.iterrows():
            obj = str(r.get(JFM_OBJECTIVE_COL, '')).strip()
            if not obj:
                continue
            for t in JFM_TEAM_COLS:
                val = r.get(t, '')
                val = '' if pd.isna(val) else str(val)
                c.execute(
                    """
                    INSERT INTO jfm_goal(objective, team, value, updated_by, updated_at_utc)
                    VALUES (?,?,?,?,?)
                    ON CONFLICT(objective, team) DO UPDATE SET
                        value=excluded.value,
                        updated_by=excluded.updated_by,
                        updated_at_utc=excluded.updated_at_utc
                    """,
                    (obj, str(t), val, updated_by, now)
                )
        c.commit()


def get_monthly_grid(sheet: str) -> pd.DataFrame:
    cols = MONTHLY_SCHEMA.get(sheet)
    if not cols or len(cols) < 3:
        return pd.DataFrame()
    metric_col = cols[1]
    month_cols = [str(c).strip() for c in cols[2:]]

    with conn() as c:
        rows = c.execute("SELECT metric, month, value FROM monthly_goal WHERE team_sheet=?", (sheet,)).fetchall()

    if not rows:
        return pd.DataFrame(columns=[metric_col] + month_cols)

    df = pd.DataFrame(rows, columns=['metric','month','value'])
    pivot = df.pivot(index='metric', columns='month', values='value').reset_index()
    pivot = pivot.rename(columns={'metric': metric_col})

    for m in month_cols:
        if m not in pivot.columns:
            pivot[m] = ''

    return pivot[[metric_col] + month_cols]


def save_monthly_grid(sheet: str, grid: pd.DataFrame, updated_by: str | None):
    cols = MONTHLY_SCHEMA.get(sheet)
    if not cols or len(cols) < 3:
        return
    metric_col = cols[1]
    month_cols = [str(c).strip() for c in cols[2:]]

    now = utc_now()
    with conn() as c:
        for _, r in grid.iterrows():
            metric = str(r.get(metric_col, '')).strip()
            if not metric:
                continue
            for m in month_cols:
                val = r.get(m, '')
                val = '' if pd.isna(val) else str(val)
                c.execute(
                    """
                    INSERT INTO monthly_goal(team_sheet, metric, month, value, updated_by, updated_at_utc)
                    VALUES (?,?,?,?,?,?)
                    ON CONFLICT(team_sheet, metric, month) DO UPDATE SET
                        value=excluded.value,
                        updated_by=excluded.updated_by,
                        updated_at_utc=excluded.updated_at_utc
                    """,
                    (sheet, metric, m, val, updated_by, now)
                )
        c.commit()


def get_personal_goals() -> pd.DataFrame:
    with conn() as c:
        rows = c.execute("SELECT id, category, goal, updated_by, updated_at_utc FROM personal_goal ORDER BY id").fetchall()
    return pd.DataFrame(rows, columns=['ID','Category','Goal','Updated By','Updated At (UTC)'])


def save_personal_goals(df: pd.DataFrame, updated_by: str | None):
    now = utc_now()
    with conn() as c:
        c.execute("DELETE FROM personal_goal")
        for _, r in df.iterrows():
            goal = str(r.get('Goal','')).strip()
            if not goal:
                continue
            cat = str(r.get('Category','')).strip() or None
            c.execute(
                "INSERT INTO personal_goal(category, goal, updated_by, updated_at_utc) VALUES (?,?,?,?)",
                (cat, goal, updated_by, now)
            )
        c.commit()


def export_to_excel_bytes() -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer:
        get_jfm_grid().to_excel(writer, sheet_name='JFM GOAL', index=False)
        for s in MONTHLY_SHEETS:
            grid = get_monthly_grid(s)
            if not grid.empty:
                grid.to_excel(writer, sheet_name=s, index=False)
        if PERSONAL_SHEET_NAME:
            get_personal_goals()[['Category','Goal']].to_excel(writer, sheet_name=PERSONAL_SHEET_NAME, index=False)
    out.seek(0)
    return out.read()


# ---------------- UI ----------------
st.set_page_config(page_title='Goals Tracker (JFM)', layout='wide')
init_db()

st.title('Goals Tracker (JFM)')
st.caption('Enter and maintain goals in the same structure as the uploaded Excel format.')

with st.sidebar:
    st.header('Setup')
    updated_by = st.text_input('Your name (audit)', value='')
    st.divider()
    st.header('Import / Export')
    template = st.file_uploader('Upload template to seed (optional)', type=['xlsx'])
    if st.button('Seed DB from template'):
        if template is None:
            st.warning('Upload the template first.')
        else:
            seed_from_template_bytes(template.read())
            st.success('Seed completed (only fills empty tables).')

    st.download_button(
        'Export to Excel',
        data=export_to_excel_bytes(),
        file_name='GOALS_Export.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

labels = ['JFM GOAL (Overall)'] + [f"Monthly - {s}" for s in MONTHLY_SHEETS] + ([PERSONAL_SHEET_NAME] if PERSONAL_SHEET_NAME else [])
tabs = st.tabs(labels)

with tabs[0]:
    st.subheader('JFM GOAL (Overall)')
    st.write('Edit values per Objective and Team. Add new Objectives by adding rows.')
    grid = get_jfm_grid()
    if grid.empty:
        grid = pd.DataFrame([{JFM_OBJECTIVE_COL: '', **{t:'' for t in JFM_TEAM_COLS}}])
    edited = st.data_editor(grid, use_container_width=True, num_rows='dynamic', hide_index=True)
    if st.button('Save JFM GOAL changes', type='primary'):
        save_jfm_grid(edited, updated_by.strip() or None)
        st.success('Saved JFM GOAL')
        st.rerun()

for i, sheet in enumerate(MONTHLY_SHEETS, start=1):
    with tabs[i]:
        st.subheader(f"Monthly Goals - {sheet}")
        cols = MONTHLY_SCHEMA.get(sheet)
        if not cols:
            st.info('No schema found for this sheet.')
        else:
            metric_col = cols[1]
            month_cols = [str(c).strip() for c in cols[2:]]
            st.write('Edit monthly values for each metric.')
            grid = get_monthly_grid(sheet)
            if grid.empty:
                grid = pd.DataFrame([{metric_col:'', **{m:'' for m in month_cols}}])
            edited = st.data_editor(grid, use_container_width=True, num_rows='dynamic', hide_index=True)
            if st.button(f"Save {sheet} changes", key=f"save_{sheet}", type='primary'):
                save_monthly_grid(sheet, edited, updated_by.strip() or None)
                st.success(f"Saved {sheet}")
                st.rerun()

if PERSONAL_SHEET_NAME:
    with tabs[-1]:
        st.subheader(PERSONAL_SHEET_NAME)
        st.write('Enter personal/leadership goals. Category is optional (e.g., PROJECT, SSA, Other).')
        pdf = get_personal_goals()
        if pdf.empty:
            pdf = pd.DataFrame([{'ID':None,'Category':'','Goal':'','Updated By':'','Updated At (UTC)':''}])
        edited = st.data_editor(
            pdf,
            use_container_width=True,
            num_rows='dynamic',
            hide_index=True,
            column_config={
                'ID': st.column_config.NumberColumn(disabled=True),
                'Updated By': st.column_config.TextColumn(disabled=True),
                'Updated At (UTC)': st.column_config.TextColumn(disabled=True),
            }
        )
        if st.button('Save personal goals', type='primary'):
            save_personal_goals(edited[['Category','Goal']], updated_by.strip() or None)
            st.success('Saved personal goals')
            st.rerun()

with st.expander('Admin / Notes'):
    st.markdown(
        "- Storage: SQLite file `goals_jfm.db` (configurable via env var `GOALS_DB_PATH`).
"
        "- Use the sidebar button **Seed DB from template** to preload objectives/metrics from the Excel template.
"
        "- Export produces an Excel with the same sheet names for easy sharing."
    )
