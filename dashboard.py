import os
import glob
import pandas as pd
import streamlit as st
import subprocess
import sys
import time
from datetime import datetime
import numpy as np
import pandas as pd

path = "latest_enriched.csv" if os.path.exists("latest_enriched.csv") else "latest.csv"
latest_df = pd.read_csv("latest_enriched.csv", dtype=str)
DATA_FILE = "latest_enriched.csv"

@st.cache_data(ttl=60*60)  # שעה
def load_or_update_data():
    # אם אין קובץ בכלל, נריץ עדכון פעם ראשונה
    if not os.path.exists(DATA_FILE):
#       subprocess.run([sys.executable, "scrape_rehovot_licenses.py"], check=True)
        subprocess.run([sys.executable, "scrape_rehovot_licenses.py", "--limit", "150"], check=True)

    # אם יש קובץ אבל הוא ריק/תקול, ננסה לייצר מחדש
    try:
        df = pd.read_csv(DATA_FILE, dtype=str)
        if len(df) == 0:
            raise ValueError("empty csv")
        return df
    except Exception:
#        subprocess.run([sys.executable, "scrape_rehovot_licenses.py"], check=True)
        subprocess.run([sys.executable, "scrape_rehovot_licenses.py", "--limit", "150"], check=True)
        return pd.read_csv(DATA_FILE, dtype=str)

def file_updated_at(path):
    if not os.path.exists(path):
        return None
    return time.strftime("%d/%m/%Y %H:%M", time.localtime(os.path.getmtime(path)))

st.set_page_config(page_title="רישיונות כריתה - רחובות", layout="wide")
st.title("דשבורד רישיונות כריתה - רחובות")

col_a, col_b = st.columns([1, 4])

with col_a:
    if st.button("עדכן עכשיו"):
        with st.status("מעדכן נתונים מהאתר (רק רישיונות חדשים)...", expanded=True) as status:
            try:
                subprocess.run([sys.executable, "scrape_rehovot_licenses.py"], check=True)
                status.update(label="העדכון הסתיים - טוען נתונים מחדש", state="complete")
                time.sleep(1)
                load_or_update_data.clear()
                st.rerun()
            except subprocess.CalledProcessError:
                status.update(label="העדכון נכשל - נשארים עם הנתונים הקיימים", state="error")

#col1, col2 = st.columns([1, 1])
#with col1:
#    st.caption(f"עודכן לאחרונה: {file_updated_at(DATA_FILE) or 'לא זמין'}")

#with col2:
#    if st.button("עדכן עכשיו"):
#        # לנקות cache כדי שהפונקציה תרוץ שוב
#        load_or_update_data.clear()
#        # להריץ עדכון מחדש ואז reload
#        subprocess.run([sys.executable, "scrape_rehovot_licenses.py"], check=True)
#        st.rerun()

# הצגת זמן עדכון אחרון
if os.path.exists("latest_enriched.csv"):
    ts = os.path.getmtime("latest_enriched.csv")
    last_update = datetime.fromtimestamp(ts)
    st.caption(f"עודכן לאחרונה: {last_update.strftime('%d/%m/%Y %H:%M')}")

    hours_since = (datetime.now() - last_update).total_seconds() / 3600
    if hours_since > 24:
        st.warning(f"הנתונים לא עודכנו כבר {int(hours_since)} שעות")
else:
    st.caption("עדיין לא קיים קובץ נתונים.")

with col_b:
    st.caption("הכפתור מריץ עדכון דלתא - רק רישיונות חדשים, ואז מרענן את הדשבורד.")

files = sorted(glob.glob(os.path.join("snapshots", "rehovot_licenses_enriched_*.csv")))
if not files:
    st.warning("לא נמצאו snapshots. תריצי קודם את scrape_rehovot_licenses.py")
    st.stop()

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df

latest_path = files[-1]
#latest_df = load_csv(latest_path)
latest_df = load_or_update_data()

prev_path = files[-2] if len(files) >= 2 else None

col1, col2, col3 = st.columns([2, 2, 3])

with col1:
    st.caption("Snapshot נוכחי")
    st.code(os.path.basename(latest_path), language="text")

with col2:
    compare_to = st.selectbox(
        "השוואה מול snapshot",
        options=[None] + files[:-1],
        format_func=lambda p: "ללא" if p is None else os.path.basename(p),
        index=(1 if prev_path else 0),
    )

with col3:
    # פילטר לפי תאריך הדפסה מתוך ה-PDF
    latest_df["_print_dt"] = pd.to_datetime(latest_df["תאריך_הדפסה"], dayfirst=True, errors="coerce")

    min_d = latest_df["_print_dt"].min()
    max_d = latest_df["_print_dt"].max()

    date_range = None
    if not (pd.isna(min_d) or pd.isna(max_d)):
        date_range = st.date_input(
            "פילטר לפי תאריך הדפסה",
            value=(min_d.date(), max_d.date()),
            min_value=min_d.date(),
            max_value=max_d.date(),
        )
    else:
        st.info("לא הצלחתי לפרסר תאריכי הדפסה בחלק מהרשומות, הפילטר עלול להיות מוגבל.")

# יוצרים filtered מה-snapshot האחרון
filtered = latest_df.copy()

filtered["_print_dt"] = pd.to_datetime(
    filtered["תאריך_הדפסה"],
    dayfirst=True,
    errors="coerce"
)

from datetime import datetime
import pandas as pd
import streamlit as st

st.subheader("פילטרים")

quick_options = ["הכל", "מתחילת שנה", "3 חודשים אחרונים", "30 ימים אחרונים", "2025", "2024"]
quick = st.selectbox("טווח זמן מהיר", quick_options, index=0)

today = pd.Timestamp.today().normalize()

today = pd.Timestamp.today().normalize()

if quick == "מתחילת שנה":
    start = pd.Timestamp(year=today.year, month=1, day=1)
    filtered = filtered[(filtered["_print_dt"] >= start) & (filtered["_print_dt"] <= today)]
elif quick == "3 חודשים אחרונים":
    start = today - pd.DateOffset(months=3)
    filtered = filtered[(filtered["_print_dt"] >= start) & (filtered["_print_dt"] <= today)]
elif quick == "30 ימים אחרונים":
    start = today - pd.Timedelta(days=30)
    filtered = filtered[(filtered["_print_dt"] >= start) & (filtered["_print_dt"] <= today)]
elif quick in ["2025", "2024"]:
    year = int(quick)
    start = pd.Timestamp(year=year, month=1, day=1)
    end = pd.Timestamp(year=year, month=12, day=31)
    filtered = filtered[(filtered["_print_dt"] >= start) & (filtered["_print_dt"] <= end)]
# אם "הכל" - לא עושים כלום

# פילטר לפי תאריך
if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
    start, end = date_range
    mask = (filtered["_print_dt"].dt.date >= start) & (filtered["_print_dt"].dt.date <= end)
    filtered = filtered[mask]

# פילטר לפי סטטוס PDF
if "pdf_status" in filtered.columns:
    status_options = ["הכל"] + sorted(filtered["pdf_status"].fillna("").unique().tolist())
    chosen = st.selectbox("פילטר לפי סטטוס PDF", status_options, index=0)
    if chosen != "הכל":
        filtered = filtered[filtered["pdf_status"] == chosen]

# חישוב דדליין ערר וסטטוס ערר
def parse_il_date(s):
    return pd.to_datetime(s, dayfirst=True, errors="coerce")

filtered["_print_dt"] = filtered["תאריך_הדפסה"].apply(parse_il_date) if "תאריך_הדפסה" in filtered.columns else pd.NaT
filtered["_from_dt"] = filtered["מתאריך"].apply(parse_il_date) if "מתאריך" in filtered.columns else pd.NaT

filtered["דדליין_ערר"] = pd.NaT
filtered["סטטוס_ערר"] = ""

mask_valid = filtered["_from_dt"].notna() & filtered["_print_dt"].notna()
today_dt = pd.Timestamp.today().normalize()

mask_valid = filtered["_from_dt"].notna() & filtered["_print_dt"].notna()

# יש חלון ערר רק אם מתאריך גדול מתאריך הדפסה
mask_window_exists = mask_valid & (filtered["_from_dt"] > filtered["_print_dt"])

# אפשר להגיש רק אם היום לפני מתאריך (כלומר הרישיון עוד לא נכנס לתוקף)
mask_can_appeal = mask_window_exists & (today_dt < filtered["_from_dt"])

filtered["דדליין_ערר"] = pd.NaT
filtered["סטטוס_ערר"] = ""

filtered.loc[mask_can_appeal, "דדליין_ערר"] = filtered.loc[mask_can_appeal, "_from_dt"] - pd.Timedelta(days=1)
filtered.loc[mask_can_appeal, "סטטוס_ערר"] = "אפשר להגיש ערר"

# אם אין חלון בכלל (מתאריך שווה או קטן מתאריך הדפסה)
mask_no_window = mask_valid & (filtered["_from_dt"] <= filtered["_print_dt"])
filtered.loc[mask_no_window, "סטטוס_ערר"] = "אין חלון ערר"

# אם היה חלון אבל הוא כבר נסגר כי מתאריך הגיע/עבר
mask_closed = mask_window_exists & ~mask_can_appeal
filtered.loc[mask_closed, "סטטוס_ערר"] = "חלון הערר נסגר"

filtered["דדליין_ערר"] = filtered["דדליין_ערר"].dt.strftime("%d/%m/%Y")

only_open = st.checkbox("הצג רק רישיונות שעדיין אפשר לערער עליהם", value=False)
if only_open and "סטטוס_ערר" in filtered.columns:
    filtered = filtered[filtered["סטטוס_ערר"] == "אפשר להגיש ערר"]

# כרטיסי UX למעלה
today = pd.Timestamp.today().normalize()
filtered["_print_dt"] = pd.to_datetime(filtered["_print_dt"], errors="coerce")
new_today = int((filtered["_print_dt"].dt.normalize() == today).sum())

bad_pdf = 0
if "pdf_status" in filtered.columns:
    bad_pdf = int(
        filtered["pdf_status"]
        .fillna("")
        .astype(str)
        .str.contains("no_text|404|403|request_error|pdf_parse_error", regex=True)
        .sum()
    )

can_appeal = int((filtered["סטטוס_ערר"] == "אפשר להגיש ערר").sum()) if "סטטוס_ערר" in filtered.columns else 0

def count_trees(series):
    total = 0
    for v in series.dropna():
        for x in str(v).split("\n"):
            try:
                total += int(x)
            except:
                pass
    return total


trees_cut = count_trees(
    filtered.loc[filtered["מהות הבקשה"].str.contains("כריתה", na=False), "מספרי_עצים"]
)

trees_move = count_trees(
    filtered.loc[filtered["מהות הבקשה"].str.contains("העתקה", na=False), "מספרי_עצים"]
)

trees_total = trees_cut + trees_move

colA, colB, colC = st.columns(3)

with colA:
    st.metric("סה״כ עצים", trees_total)

with colB:
    st.metric("עצים לכריתה", trees_cut)

with colC:
    st.metric("עצים להעתקה", trees_move)

c1, c2, c3 = st.columns(3)
c1.metric("רישיונות חדשים היום", new_today)
c2.metric("עדיין אפשר לערער", can_appeal)
c3.metric("PDF בעייתי", bad_pdf)

# תצוגה מרובת שורות לשתי העמודות בלי לאחד אותן
#multiline_view = st.checkbox("תצוגה מרובת שורות למיני עצים", value=True)

filtered_display = filtered.copy()
#if multiline_view:
for col in ["מיני_עצים", "מספרי_עצים"]:
    if col in filtered_display.columns:
        filtered_display[col] = (
            filtered_display[col]
            .fillna("")
            .astype(str)
            .str.replace("\r\n", "\n")
            .str.replace("\n", "<br>")
        )

# PDF כקישור גם ב-HTML
if "pdf_url" in filtered_display.columns:
    filtered_display["pdf_url"] = filtered_display["pdf_url"].apply(
        lambda u: f"<a href='{u}' target='_blank'>פתיחה</a>" if isinstance(u, str) and u.startswith("http") else ""
    )

if "תאריך_הדפסה" in filtered.columns:
    filtered["_sort_print_dt"] = pd.to_datetime(filtered["תאריך_הדפסה"], dayfirst=True, errors="coerce")
else:
    filtered["_sort_print_dt"] = pd.NaT

filtered = filtered.sort_values("_print_dt", ascending=False)

count_filtered = len(filtered)

open_count = 0
if "סטטוס_ערר" in filtered.columns:
    open_count = int((filtered["סטטוס_ערר"] == "אפשר להגיש ערר").sum())

st.info(f"סהכ רישיונות לפי הפילטרים: {count_filtered} | פתוחים לערר: {open_count}")

st.subheader("כל הרישיונות (מסונן)")
cols = [
    "תאריך_הדפסה","מתאריך","עד_תאריך",
    "דדליין_ערר","סטטוס_ערר",
    "שם_בעל_הרישיון","סיבת_הבקשה","סיבה_מילולית",
    "ישוב","רחוב","מס","גוש","חלקה",
    "מיני_עצים","מספרי_עצים",
    "כתובת","מהות הבקשה",
    "pdf_url"
]
show_cols = [c for c in cols if c in filtered.columns]

display_df = (
    filtered.copy()
    .astype("string")
    .replace({pd.NA: "", "nan": "", "NaN": "", "<NA>": ""})
)

#if multiline_view:
html_df = display_df[show_cols].copy()

# להפוך את pdf_url ללינק
if "pdf_url" in html_df.columns:
    html_df["pdf_url"] = html_df["pdf_url"].apply(
        lambda u: f"<a href='{u}' target='_blank'>פתיחה</a>" if isinstance(u, str) and u.startswith("http") else ""
    )

# להפוך \n לשבירת שורה אמיתית ב-HTML
for c in ["מיני_עצים", "מספרי_עצים"]:
    if c in html_df.columns:
        html_df[c] = html_df[c].astype("string").str.replace("\n", "<br>", regex=False)

# גם בעמודות טקסט נוספות אם בא לך (לא חובה):
# for c in ["כתובת", "סיבה_מילולית"]:
#     if c in html_df.columns:
#         html_df[c] = html_df[c].astype("string").str.replace("\n", "<br>", regex=False)

st.markdown(
    """
    <style>
    table td { vertical-align: top; }
    table td { white-space: normal; }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown(
    html_df.to_html(escape=False, index=False),
    unsafe_allow_html=True
)
#else:
#    st.dataframe(
#        display_df[show_cols],
#        use_container_width=True,
#        hide_index=True,
#        column_config={"pdf_url": st.column_config.LinkColumn("PDF רישיון", display_text="פתיחה")}
#    )

st.subheader("מה חדש מאז ההשוואה")
if compare_to is None:
    st.info("בחרי snapshot להשוואה כדי לראות מה חדש.")
else:
    old_df = load_csv(compare_to)
    new_rows = latest_df[~latest_df["row_id"].isin(set(old_df["row_id"].tolist()))].copy()

    st.write(f"נמצאו {len(new_rows)} רשומות חדשות.")
    st.dataframe(
        new_rows[show_cols],
        use_container_width=True,
        hide_index=True,
)