import os
import re
import glob
from datetime import date
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import fitz  # pymupdf
import argparse

URL = "https://www.rehovot.muni.il/429/"
BASE = "https://www.rehovot.muni.il/"

def load_prev_baseline() -> pd.DataFrame:
    # 1) baseline קבוע בריפו
    if os.path.exists("latest_enriched.csv"):
        try:
            df = pd.read_csv("latest_enriched.csv", dtype=str)
            if len(df) > 0:
                return df
        except Exception:
            pass

    # 2) fallback לסנאפשוטים
    files = sorted(glob.glob(os.path.join("snapshots", "rehovot_licenses_enriched_*.csv")))
    if files:
        return pd.read_csv(files[-1], dtype=str)

    return pd.DataFrame()

def extract_pdf_text(pdf_url: str) -> tuple[str, str]:
    try:
        r = requests.get(pdf_url, timeout=60)
        status = f"http_{r.status_code}"
        if r.status_code != 200:
            return "", status
    except requests.RequestException as e:
        return "", f"request_error:{type(e).__name__}"

    try:
        doc = fitz.open(stream=r.content, filetype="pdf")
        text = "\n".join([page.get_text("text") for page in doc])
        doc.close()
        if not text.strip():
            return "", status + "_no_text"
        return text, status
    except Exception as e:
        return "", f"pdf_parse_error:{type(e).__name__}"

def parse_pdf_fields(text: str) -> dict:
    t = text.replace("\u00a0", " ")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)

    def grab(pattern: str, flags=0):
        m = re.search(pattern, t, flags)
        return m.group(1).strip() if m else ""

    # תאריכים dd/mm/yy או dd/mm/yyyy
    date_pat = r"(\d{1,2}/\d{1,2}/\d{2,4})"

    # תאריך הדפסה - תופס גם כשהוא דבוק
    print_date = grab(r"תאריך\s*הדפסה\s*" + date_pat)

    # מתאריך / עד תאריך - תופס גם כשהם דבוקים
    from_date = grab(r"מתאריך\s*" + date_pat)
    to_date = grab(r"עד\s*תאריך\s*" + date_pat)

    # שם בעל הרשיון - עדיין עובד לרוב
    owner = grab(r"שם\s+בעל\s+הרשיון\s+(.+?)(?=\s+כתובת|\s+סיבת\s+הבקשה|\n)")

    # סיבת בקשה / סיבה מילולית (כבר עובד אצלך, משאירים)
    request_reason = grab(r"סיבת\s+הבקשה\s+(.+?)(?=\s+סיבה\s+מילולית|\n|$)")
    verbal_reason = grab(r"סיבה\s+מילולית\s+(.+?)(?=\s+ישוב|\s+רחוב|\n|$)")

    # מיקום - תופס גם כשזה "כותרת בשורה ואז ערך בשורה"
    city = grab(r"\bישוב\s*\n\s*([^\n]+)")
    if not city:
        city = grab(r"\bישוב\s+(.+?)(?=\s+רחוב|\n|$)")

    street = grab(r"\bרחוב\s*\n\s*([^\n]+)")
    if not street:
        street = grab(r"\bרחוב\s+(.+?)(?=\s+מס|\s+גוש|\s+חלקה|\n|$)")

    # מס - יכול להיות גם טווח כמו 33-38
    house_no = grab(r"\bמס\s*([0-9]+(?:-[0-9]+)?)")
    block = grab(r"\bגוש\s*(\d+)")
    parcel = grab(r"\bחלקה\s*([0-9,\-\s]+)")

    # רשימת עצים
    species_list = []
    count_list = []

    section = grab(r"רשימת\s+העצים\s+ברישיון(.+)$", flags=re.DOTALL)

    if section:
        # ננסה להתחיל אחרי כותרת הטבלה
        # לפעמים מופיע: "מין העץ" ואז "מספר העצים" ואז "הערות"
        parts = re.split(r"מין\s+העץ\s*\n\s*מספר\s+העצים", section, maxsplit=1)
        table = parts[1] if len(parts) == 2 else section

        for line in table.splitlines():
            line = line.strip()
            if not line:
                continue

            # תופס שורות כמו:
            # "זית אירופי1מס '1698"
            # "פיקוס השדרות4מס '1425-..."
            m = re.match(r"^(.+?)(\d+)\s*מס", line)
            if m and re.search(r"[\u0590-\u05FF]", m.group(1)):
                species_list.append(m.group(1).strip())
                count_list.append(m.group(2).strip())
                continue

            # מקרה של "זית אירופי1" - בלי רווח ובלי "מס"
            m3 = re.match(r"^(.+?)(\d+)$", line)
            if m3 and re.search(r"[\u0590-\u05FF]", m3.group(1)):
                species_list.append(m3.group(1).strip())
                count_list.append(m3.group(2).strip())
                continue

            # fallback: "מין כלשהו 12"
            m2 = re.match(r"^(.+?)\s+(\d+)$", line)
            if m2 and re.search(r"[\u0590-\u05FF]", m2.group(1)):
                species_list.append(m2.group(1).strip())
                count_list.append(m2.group(2).strip())

    return {
        "תאריך_הדפסה": print_date,
        "מתאריך": from_date,
        "עד_תאריך": to_date,
        "שם_בעל_הרישיון": owner,
        "סיבת_הבקשה": request_reason,
        "סיבה_מילולית": verbal_reason,
        "ישוב": city,
        "רחוב": street,
        "מס": house_no,
        "גוש": block,
        "חלקה": parcel,
        "מיני_עצים": "\n".join(species_list),
        "מספרי_עצים": "\n".join(count_list),
        "מספר_מינים_שחולצו": str(len(species_list)),
    }

def enrich_from_pdfs(df: pd.DataFrame) -> pd.DataFrame:
    import os
    import json

    os.makedirs("pdf_cache", exist_ok=True)

    enriched = []

    for _, row in df.iterrows():
        safe_id = safe_filename(row["row_id"])
        cache_path = os.path.join("pdf_cache", f"{safe_id}.json")

        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as f:
                fields = json.load(f)
        else:
            pdf_url = row.get("pdf_url", "")
            text, fetch_status = extract_pdf_text(pdf_url)

            if not text.strip():
                fields = {"pdf_status": fetch_status}
            else:
                save_debug_text(row["row_id"], text)
                fields = parse_pdf_fields(text)
                fields["pdf_status"] = fetch_status

            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(fields, f, ensure_ascii=False)

        enriched.append({**row.to_dict(), **fields})

    return pd.DataFrame(enriched)

def save_debug_text(row_id: str, text: str):
    import os
    os.makedirs("pdf_text_debug", exist_ok=True)
    from pathlib import Path
    # שם קובץ בטוח
    safe = safe_filename(row_id)[:120]
    Path(f"pdf_text_debug/{safe}.txt").write_text(text, encoding="utf-8")

def safe_filename(name: str) -> str:
    # מחליף כל תו שלא אות/מספר/קו תחתון/מקף בנקודה
    return re.sub(r'[^A-Za-z0-9_\-]', '_', name)
    
def fetch_table() -> pd.DataFrame:
    r = requests.get(URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    best_table = None
    best_score = 0

    # נבחר את הטבלה שיש בה הכי הרבה שורות עם לינק ל-PDF
    for tbl in soup.find_all("table"):
        score = 0
        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue
            a = tds[2].find("a")
            href = a.get("href") if a else ""
            if href and ".pdf" in href.lower():
                score += 1
        if score > best_score:
            best_score = score
            best_table = tbl

    if best_table is None or best_score == 0:
        raise RuntimeError("לא הצלחתי למצוא טבלה עם קישורי PDF בעמודה 'העתק הרישיון'")

    rows = []
    for tr in best_table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        address = tds[0].get_text(strip=True)
        request_type = tds[1].get_text(strip=True)

        copy_cell = tds[2]
        a = copy_cell.find("a")
        pdf_url = urljoin(BASE, a["href"]) if a and a.get("href") else ""

        date_text = tds[3].get_text(strip=True)

        # לדלג על שורות שאין בהן כלום
        if not (address or request_type or pdf_url or date_text):
            continue

        rows.append([address, request_type, pdf_url, date_text])

    df = pd.DataFrame(rows, columns=["כתובת", "מהות הבקשה", "pdf_url", "תאריך"])

    def parse_date(s: str):
        part = (s or "").split("-")[-1].strip()
        return pd.to_datetime(part, dayfirst=True, errors="coerce")

    df["date_parsed"] = df["תאריך"].apply(parse_date)

    df["row_id"] = (
        df["כתובת"].fillna("") + "|" +
        df["מהות הבקשה"].fillna("") + "|" +
        df["pdf_url"].fillna("") + "|" +
        df["תאריך"].fillna("")
    )

    return df.sort_values("date_parsed", ascending=False).reset_index(drop=True)

def main():
    os.makedirs("snapshots", exist_ok=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=120)
    args = parser.parse_args()

    # מביאים את הטבלה מהאתר
    df = fetch_table()
    df = df.head(args.limit)  # רק הכי חדשים לפי תאריך הדפסה
    #df = df.head(30)

    # טוענים snapshot קודם אם קיים
    prev = load_prev_baseline()

    if not prev.empty and "row_id" in prev.columns:
        prev_ids = set(prev["row_id"].dropna().astype(str).tolist())
        df_new = df[~df["row_id"].astype(str).isin(prev_ids)].copy()
    else:
        df_new = df.copy()

    print(f"Total on page: {len(df)} | New to process: {len(df_new)}")

    # מעשירים רק את החדשים
    enriched_new = enrich_from_pdfs(df_new)

    # מחברים ישנים + חדשים
    if not prev.empty:
        combined = pd.concat([prev, enriched_new], ignore_index=True)
    else:
        combined = enriched_new

    # מסירים כפילויות לפי row_id
    if "row_id" in combined.columns:
        combined = combined.drop_duplicates(subset=["row_id"], keep="last")

    # ממיינים מהחדש לישן
    if "date_parsed" in combined.columns:
        combined["date_parsed"] = pd.to_datetime(combined["date_parsed"], errors="coerce")
        combined = combined.sort_values("date_parsed", ascending=False)

    # שומרים latest
    combined.to_csv("latest_enriched.csv", index=False, encoding="utf-8-sig")

    # שומרים snapshot יומי
    snapshot_path = os.path.join(
        "snapshots",
        f"rehovot_licenses_enriched_{pd.Timestamp.now().strftime('%Y-%m-%d')}.csv"
    )
    combined.to_csv(snapshot_path, index=False, encoding="utf-8-sig")

    print("saved:", snapshot_path)

if __name__ == "__main__":
    main()