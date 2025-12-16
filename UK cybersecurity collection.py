"""
Filter & combine UK Hansard (ParlParse XML) speeches that contain:
  "cyber", "cybersecurity", "cyber security"  (case-insensitive)

Outputs (UTF-8, tab-separated):
  speeches.tsv : DocID, text
  metadata.tsv : id, date, house, member, party, debate_type, heading, file

Usage (Windows example):
  python uk_filter_and_combine.py --xml-root "C:\\Users\\green\\Desktop\\scrapedxml" --start 2015-01-01 --end 2025-09-30
"""

import argparse
import csv
from datetime import datetime as dt
from pathlib import Path
import re
import sys
from bs4 import BeautifulSoup

# ----------- defaults -----------
DEFAULT_START = "2015-01-01"
DEFAULT_END   = "2025-09-30"
KEYWORDS = [r"\bcyber\b", r"\bcybersecurity\b", r"\bcyber security\b"]
SUBDIRS = ["lords"]  # change to ["debates"] if you only want Commons
# --------------------------------

FNAME_DATE_RE = re.compile(r"(?:debates|lords|westminhall)(\d{4})-(\d{2})-(\d{2})", re.I)
KW_REGEX = re.compile("|".join(KEYWORDS), flags=re.IGNORECASE)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xml-root", required=True, help="Path to folder that contains debates/lords/westminhall")
    ap.add_argument("--start", default=DEFAULT_START, help="Start date YYYY-MM-DD (by filename date)")
    ap.add_argument("--end",   default=DEFAULT_END,   help="End date YYYY-MM-DD (by filename date)")
    ap.add_argument("--out-prefix", default="", help="Optional prefix for output files (e.g., UK_2015_2020_)")
    return ap.parse_args()

def date_from_filename(p: Path):
    m = FNAME_DATE_RE.search(p.name)
    if not m:
        return None
    y, mo, d = map(int, m.groups())
    try:
        return dt(y, mo, d)
    except ValueError:
        return None

def house_and_type(subdir: str):
    s = subdir.lower()
    if s == "debates":
        return "Commons", "Debate"
    if s == "lords":
        return "Lords", "Debate"
    if s == "westminhall":
        return "Commons", "Westminster Hall"
    return "", s

def extract_speech_text(sp_el) -> str:
    # BeautifulSoup: the simplest way is get_text with spaces
    return sp_el.get_text(" ", strip=True)

def main():
    args = parse_args()
    root = Path(args.xml_root)
    if not root.exists():
        print(f"ERROR: {root} not found.", file=sys.stderr)
        sys.exit(1)

    try:
        start_date = dt.fromisoformat(args.start)
        end_date   = dt.fromisoformat(args.end)
    except ValueError:
        print("ERROR: --start/--end must be YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    out_speeches = Path(f"{args.out_prefix}speeches.tsv")
    out_meta     = Path(f"{args.out_prefix}metadata.tsv")

    total_hits = 0
    seen_ids = set()
    speeches_rows = []
    meta_rows = []

    for sub in SUBDIRS:
        subdir = root / sub
        if not subdir.exists():
            continue
        house, debate_type = house_and_type(sub)

        xml_files = sorted(subdir.rglob("*.xml"))
        for xf in xml_files:
            fdate = date_from_filename(xf)
            if fdate is None or not (start_date <= fdate <= end_date):
                continue

            try:
                with open(xf, "rb") as fh:
                    soup = BeautifulSoup(fh, "lxml-xml")
            except Exception as e:
                print(f"[WARN] parse failed: {xf} -> {e}", file=sys.stderr)
                continue

            found = 0
            for sp in soup.find_all("speech"):
                sid = sp.get("id") or sp.get("ids") or f"{xf.name}#{len(speeches_rows)+1}"
                if sid in seen_ids:
                    continue

                txt = extract_speech_text(sp)
                if not txt or not KW_REGEX.search(txt):
                    continue

                seen_ids.add(sid)
                member = sp.get("speakername") or sp.get("speaker") or ""
                party  = sp.get("speakerparty") or sp.get("party") or ""
                heading = sp.get("heading") or ""

                speeches_rows.append((sid, txt))
                meta_rows.append({
                    "id": sid,
                    "date": fdate.strftime("%Y-%m-%d"),
                    "house": house,
                    "member": member,
                    "party": party,
                    "debate_type": debate_type,
                    "heading": heading,
                    "file": str(xf)
                })
                total_hits += 1
                found += 1

            if found:
                print(f"[INFO] {xf.name}: {found} speeches matched")

    # Write outputs (UTF-8, tab)
    with open(out_speeches, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["DocID", "text"])
        w.writerows(speeches_rows)

    fields = ["id", "date", "house", "member", "party", "debate_type", "heading", "file"]
    with open(out_meta, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        for r in meta_rows:
            w.writerow(r)

    print(f"\n[DONE] {total_hits} speeches → {out_speeches}")
    print(f"Metadata → {out_meta}")

if __name__ == "__main__":
    main()
