import requests, csv, time

BASE = "https://kokkai.ndl.go.jp/api/speech"
TERMS = ["サイバーセキュリティ", "サイバー"]
DATE_FROM, DATE_UNTIL = "2015-01-01", "2025-09-30"
PAGE_SIZE, DELAY = 100, 1.5

def fetch(term, start):
    r = requests.get(BASE, params={
        "any": term,
        "startRecord": str(start),
        "maximumRecords": str(PAGE_SIZE),
        "recordPacking": "json",
        "from": DATE_FROM,
        "until": DATE_UNTIL
    }, timeout=30)
    r.raise_for_status()
    return r.json()

speeches, seen = [], set()

for term in TERMS:
    start, total, done = 1, None, 0
    while True:
        try:
            data = fetch(term, start)
        except Exception as e:
            print(f"[WARN] {term} start={start} failed: {e}")
            break
        if total is None:
            total = int(data.get("numberOfRecords", 0))
            print(f"[INFO] {term}: {total} hits")
        recs = data.get("speechRecord", []) or []
        if not recs: break
        for r in recs:
            sid, txt = r.get("speechID"), (r.get("speech") or "").strip()
            if sid and txt and sid not in seen:
                seen.add(sid)
                speeches.append((sid, txt))
        done += len(recs)
        next_pos = data.get("nextRecordPosition")
        if not next_pos or done >= total: break
        start = int(next_pos)
        time.sleep(DELAY)

with open("FINAL.CYBER.speeches.tsv", "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f, delimiter="\t")
    w.writerow(["DocID", "text"])
    w.writerows(speeches)

print(f" Finished. Saved {len(speeches)} speeches to FINAL.CYBER.speeches.tsv")


