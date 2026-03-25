from dict.scripts.paths import DB_PATH, RAW_DIR
import sqlite3

CORPORA = [
    "swe_news_2023_1M-words.txt",
    "swe_newscrawl_2018_1M-words.txt",
    "swe_wikipedia_2021_1M-words.txt",
    "swe-se_web_2020_1M-words.txt"
]

def normalize(token: str, known_lemmas: set[str]) -> str:
    if token in known_lemmas:
        return token          # preserve: it's a known cased lemma (e.g. Mars)
    return token.lower()


def get_lemmas(c: sqlite3.Cursor) -> set[str]:
    c.execute("""
        SELECT DISTINCT word FROM braxen
        UNION
        SELECT DISTINCT form FROM sv_wiktionary
    """)
    return {lemma for (lemma,) in c.fetchall()}


with sqlite3.connect(DB_PATH) as conn:
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS freq")
    c.execute("""
        CREATE TABLE freq (
            wordform  TEXT PRIMARY KEY,
            count     INTEGER NOT NULL
        )
    """)

    aggregated: dict[str, int] = {}
    known_lemmas = get_lemmas(c)

    for filename in CORPORA:
        path = RAW_DIR / filename
        with open(path, encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) != 3:
                    continue
                _, token, count_str = parts
                if not token.isalpha():
                    continue
                try:
                    count = int(count_str)
                except ValueError:
                    continue
                key = normalize(token, known_lemmas)
                aggregated[key] = aggregated.get(key, 0) + count


    c.executemany(
        "INSERT INTO freq (wordform, count) VALUES (?, ?)",
        aggregated.items()
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_freq_count ON freq(count DESC)")
    conn.commit()

total_tokens = sum(aggregated.values())
print(
    f"Loaded {len(aggregated):,} wordforms, {total_tokens:,} total token count")
