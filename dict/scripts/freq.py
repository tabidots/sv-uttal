import xml.etree.ElementTree as ET
import sqlite3
from dict.scripts.paths import DB_PATH, RAW_DIR
import re

# https://spraakbanken.gu.se/en/resources/kelly
with open(RAW_DIR / 'kelly.xml', 'rb') as f:
    root = ET.fromstring(f.read())

NORMALIZED_POS = {
    # 'noun-ett': "noun",
    # 'noun-en': "noun",
    # 'noun': "noun",
    # 'noun-en/-ett': "noun",
    'interj': "intj",
    'particip': "participle",
    'aux verb': "verb",
    'adjective': "adj",
    'subj': "conj",
    'particle': "particle",
    'adverb': "adv",
    'proper name': "name",
}

with sqlite3.connect(DB_PATH) as conn:
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS kelly")
    c.execute("""
        CREATE TABLE IF NOT EXISTS kelly (
            id INTEGER PRIMARY KEY,
            word TEXT NOT NULL,
            pos TEXT NOT NULL,
            cefr INTEGER,
            freq INTEGER,
            wpm FLOAT
        )
    """)

    batch = []
    for child in root:
        word = child.find('gf').text
        raw_pos = child.find('pos').text
        cefr = child.find('cefr').text
        pos = NORMALIZED_POS.get(raw_pos, raw_pos)
        freq = None
        if freq := child.find('raw').text:
            freq = int(freq)
        wpm = float(child.find('wpm').text.replace(",", "."))
        if wpm == 1_000_000.00:
            wpm = None
        if " (" in word:
            word = word[:word.index(" (")]
        if "(" in word:
            short_word = word[:word.index("(")] + word[word.index(")") + 1:]
            batch.append((short_word, pos, cefr, wpm, freq))
            word = word.replace("(", "").replace(")", "")
        batch.append((word, pos, cefr, wpm, freq))

    c.executemany(
        "INSERT INTO freq (word, pos, cefr, wpm, kelly) VALUES (?, ?, ?, ?, ?)", batch)

    