import requests
import sqlite3
from dict.scripts.paths import DB_PATH
import json
import time
from pprint import pprint


def fetch_lexin(word):
    url = f"https://lexin.nada.kth.se/lexin/service?searchinfo=both,swe_swe,{word}&output=JSON"
    r = requests.get(url)
    return json.loads(r.text)


def parse_noun(word, data):
    if data['Status'] != 'found':
        return []
    result = []
    for lexeme in data['Result']:
        if lexeme.get('Type', "") != "subst.":
            continue
        if lexeme.get('Value', "") != word:
            continue
        phonetics = [p.get('Content') for p in lexeme.get('Phonetic', [])]
        phonetic = ",".join(phonetics)
        
        inflections = {i['Form']: 
                       i['Content'] + (f" ({i['Phonetic'][0]["Content"]})" if i.get("Phonetic") else "")
                       for i in lexeme.get('Inflection', [])}
        result.append({
            'phonetic': phonetic,
            'def_sg': inflections.get('best.f.sing.'),
            'ind_pl': inflections.get('obest.f.pl.'),
            'def_pl': inflections.get('best.f.pl.'),
        })
    return result


def main():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT DISTINCT lemma FROM nouns 
            WHERE pitch_pattern IN
            ('backward-moving stress', 'forward-moving grave to acute', 
            'manual review required', 'monosyllabic with hidden grave accent', 
                  'stationary grave to acute')
        """)
        nouns = [word for (word,) in c.fetchall()]
        batch = []
        for i, word in enumerate(nouns):
            data = fetch_lexin(word)
            for result in parse_noun(word, data):
                batch.append((word, result['phonetic'], result['def_sg'], result['ind_pl'], result['def_pl']))
            time.sleep(2)

            print(f"{i} / {len(nouns)} processed...")

        c.execute("""
            CREATE TABLE IF NOT EXISTS lexin (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lemma TEXT,
                phonetic TEXT,
                def_sg TEXT,
                ind_pl TEXT,
                def_pl TEXT
            )
        """)

        c.executemany("""
            INSERT INTO lexin (lemma, phonetic, def_sg, ind_pl, def_pl)
            VALUES (?, ?, ?, ?, ?);
        """, batch)


if __name__ == "__main__":
    main()