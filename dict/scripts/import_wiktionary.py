import sqlite3
import json
from pprint import pprint
from dict.scripts.paths import DB_PATH, RAW_DIR

NOUN_SLOTS = [
    ({'indefinite', 'nominative', 'singular'}, 'IND_SG'),
    ({'definite', 'nominative', 'singular'}, 'DEF_SG'),
    ({'indefinite', 'nominative', 'plural'}, 'IND_PL'),
    ({'definite', 'nominative', 'plural'}, 'DEF_PL'),
]

VERB_SLOTS = [
    ({'active', 'infinitive'}, 'INF'),
    ({'active', 'indicative', 'present'}, 'PRS'),
    ({'active', 'indicative', 'past'}, 'PRT'),
    ({'active', 'supine'}, 'SUP'),
    ({'active', 'imperative'}, 'IMP'),
    ({'infinitive', 'passive'}, 'INF_PASS'),
    ({'indicative', 'passive', 'present'}, 'PRS_PASS'),
    ({'indicative', 'passive', 'past'}, 'PRT_PASS'),
    ({'passive', 'supine'}, 'SUP_PASS'),
]

ADJ_SLOTS = [
    ({'indefinite', 'positive', 'error-unrecognized-form'}, 'POS_UTR'),
    ({'indefinite', 'neuter', 'positive', 'singular'}, 'POS_NEU'),
    ({'indefinite', 'plural', 'positive'}, 'POS_PL'),
    ({'definite', 'positive'}, 'POS_DEF'), # exceptional
    ({'comparative', 'indefinite', 'plural'}, 'COMP'),
    ({'superlative', 'indefinite', 'plural'}, 'SUPERL_IND'),
    ({'superlative', 'definite'}, 'SUPERL_DEF'),
]

POS_SLOTS = {'noun': NOUN_SLOTS, 'verb': VERB_SLOTS, 'adj': ADJ_SLOTS}


def get_slot(pos, tagset):
    for tags, slot in POS_SLOTS.get(pos, []):
        if tags <= tagset:
            return slot
    return None

TAGS_TO_EXCLUDE = {
    'archaic', 'dated', 'table-tags', 'form-of',
    'inflection-template', 'obsolete',
}

INVALID_POS = {
    'character', 'symbol', 'punct', 'interfix', 'suffix', 'prefix',
    'phrase', 'prep_phrase', 'proverb', 'contraction',
}

def main():
    
    seen = set()
    
    with sqlite3.connect(DB_PATH) as conn, \
        open(RAW_DIR / "kaikki.org-dictionary-Swedish.jsonl") as f:

        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS sv_wiktionary")
        c.execute("""
            CREATE TABLE IF NOT EXISTS sv_wiktionary (
                id INTEGER PRIMARY KEY,
                braxen_ids TEXT,
                lemma TEXT NOT NULL,
                pos TEXT NOT NULL,
                gender TEXT,
                which_lexeme INTEGER DEFAULT 0,
                form TEXT NOT NULL,
                slot TEXT
            )
        """)

        batch = []
        for line in f:
            data = json.loads(line)
            word = data["word"]
            pos = data["pos"]
            if pos in INVALID_POS:
                continue

            if word in {"känna som sin egen ficka", "låtsas som att det regnar"}:
                continue

            gender = None
            senses = data.get("senses", [])
            first_sense = senses[0]
            if any(" century spelling" in g for g in first_sense.get("glosses", [])):
                continue
                
            if all(set(sense.get("tags", [])) & TAGS_TO_EXCLUDE for sense in senses):
                continue
            first_tags = set(first_sense.get("tags", []))
            if first_tags & {'form-of', 'misspelling', 'alt-of'}:
                continue
            if {"common-gender", "neuter"} <= first_tags:
                gender = "e" # either
            elif "common-gender" in first_tags:
                gender = "c"
            elif "neuter" in first_tags:
                gender = "n"

            if pos == "noun" and not gender:
                continue

            which_lexeme = data.get("etymology_number", 0)
            key = f"{word}|{pos}|{gender or ""}|{str(which_lexeme)}"
            # Allow multiple lexemes even when not clear from the data (e.g. påta)
            if key in seen:
                which_lexeme += 1
                key = f"{word}|{pos}|{gender or ""}|{str(which_lexeme)}"
            seen.add(key)

            if word == "hov" and which_lexeme == 3:
                continue
            if word == "må" and which_lexeme == 1:
                continue
            
            table_tags = 0
            for form_data in data.get("forms", []):
                if table_tags > 1:
                    break

                form = form_data["form"]
                if form == "-":
                    continue
                try:
                    tagset = set(form_data["tags"])
                except KeyError:
                    continue

                # Wiktionary errors
                if word == "avsäga":
                    form = form.replace("från", "av")
                if word == "dra en Tarzan":
                    form = form.replace("dra en Tarzan en Tarzan", "dra en Tarzan")
                    form = form.replace("dra en Tarzans en Tarzan", "dras en Tarzan")
                if word == "kaskadspy" and "kaskad" not in form:
                    form = "kaskad" + form
                if word == "räkna ned":
                    form = form.replace("upp", "ned")
                if word == "framlägga" and " fram" in form:
                    form = "fram" + form.replace(" fram", "")
                if word == "huta":
                    word = "huta åt"
                if form == "förkylr sig":
                    form = "förkyler sig"
                if word == "annalka":
                    pos = "adj"
                    word = "annalkande"
                if form == "bedjande":
                    pos = "adj"
                    word = "bedjande"

                if tagset & TAGS_TO_EXCLUDE:
                    if "table-tags" in tagset:
                        # Take only the first table to prevent duplicates
                        # Subsequent tables might be for archaic forms, etc.
                        table_tags += 1
                    continue
                if pos == 'adj' and 'masculine' in tagset:
                    continue
                
                slot = get_slot(pos, tagset)
                if pos == "verb" and 'participle' in tagset:
                    slot = 'PRS_PART' if 'present' in tagset else 'PRT_PART'
                    batch.append((word, 'participle', gender, which_lexeme, form, slot))
                    continue
                elif slot is None:
                    continue
                elif slot == "POS_DEF" and not form.endswith(("lilla", "blå", "blåa", "grå", "gråa")):
                    continue

                if word == "forkyla sig" and slot == "INF":
                    form = "forkyla sig"

                batch.append((word, pos, gender, which_lexeme, form, slot))
            
            if pos not in {"verb", "adj", "noun", "participle"} or not data.get("forms"):
                batch.append((word, pos, gender, which_lexeme, word, None))
            
            if len(batch) == 1000:
                c.executemany(
                    "INSERT INTO sv_wiktionary (lemma, pos, gender, which_lexeme, form, slot) VALUES (?, ?, ?, ?, ?, ?)", batch)
                batch = []

        # Second sense of förslag; should be a separate lexeme
        batch.append(('förslag', 'noun', 'n', 1, 'förslag', 'IND_SG'))
        batch.append(('förslag', 'noun', 'n', 1, 'förslaget', 'DEF_SG'))
        # Second sense of kanon (KAnon); should be a separate lexeme
        batch.append(('kanon', 'noun', 'c', 1, 'kanon', 'IND_SG'))
        batch.append(('kanon', 'noun', 'c', 1, 'kanonen', 'DEF_SG'))
        batch.append(('kanon', 'noun', 'c', 1, 'kanoner', 'IND_PL'))
        batch.append(('kanon', 'noun', 'c', 1, 'kanonerna', 'DEF_PL'))
        # Sixth sense of mark (pl. mArkEr); should be a separate lexeme
        batch.append(('mark', 'noun', 'c', 1, 'mark', 'IND_SG'))
        batch.append(('mark', 'noun', 'c', 1, 'marken', 'DEF_SG'))
        batch.append(('mark', 'noun', 'c', 1, 'marker', 'IND_PL'))
        batch.append(('mark', 'noun', 'c', 1, 'markerna', 'DEF_PL'))
        # Second sense of jam (jam session); only in Swedish Wiktionary; should be a separate lexeme
        batch.append(('jam', 'noun', 'n', 1, 'jam', 'IND_SG'))
        batch.append(('jam', 'noun', 'n', 1, 'jammet', 'DEF_SG'))
        batch.append(('jam', 'noun', 'n', 1, 'jam', 'IND_PL'))
        batch.append(('jam', 'noun', 'n', 1, 'jammena', 'DEF_PL'))
        # Second sense of cykel; should be a separate lexeme
        batch.append(('cykel', 'noun', 'c', 1, 'cykel', 'IND_SG'))
        batch.append(('cykel', 'noun', 'c', 1, 'cykeln', 'DEF_SG'))
        batch.append(('cykel', 'noun', 'c', 1, 'cyklar', 'IND_PL'))
        batch.append(('cykel', 'noun', 'c', 1, 'cyklarna', 'DEF_PL'))

        c.executemany(
            "INSERT INTO sv_wiktionary (lemma, pos, gender, which_lexeme, form, slot) VALUES (?, ?, ?, ?, ?, ?)", batch)

        conn.commit()
        c.execute(
            "CREATE INDEX idx_paradigm ON sv_wiktionary(lemma, pos, which_lexeme);")
        conn.commit()
    
if __name__ == "__main__":
    main()