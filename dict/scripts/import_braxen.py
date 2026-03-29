import sqlite3
from dict.scripts.paths import DB_PATH, RAW_DIR
import lemmy

NORMALIZED_POS = {
    "AB": "adv",
    "ACR": "noun",  # acronym
    "DT": "det",
    "HA": "adv",
    "HD": "det",
    "HP": "pron",
    "HS": "pron",
    "IE": "particle",
    "IN": "intj",
    "JJ": "adj",
    "KN": "conj",
    "NN": "noun",
    "PC": "participle",
    "PL": "adv",
    "PM": "name",
    "PN": "pron",
    "PP": "prep",
    "PS": "pron",
    "RG": "num",
    "RO": "num",
    "SN": "conj",
    "VB": "verb",
}

PHONETIC_CORRECTIONS = {
    "Andra Timotheosbrevet": "a n . d r ,a | t i . m oh . t 'e: . u s - b r ,e: . v ex t",
    "neofascism": "n eh . u . f a . rs 'i s m",
    "kreationister": "k r e: . a . x u . n 'i . s t ex r",
    "överlevnadschans": '"ö: . v ex r - l e: v . n a d s - c ,a n s',
    "operatorer": "u . p eh . r a . t 'u: . r ex r",
    "mogul": "m o: . g 'uu l",
    "mambor": "m 'a m . b u r",
    "kejsarinnor": 'c ä j . s a . r "i . n ,u r',
    "kejsarinnorna": 'c ä j . s a . r "i . n ,u . rn a',
    "frikativa": 'f r "i . k a . t ,i: . v a',
    "frikativan": 'f r "i . k a . t ,i: . v a n',
    "försiggås": 'f "oe: ~ rs i - g ,o: s',
    "försiggick": 'f "oe: ~ rs i - j ,i k',
    "beredes": "b eh . r 'e: . d ex s",
    "borna": 'b "u: . rn ,a',
}

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS braxen")
    c.execute("""
        CREATE TABLE IF NOT EXISTS braxen (
            id INTEGER PRIMARY KEY,
            word TEXT NOT NULL,
            lemma TEXT,
            pos TEXT NOT NULL,
            morph TEXT,
            phonetic TEXT,
            syllables TEXT,
            stress TEXT,
            irregular_pron JSON
        )
    """)
    with open(RAW_DIR / "braxen-sv.tsv", "r", encoding="utf-8") as f:
        batch = []
        for line in f:
            try:
                word, phonetic, posmorph, lang, *_ = line.strip().split("\t")
            except ValueError:
                continue
            if "ﬁ" in word or "ﬂ" in word or "ﬀ" in word:
                continue
            if "GEN" in posmorph or "SMS" in posmorph:
                continue
            if lang != "swe":
                continue

            if " " in posmorph:
                pos, morph = posmorph.split(" ", 1)
            else:
                pos = posmorph
                morph = None
            
            pos = NORMALIZED_POS[pos]

            # Exceptions and corrections
            if word == "Kirgizistan" and phonetic == "k i r . g i . s t 'a: n":
                continue
            if word == "Makedonien" and phonetic == "m a d . k eh . d 'u: . n ih . ex n":
                continue
            if word in {"not", "notar", "notarna", "noten"} and "o" in phonetic:
                continue
            if word in {"friser", "friserna"} and "f r 'i: . s" in phonetic:
                continue

            for w, p in PHONETIC_CORRECTIONS.items():
                if word == w:
                    phonetic = p
                    break

            if word == "ön":
                morph = morph.replace("SIN", "PLU")
            if word == "scharlakan":
                morph = morph.replace("UTR", "NEU")
            if word == "manskör":
                pos = "noun"
                morph = "UTR SIN IND NOM"
            if word == "akvarierna":
                morph = "NEU PLU DEF NOM"
            if word == "minnes":
                pos = "verb"
                morph = "PRS SFO"
            if "f uu ng . k . x ,u: n" in phonetic:
                phonetic = phonetic.replace("f uu ng . k . x ,u: n", "f uu n g k . x ,u: n")
            
            batch.append((word, pos, morph, phonetic))
            if len(batch) == 1000:
                c.executemany("INSERT INTO braxen (word, pos, morph, phonetic) VALUES (?, ?, ?, ?)", batch)
                batch = []

        if batch:
            c.executemany("INSERT INTO braxen (word, pos, morph, phonetic) VALUES (?, ?, ?, ?)", batch)

    conn.commit()


def add_lemmas():
    with sqlite3.connect(DB_PATH) as conn:
        lem = lemmy.load("sv")

        c = conn.cursor()
        c.execute("""
            SELECT id, pos, morph, word FROM braxen 
            WHERE pos IN ('noun', 'verb', 'adj', 'participle')
        """)

        batch = []
        for (id, pos, morph, word) in c.fetchall():
            morph = set(morph.split()) if morph else set()

            is_base_form = (
                (pos == "noun" and {"SIN", "NOM", "IND"} <= morph) or
                (pos == "verb" and {"INF", "AKT"} <= morph) or
                (pos == "adj" and {"POS", "UTR", "SIN", "IND", "NOM"} <= morph)
            )

            if is_base_form:
                batch.append((word, id))
                continue

            lemma = []
            if pos == "participle":
                pos = "verb"

            if "-" in word:
                static_parts = word[:word.rfind("-") + 1]
                inflecting_part = word[word.rfind("-") + 1:]
                lemma = [static_parts + l for l in
                        lem.lemmatize(pos.upper(), inflecting_part)]
            else:
                lemma = lem.lemmatize(pos.upper(), word)
                if word == "poppen" and lemma == "popp":
                    lemma = "pop"
                if word in {"kikärta", "kikärtan", "kikärtor", "kikärtorna"}:
                    lemma = "kikärta"

            if not is_base_form and len(lemma) > 1:
                lemma = [l for l in lemma if l != word]

            lemma = ",".join(lemma)
            batch.append((lemma, id))
            
            if len(batch) >= 1000:
                c.executemany("UPDATE braxen SET lemma = ? WHERE id = ?", batch)
                batch = []
                conn.commit()

        if batch:
            c.executemany("UPDATE braxen SET lemma = ? WHERE id = ?", batch)
            conn.commit()
        
        c.execute("""
            UPDATE braxen SET lemma = word WHERE pos NOT IN ('noun', 'verb', 'adj', 'participle');
        """)

        c.execute("""
            DELETE FROM braxen WHERE lemma = 'signa' AND phonetic NOT LIKE '%s "a j%'
        """)
        conn.commit()

        c.execute("CREATE INDEX IF NOT EXISTS braxen_lemma_idx ON braxen (lemma);")
        c.execute("CREATE INDEX IF NOT EXISTS braxen_word_idx ON braxen (word);")
            

if __name__ == "__main__":
    main()
    add_lemmas()