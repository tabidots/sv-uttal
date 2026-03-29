import sqlite3
from dict.scripts.paths import DB_PATH
from collections import Counter

SLOT_TO_MORPH = {
    # nouns
    'IND_SG': ('SIN', 'IND'),
    'DEF_SG': ('SIN', 'DEF'),
    'IND_PL': ('PLU', 'IND'),
    'DEF_PL': ('PLU', 'DEF'),
    # adjectives
    'POS_UTR': ('POS', 'UTR', 'SIN'),
    'POS_NEU': ('POS', 'NEU', 'SIN'),
    'POS_PL': ('PLU',),
    'POS_DEF': ('SIN', 'DEF'),
    'COMP': ('KOM',),
    'SUPERL_IND': ('SUV', 'IND'),
    'SUPERL_DEF': ('SUV', 'DEF'),
    # verbs
    'INF': ('INF', 'AKT'),
    'PRS': ('PRS', 'AKT'),
    'PRT': ('PRT', 'AKT'),
    'SUP': ('SUP', 'AKT'),
    'IMP': ('IMP', 'AKT'),
    'INF_PASS': ('INF', 'SFO'),
    'PRS_PASS': ('PRS', 'SFO'), # this is the same as the infinitive passive
    'PRT_PASS': ('PRT', 'SFO'),
    'SUP_PASS': ('SUP', 'SFO'),
    'IMP_PASS': ('IMP', 'SFO'),
    # participles
    'PRS_PART': ('PRS',),
    'PRT_PART': ('PRF', 'UTR', 'SIN', 'IND', 'NOM'),
}


def morph_matches_slot(morph, slot, gender=None):
    if not slot or slot not in SLOT_TO_MORPH:
        return True  # non-noun slots, pass through
    required = SLOT_TO_MORPH[slot]
    if not gender or gender == "e":
        return all(r in morph for r in required)
    if gender == "c":
        return all(r in morph for r in required) and "UTR" in morph
    if gender == "n":
        return all(r in morph for r in required) and "NEU" in morph


def main():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        # Load Braxen into memory
        c.execute(
            "SELECT id, word, lemma, pos, morph FROM braxen")
        braxen_lookup = {}
        for braxen_id, word, lemma, pos, morph in c.fetchall():
            key = (word, pos)
            if key not in braxen_lookup:
                braxen_lookup[key] = []
            braxen_lookup[key].append((braxen_id, lemma, morph))

        # Process Wiktionary rows
        c.execute(
            "SELECT id, lemma, pos, gender, form, slot FROM sv_wiktionary")
        wiktionary_rows = c.fetchall()

        batch = []
        for wik_id, lemma, pos, gender, form, slot in wiktionary_rows:
            matching_ids = [
                str(braxen_id)
                for braxen_id, braxen_lemma, morph in braxen_lookup.get((form, pos), [])
                if lemma in braxen_lemma.split(",")
                and morph_matches_slot(morph or set(), slot, gender=gender)
            ]

            batch.append(((",".join(matching_ids) or None), wik_id))

        c.executemany(
            "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", batch)
        conn.commit()


def add_phrasal_verbs_to_braxen():
    """Supplement missing phonetic data in Braxen for phrasal verbs."""

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        # If this function has already been run, don't run it again
        c.execute("""
            SELECT 1 FROM braxen WHERE lemma = 'komma ihåg';
        """)
        if c.fetchone():
            return

        c.execute("""
            SELECT form FROM sv_wiktionary
            WHERE lemma LIKE '% %' AND form LIKE '% %' AND pos = 'verb';
        """)
        parts = {part for (form,) in c.fetchall() for part in form.split()}
        c.execute("""
            CREATE TEMP TABLE IF NOT EXISTS tmp_verb_parts (
                part TEXT NOT NULL,
                phonetic TEXT,
                syllables TEXT,
                stress TEXT
            )
        """)
        c.executemany("""
            INSERT INTO tmp_verb_parts (part) VALUES (?);
        """, [(part,) for part in parts])
        c.execute("""
            UPDATE tmp_verb_parts
            SET phonetic = b.phonetic,
                syllables = b.syllables,
                stress = b.stress
            FROM braxen b
            WHERE b.word = tmp_verb_parts.part;
        """)

        c.execute("SELECT part, phonetic, syllables, stress FROM tmp_verb_parts")
        parts_lookup = {
            part: {
                "phonetic": phonetic,
                "syllables": syllables,
                "stress": stress
            }
            for part, phonetic, syllables, stress in c.fetchall()
        }
        c.execute("""
            SELECT lemma, form FROM sv_wiktionary
            WHERE lemma LIKE '% %' AND pos = 'verb';
        """)
        batch = []
        for (lemma, form) in c.fetchall():
            parts = form.split()
            try:
                phonetic = " | ".join(
                    parts_lookup[part]["phonetic"] for part in parts)
                syllables = " ".join(
                    parts_lookup[part]["syllables"] for part in parts)
                stress = parts_lookup[parts[0]]["stress"]
            except TypeError, KeyError:
                continue
            batch.append((lemma, form, 'verb', phonetic, syllables, stress))

        c.executemany("""
            INSERT INTO braxen (lemma, word, pos, phonetic, syllables, stress)
            VALUES (?, ?, ?, ?, ?, ?);
        """, batch)
        conn.commit()

        c.execute("""
            UPDATE sv_wiktionary
            SET braxen_ids = CAST(b.id AS TEXT)
            FROM braxen b
            WHERE b.word = sv_wiktionary.form
                AND b.pos = 'verb'
                AND sv_wiktionary.form LIKE '% %'
                AND sv_wiktionary.pos = 'verb';
        """)
        conn.commit()


def fill_in_missing_braxen_ids():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        # Load all wiktionary rows with braxen_ids into a lookup
        c.execute("""
            SELECT lemma, pos, which_lexeme, slot, form, braxen_ids 
            FROM sv_wiktionary 
            WHERE braxen_ids IS NOT NULL
        """)
        filled = c.fetchall()

        # Build lookups
        inf_lookup = {}      # (lemma, pos, which_lexeme) -> braxen_ids
        form_lookup = {}     # (lemma, pos, which_lexeme, form) -> braxen_ids

        for lemma, pos, which_lexeme, slot, form, braxen_ids in filled:
            key = (lemma, pos, which_lexeme)
            if slot == 'INF':
                inf_lookup[key] = braxen_ids
            form_key = (lemma, pos, which_lexeme, form)
            if form_key not in form_lookup:
                form_lookup[form_key] = braxen_ids

        # Fix IMP = INF
        c.execute("""
            SELECT id, lemma, pos, which_lexeme
            FROM sv_wiktionary 
            WHERE slot = 'IMP' AND form = lemma AND braxen_ids IS NULL
        """)
        imp_batch = [
            (inf_lookup.get((lemma, pos, which_lexeme)), row_id)
            for row_id, lemma, pos, which_lexeme in c.fetchall()
        ]
        imp_batch = [(b, i) for b, i in imp_batch if b is not None]
        c.executemany(
            "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", imp_batch)

        # Fix INF_PASS syncretism
        c.execute("""
            SELECT id, lemma, pos, which_lexeme, form 
            FROM sv_wiktionary 
            WHERE slot = 'INF_PASS' AND braxen_ids IS NULL
        """)
        pass_batch = [
            (form_lookup.get((lemma, pos, which_lexeme, form)), row_id)
            for row_id, lemma, pos, which_lexeme, form in c.fetchall()
        ]
        pass_batch = [(b, i) for b, i in pass_batch if b is not None]
        c.executemany(
            "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", pass_batch)
        
        # Fix IND_PL syncretism (null-plural cases)
        c.execute("""
            SELECT w2.id, w1.braxen_ids
            FROM sv_wiktionary w1
            JOIN sv_wiktionary w2 ON w2.lemma = w1.lemma 
                AND w2.pos = w1.pos
                AND w2.which_lexeme = w1.which_lexeme
            WHERE w1.braxen_ids NOT LIKE '%,%'
                AND w2.braxen_ids IS NULL
                AND w1.slot = 'IND_SG'
                AND w2.slot = 'IND_PL'
                AND w1.form = w2.form
        """)
        pl_batch = [(b, i) for i, b in c.fetchall() if b is not None]
        # Fix DEF_PL syncretism (where DEF_SG = DEF_PL)
        c.execute("""
            SELECT w4.id, w2.braxen_ids
            FROM sv_wiktionary w2
            JOIN sv_wiktionary w4 ON w4.lemma = w2.lemma
                AND w4.pos = w2.pos
                AND w4.which_lexeme = w2.which_lexeme
            WHERE w2.braxen_ids NOT LIKE '%,%'
                AND w4.braxen_ids IS NULL
                AND w2.form = w4.form
                AND w2.slot = 'DEF_SG'
                AND w4.slot = 'DEF_PL'
        """)
        pl_batch += [(b, i) for i, b in c.fetchall() if b is not None]

        # Fix DEF_SG syncretism (where IND_SG = DEF_SG)
        c.execute("""
            SELECT w2.id, w1.braxen_ids
            FROM sv_wiktionary w1
            JOIN sv_wiktionary w2 ON w2.lemma = w1.lemma
                AND w2.pos = w1.pos
                AND w2.which_lexeme = w1.which_lexeme
            WHERE w1.braxen_ids NOT LIKE '%,%'
                AND w2.braxen_ids IS NULL
                AND w1.form = w2.form
                AND w1.slot = 'IND_SG'
                AND w2.slot = 'DEF_SG'
        """)
        pl_batch += [(b, i) for i, b in c.fetchall() if b is not None]

        c.executemany(
            "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", pl_batch)
        
        # Fix missing adjective forms for a handful of forms where Braxen considers them adverbs
        c.execute("""
            SELECT word, id FROM braxen WHERE word IN
            ('bäst', 'störst', 'äldst', 'längre', 'längst', 'minst', 'sämst', 'värst')
            AND syllables IS NOT NULL
            GROUP BY word
        """)
        adv_lookups = {
            word: str(braxen_id) for word, braxen_id in c.fetchall()
        }
        c.execute("""
            SELECT id, form FROM sv_wiktionary
            WHERE pos = 'adj' AND braxen_ids IS NULL
            AND form IN ('bäst', 'störst', 'äldst', 'längre', 'längst', 'minst', 'sämst', 'värst')
        """)
        adj_batch = [
            (adv_lookups.get(form), row_id) for row_id, form in c.fetchall()
        ]
        adj_batch = [(b, i) for b, i in adj_batch if b is not None]
        c.executemany(
            "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", adj_batch)
        conn.commit()
        
        # Supplement participles with any word matching the form, regardless of POS
        c.execute("""
            UPDATE sv_wiktionary
            SET braxen_ids = CAST(b.id AS TEXT)
            FROM braxen b
            WHERE b.word = sv_wiktionary.form
                AND sv_wiktionary.pos = 'participle'
                AND sv_wiktionary.braxen_ids IS NULL
        """)
        conn.commit()

        # Supplement definite and plural adjectives with any word matching the form and lemma
        c.execute("""
            UPDATE sv_wiktionary
            SET braxen_ids = CAST(b.id AS TEXT)
            FROM braxen b
            WHERE b.word = sv_wiktionary.form
                AND b.lemma = sv_wiktionary.lemma
                AND sv_wiktionary.slot IN ('POS_DEF', 'POS_PL')
                AND sv_wiktionary.pos = 'adj'
                AND b.pos = 'adj'
                AND sv_wiktionary.braxen_ids IS NULL
        """)
        
        conn.commit()


def resolve_ambiguous():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c2 = conn.cursor()

        c.execute("""
            SELECT b.id, w.lemma
            FROM sv_wiktionary w
                LEFT JOIN
                braxen b ON CAST (w.braxen_ids AS INTEGER) = b.id AND
                            w.pos = b.pos
            WHERE w.braxen_ids NOT LIKE '%,%' AND b.lemma LIKE '%,%'
        """)
        batch = [(lemma, b_id) for b_id, lemma in c.fetchall()]
        c.executemany("""
            UPDATE braxen SET lemma = ?
            WHERE id = ?
        """, batch)
        conn.commit()

        # Ambiguous definite plural nouns
        c.execute("""
            SELECT w1.id, w1.braxen_ids as ambiguous_ids, b_sg.stress as sg_stress
            FROM sv_wiktionary w1
            JOIN sv_wiktionary w2 ON w2.lemma = w1.lemma 
                AND w2.pos = w1.pos
                AND w1.pos = 'noun'
                AND w2.which_lexeme = w1.which_lexeme
                AND w2.slot = 'IND_PL'
                AND w2.form <> w1.form
            JOIN braxen b_sg ON b_sg.id = CAST(w2.braxen_ids AS INTEGER)
            WHERE w1.braxen_ids LIKE '%,%'
        """)

        batch = []
        for wik_id, ambiguous_ids, sg_stress in c.fetchall():
            ids = ambiguous_ids.split(",")
            c2.execute(f"""
                SELECT id, stress FROM braxen 
                WHERE id IN ({','.join('?' * len(ids))})
            """, ids)
            candidates = c2.fetchall()

            # Pick candidate whose stress pattern matches IND_PL
            # accent 2 IND_PL (0-1) -> prefer DEF_SG with secondary stress
            # accent 1 IND_PL (0) -> prefer DEF_SG without secondary stress
            has_secondary = sg_stress and "-" in sg_stress
            match = next(
                (str(cid) for cid, cstress in candidates
                if cstress and ("-" in cstress) == has_secondary),
                None
            )
            if match:
                batch.append((match, wik_id))

        c.executemany("UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", batch)
        conn.commit()

        # Build IND_SG stress lookup from unambiguous rows
        c.execute("""
            SELECT w.lemma, b.stress
            FROM sv_wiktionary w
            JOIN braxen b ON b.id = CAST(w.braxen_ids AS INTEGER)
            WHERE w.slot = 'IND_SG'
            AND w.braxen_ids NOT LIKE '%,%'
        """)
        ind_sg_stress = dict(c.fetchall())  # lemma -> stress

        c.execute("""
            SELECT id, lemma, braxen_ids FROM sv_wiktionary
            WHERE braxen_ids LIKE '%,%'
        """)
        batch = []
        for wik_id, wik_lemma, braxen_ids in c.fetchall():
            ids = braxen_ids.split(",")
            c2.execute(f"""
                SELECT id, lemma, stress FROM braxen
                WHERE id IN ({','.join('?' * len(ids))})
            """, ids)
            candidates = c2.fetchall()

            expected_stress = ind_sg_stress.get(wik_lemma)
            has_secondary = expected_stress and "-" in expected_stress

            match = next(
                (str(bid) for bid, b_lemma, b_stress in candidates
                if wik_lemma in (b_lemma or "").split(",")
                and b_stress and ("-" in b_stress) == has_secondary),
                None
            )
            if match:
                batch.append((match, wik_id))

        c.executemany("UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", batch)
        conn.commit()

        # Specific ambiguous cases that aren't originally separate lexemes in Wiktionary
        c.execute("""
            SELECT id, word, phonetic FROM braxen 
            WHERE lemma = 'kanon'
        """)
        b_ids_by_phonetic = {0: {}, 1: {}}
        for bid, word, p in c.fetchall():
            if "'u:" in p:
                b_ids_by_phonetic[0][word] = str(bid)
            else:
                b_ids_by_phonetic[1][word] = str(bid)
        c.execute("""
            SELECT id, form, which_lexeme FROM sv_wiktionary 
            WHERE lemma = 'kanon'
        """)
        for wik_id, form, which_lexeme in c.fetchall():
            b_id = None
            for word in b_ids_by_phonetic[which_lexeme]:
                if form == word:
                    b_id = b_ids_by_phonetic[which_lexeme][form]
                    break
            c.execute(
                "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", (b_id, wik_id))
        conn.commit()

        c.execute("""
            SELECT id, word, phonetic FROM braxen
            WHERE (word IN ('nubb', 'nubben') AND phonetic LIKE '%''uu%')
                  OR word IN ('nubbar', 'nubbarna');
        """)
        b_ids_by_phonetic = {}
        for bid, word, p in c.fetchall():
            b_ids_by_phonetic[word] = str(bid)
        c.execute("""
            SELECT id, form, which_lexeme FROM sv_wiktionary
            WHERE lemma = 'nubb';
        """)
        for wik_id, form, which_lexeme in c.fetchall():
            b_id = b_ids_by_phonetic[form]
            c.execute(
                "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", (b_id, wik_id))
        conn.commit()

        c.execute("""
            SELECT id, word, phonetic FROM braxen 
            WHERE lemma = 'hov'
        """)
        b_ids_by_phonetic = {1: {}, 2: {}}
        for bid, word, p in c.fetchall():
            if "u:" in p:
                b_ids_by_phonetic[1][word] = str(bid)
            else:
                b_ids_by_phonetic[2][word] = str(bid)
        c.execute("""
            SELECT id, form, which_lexeme FROM sv_wiktionary 
            WHERE lemma = 'hov'
        """)
        for wik_id, form, which_lexeme in c.fetchall():
            b_id = None
            for word in b_ids_by_phonetic[which_lexeme]:
                if form == word:
                    b_id = b_ids_by_phonetic[which_lexeme][form]
                    break
            c.execute(
                "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", (b_id, wik_id))
        conn.commit()

        c.execute("""
            SELECT id, word, phonetic FROM braxen 
            WHERE word IN ('cykel', 'cykeln')
        """)
        b_ids_by_phonetic = {0: {}, 1: {}}
        for bid, word, p in c.fetchall():
            if 'y:' in p:
                b_ids_by_phonetic[1][word] = str(bid)
            else:
                b_ids_by_phonetic[0][word] = str(bid)
        c.execute("""
            SELECT id, form, which_lexeme FROM sv_wiktionary 
            WHERE form IN ('cykel', 'cykeln')
        """)
        for wik_id, form, which_lexeme in c.fetchall():
            b_id = None
            for word in b_ids_by_phonetic[which_lexeme]:
                if form == word:
                    b_id = b_ids_by_phonetic[which_lexeme][form]
                    break
            c.execute(
                "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", (b_id, wik_id))
        conn.commit()

        c.execute("""
            SELECT id, word, phonetic FROM braxen 
            WHERE word IN ('marker', 'markerna');
        """)
        b_ids_by_phonetic = {0: {}, 1: {}}
        for bid, word, p in c.fetchall():
            if ",ex" in p:
                b_ids_by_phonetic[1][word] = str(bid)
            else:
                b_ids_by_phonetic[0][word] = str(bid)
        c.execute("""
            SELECT id, form, which_lexeme FROM sv_wiktionary 
            WHERE form IN ('marker', 'markerna');
        """)
        for wik_id, form, which_lexeme in c.fetchall():
            b_id = None
            for word in b_ids_by_phonetic[which_lexeme]:
                if form == word:
                    b_id = b_ids_by_phonetic[which_lexeme][form]
                    break
            c.execute(
                "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", (b_id, wik_id))
        conn.commit()

        c.execute("""
            UPDATE sv_wiktionary 
            SET braxen_ids = CAST(b.id AS TEXT)
            FROM braxen b
            WHERE sv_wiktionary.form = 'lov' AND sv_wiktionary.which_lexeme = 1
            AND b.phonetic = 'l ''o: v';
        """)

        c.execute("""
            UPDATE sv_wiktionary 
            SET braxen_ids = CAST(b.id AS TEXT)
            FROM braxen b
            WHERE sv_wiktionary.form IN ('lavar', 'lavarna')
            AND b.word = sv_wiktionary.form;
        """)

        c.execute("""
            UPDATE sv_wiktionary 
            SET braxen_ids = CAST(b.id AS TEXT)
            FROM braxen b
            WHERE sv_wiktionary.form = 'synkopen' AND sv_wiktionary.lemma = 'synkop'
            AND b.phonetic = 's y ng . k ''o: . p ex n';
        """)

        c.execute("""
            UPDATE sv_wiktionary 
            SET braxen_ids = CAST(b.id AS TEXT)
            FROM braxen b
            WHERE sv_wiktionary.form = 'byte' AND sv_wiktionary.which_lexeme = 2
            AND b.phonetic = 'b ''a j';
        """)

        c.execute("""
            UPDATE sv_wiktionary 
            SET braxen_ids = CAST(b.id AS TEXT)
            FROM braxen b
            WHERE sv_wiktionary.form = 'raster' and sv_wiktionary.lemma = 'raster'
            AND b.phonetic = '''a';
        """)

        # Everything else
        c.execute("""
            SELECT id, lemma, slot, pos, form, gender, which_lexeme, braxen_ids FROM sv_wiktionary
            WHERE braxen_ids LIKE '%,%'
        """)
        batch = []
        for wik_id, wik_lemma, slot, wik_pos, form, gender, which_lexeme, braxen_ids in c.fetchall():
            ids = braxen_ids.split(",")
            if wik_lemma in {'kommersialisera', 'shiamuslimsk', 'parser'}:
                # Just take the first one
                batch.append((ids[0], wik_id))
                continue
            placeholders = ",".join("?" * len(ids))
            c2.execute(f"""
                SELECT id, pos, morph, phonetic, syllables, stress FROM braxen
                WHERE id IN ({placeholders})
            """, ids)
            results = c2.fetchall()
            unique_phonetics = {p for _, _, _, p, _, _ in results}
            if len(unique_phonetics) == 1 or wik_pos == "name":
                # Just take the first one
                batch.append((ids[0], wik_id))
                continue
            for b_id, b_pos, morph, phonetic, syllables, stress in results:
                if any(all(conditions) for conditions in
                       [
                           [wik_lemma == "beta", which_lexeme < 4, '"e' in phonetic],
                           [wik_lemma == "beta", which_lexeme == 4, ",a" in phonetic],
                           [wik_lemma == "kapris", which_lexeme == 1, "'i:" in phonetic],
                           [wik_lemma == "kapris", which_lexeme == 2, "'a:" in phonetic],
                           [wik_lemma == "kis", which_lexeme == 1, "k" in phonetic],
                           [wik_lemma == "kis", which_lexeme == 2, "c" in phonetic],
                           [wik_lemma == "köra", which_lexeme == 1, "c" in phonetic],
                           [wik_lemma == "köra", which_lexeme == 2, "k" in phonetic],
                           [wik_lemma == "killa", "k" in phonetic],
                           [wik_lemma == "kola", which_lexeme == 2, "o:" in phonetic],
                           [wik_lemma == "kola", which_lexeme == 3, "u:" in phonetic],
                           [wik_lemma == "lova", which_lexeme == 1, "o:" in phonetic],
                           [wik_lemma == "lova", which_lexeme == 2, "u:" in phonetic],
                           [wik_lemma == "regel", which_lexeme == 1, "'e:" in phonetic],
                           [wik_lemma == "regel", which_lexeme == 2, '"e:' in phonetic],
                           [wik_lemma == "polska", '"o' in phonetic],
                           [wik_lemma == "lama", '"a:' in phonetic],
                           [wik_lemma == "stalla", not "o:" in phonetic],
                           [wik_lemma == "ådra", "-" in phonetic],
                           [wik_lemma == "vederbörande", "-" in phonetic],
                           [wik_lemma == "påta", which_lexeme == 1, "-" in phonetic],
                           [wik_lemma == "ålägga", "-" in phonetic],
                           [wik_lemma == "hängar", not "-" in phonetic],
                           [wik_lemma == "påta", which_lexeme == 0, not "-" in phonetic],
                           [wik_lemma == "åla", not "-" in phonetic],
                           [wik_lemma == "förbunden", not '"' in phonetic],
                           [wik_lemma == "förtrycka", not '"' in phonetic],
                           [wik_lemma == "förslag", which_lexeme == 0, not "-" in phonetic],
                           [wik_lemma == "förslag", which_lexeme == 1, "-" in phonetic],
                           [wik_lemma == "åta", ",a:" in phonetic],
                           [wik_lemma == "förstå", "'o:" in phonetic],
                           [wik_lemma == "intersektionalitet", "~" in phonetic],
                           [wik_lemma == "förmyndarskap", "~" in phonetic],
                           [wik_lemma == "dolme", ",ex" in phonetic],
                           [wik_lemma == "sinka", "ng" in phonetic],
                           [wik_lemma == "ståt", '"' not in phonetic],
                           [wik_lemma == "relativ", "r e . l a" in phonetic],
                           [wik_lemma == "släkte", ",ex" in phonetic],
                           [wik_lemma == "isomorfi", "i:" in phonetic],
                           [wik_lemma == "cytosin", "y ." in phonetic],
                           [wik_lemma == "väl", "'ä: l" in phonetic],
                           [wik_lemma == "förut", which_lexeme == 1, '"oe:' in phonetic],
                           [wik_lemma == "förut", which_lexeme == 2, "'oe:" in phonetic],
                           [wik_lemma == "ådra", ",a:" in phonetic],
                           [wik_lemma == "grammatik", "'a" in phonetic],
                           [wik_lemma == "historik", "'u:" in phonetic],
                           [wik_lemma == "praktik", "'a" in phonetic],
                           [wik_lemma == "teknik", "'e" in phonetic],
                           [wik_lemma == "polemik", "'e:" in phonetic],
                           [wik_lemma == "logiker", "'o:" in phonetic],
                           [wik_lemma == "klinik", "'i:" in phonetic],
                           [wik_lemma == "taktik", "'a" in phonetic],
                           [wik_lemma == "hemofili", "ä" in phonetic],
                           [wik_lemma == "länd", "'ä" in phonetic],
                           [wik_lemma == "fajt", ",ex" in phonetic],
                           [wik_lemma == "kreativ", "'e:" in phonetic],
                           [wik_lemma == "passiv", "'a" in phonetic],
                           [wik_lemma == "matte", '"a' in phonetic],
                           [wik_lemma == "hänga", "ng" in phonetic],
                           [wik_lemma == "tänka", "'ä" in phonetic],
                           [wik_lemma == "boren", '"u:' in phonetic],
                           [wik_lemma == "mull", "'uu" in phonetic],
                           [wik_lemma == "ide", ",ex" in phonetic],
                           [wik_lemma == "jam", "'a " in phonetic],
                           [wik_lemma == "mjölk", "k ex" in phonetic],
                           [wik_lemma == "finska", not "-" in phonetic],
                           [wik_lemma == "finsko", "-" in phonetic],
                           [wik_lemma == "krater", "'a" in phonetic],
                           [wik_lemma == "gem", "g" in phonetic],
                           [wik_lemma == "box", '"o' in phonetic],
                           [wik_lemma == "men", "'e:" in phonetic],
                           [wik_lemma == "pop", which_lexeme ==
                               0, not ":" in phonetic],
                           [wik_lemma == "pop", which_lexeme == 1, ":" in phonetic],
                       ]):
                    batch.append((b_id, wik_id))

        c.executemany("UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", batch)
        conn.commit()

        
if __name__ == "__main__":
    main()
    add_phrasal_verbs_to_braxen()
    fill_in_missing_braxen_ids()
    resolve_ambiguous()