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
}


def morph_matches_slot(morph, slot, gender):
    if not slot or slot not in SLOT_TO_MORPH:
        return True  # non-noun slots, pass through
    required = SLOT_TO_MORPH[slot]
    if not gender:
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
                and morph_matches_slot(morph or set(), slot, gender)
            ]
            
            batch.append(((",".join(matching_ids) or None), wik_id))

        c.executemany(
            "UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", batch)
        conn.commit()


def add_syncretic_ids():
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


def resolve_ambiguous():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c2 = conn.cursor()

        # Amibiguous definite plural nouns
        c.execute("""
            SELECT w1.id, w1.braxen_ids as ambiguous_ids, b_sg.stress as sg_stress
            FROM sv_wiktionary w1
            JOIN sv_wiktionary w2 ON w2.lemma = w1.lemma 
                AND w2.pos = w1.pos 
                AND w2.which_lexeme = w1.which_lexeme
                AND w2.slot = 'IND_SG'
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

            # Pick candidate whose stress pattern matches IND_SG
            # accent 2 IND_SG (0-1) -> prefer DEF_SG with secondary stress
            # accent 1 IND_SG (0) -> prefer DEF_SG without secondary stress
            has_secondary = sg_stress and "-" in sg_stress
            match = next(
                (str(cid) for cid, cstress in candidates
                if cstress and ("-" in cstress) == has_secondary),
                ids[0]  # fallback to first if no clear match
            )
            batch.append((match, wik_id))

        c.executemany("UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", batch)
        conn.commit()

        # Specific ambiguous verbs
        c.execute("""
            SELECT id, phonetic FROM braxen
            WHERE word = 'köra' AND morph = 'INF AKT'
        """)
        b_ids_by_phonetic = {}
        for bid, p in c.fetchall():
            if p.startswith("k"):
                b_ids_by_phonetic[2] = str(bid)
            else:
                b_ids_by_phonetic[1] = str(bid)

        c.execute("""
            SELECT id, which_lexeme FROM sv_wiktionary 
            WHERE form = 'köra' AND braxen_ids LIKE '%,%'
        """)
        for wik_id, which_lexeme in c.fetchall():
            b_id = b_ids_by_phonetic[which_lexeme]
            c.execute("UPDATE sv_wiktionary SET braxen_ids = ? WHERE id = ?", (b_id, wik_id))
        conn.commit()
        


def analyze():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT w.lemma,
                w.pos,
                w.which_lexeme,
                GROUP_CONCAT(w.slot) AS slots,
                GROUP_CONCAT(w.gender) AS genders,
                GROUP_CONCAT(b.syllables) AS syllables,
                GROUP_CONCAT(b.stress) AS stresses,
                count(*) AS count
            FROM sv_wiktionary w
            JOIN braxen b ON CAST(w.braxen_ids AS INTEGER) = b.id
            WHERE w.braxen_ids IS NOT NULL
            AND w.braxen_ids NOT LIKE '%,%'
            AND b.stress IS NOT NULL
            AND w.pos = 'noun'
            GROUP BY w.lemma, w.pos, w.which_lexeme
        """)

        exceptional_count = 0
        total_count = 0

        tor_sor = []
        ium = []
        ALL_STRESSES_BY_SLOT = Counter()

        for lemma, pos, which_lexeme, slots_g, genders_g, syllables_g, stresses_g, count in c.fetchall():
            slots = slots_g.split(",") if slots_g else ["" for _ in range(count)]
            genders = genders_g.split(",") if genders_g else ["" for _ in range(count)]

            if "|" in syllables_g:
                continue
            syllables = syllables_g.split(",")
            stresses = stresses_g.split(",")

            entries = list(zip(slots, genders, syllables, stresses))

            total_count += 1

            if not stresses:
                continue
            if len(set(stresses)) == 1:
                continue

            if lemma.endswith('tor') or lemma.endswith('sor'):
                tor_sor.append(lemma)
                continue
            if lemma.endswith('ium'):
                ium.append(lemma)
                continue

            stresses_by_slot = {}
            for slot, gender, syl, stress in entries:
                stresses_by_slot[slot] = stress
            
            try:
                if stresses_by_slot["IND_SG"] != stresses_by_slot["DEF_SG"]:
                    print(lemma)
                elif stresses_by_slot["IND_PL"] != stresses_by_slot["DEF_PL"]:
                    print(lemma)
            except KeyError:
                pass
            
            key = tuple(sorted(stresses_by_slot.items()))

            ALL_STRESSES_BY_SLOT[key] += 1
                
                
                # exceptional_count += 1
                # lexeme_label = f" [{which_lexeme}]" if which_lexeme else ""
                # print(f"\n{lemma}{lexeme_label} ({pos}): MIXED PATTERN")
                # for slot, gender, syl, stress in entries:
                #     print(f"\t{slot}\t{syl}\t{stress}")

        print(f"\nTotal count: {total_count}")
        print(f"Exceptional count: {exceptional_count}")
        
        for slot, count in ALL_STRESSES_BY_SLOT.most_common():
            print(f"\t{slot}: {count}")

        print(f"\nTor/Sor: {tor_sor}")
        print(f"\nIum: {ium}")


if __name__ == "__main__":
    main()
    add_syncretic_ids()
    resolve_ambiguous()
    # analyze()