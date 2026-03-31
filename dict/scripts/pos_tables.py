import sqlite3
from dict.scripts.paths import DB_PATH
from collections import defaultdict


def build_noun_paradigms():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        # Fetch all noun rows grouped by lexeme
        c.execute("""
            SELECT w.lemma, w.gender, w.which_lexeme, w.slot, w.form, w.braxen_ids,
                   b.phonetic, b.syllables
            FROM sv_wiktionary w
            LEFT JOIN braxen b ON CAST(w.braxen_ids AS INTEGER) = b.id
            WHERE w.pos = 'noun'
            ORDER BY w.lemma, w.which_lexeme, w.slot
        """)
        rows = c.fetchall()

        # Group by lexeme
        lexemes = defaultdict(lambda: defaultdict(list))

        for lemma, gender, which_lexeme, slot, form, braxen_ids, phonetic, syllables in rows:
            key = (lemma, which_lexeme, gender)
            if slot and form:
                lexemes[key][slot].append((form, syllables))

        # Fetch IND_SG phonetic/stress/morph_type as the canonical row
        c.execute("""
            SELECT w.lemma, w.gender, w.which_lexeme, b.phonetic, b.syllables, b.irregular_pron
            FROM sv_wiktionary w
            JOIN braxen b ON CAST(w.braxen_ids AS INTEGER) = b.id
            WHERE w.pos = 'noun' AND w.slot = 'IND_SG'
            AND w.braxen_ids NOT LIKE '%,%'
        """)
        ind_sg_data = {(lemma, which_lexeme, gender): (phonetic, syllables, irregular_pron)
                       for lemma, gender, which_lexeme, phonetic, syllables, irregular_pron in c.fetchall()}

        SLOTS = ['IND_SG', 'DEF_SG', 'IND_PL', 'DEF_PL']

        def canonical(slot_entries):
            """Deduplicate forms; pick one syllables value (prefer non-null)."""
            forms = []
            syllables_val = None
            seen = set()
            for form, syllables in slot_entries:
                if form not in seen:
                    seen.add(form)
                    forms.append(form)
                if syllables and not syllables_val:
                    syllables_val = syllables
            return ",".join(forms) or None, syllables_val

        c.execute("DROP TABLE IF EXISTS nouns")
        c.execute("""
            CREATE TABLE IF NOT EXISTS nouns (
                id INTEGER PRIMARY KEY,
                lemma TEXT,
                gender TEXT,
                phonetic TEXT,
                pitch_pattern TEXT,
                morph_type TEXT,
                ind_sg TEXT, ind_sg_syllables TEXT,
                def_sg TEXT, def_sg_syllables TEXT,
                ind_pl TEXT, ind_pl_syllables TEXT,
                def_pl TEXT, def_pl_syllables TEXT,
                irregular_pron JSON
            )
        """)

        batch = []
        for key, slots in lexemes.items():
            lemma, which_lexeme, gender = key
            phonetic, _, irregular_pron = ind_sg_data.get(
                key, (None, None, None))

            row = [lemma, gender, phonetic]
            for slot in SLOTS:
                form, syllables = canonical(slots.get(slot, []))
                row += [form, syllables]
            row += [irregular_pron]

            batch.append(row)

        c.executemany("""
            INSERT INTO nouns (lemma, gender, phonetic, ind_sg, ind_sg_syllables, 
                      def_sg, def_sg_syllables, ind_pl, ind_pl_syllables, def_pl, 
                      def_pl_syllables, irregular_pron) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)
        conn.commit()


def enhance_noun_data():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            UPDATE nouns
            SET morph_type = 'indeclinable', 
                  def_pl = def_sg, def_pl_syllables = def_sg_syllables,
                  ind_sg_syllables = def_sg_syllables
            WHERE ind_sg = def_sg AND ind_sg = ind_pl
        """)
        conn.commit()

        c.execute("""
            UPDATE nouns
            SET morph_type = 'singular-only'
            WHERE ind_sg IS NOT NULL AND def_sg IS NOT NULL AND ind_pl IS NULL
            AND morph_type IS NULL
        """)
        conn.commit()

        c.execute("""
            UPDATE nouns
            SET morph_type = 'null-plural'
            WHERE ind_sg = ind_pl AND morph_type IS NULL
        """)
        conn.commit()

        c.execute("""
            UPDATE nouns
            SET gender = 'c'
            WHERE gender = 'e' AND def_sg NOT LIKE '%,%'
            AND def_sg LIKE '%n'
        """)
        c.execute("""
            UPDATE nouns
            SET gender = 'n'
            WHERE gender = 'e' AND def_sg NOT LIKE '%,%'
            AND def_sg LIKE '%t'
        """)

        # Fill in missing syllabifications
        c.execute("""
            UPDATE nouns
            SET def_pl_syllables = ind_pl_syllables || 'na'
            WHERE def_pl = ind_pl || 'na'
                AND def_pl_syllables IS NULL AND ind_pl_syllables IS NOT NULL
        """)
        c.execute("""
            UPDATE nouns
            SET def_pl_syllables = ind_pl_syllables || 'a'
            WHERE def_pl = ind_pl || 'a'
                AND def_pl_syllables IS NULL AND ind_pl_syllables IS NOT NULL
        """)
        c.execute("""
            UPDATE nouns
            SET def_pl_syllables = ind_pl_syllables || 'en'
            WHERE def_pl = ind_pl || 'en'
                AND def_pl_syllables IS NULL AND ind_pl_syllables IS NOT NULL
                AND ind_pl_syllables LIKE '%↗%';
        """)
        c.execute("""
            UPDATE nouns
            SET def_pl_syllables = SUBSTR(def_sg_syllables, 1, LENGTH(def_sg_syllables) - LENGTH('t')) || 'n'
            WHERE def_pl = ind_pl || 'en'
                AND def_pl_syllables IS NULL AND ind_pl_syllables IS NOT NULL
                AND ind_pl_syllables NOT LIKE '%↗%';
        """)
        c.execute("""
            UPDATE nouns
            SET ind_pl_syllables = SUBSTR(def_pl_syllables, 1, LENGTH(def_pl_syllables) - LENGTH('na'))
            WHERE def_pl = ind_pl || 'na'
                AND def_pl_syllables IS NOT NULL 
                AND ind_pl_syllables IS NULL
                AND def_pl_syllables LIKE '%na';
        """)
        c.execute("""
            UPDATE nouns
            SET ind_pl_syllables = SUBSTR(def_pl_syllables, 1, LENGTH(def_pl_syllables) - LENGTH('a'))
            WHERE def_pl = ind_pl || 'a'
                AND def_pl_syllables IS NOT NULL 
                AND ind_pl_syllables IS NULL
                AND def_pl_syllables LIKE '%a';
        """)
        c.execute("""
            UPDATE nouns
            SET def_sg_syllables = ind_sg_syllables || 'en'
            WHERE def_sg = ind_sg || 'en'
                AND def_sg_syllables IS NULL AND ind_sg_syllables IS NOT NULL
        """)
        c.execute("""
            UPDATE nouns
            SET def_sg_syllables = ind_sg_syllables || 'n'
            WHERE def_sg = ind_sg || 'n'
                AND def_sg_syllables IS NULL AND ind_sg_syllables IS NOT NULL
        """)
        c.execute("""
            UPDATE nouns
            SET def_sg_syllables = ind_sg_syllables || 'et'
            WHERE def_sg = ind_sg || 'et'
                AND def_sg_syllables IS NULL AND ind_sg_syllables IS NOT NULL
        """)
        c.execute("""
            UPDATE nouns
            SET def_sg_syllables = ind_sg_syllables || 't'
            WHERE def_sg = ind_sg || 't'
                AND def_sg_syllables IS NULL AND ind_sg_syllables IS NOT NULL
        """)
        c.execute("""
            UPDATE nouns
            SET ind_sg_syllables = SUBSTR(def_sg_syllables, 1, LENGTH(def_sg_syllables) - LENGTH('en'))
            WHERE def_sg = ind_sg || 'en'
                AND def_sg_syllables IS NOT NULL 
                AND ind_sg_syllables IS NULL
                AND def_sg_syllables LIKE '%en';
        """)
        c.execute("""
            UPDATE nouns
            SET ind_sg_syllables = SUBSTR(def_sg_syllables, 1, LENGTH(def_sg_syllables) - LENGTH('n'))
            WHERE def_sg = ind_sg || 'n'
                AND def_sg_syllables IS NOT NULL 
                AND ind_sg_syllables IS NULL
                AND def_sg_syllables LIKE '%n';
        """)
        c.execute("""
            UPDATE nouns
            SET ind_sg_syllables = SUBSTR(def_sg_syllables, 1, LENGTH(def_sg_syllables) - LENGTH('et'))
            WHERE def_sg = ind_sg || 'et'
                AND def_sg_syllables IS NOT NULL 
                AND ind_sg_syllables IS NULL
                AND def_sg_syllables LIKE '%et';
        """)
        c.execute("""
            UPDATE nouns
            SET ind_sg_syllables = SUBSTR(def_sg_syllables, 1, LENGTH(def_sg_syllables) - LENGTH('t'))
            WHERE def_sg = ind_sg || 't'
                AND def_sg_syllables IS NOT NULL 
                AND ind_sg_syllables IS NULL
                AND def_sg_syllables LIKE '%t';
        """)
        

        c.execute("""
            UPDATE nouns
            SET pitch_pattern = 'stable monosyllabic'
            WHERE (morph_type = 'null-plural' OR morph_type IS NULL)
                AND pitch_pattern IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables NOT LIKE '%↗%'
                AND ind_sg_syllables NOT LIKE '%↘%'
                AND def_sg_syllables NOT LIKE '%↘%'
                AND ind_pl_syllables NOT LIKE '%↘%'
        """)
        # To catch a few that slipped through the cracks
        c.execute("""
            UPDATE nouns
            SET pitch_pattern = 'stable monosyllabic'
            WHERE morph_type = 'null-plural'
                AND ind_sg_syllables NOT LIKE '%↘%'
                AND ind_sg_syllables NOT LIKE '%↗%'
                AND pitch_pattern IS NULL;
        """)
        c.execute("""
            UPDATE nouns
            SET pitch_pattern = 'monosyllabic with hidden grave accent'
            WHERE morph_type IS NULL AND pitch_pattern IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables NOT LIKE '%↗%'
                AND ind_sg_syllables NOT LIKE '%↘%'
                AND def_sg_syllables LIKE '%↘%'
                AND ind_pl_syllables LIKE '%↘%'
        """)
        c.execute("""
            UPDATE nouns
            SET pitch_pattern = 'shifting monosyllabic'
            WHERE morph_type IS NULL AND pitch_pattern IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables NOT LIKE '%↗%'
                AND ind_sg_syllables NOT LIKE '%↘%'
                AND ind_pl_syllables LIKE '%↘%'
        """)
        c.execute("""
            UPDATE nouns
            SET pitch_pattern = 'shifting polysyllabic'
            WHERE pitch_pattern IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables NOT LIKE '%↘%'
                AND ind_pl_syllables LIKE '%↘%'
        """)

        c.execute("""
            UPDATE nouns
            SET pitch_pattern = 'stationary grave to acute'
            WHERE pitch_pattern IS NULL
                AND lemma LIKE '%um%'
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables LIKE '%↘%'
                AND ind_pl_syllables NOT LIKE '%↘%'
        """)
        # -tor/-sor pattern
        c.execute("""
            UPDATE nouns
            SET pitch_pattern = 'forward-moving grave to acute'
            WHERE pitch_pattern IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables LIKE '%↘%'
                AND ind_pl_syllables NOT LIKE '%↘%'
        """)
        c.execute("""
            UPDATE nouns
            SET pitch_pattern = 'backward-moving stress'
            WHERE pitch_pattern IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables LIKE '%↗ik%'
                AND ind_pl_syllables NOT LIKE '%↗ik%'
        """)
        c.execute("""
            UPDATE nouns
            SET pitch_pattern = 'manual review required'
            WHERE lemma IN ('bet', 'bete', 'ort', 'rigg', 'sköt', 'sköte', 
                  'snopp', 'törn', 'törne', 'bonde', 'donjuan', 'frände', 
                  'fader', 'farao', 'femininum', 'foton', 'kansler', 'kompani', 
                  'konsul', 'korre', 'ninja', 'samba')
        """)
        conn.commit()

        # Peek at the data
        c.execute("""
            SELECT 
                pitch_pattern, 
                COUNT(*) as count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
            FROM nouns
            WHERE (morph_type NOT IN ('singular-only', 'indeclinable') or morph_type IS NULL)
            AND ind_sg_syllables IS NOT NULL and def_sg_syllables IS NOT NULL and ind_pl_syllables IS NOT NULL and def_pl_syllables IS NOT NULL
            GROUP BY pitch_pattern
        """)

def build_verb_paradigms():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        # Fetch all verb rows grouped by lexeme
        c.execute("""
            SELECT w.lemma, w.which_lexeme, w.slot, w.form, b.phonetic, b.syllables
            FROM sv_wiktionary w
            LEFT JOIN braxen b ON CAST(w.braxen_ids AS INTEGER) = b.id
            WHERE w.pos IN ('verb', 'participle')
            ORDER BY w.lemma, w.which_lexeme, w.slot
        """)
        rows = c.fetchall()

        # Group by lexeme
        lexemes = defaultdict(lambda: defaultdict(list))

        for lemma, which_lexeme, slot, form, phonetic, syllables in rows:
            key = (lemma, which_lexeme)
            # Exclude old-fashioned "-es" present passive forms
            if slot == "PRS_PASS" and form.split(" ")[0].endswith("es"):
                continue
            if slot and form:
                lexemes[key][slot].append((form, syllables))

        # Fetch INF phonetic/syllables as the canonical row
        c.execute("""
            SELECT w.lemma, w.which_lexeme, b.phonetic, b.syllables, b.irregular_pron
            FROM sv_wiktionary w
            JOIN braxen b ON CAST(w.braxen_ids AS INTEGER) = b.id
            WHERE w.pos = 'verb' AND w.slot = 'INF'
            AND w.braxen_ids NOT LIKE '%,%'
        """)
        inf_data = {(lemma, which_lexeme): (phonetic, syllables, irregular_pron)
                       for lemma, which_lexeme, phonetic, syllables, irregular_pron in c.fetchall()}
        
        SLOTS = ['INF', 'PRS', 'PRT', 'SUP', 'IMP', 'INF_PASS', 'PRS_PASS', 'PRT_PASS', 'SUP_PASS',
                 'PRS_PART', 'PRT_PART']

        def canonical(slot_entries):
            """Deduplicate forms; pick one syllables value (prefer non-null)."""
            forms = []
            syllables_val = None
            seen = set()
            for form, syllables in slot_entries:
                if form not in seen:
                    seen.add(form)
                    forms.append(form)
                if syllables and not syllables_val:
                    syllables_val = syllables
            return ",".join(forms) or None, syllables_val

        c.execute("DROP TABLE IF EXISTS verbs")
        c.execute("""
            CREATE TABLE IF NOT EXISTS verbs (
                id INTEGER PRIMARY KEY,
                lemma TEXT,
                phonetic TEXT,
                pitch_pattern TEXT,
                morph_type JSON,
                infin TEXT, infin_syllables TEXT,
                prs TEXT, prs_syllables TEXT,
                prt TEXT, prt_syllables TEXT,
                sup TEXT, sup_syllables TEXT,
                imp TEXT, imp_syllables TEXT,
                inf_pass TEXT, inf_pass_syllables TEXT,
                prs_pass TEXT, prs_pass_syllables TEXT,
                prt_pass TEXT, prt_pass_syllables TEXT,
                sup_pass TEXT, sup_pass_syllables TEXT,
                prs_part TEXT, prs_part_syllables TEXT,
                prt_part TEXT, prt_part_syllables TEXT,
                irregular_pron JSON
            )
        """)

        batch = []
        for key, slots in lexemes.items():
            lemma, which_lexeme = key
            phonetic, _, irregular_pron = inf_data.get(
                key, (None, None, None))

            row = [lemma, phonetic]
            for slot in SLOTS:
                form, syllables = canonical(slots.get(slot, []))
                row += [form, syllables]
            row += [irregular_pron]

            # morph_type = {
            #     "is_deponent": bool,
            #     "is_phrasal": bool,
            #     "is_prefixed": bool,
            #     "core_verb": bool,
            #     "present_type": str,  # -ar, -er, -r
            #     "past_type": str,
            #     "supine_type": str,
            # }

            batch.append(row)

        c.executemany("""
            INSERT INTO verbs (
                lemma, phonetic, infin, infin_syllables, prs, prs_syllables,
                prt, prt_syllables, sup, sup_syllables, imp, imp_syllables,
                inf_pass, inf_pass_syllables, prs_pass, prs_pass_syllables,
                prt_pass, prt_pass_syllables, sup_pass, sup_pass_syllables,
                prs_part, prs_part_syllables, prt_part, prt_part_syllables,
                irregular_pron
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)
        conn.commit()


def enhance_verb_data():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        # TODO: Which forms are supposed to be the same?
        

        c.execute("""
            UPDATE verbs
            SET morph_type = 'deponent'
            WHERE infin IS NULL AND inf_pass IS NOT NULL
        """)
        
        # Exclude phrasal verbs when labeling accent pattern types;
        # those can be handled later with a single query
        c.execute("""
            UPDATE verbs
            SET morph_type = 'short'
            WHERE lemma NOT LIKE '% %' 
                AND infin_syllables IS NOT NULL
                AND infin_syllables NOT LIKE '%↗%'
                AND infin_syllables NOT LIKE '%↘%'
        """)


if __name__ == "__main__":
    # build_noun_paradigms()
    # enhance_noun_data()
    build_verb_paradigms()
    pass
