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
            CREATE TABLE nouns (
                lemma TEXT,
                gender TEXT,
                phonetic TEXT,
                stress_type TEXT,
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

            row = [lemma, gender, phonetic, None, None]
            for slot in SLOTS:
                form, syllables = canonical(slots.get(slot, []))
                row += [form, syllables]
            row += [irregular_pron]

            batch.append(row)

        c.executemany("""
            INSERT INTO nouns VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
            SET stress_type = 'stable monosyllabic'
            WHERE (morph_type = 'null-plural' OR morph_type IS NULL)
                AND stress_type IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables NOT LIKE '%↗%'
                AND ind_sg_syllables NOT LIKE '%↘%'
                AND def_sg_syllables NOT LIKE '%↘%'
                AND ind_pl_syllables NOT LIKE '%↘%'
        """)
        # To catch a few that slipped through the cracks
        c.execute("""
            UPDATE nouns
            SET stress_type = 'stable monosyllabic'
            WHERE morph_type = 'null-plural'
                AND ind_sg_syllables NOT LIKE '%↘%'
                AND ind_sg_syllables NOT LIKE '%↗%'
                AND stress_type IS NULL;
        """)
        c.execute("""
            UPDATE nouns
            SET stress_type = 'shifting monosyllabic'
            WHERE morph_type IS NULL AND stress_type IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables NOT LIKE '%↗%'
                AND ind_sg_syllables NOT LIKE '%↘%'
                AND ind_pl_syllables LIKE '%↘%'
        """)
        c.execute("""
            UPDATE nouns
            SET stress_type = 'shifting polysyllabic'
            WHERE stress_type IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables NOT LIKE '%↘%'
                AND ind_pl_syllables LIKE '%↘%'
        """)

        c.execute("""
            UPDATE nouns
            SET stress_type = 'stationary grave to acute'
            WHERE stress_type IS NULL
                AND lemma LIKE '%um%'
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables LIKE '%↘%'
                AND ind_pl_syllables NOT LIKE '%↘%'
        """)
        # -tor/-sor pattern
        c.execute("""
            UPDATE nouns
            SET stress_type = 'forward-moving grave to acute'
            WHERE stress_type IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables LIKE '%↘%'
                AND ind_pl_syllables NOT LIKE '%↘%'
        """)
        c.execute("""
            UPDATE nouns
            SET stress_type = 'backward-moving stress'
            WHERE stress_type IS NULL
                AND ind_sg_syllables IS NOT NULL
                AND ind_sg_syllables LIKE '%↗ik%'
                AND ind_pl_syllables NOT LIKE '%↗ik%'
        """)
        c.execute("""
            UPDATE nouns
            SET stress_type = 'anomalous'
            WHERE lemma IN ('bet', 'bete', 'ort', 'rigg', 'sköt', 'sköte', 
                  'snopp', 'törn', 'törne', 'bonde', 'donjuan', 'frände', 
                  'fader', 'farao', 'femininum', 'foton', 'kansler', 'kompani', 
                  'konsul', 'korre', 'ninja', 'samba')
        """)

        # Peek at the data
        c.execute("""
            SELECT 
                stress_type, 
                COUNT(*) as count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
            FROM nouns
            WHERE (morph_type NOT IN ('singular-only', 'indeclinable') or morph_type IS NULL)
            AND ind_sg_syllables IS NOT NULL and def_sg_syllables IS NOT NULL and ind_pl_syllables IS NOT NULL and def_pl_syllables IS NOT NULL
            GROUP BY stress_type
        """)

if __name__ == "__main__":
    build_noun_paradigms()
    enhance_noun_data()
