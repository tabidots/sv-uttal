from dict.scripts.paths import DB_PATH
from collections import Counter
import sqlite3

def analyze():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        wordforms = {}
        c.execute("""
            SELECT w.lemma, w.which_lexeme, w.form, w.slot, b.phonetic, b.syllables, b.stress 
            FROM sv_wiktionary w
            JOIN braxen b ON CAST(w.braxen_ids AS INTEGER) = b.id
            WHERE w.braxen_ids NOT LIKE '%,%'
        """)
        for lemma, which_lexeme, form, slot, phonetic, syllables, stress in c.fetchall():
            key = lemma
            if which_lexeme > 0:
                key += f"_{which_lexeme}"
            
            is_monosyllable = not any(x in phonetic for x in ".~-")

            if key not in wordforms:
                wordforms[key] = {
                    "which_lexeme": which_lexeme,
                    "is_monosyllable": is_monosyllable,
                    "forms": {},
                }
            wordforms[key]["forms"][slot] = {
                "form": form,
                "syllables": syllables,
                "stress": stress,
            }


def count_syllables(phonetic: str) -> int:
    if not phonetic:
        return 0
    return phonetic.count('.') + phonetic.count('-') + phonetic.count('~') + 1


def analyze_accent_patterns():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        # Group by lexeme (lemma + which_lexeme)
        lexemes = {}

        c.execute("""
            SELECT w.lemma, w.which_lexeme, w.form, w.slot, b.phonetic, b.syllables, b.stress 
            FROM sv_wiktionary w
            JOIN braxen b ON CAST(w.braxen_ids AS INTEGER) = b.id
            WHERE w.braxen_ids NOT LIKE '%,%'
              AND w.pos = 'noun'
        """)

        for lemma, which_lexeme, form, slot, phonetic, syllables, stress in c.fetchall():
            key = lemma if which_lexeme == 0 else f"{lemma}_{which_lexeme}"

            # Detect if lexeme base form is monosyllabic
            # More accurate: check the IND_SG form
            is_monosyllabic_base = None

            lexemes.setdefault(key, {
                "lemma": lemma,
                "which_lexeme": which_lexeme,
                "forms": {},
                "syllable_count": {},
                "stress_patterns": {}
            })

            lexemes[key]["forms"][slot] = {
                "form": form,
                "syllables": syllables,
                "stress": stress,
                "phonetic": phonetic
            }

        # Now analyze each lexeme
        patterns = {
            "stable_polysyllabic": [],
            "shifting_polysyllabic": [],
            "hidden_accent_monosyllabic": [],
            "simple_monosyllabic": [],
            "suspicious": [],
            "tor_sor": [],
            "ium": []
        }

        for key, data in lexemes.items():
            forms = data["forms"]

            # Skip if missing key forms
            if 'IND_SG' not in forms or 'IND_PL' not in forms or 'DEF_SG' not in forms or 'DEF_PL' not in forms:
                continue

            ind_sg = forms['IND_SG']
            ind_pl = forms['IND_PL']

            # Get stress patterns
            sg_stress = ind_sg.get('stress')
            pl_stress = ind_pl.get('stress')

            if not sg_stress or not pl_stress:
                continue

            # Check if forms are distinct or syncretic
            forms_distinct = ind_sg['form'] != ind_pl['form']
            umlaut_plural = ind_pl['form'].count("ö") > ind_sg['form'].count(
                "ö") or ind_pl['form'].count("ä") > ind_sg['form'].count("ä")
                

            # Determine syllable count from IND_SG phonetic
            sg_phonetic = ind_sg['phonetic']
            sg_syllable_count = count_syllables(sg_phonetic)

            # Check if base is monosyllabic
            is_monosyllabic = sg_syllable_count == 1

            # Analyze stress pattern
            sg_has_secondary = '-' in sg_stress if sg_stress else False
            pl_has_secondary = '-' in pl_stress if pl_stress else False

            # Categorize
            if not forms_distinct:
                # Syncretic forms - skip or mark separately
                continue

            elif is_monosyllabic:
                # Hidden accent pattern: monosyllabic base should gain secondary in inflected
                if not sg_has_secondary and pl_has_secondary:
                    patterns["hidden_accent_monosyllabic"].append({
                        "lexeme": key,
                        "word": ind_sg['form'],
                        "sg_stress": sg_stress,
                        "pl_stress": pl_stress,
                        "pl_word": ind_pl['form']
                    })
                else:
                    patterns["simple_monosyllabic"].append({
                        "lexeme": key,
                        "word": ind_sg['form'],
                        "pl_word": ind_pl['form'],
                        "type": "simple_monosyllabic",
                        "sg_stress": sg_stress,
                        "pl_stress": pl_stress
                    })

            elif data["lemma"].endswith(('tor', 'sor')):
                # Latin -tor/-sor nouns
                patterns["tor_sor"].append({
                    "lexeme": key,
                    "word": ind_sg['form'],
                    "sg_stress": sg_stress,
                    "pl_stress": pl_stress,
                    "sg_has_secondary": sg_has_secondary,
                    "pl_has_secondary": pl_has_secondary
                })

            elif data["lemma"].endswith('ium'):
                # Latin -ium nouns
                patterns["ium"].append({
                    "lexeme": key,
                    "word": ind_sg['form'],
                    "sg_stress": sg_stress,
                    "pl_stress": pl_stress
                })

            elif sg_has_secondary == pl_has_secondary:
                # Stable accent pattern
                if sg_syllable_count > 1:
                    patterns["stable_polysyllabic"].append({
                        "lexeme": key,
                        "word": ind_sg['form'],
                        "sg_stress": sg_stress,
                        "pl_stress": pl_stress,
                        "accent_type": "accent2" if sg_has_secondary else "accent1"
                    })
                
            elif not sg_has_secondary and pl_has_secondary:
                patterns["shifting_polysyllabic"].append({
                    "lexeme": key,
                    "word": ind_sg['form'],
                    "sg_stress": sg_stress,
                    "pl_stress": pl_stress,
                    "sg_has_secondary": sg_has_secondary,
                    "pl_has_secondary": pl_has_secondary
                })

            else:
                # Shifting pattern that doesn't match known categories
                patterns["suspicious"].append({
                    "lexeme": key,
                    "word": ind_sg['form'],
                    "sg_stress": sg_stress,
                    "pl_stress": pl_stress,
                    "sg_has_secondary": sg_has_secondary,
                    "pl_has_secondary": pl_has_secondary
                })

        # Print summary
        print(f"=== Swedish Noun Accent Analysis ===\n")

        print(f"Stable polysyllabic: {len(patterns['stable_polysyllabic'])}")
        print(
            f"Shifting polysyllabic: {len(patterns['shifting_polysyllabic'])}")
        print(
            f"Hidden accent monosyllabic: {len(patterns['hidden_accent_monosyllabic'])}")
        print(f"Simple monosyllabic: {len(patterns['simple_monosyllabic'])}")
        print(f"-tor/-sor nouns: {len(patterns['tor_sor'])}")
        print(f"-ium nouns: {len(patterns['ium'])}")
        print(f"Suspicious: {len(patterns['suspicious'])}\n")

        # Show examples
        print("=== Hidden Accent Examples (Monosyllabic → Accent 2) ===")
        for ex in patterns['hidden_accent_monosyllabic'][:10]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → {ex['pl_word']} (PL: {ex['pl_stress']})")
            
        print("\n=== Simple Monosyllabic Examples ===")
        for ex in patterns['simple_monosyllabic'][:10]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → PL: {ex['pl_word']} {ex['pl_stress']}")

        print("\n=== -tor/-sor Pattern Examples ===")
        for ex in patterns['tor_sor'][:10]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → PL: {ex['pl_stress']}")

        print("\n=== -ium Pattern Examples ===")
        for ex in patterns['ium'][:10]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → PL: {ex['pl_stress']}")
            
        print("\n=== Stable Polysyllabic Examples ===")
        for ex in patterns['stable_polysyllabic'][20:30]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → PL: {ex['pl_stress']}")

        print("\n=== Shifting Stress in Plural Examples ===")
        for ex in patterns['shifting_polysyllabic'][:10]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → PL: {ex['pl_stress']}")

        print(f"\n=== Suspicious Cases ===")
        for ex in patterns['suspicious'][:10]:
            print(
                f"  {ex['lexeme']}: (SG: {ex['sg_stress']}, PL: {ex['pl_stress']})")

        return patterns
            
            

if __name__ == "__main__":
    # analyze()
    analyze_accent_patterns()