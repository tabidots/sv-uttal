from dict.scripts.paths import DB_PATH
from collections import Counter
import sqlite3
from pprint import pprint


def count_syllables(phonetic: str) -> int:
    if not phonetic:
        return 0
    return phonetic.count('.') + phonetic.count('-') + phonetic.count('~') + 1


def is_umlaut_plural(sg: str, pl: str) -> bool:
    return pl.count("ä") > sg.count("ä") or pl.count("ö") > sg.count("ö")


def analyze_accent_patterns():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        # Group by lexeme (lemma + which_lexeme)
        lexemes = {}

        c.execute("""
            SELECT w.lemma, w.which_lexeme, w.gender, w.form, w.slot, b.phonetic, b.syllables, b.stress 
            FROM sv_wiktionary w
            JOIN braxen b ON CAST(w.braxen_ids AS INTEGER) = b.id
            WHERE w.braxen_ids NOT LIKE '%,%'
              AND w.pos = 'noun'
              AND b.syllables NOT LIKE '%|%'
        """)

        for lemma, which_lexeme, gender, form, slot, phonetic, syllables, stress in c.fetchall():
            key = f"{lemma}_{gender}" if which_lexeme == 0 else f"{lemma}_{gender}{which_lexeme}"

            lexemes.setdefault(key, {
                "lemma": lemma,
                "which_lexeme": which_lexeme,
                "forms": {},
                "stress_patterns": {}
            })

            lexemes[key]["forms"][slot] = {
                "form": form,
                "syllables": syllables,
                "is_monosyllable": count_syllables(phonetic) == 1,
                "stress": stress,
                "phonetic": phonetic
            }

        # Now analyze each lexeme
        patterns = {
            "acute_to_grave": [],
            "simple_monosyllabic": [],
            "hidden_grave_accent": [],
            "suspicious": [],
            "tor_sor": [],
            "um": [],
            "ik": [],
        }

        stable_polysyllabic_count = 0
        hidden_grave_accent_count = 0
        mono_null_plurals = set()

        for key, data in lexemes.items():
            forms = data["forms"]

            # Skip if missing key forms; Braxen is missing a lot of DEF_PL forms
            if 'IND_SG' not in forms or 'IND_PL' not in forms:
                continue

            ind_sg = forms['IND_SG']
            ind_pl = forms['IND_PL']

            # Get stress patterns
            sg_stress = ind_sg.get('stress')
            pl_stress = ind_pl.get('stress')

            # Missing data
            if not sg_stress or not pl_stress:
                continue
            # Stable polysyllabic stress
            if sg_stress == pl_stress and not ind_sg.get("is_monosyllable"):
                stable_polysyllabic_count += 1
                continue
            # Hidden grave accent
            if sg_stress != pl_stress and ind_sg.get("is_monosyllable"):
                if def_sg_stress := forms.get('DEF_SG', {}).get('stress'):
                    # Monosyllabic nouns' definite singular also with grave accent
                    # bet, ort, rigg, pir, sköt, snopp, törn
                    if def_sg_stress != sg_stress:
                        patterns["hidden_grave_accent"].append({
                            "lexeme": key,
                            "word": ind_sg['form'],
                            "sg_stress": sg_stress,
                            "pl_stress": pl_stress,
                            "def_sg": forms.get('DEF_SG', {}).get('form'),
                            "ind_pl": forms.get('IND_PL', {}).get('form'),
                            "word_syllables": ind_sg.get('syllables'),
                            "def_sg_syllables": forms.get('DEF_SG', {}).get('syllables'),
                            "ind_pl_syllables": forms.get('IND_PL', {}).get('syllables'),
                        })
                # Monosyllabic nouns with hidden grave whose def sg is also monosyllabic
                # sky, by, fe, vy, lo, bro, sjö, mo, så, slå, fru, kö
                hidden_grave_accent_count += 1
                continue
            # Null plural (ett bolag, flera bolag; ett mål, flera mål)
            # There are no instances where a *single* lexeme will have a null plural with
            # a different stress pattern than the singular
            if ind_pl.get("form") == ind_sg.get("form"):
                mono_null_plurals.add(ind_sg.get("form"))
                continue

            # Analyze stress pattern
            sg_has_secondary = '-' in sg_stress if sg_stress else False
            pl_has_secondary = '-' in pl_stress if pl_stress else False

            if ind_sg.get("is_monosyllable"):
                patterns["simple_monosyllabic"].append({
                    "lexeme": key,
                    "word": ind_sg['form'],
                    "sg_stress": sg_stress,
                    "pl_stress": pl_stress,
                })

            elif data["lemma"].endswith(('tor', 'sor')):
                # Latin -tor/-sor nouns
                patterns["tor_sor"].append({
                    "lexeme": key,
                    "word": ind_sg['form'],
                    "sg_stress": sg_stress,
                    "pl_stress": pl_stress,
                    "def_sg": forms.get('DEF_SG', {}).get('form'),
                    "ind_pl": forms.get('IND_PL', {}).get('form'),
                    "word_syllables": ind_sg.get('syllables'),
                    "def_sg_syllables": forms.get('DEF_SG', {}).get('syllables'),
                    "ind_pl_syllables": forms.get('IND_PL', {}).get('syllables'),
                })

            elif data["lemma"].endswith(('ium', 'eum')):
                # Latin -ium/-eum nouns
                patterns["um"].append({
                    "lexeme": key,
                    "word": ind_sg['form'],
                    "sg_stress": sg_stress,
                    "pl_stress": pl_stress,
                    "def_sg": forms.get('DEF_SG', {}).get('form'),
                    "ind_pl": forms.get('IND_PL', {}).get('form'),
                    "word_syllables": ind_sg.get('syllables'),
                    "def_sg_syllables": forms.get('DEF_SG', {}).get('syllables'),
                    "ind_pl_syllables": forms.get('IND_PL', {}).get('syllables'),
                })

            elif data["lemma"].endswith("ik"):
                # Latin -ik nouns
                patterns["ik"].append({
                    "lexeme": key,
                    "word": ind_sg['form'],
                    "sg_stress": sg_stress,
                    "pl_stress": pl_stress,
                    "def_sg": forms.get('DEF_SG', {}).get('form'),
                    "ind_pl": forms.get('IND_PL', {}).get('form'),
                    "word_syllables": ind_sg.get('syllables'),
                    "def_sg_syllables": forms.get('DEF_SG', {}).get('syllables'),
                    "ind_pl_syllables": forms.get('IND_PL', {}).get('syllables'),
                })
                
            elif not sg_has_secondary and pl_has_secondary:
                patterns["acute_to_grave"].append({
                    "lexeme": key,
                    "word": ind_sg['form'],
                    "sg_stress": sg_stress,
                    "pl_stress": pl_stress,
                })

            else:
                # Shifting pattern that doesn't match known categories
                patterns["suspicious"].append({
                    "lexeme": key,
                    "word": ind_sg['form'],
                    "sg_stress": sg_stress,
                    "pl_stress": pl_stress,
                    "def_sg": forms.get('DEF_SG', {}).get('form'),
                    "ind_pl": forms.get('IND_PL', {}).get('form'),
                    "word_syllables": ind_sg.get('syllables'),
                    "def_sg_syllables": forms.get('DEF_SG', {}).get('syllables'),
                    "ind_pl_syllables": forms.get('IND_PL', {}).get('syllables'),
                })

        # Print summary
        print(f"=== Swedish Noun Accent Analysis ===\n")

        print(f"Stable polysyllabic: {stable_polysyllabic_count}")
        print(
            f"Hidden accent monosyllabic: {hidden_grave_accent_count}\n")
        print(
            f"Acute to grave: {len(patterns['acute_to_grave'])}")
        print(f"Simple monosyllabic: {len(patterns['simple_monosyllabic'])}")
        print(f"-tor/-sor nouns: {len(patterns['tor_sor'])}")
        print(f"-um nouns: {len(patterns['um'])}")
        print(f"Suspicious: {len(patterns['suspicious'])}\n")

        # Show examples
        print("\n=== -tor/-sor Pattern Examples ===")
        for ex in patterns['tor_sor'][:10]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → PL: {ex['pl_stress']}")
        print({f"{ex['sg_stress']} → {ex['pl_stress']}" for ex in patterns['tor_sor']})

        print("\n=== -um Pattern Examples ===")
        for ex in patterns['um'][:10]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → PL: {ex['pl_stress']}")
        print({f"{ex['sg_stress']} → {ex['pl_stress']}" for ex in patterns['um']})

        print("\n=== -ik Pattern Examples ===")
        for ex in patterns['ik'][:10]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → PL: {ex['pl_stress']}")
        print({f"{ex['sg_stress']} → {ex['pl_stress']}" for ex in patterns['ik']})

        print("\n=== Simple Monosyllabic Pattern Examples ===")
        for ex in patterns['simple_monosyllabic'][:10]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → PL: {ex['pl_stress']}")

        print("\n=== Polysyllabic Acute → Grave Examples ===")
        for ex in patterns['acute_to_grave'][:10]:
            print(
                f"  {ex['word']} (SG: {ex['sg_stress']}) → PL: {ex['pl_stress']}")
        print(
            {f"{ex['sg_stress']} → {ex['pl_stress']}" for ex in patterns['acute_to_grave']})
        
        print(f"\n=== Suspicious Cases ===")
        for ex in patterns['suspicious']:
            print(
                f"  {ex['lexeme']}: (SG: {ex['sg_stress']}, PL: {ex['pl_stress']})")

        # print(len(mono_null_plurals))
        # print(sorted(x for x in mono_null_plurals))

        for ex in patterns['hidden_grave_accent']:
            print(f"| <ShowTones w=\"{ex['word_syllables']}\" /> <AudioButton word=\"{ex['word']}\" /> |"
                  f" <ShowTones w=\"{ex['def_sg_syllables']}\" /> <AudioButton word=\"{ex['def_sg']}\" /> |"
                  f" <ShowTones w=\"{ex['ind_pl_syllables']}\" /> <AudioButton word=\"{ex['ind_pl']}\" /> |")

        return patterns
            
            

if __name__ == "__main__":
    analyze_accent_patterns()