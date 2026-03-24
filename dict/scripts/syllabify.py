import sqlite3
from dict.scripts.paths import DB_PATH, RAW_DIR
import re
import json

VOWELS = "aeiouyäöåôøáéóíúêàèîüæ"
VOWELS_RE = re.compile(r'([' + VOWELS + r'])', re.IGNORECASE)
BOUNDARIES_RE = re.compile(r' [~\.\-\|] ')
CONS_RE = re.compile(r'([bcdfghjklmnpqrstvwxzŋ]+)', re.IGNORECASE)

PHONEME_TO_GRAPHEME = {
    'c': r'tj|ti|skj|sk|ch|q|k(?=[jeiyäö])|c',
    'x': r'tj|ti|sh|stg|sc|s[kt]j|s?sj|sk|si|s?ch|g|j|k(?=[eiyäö])',
    'j': r'lj|dj|gj|hj|[gyjl]',
    's': r't(?=i)|[szxç]|c(?=[eiy])',
    'tc': r'c[chz]|tj',
    'rh': r'w?r',
    'rs': r'sch|ch|ti|rz|(?<=r)s|(?<=r)c(?=[eiy])|s|g|j',
    'rl': r'(?<=r)l|l+',
    'rn': r'(?<=r)n|n+',
    'rd': r'(?<=r)d|d+',
    'rt': r'(?<=r)t|t+',
    'v': r'[wvfu]',
    'ng': r'n(?=c?k|[cgdstj])|g(?=n)|(?<=ge)n(?=er)',
    'k': r'[ck]?k|[cqxg]+',
    'm': r'(?<=m)b|m+',
    'd': r'g(?=y)|d+',
    'dj': r'dge?|ge|dj|j',
    'g': r'g+',
    'f': r'ph|[uv]|f+',
}

CONS_CLUSTERS_SHIFT_LEFT = {"ck"}
CONS_DIGRAPHS_SPLIT = {"kn", "gl", "st", "sk", "sv", "sp", "sn", "sm", "sr", 
                       "rt", "rl", "rd", "rs", "rn",
                       'kk', 'gg', 'tt', 'dd', 'pp', 'bb', 'll', 'mm', 'nn', 'rr', 'ss', 'ff', 'vv'}

def clean_phonetic(word: str) -> str:
    word = word.replace("k . s", "k s .") # Ensure "x" isn't split across syllables
    word = word.replace("t . x", ". t x") # Ensure "tion" isn't split across syllables
    word = word.replace("k . x", ". k x")
    word = word.replace("t . s", "t s .") # Ensure Italian "z" isn't split across syllables
    word = word.replace('p a t . j "o m', 'p a t . "jo m ')  # Russian ё spelled as e
    word = word.replace("r0", "r")
    word = word.replace("a j", "aj")
    word = word.replace("ä j", "äj")
    word = word.replace("e j", "ej")
    return word

def process_entry(word: str, phonetic: str, lemma: str) -> tuple[str, str, list[dict] | None]:
    """
    Process a single entry in the word list, generating its syllable structure and associated 
    stress code and irregular pronunciations.

    Parameters:
        word (str): The word being processed.
        phonetic (str): The phonetic transcription of the word.
        lemma (str): The base form of the word.

    Returns:
        A tuple containing the syllable structure of the word as a string, 
        the stress code as a string, and a list of irregular pronunciations or None.
    """
    lower_word = word.lower()
    lower_word = lower_word.replace("oia", "oja")
    lower_word = lower_word.replace("ensemble", "ensambel")
    lower_word = lower_word.replace("genre", "gener")

    irregular = []
    
    primary_stress_idx = None
    secondary_stress_idx = None
    for i, syl in enumerate(BOUNDARIES_RE.split(phonetic)):
        if "," in syl:
            secondary_stress_idx = i
        if '"' in syl or "'" in syl:
            primary_stress_idx = i
    
    cleaned = clean_phonetic(phonetic)
    boundaries = [b.strip() for b in BOUNDARIES_RE.findall(cleaned)] + [""]
    phonetic_syllables = BOUNDARIES_RE.split(cleaned)
    vowel_idxs = [match.start() for match in VOWELS_RE.finditer(lower_word)]
    
    syllable_starts = []
    last_final = -1

    # PHONEME TO GRAPHEME ALIGNMENT BY SYLLABLE
    for i, ps in enumerate(phonetic_syllables):
        phonemes = ps.split()
        initial = phonemes[0]
        final = phonemes[-1]

        if i == len(vowel_idxs):
            raise ValueError(f"Out of vowels in {word} ({word[last_final:]})")

        # MATCH INITIAL GRAPHEME OF SYLLABLE
        if "-" in word[last_final:vowel_idxs[i]]:
            winner = last_final + word[last_final:vowel_idxs[i]].index("-")
        elif VOWELS_RE.search(initial):
            while vowel_idxs and vowel_idxs[i] < last_final:
                vowel_idxs.pop(0)
            winner = vowel_idxs[i]
        else:
            # Use lookakead to find overlapping matches
            pattern = fr'(?=({PHONEME_TO_GRAPHEME.get(initial, initial)}))'
            candidates = re.finditer(pattern, lower_word)
            winner = next((c.start(1) for c in candidates if c.start(1) > last_final), None)
        
        if winner is None:
            raise ValueError(
                f"Couldn't find initial grapheme for /{ps}/ in {word} ({word[last_final:]}) (phonetic: {cleaned})")
        
        # IRREGULAR PRONUNCIATIONS
        remainder = lower_word[winner:]
        if ps.startswith("t x") and remainder.startswith("ti"):
            irregular.append({"span": (winner, winner + 2), "type": "ti as t+sj"})
            vowel_idxs.pop(i)
        elif ps.startswith("k x") and remainder.startswith("xi"):
            irregular.append({"span": (winner, winner + 2), "type": "xi as k+sj"})
            vowel_idxs.pop(i)
        elif initial == "c" and remainder.startswith("ker"):
            if lemma in {"kerub", "keramik"}:
                irregular.append({"span": (winner, winner+1), "type": "soft k in -ker-"})
            elif lemma == "ske":
                irregular.append({"span": (winner - 1, winner+1), "type": "soft sk in -sker-"})
        elif re.search(f"s ['\",]?i", ps) and remainder.startswith("sion"):
            irregular.append({"span": (winner, winner+3), "type": f"sio as {initial}i+o"})
        elif initial == "rs" and remainder.startswith("j"):
            irregular.append({"span": (winner, winner+1), "type": "j as rs-sound"})
        elif initial in "fv" and lower_word[winner-1:winner+1] == "eu":
            irregular.append({"span": (winner-1, winner+1), "type": f"eu as e{initial}"})

        elif initial == "x":
            if vowel_idxs[i] < winner:
                vowel_idxs.pop(0)
            grapheme = lower_word[winner:vowel_idxs[i]]
            if remainder.startswith("gering") or remainder.startswith("gera"):
                irregular.append({"span": (winner, winner + 1), "type": "g as sj in -gera/-gering"}) 
            elif grapheme in {"c", "g", "j", "ch", "stg", "sch", "sc"}:
                irregular.append({"span": (winner, vowel_idxs[i]), "type": f"{grapheme} as sj-sound"})
            # The following applies even if the syllable is "tio(n)"
            if lower_word[vowel_idxs[i]:vowel_idxs[i]+2] == "io":
                vowel_idxs.pop(i)

        elif initial == "c" and remainder.startswith("ch"):
            irregular.append({"span": (winner, winner + 2), "type": "ch as tj-sound"})

        elif any(remainder.startswith(pref) for pref in {
                 "ken", "kisk", "kism", "kist", "kighet", "kig", "ker", "kera", "kering",
                 "skisk", "skism", "skist", "skighet", "skig", "skera", "skering",
                 "gj", "glj", "gisk", "gism", "gist", "gighet",  "gig", "gu", "gh",
                 "gisering"
                }) and "energisk" not in lower_word:
            pass

        elif remainder.startswith("ghj"):  # fattighjon
            winner += 1
        
        elif any(ps.startswith(phon) for phon in {"s k ex", "k ex", "g ex", "k eh"}) and not lower_word.startswith("vege"):
            pass  # unstressed ske/ke/ge always hard
        elif ps.startswith("s k ,ex") and lemma in {"risk", "kiosk", "mask"}:
            pass  # risk, kiosk take -er in plural and are grave-accented
            
        elif lower_word[winner:winner+1] == "g":
            soft_g_hard_vowel = re.search(r"(?<![lr] \. )j ['\",]?[aouå](?!e)", ps)
            if remainder.startswith("gg") and last_final == vowel_idxs[i-1]:
                winner += 1  # anhängiggöra
            elif soft_g_hard_vowel and lower_word[winner:winner+3] == "gio":
                irregular.append({"span": (winner, winner + 3), "type": "gio as ju"})
                vowel_idxs.pop(i)
            elif soft_g_hard_vowel:
                irregular.append({"span": (winner, winner + 1), "type": "soft g before hard vowel"})
            elif remainder.startswith("gering") or remainder.startswith("gera"):
                if winner > 0 and lower_word[winner-1:winner] == "r":
                    pass
                elif initial == "j":
                    irregular.append({"span": (winner, winner + 1), "type": "soft g in -gera/-gering"}) 
            elif any(part in lower_word for part in {"geri", "algi", "fagi", "logi", "urgi"}):
                pass
            elif remainder.startswith("ga"): # game, gaelisk
                pass
            elif re.search(f"g ['\",]?(?:[eiyäö]|oe)", ps):
                irregular.append({"span": (winner, winner + 1), "type": "hard g before soft vowel"})

        elif initial == "s" and re.search(f"s k ['\",]?(?:[eiyäö]|oe)", ps):
            span_end = winner + 3 if remainder.startswith("sch") else winner + 2
            irregular.append({"span": (winner, span_end), "type": "hard sk before soft vowel"})

        elif initial == lower_word[winner:winner+1] == "k" and re.search(f"k ['\",]?(?:[eiyäö]|oe)", ps):
            span_end = winner + 1
            if lemma in {"bostadskö", "gosskör", "manskör", "tonårskille"}:
                irregular.append({"span": (winner, winner + 1), "type": "hard k before soft vowel"})
            elif winner > 0 and lower_word[winner-1] == "s":
                irregular.append({"span": (winner - 1, winner + 1), "type": "hard sk before soft vowel"})
            elif remainder.startswith("kk"):
                pass
            else:
                irregular.append({"span": (winner, winner + 1), "type": "hard k before soft vowel"})

        syllable_starts.append(winner)
        
        # MATCH FINAL GRAPHEME OF SYLLABLE
        if VOWELS_RE.search(final):
            while vowel_idxs and vowel_idxs[i] < winner:
                vowel_idxs.pop(0)
            last_final = vowel_idxs[i]
        else:
            pattern = fr'(?=({PHONEME_TO_GRAPHEME.get(final, final)}))'
            candidates = re.finditer(pattern, lower_word)
            winner_final = next((c.end(1) for c in candidates if c.start(1) > last_final
                           and c.start(1) > vowel_idxs[i]), None)
            
            if final in "fv" and lower_word[winner-1:winner+1] == "eu":
                irregular.append({"span": (winner-1, winner+1), "type": f"eu as e{final}"})
                vowel_idxs.pop(i)
            if final == "rs" and lower_word[winner_final-1:winner_final+1] == "ge":
                irregular.append({"span": (winner_final-1, winner_final+1), "type": f"g as rs-sound"})
                # Adjacent vowels require a shift here
                if lemma in {"plantageägare", "massageolja", "bagageutrymme"}:
                    winner_final += 2

            if winner_final is None:
                remainder = word[last_final:] if i > 0 else word
                raise ValueError(f"Couldn't find final grapheme for /{ps}/ in {word} ({remainder}) (phonetic: {cleaned})")
            last_final = winner_final - 1

        # ADD NOTES FOR PRONUNCIATION OF O AS Å
        # Note: Actually this won't work because the "two consonant rule" is too naive

        # if lower_word[vowel_idxs[i]] == "o":
        #     if vowel_phoneme := next((p for p in ps.split() if VOWELS_RE.search(p)), None):
        #         if vowel_phoneme[0] not in "'\",":
        #             continue
        #         vowel_phoneme = vowel_phoneme[1:]
        #         consonant_context = lower_word[vowel_idxs[i] + 1:].replace("ng", "ŋ")
        #         m = CONS_RE.match(consonant_context)
                
        #         if not m:
        #             expected = "long"
        #         elif m.end() - m.start() == 1:
        #             expected = "long"
        #         elif m.group().startswith("r"):
        #             continue
        #         else:
        #             expected = "short"

        #         if vowel_phoneme == "o:" and expected == "long":
        #             irregular.append({
        #                 "span": (vowel_idxs[i], vowel_idxs[i]+1), 
        #                 "type": "long o pronounced as long å"
        #             })
        #         elif vowel_phoneme == "u:" and expected == "long":
        #             irregular.append({
        #                 "span": (vowel_idxs[i], vowel_idxs[i]+1),
        #                 "type": "long o actually pronounced as long o"
        #             })
        #         elif vowel_phoneme == "o" and expected == "short":
        #             irregular.append({
        #                 "span": (vowel_idxs[i], vowel_idxs[i]+1), 
        #                 "type": "short o pronounced as short å"
        #             })
        #         elif vowel_phoneme == "u" and expected == "short":
        #             irregular.append({
        #                 "span": (vowel_idxs[i], vowel_idxs[i]+1),
        #                 "type": "short o actually pronounced as short o"
        #             })


    syllable_ends = syllable_starts[1:] + [len(word)]

    # Adjust syllable boundaries and add morpheme boundary markers

    syllables = []
    for s, e in zip(syllable_starts, syllable_ends):
        syllables.append(word[s:e])
    
    for i, (syl, b) in enumerate(zip(syllables[:], boundaries)):
        if not b or b == ".":
            b = ""
        else:
            b = "|"
        if not syl:
            continue

        syl = syl.strip("-")
        if len(syllables) == 1:
            syllables[i] = syl
            break
        vowel_nucleus = VOWELS_RE.search(syl)
        if not vowel_nucleus: 
            raise ValueError(f"Couldn't find vowel nucleus in [{syl}] of {word} ({syllables})")
        vowel_idx = vowel_nucleus.start()
        if i == primary_stress_idx:
            syllables[i] = syl[:vowel_idx] + "↗" + syl[vowel_idx:] + b
        elif i == secondary_stress_idx:
            syllables[i] = syl[:vowel_idx] + "↘" + syl[vowel_idx:] + b
        else:
            syllables[i] = syl + b

    stress_code = f"{primary_stress_idx}"
    if secondary_stress_idx:
        stress_code += f"-{secondary_stress_idx}"

    # For inflected forms, exclude any irregular pronunciations that begin
    # at the last letter of the lemma
    if word != lemma:
        irregular = [i for i in irregular if i["span"][0] + 1 < len(lemma)]

    return (''.join(syllables), stress_code, irregular or None)


def main():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        # c.execute("""
        #     UPDATE braxen SET syllables = NULL
        # """)

        # Test coverage among Wiktionary lemmas
        # c.execute("""
        #     SELECT b.id, 
        #         b.lemma,
        #         b.lemma,
        #         b.phonetic
        #     FROM braxen b
        #         LEFT JOIN
        #         sv_wiktionary w ON b.lemma = w.lemma
        #     WHERE b.word = b.lemma AND
        #         b.word = w.form AND
        #         syllables IS NULL
        #     GROUP BY b.lemma
        #     ORDER BY random();
        # """)
        
        # Pass 1: process only lemmas (word = lemma)
        c.execute("""
            SELECT id, lemma, word, phonetic FROM braxen
            WHERE word = lemma
        """)

        batch = []
        errors = []
        lemmas_with_no_irreg = set()

        for id, lemma, word, phonetic in c.fetchall():
            if any(c.isdigit() for c in word):
                continue
            try:
                syls, stress_code, irregular_pron = process_entry(word, phonetic, lemma)
                if not any(syls) or not all(syls):
                    errors.append((word, phonetic, syls))
                    continue
                if irregular_pron:
                    irregular_pron = json.dumps(irregular_pron, ensure_ascii=False)
                else:
                    lemmas_with_no_irreg.add(lemma)
                # print(word, phonetic, syls, irregular_pron)
                batch.append((syls, stress_code, irregular_pron, id))
            except:
                errors.append((word, phonetic, None))
                # raise

            if len(batch) == 1000:
                c.executemany("UPDATE braxen SET syllables = ?, stress = ?, irregular_pron = ? WHERE id = ?", batch)
                conn.commit()
                batch = []
        
        if batch:
            c.executemany("UPDATE braxen SET syllables = ?, stress = ?, irregular_pron = ? WHERE id = ?", batch)
        conn.commit()

        print("Syllabification of lemmas complete.")
        
        batch = []
        # Pass 2: process non-lemmas but add irregular_pron only where lemma has irregular_pron
        c.execute("""
            SELECT id, lemma, word, phonetic 
            FROM braxen WHERE word != lemma
        """)

        for id, lemma, word, phonetic in c.fetchall():
            if any(ch.isdigit() for ch in word):
                continue
            # Only suppress if lemma exists and did not have irregular pronunciation
            suppress_irreg = lemma in lemmas_with_no_irreg
            try:
                syls, stress_code, irregular_pron = process_entry(word, phonetic, lemma)
                if not any(syls) or not all(syls):
                    errors.append((word, phonetic, syls))
                    continue
                if suppress_irreg:
                    irregular_pron = None
                elif irregular_pron:
                    irregular_pron = json.dumps(irregular_pron, ensure_ascii=False)
                batch.append((syls, stress_code, irregular_pron, id))
            except:
                errors.append((word, phonetic, None))

            if len(batch) == 1000:
                c.executemany(
                    "UPDATE braxen SET syllables = ?, stress = ?, irregular_pron = ? WHERE id = ?", batch)
                conn.commit()
                batch = []

        if batch:
            c.executemany(
                "UPDATE braxen SET syllables = ?, stress = ?, irregular_pron = ? WHERE id = ?", batch)
        conn.commit()

        print("Syllabification of non-lemma forms complete.")

        print("Errors:", len(errors))
        # for word, phonetic, syls in errors:
        #     print(word, phonetic, syls)


def test(words: list[str]) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        for word in words:
            c.execute("""
                SELECT word, phonetic, lemma FROM braxen
                WHERE word = ?
            """, (word,))
            word, phonetic, lemma = c.fetchone()
            syls, stress_code, irregular_pron = process_entry(word, phonetic, lemma)
            print(word, f"({phonetic}) ->", syls, stress_code, irregular_pron)


if __name__ == "__main__":
    main()