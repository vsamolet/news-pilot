#!/usr/bin/env python3
"""Проверка уникальности рерайта: похожесть на оригинал по 3-/4-словным шинглам.

Страховка для контентного пайплайна — после рерайта YandexGPT (см.
build_system_prompt в pipeline.py) текст сверяется с исходным материалом
источника. Если формулировки и структура предложений слишком близки к
оригиналу, это риск претензии по авторскому праву — материал нужно не
публиковать, а отправить на повторную генерацию.

Метод: очищаем оба текста от пунктуации/цифр и стоп-слов, грубо стеммируем
слова, строим множества 3- и 4-словных шинглов и считаем их похожесть по
Жаккару (|A∩B| / |A∪B|). Итоговая похожесть — среднее по 3- и 4-граммам:
3-граммы ловят точечные повторы оборотов, 4-граммы — более длинные
скопированные куски, которые важнее для риска претензии.

CLI:
    python scripts/check_plagiarism.py --original-text "..." --rewritten-text "..."
    python scripts/check_plagiarism.py --original original.txt --rewritten rewritten.txt

Программный API:
    from check_plagiarism import check_plagiarism
    result = check_plagiarism(original_text, rewritten_text)
    result.status            # "PASS" | "FAIL"
    result.similarity        # 0.0–1.0, среднее по 3- и 4-граммам
    result.similarity_percent  # то же самое в процентах, округлено
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass

# Стоп-слова русского языка: предлоги, союзы, частицы, местоимения — их
# совпадение между текстами ничего не говорит о заимствовании формулировок,
# поэтому исключаем их перед построением шинглов.
RUSSIAN_STOPWORDS = frozenset(
    """
    а без более больше будет будто бы был была были было быть в вам вас вдруг
    ведь во вот впрочем все всегда всего всех всю вы г где да даже два для до
    его ее ей ему если есть еще ж же за зачем здесь и из или им иногда их к
    как какая какой когда конечно кто куда ли лучше между меня мне много может
    можно мой моя мы на над надо назад наконец нас не него нее ней нельзя нет
    ни нибудь никогда ним них ничего но ну о об один он она они опять от перед
    по под потом потому почти при про раз разве с сам свою себе себя сегодня
    сейчас со совсем так такой там те тебя тем теперь то тогда того тоже
    только том тот три тут ты у уж уже хоть хорошо чего чем через что чтоб
    чтобы чуть эти этого этой этом этот эту я
    """.split()
)

# Грубый суффиксальный стеммер — не полноценный морфологический разбор, а
# упрощение специально под сравнение шинглов: без него две фразы с одинаковым
# набором слов, но разными падежными/глагольными окончаниями ("выросли" vs
# "выросла"), считались бы разными n-граммами и занижали бы похожесть —
# то есть маскировали бы реальное копирование формулировок.
_STEM_SUFFIXES = sorted(
    {
        "ического", "ическому", "ическими", "ическом", "ическая", "ическое",
        "ические", "ический",
        "ением", "ению", "ения", "ение", "ении",
        "анием", "анию", "ания", "ание", "ании",
        "ивать", "ывать", "евать", "овать",
        "ующий", "ующая", "ующее", "ующие",
        "остью", "ости", "ость",
        "ями", "иями", "ами", "ов", "ев", "ах", "ях",
        "ому", "ему", "его", "ого", "ыми", "ими",
        "ая", "яя", "ое", "ее", "ых", "их", "ым", "им", "ую", "юю",
        "ешь", "ишь", "ете", "ите", "ем", "ют", "ят", "ет", "ит",
        "ла", "ло", "ли",
        "ы", "и", "а", "я", "у", "ю", "е", "о", "й", "л",
    },
    key=len,
    reverse=True,
)


def _stem(word: str) -> str:
    """Срезает самый длинный подходящий суффикс из _STEM_SUFFIXES, если после
    этого остаётся не меньше 3 символов (чтобы не съесть короткое слово целиком)."""
    for suffix in _STEM_SUFFIXES:
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            return word[: -len(suffix)]
    return word


_WORD_RE = re.compile(r"[a-zA-Zа-яА-ЯёЁ]+")


def normalize(text: str) -> list[str]:
    """Токенизация и очистка текста: убирает пунктуацию и цифры, приводит к
    нижнему регистру, отбрасывает стоп-слова и слова короче 3 букв, грубо
    стеммирует оставшееся."""
    words = _WORD_RE.findall(text.lower())
    return [_stem(w) for w in words if len(w) > 2 and w not in RUSSIAN_STOPWORDS]


def get_shingles(tokens: list[str], n: int) -> set[str]:
    """Множество словосочетаний длины n (n-грамм) из последовательности токенов."""
    if len(tokens) < n:
        return set()
    return {" ".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)}


def jaccard_similarity(set_a: set[str], set_b: set[str]) -> float:
    """|A∩B| / |A∪B|. 0.0, если оба множества пусты (нечего сравнивать —
    ничего похожего, но и текста для анализа не было)."""
    union = set_a | set_b
    if not union:
        return 0.0
    return len(set_a & set_b) / len(union)


@dataclass
class PlagiarismResult:
    status: str  # "PASS" | "FAIL"
    similarity: float  # 0.0-1.0 — среднее jaccard_3/jaccard_4, сравнивается с threshold
    jaccard_3: float
    jaccard_4: float
    threshold: float

    @property
    def similarity_percent(self) -> float:
        return round(self.similarity * 100, 1)


def check_plagiarism(
    original_text: str,
    rewritten_text: str,
    threshold: float = 0.2,
) -> PlagiarismResult:
    """PASS/FAIL по похожести формулировок original_text и rewritten_text.

    FAIL (похожесть выше threshold, по умолчанию 20%) — сигнал отправить
    текст на повторную регенерацию рерайта, а не публиковать как есть.
    """
    original_tokens = normalize(original_text)
    rewritten_tokens = normalize(rewritten_text)

    jaccard_3 = jaccard_similarity(
        get_shingles(original_tokens, 3), get_shingles(rewritten_tokens, 3)
    )
    jaccard_4 = jaccard_similarity(
        get_shingles(original_tokens, 4), get_shingles(rewritten_tokens, 4)
    )
    similarity = (jaccard_3 + jaccard_4) / 2

    status = "FAIL" if similarity > threshold else "PASS"
    return PlagiarismResult(
        status=status,
        similarity=similarity,
        jaccard_3=jaccard_3,
        jaccard_4=jaccard_4,
        threshold=threshold,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Проверка похожести рерайта на оригинал по 3-/4-словным шинглам (Jaccard).",
    )
    parser.add_argument("--original", help="Путь к файлу с исходным текстом")
    parser.add_argument("--rewritten", help="Путь к файлу с текстом рерайта")
    parser.add_argument("--original-text", help="Исходный текст прямо в аргументе (вместо файла)")
    parser.add_argument("--rewritten-text", help="Текст рерайта прямо в аргументе (вместо файла)")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.2,
        help="Порог похожести для FAIL, доля от 0 до 1 (по умолчанию 0.2 = 20%%)",
    )
    args = parser.parse_args()

    def _read(path_arg: str | None, text_arg: str | None, label: str) -> str:
        if text_arg is not None:
            return text_arg
        if path_arg:
            with open(path_arg, encoding="utf-8") as f:
                return f.read()
        parser.error(f"нужно указать --{label} или --{label}-text")
        raise SystemExit(2)  # parser.error уже завершает процесс — это только для типизации

    original_text = _read(args.original, args.original_text, "original")
    rewritten_text = _read(args.rewritten, args.rewritten_text, "rewritten")

    result = check_plagiarism(original_text, rewritten_text, threshold=args.threshold)
    print(f"Статус: {result.status}")
    print(
        f"Похожесть: {result.similarity_percent}% "
        f"(3-граммы: {round(result.jaccard_3 * 100, 1)}%, "
        f"4-граммы: {round(result.jaccard_4 * 100, 1)}%, "
        f"порог: {round(result.threshold * 100, 1)}%)"
    )
    return 0 if result.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
