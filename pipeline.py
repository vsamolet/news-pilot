#!/usr/bin/env python3
"""
Пайплайн автогенерации новостных статей:

1. Парсит RSS-ленты РБК, «Коммерсанта» и «Ведомостей» (feedparser) — см. SOURCES.
2. Сверяется с history.json, чтобы не брать уже опубликованные ссылки.
3. Если несколько источников за один проход дали новости на одну и ту же тему
   (высокое пересечение слов в заголовках), используется только одна запись —
   от источника с более высоким приоритетом (порядок в SOURCES), остальные
   отбрасываются как дубликаты темы (см. deduplicate_by_topic).
4. Проверяет месячный лимит публикаций (MONTHLY_ARTICLE_LIMIT, см. publish_log.json)
   — если лимит на текущий календарный месяц исчерпан, проход останавливается
   ДО обращения к YandexGPT/Unsplash, чтобы не тратить бюджет сверх плана.
5. Скачивает страницу статьи по ссылке из RSS и извлекает полный текст
   первоисточника через trafilatura (если не получилось — откатывается
   на короткий анонс из RSS).
6. Пересказывает новость через YandexGPT API (строгий JSON-ответ, развёрнутая
   заметка 3-5 абзацев с атрибуцией конкретного источника).
7. Ищет и скачивает иллюстрацию через Unsplash API, конвертирует в WebP.
8. Сохраняет готовую статью в src/content/news/<slug>.md с фронтматтером,
   совместимым со схемой контент-коллекции Astro-сайта.

Запуск:
    python pipeline.py                    # один проход: обработать все новые записи и выйти
    python pipeline.py --loop              # непрерывно, проверка каждые 15 минут (по умолчанию)
    python pipeline.py --loop --interval 300   # тот же цикл, но раз в 5 минут
    python pipeline.py --monthly-limit 50      # свой месячный лимит статей вместо 100 по умолчанию

Ключи API читаются из .env (YANDEX_API_KEY, YANDEX_FOLDER_ID, UNSPLASH_ACCESS_KEY).
"""

from __future__ import annotations

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from time import mktime, struct_time
from typing import Any

import feedparser
import requests
import trafilatura
from dotenv import load_dotenv
from PIL import Image

import os

load_dotenv()

# --- Конфигурация -----------------------------------------------------------

# Источники в порядке приоритета: если одна и та же тема встретилась у
# нескольких источников за один проход, остаётся запись от того, что стоит
# в списке раньше (см. deduplicate_by_topic).
SOURCES: list[dict[str, str]] = [
    {"name": "РБК", "rss_url": "https://rssexport.rbc.ru/rbcnews/news/30/full.rss"},
    {"name": "Коммерсантъ", "rss_url": "https://www.kommersant.ru/RSS/news.xml"},
    {"name": "Ведомости", "rss_url": "https://www.vedomosti.ru/rss/news"},
]

ROOT_DIR = Path(__file__).resolve().parent
HISTORY_PATH = ROOT_DIR / "history.json"
PUBLISH_LOG_PATH = ROOT_DIR / "publish_log.json"
IMAGES_DIR = ROOT_DIR / "public" / "images"
CONTENT_DIR = ROOT_DIR / "src" / "content" / "news"

# Сколько статей допускается публиковать за календарный месяц (защита бюджета
# на YandexGPT/Unsplash). Переопределяется флагом --monthly-limit.
MONTHLY_ARTICLE_LIMIT = 100

YANDEX_API_KEY = os.getenv("YANDEX_API_KEY")
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID")
UNSPLASH_ACCESS_KEY = os.getenv("UNSPLASH_ACCESS_KEY")

YANDEXGPT_COMPLETION_URL = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
UNSPLASH_RANDOM_PHOTO_URL = "https://api.unsplash.com/photos/random"

MIN_FULL_TEXT_CHARS = 200  # ниже этого порога считаем извлечение неудачным
MIN_BODY_CHARS = 1200  # минимум по ТЗ для тела статьи


def build_system_prompt(source_name: str) -> str:
    """Системный промпт для YandexGPT. Название источника подставляется в
    правило атрибуции (пункт 5) — у каждой записи из SOURCES оно своё."""
    return f"""Ты — выпускающий редактор независимого делового издания. Твоя задача — написать полноценную, развернутую новостную заметку на основе предоставленного исходного материала.

Правила оформления:
1. Заголовок (title): строгий, информативный, без кликбейта и кавычек в начале.
2. Лид (lead): 1-2 предложения, отвечающие на вопросы "Кто? Что? Где? Когда?".
3. Тело статьи (body): 3–5 развернутых абзацев (минимум 1200–1500 знаков). Полный глубокий пересказ своими словами.
4. Фактура: сохрани абсолютно все цифры, даты, проценты, денежные суммы, должности и фамилии спикеров без искажений.
5. Атрибуция источника: обязательно упомяни первоисточник органично внутри текста (во 2-м или 3-м абзаце), используя конструкции вроде: "сообщает {source_name}", "как передает {source_name}", "об этом пишет {source_name}". Никогда не ставь ссылку в заголовок или самое первое предложение лида.
6. Стиль: строгий новостной нейтралитет, деловая журналистика. Никаких оценочных суждений, штампов ("как стало известно", "эксперты бьют тревогу") и выдуманных фактов.
7. Подбор визуала: image_query — 2-3 конкретных ключевых слова на английском языке для поиска релевантного фото на Unsplash (например: "container port", "medical surgery", "wind turbine").

Технически важно: в значении body разделяй абзацы двойным переносом строки (\\n\\n) — так, чтобы при рендере получилось 3–5 отдельных абзацев, а не один сплошной блок текста.

Формат ответа: СТРОГО валидный JSON без markdown-оберток (без ```json):
{{
  "title": "...",
  "lead": "...",
  "body": "...",
  "category": "...",
  "image_query": "..."
}}"""

# Категории должны совпадать с src/lib/categories.ts на сайте.
ALLOWED_CATEGORIES = {"economics", "business", "markets", "tech", "society"}
DEFAULT_CATEGORY = "society"

# YandexGPT часто возвращает произвольные русские ярлыки вместо ключей схемы
# («Экономика», «Новости бизнеса», «Технологии и оборона» и т.п.) — сопоставляем
# по ключевым словам, чтобы статьи не проваливались все подряд в DEFAULT_CATEGORY.
_CATEGORY_KEYWORDS: list[tuple[str, str]] = [
    ("рынк", "markets"), ("бирж", "markets"), ("валют", "markets"), ("акци", "markets"), ("финанс", "markets"),
    ("эконом", "economics"), ("инфляц", "economics"), ("ввп", "economics"),
    ("технолог", "tech"), ("ии", "tech"), ("искусственн", "tech"), ("робот", "tech"), ("гаджет", "tech"),
    ("бизнес", "business"), ("компани", "business"), ("ритейл", "business"), ("предприят", "business"),
]


def normalize_category(raw_category: str) -> str:
    lowered = raw_category.strip().lower()
    if lowered in ALLOWED_CATEGORIES:
        return lowered
    for keyword, slug in _CATEGORY_KEYWORDS:
        if keyword in lowered:
            return slug
    return DEFAULT_CATEGORY


# --- Пре-фильтр по рубрике RSS (до вызова GPT) --------------------------------
# Мы деловое издание — политику не берём вообще, а «Общество» пропускаем только
# если материал по факту про бизнес/деньги/экономику. Работает на данных самой
# RSS-записи (категория + title/summary), без единого сетевого запроса к
# YandexGPT/Unsplash — экономит бюджет на явно нецелевых новостях (по логам
# сессии на политику/спорт/происшествия уходило около трети всех вызовов GPT).

# Рубрика содержит один из этих кусков — новость отбрасывается сразу, без
# проверки текста. "мир" — намеренно сюда же: у "Коммерсанта" это фактически
# рубрика геополитики/дипломатии (Песков, Лавров и т.п.), международный бизнес
# там же помечается отдельно как "Бизнес"/"Экономика".
EXCLUDED_CATEGORY_KEYWORDS = [
    "полит", "мир", "происшеств", "спорт", "культур", "радио", "туризм", "погод",
]

# Рубрика содержит один из этих кусков — новость сразу считается деловой,
# текст можно не проверять.
BUSINESS_CATEGORY_KEYWORDS = [
    "бизнес", "эконом", "инвестиц", "финанс", "рынк", "технолог", "телекоммуник",
    "авто", "медиа", "торговл", "агропром", "транспорт", "тэк", "it-бизнес",
]

# Если рубрика неопределённая/общая ("Общество", "Новости" и т.п.) — решаем по
# ключевым словам в заголовке+анонсе: похоже это на деловую новость или нет.
BUSINESS_TEXT_KEYWORDS = [
    "компани", "бизнес", "рынок", "рынка", "рынке", "рынку", "цена", "цены", "ценах",
    "тариф", "банк", "рубл", "доллар", "инвестор", "инвестиц", "экономик", "налог",
    "зарплат", "кредит", "ипотек", "акци", "бирж", "выручк", "прибыл", "убыт",
    "капитал", "миллиард", "миллион", "трлн", "триллион", "поставк",
    "экспорт", "импорт", "производств", "завод", "выпуск", "сделк", "контракт",
    "ставк", "ввп", "инфляц", "долг", "бюджет", "холдинг", "ритейл", "магазин",
]


def _has_any(text: str, keywords: list[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def passes_topic_filter(entry: Any) -> bool:
    """Пре-фильтр по рубрике/тексту RSS-записи — работает до любого сетевого
    вызова к платным API. Возвращает False для явно не нашей тематики
    (политика и её эквиваленты у разных источников, спорт, происшествия,
    культура и т.п.) и для «Общества»/generic-рубрик без бизнес-контекста."""
    tags = entry.get("tags") or []
    category = (tags[0].get("term") if tags else "") or ""
    category_lower = category.lower()

    if _has_any(category_lower, EXCLUDED_CATEGORY_KEYWORDS):
        return False

    if _has_any(category_lower, BUSINESS_CATEGORY_KEYWORDS):
        return True

    # Рубрика неопределённая (в т.ч. «Общество») — смотрим на текст.
    title = (entry.get("title") or "").lower()
    summary = (entry.get("summary") or entry.get("description") or "").lower()
    return _has_any(title + " " + summary, BUSINESS_TEXT_KEYWORDS)


REQUEST_TIMEOUT = 60
DEFAULT_INTERVAL_SECONDS = 15 * 60  # 15 минут

# --- История обработанных ссылок --------------------------------------------


def load_history() -> set[str]:
    if not HISTORY_PATH.exists():
        return set()
    try:
        data = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        print(f"Предупреждение: {HISTORY_PATH.name} повреждён, начинаю с пустой истории.", file=sys.stderr)
        return set()
    return set(data.get("published_links", []))


def save_history(links: set[str]) -> None:
    payload = {"published_links": sorted(links)}
    HISTORY_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


# --- Месячный лимит публикаций ------------------------------------------------
# history.json защищает от повторной публикации ОДНОЙ и той же ссылки, но не
# ограничивает суммарное число статей в месяц. publish_log.json — отдельный
# журнал с датой каждой публикации, по нему run_once считает, сколько статей
# уже вышло в текущем календарном месяце, и останавливается ДО вызова
# YandexGPT/Unsplash, как только лимит исчерпан.


def load_publish_log() -> list[dict[str, str]]:
    if not PUBLISH_LOG_PATH.exists():
        return []
    try:
        data = json.loads(PUBLISH_LOG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        print(f"Предупреждение: {PUBLISH_LOG_PATH.name} повреждён, начинаю с пустого журнала.", file=sys.stderr)
        return []
    return data.get("published", [])


def append_publish_log(link: str) -> None:
    log = load_publish_log()
    log.append({"link": link, "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")})
    payload = {"published": log}
    PUBLISH_LOG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _current_month_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def count_published_this_month(log: list[dict[str, str]] | None = None) -> int:
    log = load_publish_log() if log is None else log
    month = _current_month_key()
    return sum(1 for item in log if item.get("at", "").startswith(month))


# --- RSS ----------------------------------------------------------------------


def fetch_fresh_entries_all_sources(history: set[str]) -> list[Any]:
    """Парсит RSS всех источников из SOURCES, размечает каждую запись именем
    источника (entry["source_name"]) и убирает уже встречавшиеся ссылки, а
    также записи не по нашей тематике (passes_topic_filter — политика и
    подобные рубрики отсеиваются здесь же, без единого вызова GPT). Дальше —
    deduplicate_by_topic убирает дубли темы между источниками, а результат
    сортируется по дате публикации (от старых к новым)."""
    all_fresh: list[Any] = []

    for source in SOURCES:
        try:
            feed = feedparser.parse(source["rss_url"])
        except Exception as exc:  # noqa: BLE001 — один упавший источник не должен ронять остальные
            print(f"Не удалось получить RSS «{source['name']}»: {exc}", file=sys.stderr)
            continue

        if feed.bozo and not feed.entries:
            print(
                f"Предупреждение: RSS «{source['name']}» не распарсился ({feed.bozo_exception}).",
                file=sys.stderr,
            )
            continue

        new_entries = [e for e in feed.entries if e.get("link") and e.get("link") not in history]
        fresh = [e for e in new_entries if passes_topic_filter(e)]
        filtered_out = len(new_entries) - len(fresh)
        for entry in fresh:
            entry["source_name"] = source["name"]
        print(
            f"{source['name']}: {len(fresh)} новых записей по теме "
            f"(отфильтровано как нецелевые: {filtered_out})."
        )
        all_fresh.extend(fresh)

    deduped = deduplicate_by_topic(all_fresh)
    deduped.sort(key=entry_pub_date)
    return deduped


def deduplicate_by_topic(entries: list[Any]) -> list[Any]:
    """Если новости из РАЗНЫХ источников описывают одну и ту же тему (высокое
    пересечение слов в заголовке — см. _titles_match), оставляет только одну:
    от источника с более высоким приоритетом (порядок в SOURCES). Записи из
    одного и того же источника друг с другом не сравниваются — считаем, что
    один источник дублей темы внутри своей ленты не даёт."""
    source_priority = {s["name"]: i for i, s in enumerate(SOURCES)}
    # Обрабатываем в порядке приоритета источника, чтобы более приоритетный
    # всегда оказывался в kept раньше и «побеждал» при сравнении.
    ordered = sorted(entries, key=lambda e: source_priority.get(e.get("source_name"), len(SOURCES)))

    kept: list[Any] = []
    for entry in ordered:
        title = entry.get("title") or ""
        duplicate_of = next(
            (
                kept_entry
                for kept_entry in kept
                if kept_entry.get("source_name") != entry.get("source_name")
                and _titles_match(title, kept_entry.get("title") or "")
            ),
            None,
        )
        if duplicate_of is not None:
            print(
                f"Дубликат темы: «{title}» ({entry.get('source_name')}) совпадает с уже отобранной "
                f"«{duplicate_of.get('title')}» ({duplicate_of.get('source_name')}) — "
                f"оставляю версию {duplicate_of.get('source_name')}, эту пропускаю."
            )
            continue
        kept.append(entry)

    return kept


def entry_summary_fallback(entry: Any) -> str:
    summary = entry.get("summary") or entry.get("description") or ""
    summary = re.sub(r"<[^>]+>", " ", summary)
    summary = re.sub(r"\s+", " ", summary).strip()
    return summary


TITLE_OVERLAP_THRESHOLD = 0.3  # доля общих слов заголовков (см. _titles_match) — используется и
# для детекта подмены страницы (fetch_full_text), и для кросс-source дедупликации тем (deduplicate_by_topic)
_STEM_LEN = 5  # грубый стемминг: обрезаем слово длиннее этого до первых N символов, чтобы
# не терять совпадения из-за русских падежных окончаний («Венгрия» / «Венгрии»)

# ВНИМАНИЕ: некоторые URL РБК не являются стабильными постоянными ссылками —
# в первую очередь короткие идентификаторы вида /rbcfreenews/<hash> (а иногда и
# /quote/.../<hash>). РБК может позже переиспользовать тот же URL под совсем
# другой материал (тот же canonical/og:url, но другой текст и заголовок).
# Наблюдалось на практике: full-text extraction по такой ссылке через дни/часы
# после публикации вернул текст ДРУГОЙ новости с тем же адресом страницы.
# Пока ссылка обрабатывается сразу же (штатный сценарий pipeline.py — свежая
# запись из RSS), это не страшно: страница ещё актуальна. Проблема возникает
# только при ПОВТОРНОМ fetch уже старых ссылок спустя время (например, при
# ручной регенерации существующих статей). Поэтому ниже — не блокировка по
# паттерну URL (это дало бы много ложных срабатываний на свежих ссылках), а
# сверка заголовка страницы с заголовком из RSS: если они разошлись, это и
# есть признак подмены — используем анонс из RSS вместо потенциально
# нерелевантного полного текста.


def _titles_match(expected: str, actual: str, threshold: float = TITLE_OVERLAP_THRESHOLD) -> bool:
    def words(s: str) -> set[str]:
        raw = re.findall(r"[а-яёa-z0-9]+", s.lower())
        return {w[:_STEM_LEN] if len(w) > _STEM_LEN else w for w in raw}

    expected_words, actual_words = words(expected), words(actual)
    if not expected_words or not actual_words:
        return True  # недостаточно данных для сравнения — не блокируем

    overlap = len(expected_words & actual_words) / len(expected_words | actual_words)
    return overlap >= threshold


def fetch_full_text(url: str, expected_title: str | None = None) -> str | None:
    """Скачивает страницу первоисточника и извлекает основной текст статьи
    через trafilatura. Возвращает None, если скачать/извлечь не удалось —
    либо если заголовок скачанной страницы явно разошёлся с ожидаемым
    (см. предупреждение про нестабильные URL РБК выше)."""
    try:
        downloaded = trafilatura.fetch_url(url)
    except Exception:
        downloaded = None

    if not downloaded:
        return None

    try:
        result_json = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=False,
            favor_recall=True,
            output_format="json",
            with_metadata=True,
        )
    except Exception:
        result_json = None

    if not result_json:
        return None

    try:
        data = json.loads(result_json)
    except json.JSONDecodeError:
        return None

    text = (data.get("text") or "").strip()
    page_title = (data.get("title") or "").strip()

    if not text or len(text) < MIN_FULL_TEXT_CHARS:
        return None

    if expected_title and page_title and not _titles_match(expected_title, page_title):
        print(
            f"⚠️  Предупреждение: страница {url}, похоже, была подменена РБК — "
            f"заголовок на странице сейчас «{page_title}», а ожидался «{expected_title}». "
            "Такое случается с недолговечными URL (/rbcfreenews/, иногда /quote/) — "
            "РБК может со временем переиспользовать тот же адрес под другой материал. "
            "Игнорирую скачанный полный текст, использую анонс из RSS.",
            file=sys.stderr,
        )
        return None

    return text


def build_source_material(entry: Any) -> str:
    """Готовит текст для передачи модели: полный текст первоисточника,
    а если извлечь не удалось (или страница оказалась подменена) — краткий
    анонс из RSS (title + summary)."""
    title = (entry.get("title") or "").strip()
    link = entry.get("link") or ""
    source_name = entry.get("source_name") or "источник"

    full_text = fetch_full_text(link, expected_title=title) if link else None
    if full_text:
        print(f"Полный текст первоисточника извлечён ({len(full_text)} симв.).")
        body_text = full_text
    else:
        print("Не удалось извлечь полный текст — использую анонс из RSS.")
        body_text = entry_summary_fallback(entry)

    return (
        f"Источник: {source_name}\n"
        f"Заголовок исходной новости: {title}\n\n"
        f"Текст:\n{body_text}"
    )


def entry_pub_date(entry: Any) -> datetime:
    parsed: struct_time | None = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed is None:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(mktime(parsed), tz=timezone.utc)


# --- YandexGPT ------------------------------------------------------------


def call_yandex_gpt(source_material: str, source_name: str) -> dict[str, str]:
    if not YANDEX_API_KEY or not YANDEX_FOLDER_ID:
        raise RuntimeError("YANDEX_API_KEY / YANDEX_FOLDER_ID не заданы в .env")

    payload = {
        "modelUri": f"gpt://{YANDEX_FOLDER_ID}/yandexgpt/latest",
        "completionOptions": {
            "stream": False,
            "temperature": 0.3,
            "maxTokens": 2500,  # с запасом над минимумом в 2000 — тело статьи требуется развёрнутое
        },
        "messages": [
            {"role": "system", "text": build_system_prompt(source_name)},
            {"role": "user", "text": source_material},
        ],
    }
    headers = {
        "Authorization": f"Api-Key {YANDEX_API_KEY}",
        "x-folder-id": YANDEX_FOLDER_ID,
        "Content-Type": "application/json",
    }

    response = requests.post(
        YANDEXGPT_COMPLETION_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    data = response.json()

    try:
        raw_text = data["result"]["alternatives"][0]["message"]["text"]
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Неожиданный формат ответа YandexGPT: {data}") from exc

    return parse_model_json(raw_text)


def parse_model_json(raw_text: str) -> dict[str, str]:
    cleaned = raw_text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.MULTILINE).strip()

    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if not match:
        raise ValueError(f"Не удалось найти JSON в ответе модели: {cleaned[:300]!r}")

    parsed = json.loads(match.group(0))

    required_keys = {"title", "lead", "body", "category", "image_query"}
    missing = required_keys - parsed.keys()
    if missing:
        raise ValueError(f"В ответе модели отсутствуют ключи: {sorted(missing)}")

    normalized = normalize_category(parsed["category"])
    if normalized != parsed["category"].strip().lower():
        print(
            f"Предупреждение: категория модели {parsed['category']!r} сопоставлена с {normalized!r}.",
            file=sys.stderr,
        )
    parsed["category"] = normalized

    body_len = len(parsed["body"].strip())
    if body_len < MIN_BODY_CHARS:
        print(
            f"Предупреждение: тело статьи короче ожидаемого ({body_len} знаков, минимум {MIN_BODY_CHARS}).",
            file=sys.stderr,
        )

    return parsed


# --- Unsplash -----------------------------------------------------------------


def _search_unsplash_photo(query: str) -> dict:
    response = requests.get(
        UNSPLASH_RANDOM_PHOTO_URL,
        params={"query": query, "orientation": "landscape", "content_filter": "high"},
        headers={"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def fetch_unsplash_image(query: str, slug: str) -> str:
    if not UNSPLASH_ACCESS_KEY:
        raise RuntimeError("UNSPLASH_ACCESS_KEY не задан в .env")

    try:
        photo = _search_unsplash_photo(query)
    except requests.HTTPError as exc:
        if exc.response is None or exc.response.status_code != 404:
            raise
        # Составные запросы вида "gold trading, stock exchange" иногда не дают
        # результатов на /photos/random — пробуем первую фразу до запятой.
        fallback_query = query.split(",")[0].strip()
        if not fallback_query or fallback_query == query:
            raise
        print(f"Unsplash не нашёл фото по «{query}», пробую «{fallback_query}»...")
        photo = _search_unsplash_photo(fallback_query)
    if isinstance(photo, list):  # на случай, если Unsplash вернёт список
        photo = photo[0]
    image_url = photo["urls"]["regular"]

    image_response = requests.get(image_url, timeout=REQUEST_TIMEOUT)
    image_response.raise_for_status()

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{slug}.webp"
    filepath = IMAGES_DIR / filename

    image = Image.open(io.BytesIO(image_response.content)).convert("RGB")
    image.save(filepath, "WEBP", quality=85)

    return f"/images/{filename}"


# --- Markdown -------------------------------------------------------------------

_TRANSLIT_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "",
    "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}


def slugify(text: str) -> str:
    lowered = text.lower()
    transliterated = "".join(_TRANSLIT_MAP.get(ch, ch) for ch in lowered)
    slug = re.sub(r"[^a-z0-9]+", "-", transliterated).strip("-")
    return slug or "news"


def unique_slug(base_slug: str) -> str:
    slug = base_slug
    counter = 2
    while (CONTENT_DIR / f"{slug}.md").exists():
        slug = f"{base_slug}-{counter}"
        counter += 1
    return slug


def yaml_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
    return f'"{escaped}"'


def build_markdown(
    article: dict[str, str],
    slug: str,
    image_path: str,
    source_url: str,
    pub_date: datetime,
) -> Path:
    frontmatter_lines = [
        "---",
        f"title: {yaml_quote(article['title'].strip())}",
        f"lead: {yaml_quote(article['lead'].strip())}",
        f"pubDate: {pub_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')}",
        f"category: {article['category']}",
        f"image: {image_path}",
        f"source_url: {yaml_quote(source_url)}",
        "---",
        "",
    ]

    body = article["body"].strip() + "\n"
    content = "\n".join(frontmatter_lines) + body

    CONTENT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = CONTENT_DIR / f"{slug}.md"
    filepath.write_text(content, encoding="utf-8")
    return filepath


# --- main -----------------------------------------------------------------------


def process_entry(entry: Any, history: set[str]) -> Path:
    """Обрабатывает одну запись ленты и сохраняет её в history сразу после успеха,
    чтобы сбой на следующей записи не привёл к повторной публикации этой."""
    source_name = entry.get("source_name") or "источник"
    print(f"Новая новость ({source_name}): {entry.title}\n{entry.link}")

    source_material = build_source_material(entry)

    print("Отправляю текст в YandexGPT...")
    article = call_yandex_gpt(source_material, source_name)

    base_slug = slugify(article["title"])
    slug = unique_slug(base_slug)

    print(f"Ищу изображение по запросу «{article['image_query']}» на Unsplash...")
    image_path = fetch_unsplash_image(article["image_query"], slug)

    pub_date = entry_pub_date(entry)
    filepath = build_markdown(article, slug, image_path, entry.link, pub_date)

    history.add(entry.link)
    save_history(history)
    append_publish_log(entry.link)

    print(f"Готово: {filepath.relative_to(ROOT_DIR)}")
    print(f"Изображение: {image_path}")
    return filepath


def run_once(history: set[str], monthly_limit: int = MONTHLY_ARTICLE_LIMIT) -> int:
    """Один проход: забирает все новые (не встречавшиеся в history.json) записи
    ленты и публикует каждую — но не больше, чем позволяет остаток месячного
    лимита (monthly_limit, считается по publish_log.json за текущий календарный
    месяц). Ошибка на одной записи не прерывает обработку остальных и не
    помечает её как обработанную — она будет подхвачена на следующем проходе."""
    published_this_month = count_published_this_month()
    if published_this_month >= monthly_limit:
        print(
            f"Месячный лимит публикаций исчерпан: {published_this_month}/{monthly_limit} за "
            f"{_current_month_key()}. Пропускаю проверку лент — не трачу бюджет на YandexGPT/Unsplash "
            "до начала следующего месяца (или запустите с --monthly-limit больше)."
        )
        return 0

    print(f"Получаю RSS-ленты источников: {', '.join(s['name'] for s in SOURCES)}...")
    fresh_entries = fetch_fresh_entries_all_sources(history)

    if not fresh_entries:
        print("Новых новостей нет — все ссылки из ленты уже в history.json.")
        return 0

    remaining_budget = monthly_limit - published_this_month
    print(f"Месячный лимит: опубликовано {published_this_month}/{monthly_limit}, доступно ещё {remaining_budget}.")

    published = 0
    for entry in fresh_entries:
        if published >= remaining_budget:
            skipped = len(fresh_entries) - published
            print(
                f"Достигнут месячный лимит публикаций ({monthly_limit}) — останавливаюсь, "
                f"{skipped} оставшихся новостей будут обработаны в следующем месяце."
            )
            break
        try:
            process_entry(entry, history)
            published += 1
        except Exception as exc:  # noqa: BLE001 — не прерываем цикл из-за одной новости
            print(f"Пропускаю «{entry.get('title', entry.get('link'))}»: {exc}", file=sys.stderr)

    print(f"Опубликовано новых статей: {published}/{len(fresh_entries)}")
    return published


def run_loop(interval: int, monthly_limit: int = MONTHLY_ARTICLE_LIMIT) -> int:
    print(f"Режим цикла: проверяю RSS-ленты каждые {interval} сек. Месячный лимит: {monthly_limit}. Остановка — Ctrl+C.")
    history = load_history()
    while True:
        try:
            run_once(history, monthly_limit)
        except Exception as exc:  # noqa: BLE001 — сбой одной проверки не должен убивать цикл
            print(f"Ошибка при проверке ленты: {exc}", file=sys.stderr)

        try:
            print(f"Следующая проверка через {interval} сек.\n")
            time.sleep(interval)
        except KeyboardInterrupt:
            print("Остановлено пользователем.")
            return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Автопубликация новостей (РБК, Коммерсантъ, Ведомости) через YandexGPT + Unsplash."
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Не выходить после одного прохода, а проверять ленту периодически.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help=f"Интервал между проверками в секундах в режиме --loop (по умолчанию {DEFAULT_INTERVAL_SECONDS} = 15 минут).",
    )
    parser.add_argument(
        "--monthly-limit",
        type=int,
        default=MONTHLY_ARTICLE_LIMIT,
        help=f"Максимум публикаций за календарный месяц (по умолчанию {MONTHLY_ARTICLE_LIMIT}). "
        "При достижении лимита проход останавливается до вызова YandexGPT/Unsplash.",
    )
    args = parser.parse_args()

    if args.loop:
        return run_loop(args.interval, args.monthly_limit)

    history = load_history()
    run_once(history, args.monthly_limit)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Остановлено пользователем.")
        sys.exit(0)
    except Exception as exc:  # noqa: BLE001 — единая точка отчёта об ошибке для CLI-скрипта
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)
