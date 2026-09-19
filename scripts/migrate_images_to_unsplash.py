#!/usr/bin/env python3
"""
Одноразовый миграционный скрипт: заменяет локальные /images/*.webp (скачанные
и захостенные в public/images/, раздача которых зависает на стриминге тела
ответа через Cloudflare Workers Static Assets — см. коммиты про переход
pipeline.py на прямые CDN-ссылки Unsplash) на новые прямые ссылки на CDN
Unsplash в frontmatter существующих статей src/content/news/*.md.

Оригинальные Unsplash-ссылки/поисковые запросы для уже опубликованных статей
НЕ сохранялись нигде (ни в history.json, ни в publish_log.json, ни в самом
frontmatter) — восстановить точно те же фото невозможно. Поэтому запрос к
Unsplash строится по категории статьи (см. CATEGORY_QUERIES) — это НЕ то же
фото, что было выбрано изначально, но тематически уместное.

НЕ трогает:
- статьи с картинками-заглушками (*.svg в public/images/) — маленькие файлы,
  раздача которых не подвержена багу зависания (воспроизводится только на
  крупных бинарниках);
- src/content/news/dmitriy-lgovskiy-izmenil-napravlenie-dvizheniya.md
  (friend.jpg) — личное фото для отдельной шутки, не с Unsplash, не должно
  подменяться случайным стоковым фото.

Ограничение Unsplash API (demo-тир): 50 запросов/час, тем же ключом
одновременно пользуется cron-пайплайн (pipeline.py). Поэтому запросы
намеренно разнесены по времени (SLEEP_SECONDS) — весь проход по ~48 статьям
занимает около 1.5 часов, чтобы не выедать квоту у cron и оставлять запас.

Запуск (полностью автономный, безопасно прерывать и перезапускать —
уже смигрированные статьи пропускаются):
    python scripts/migrate_images_to_unsplash.py
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

import pipeline  # noqa: E402 — путь добавлен строкой выше

CONTENT_DIR = ROOT_DIR / "src" / "content" / "news"

# Пауза между запросами к Unsplash — 48 статей * 110с ≈ 88 минут, с запасом
# под лимит 50/час и не отъедая всю квоту у параллельно работающего cron.
SLEEP_SECONDS = 110

# Явно исключаем не-Unsplash контент — см. docstring выше.
SKIP_SLUGS = {"dmitriy-lgovskiy-izmenil-napravlenie-dvizheniya"}

CATEGORY_QUERIES: dict[str, list[str]] = {
    "economics": [
        "stock exchange trading floor",
        "central bank building",
        "financial district skyline",
        "currency exchange rates",
    ],
    "business": [
        "corporate office meeting",
        "factory production line",
        "shipping container port",
        "retail store interior",
    ],
    "markets": [
        "stock market ticker screens",
        "commodity trading warehouse",
        "oil refinery",
        "gold bars vault",
    ],
    "tech": [
        "data center servers",
        "software engineer coding",
        "semiconductor microchip",
        "robotics factory",
    ],
    "society": [
        "city street crowd",
        "government building",
        "public transportation",
        "urban skyline",
    ],
}
DEFAULT_QUERIES = ["business office", "city skyline"]

LOCAL_WEBP_RE = re.compile(r'^image:\s*/images/[^\s]+\.webp\s*$', re.MULTILINE)
CATEGORY_RE = re.compile(r'^category:\s*(\S+)\s*$', re.MULTILINE)


def pick_query(category: str, counter: dict[str, int]) -> str:
    candidates = CATEGORY_QUERIES.get(category, DEFAULT_QUERIES)
    idx = counter.get(category, 0) % len(candidates)
    counter[category] = counter.get(category, 0) + 1
    return candidates[idx]


def migrate_file(path: Path, counter: dict[str, int]) -> bool:
    text = path.read_text(encoding="utf-8")
    if not LOCAL_WEBP_RE.search(text):
        return False  # уже смигрирован или это заглушка/friend.jpg

    cat_match = CATEGORY_RE.search(text)
    category = cat_match.group(1) if cat_match else ""
    query = pick_query(category, counter)

    slug = path.stem
    print(f"[{slug}] запрос «{query}» ({category})...", flush=True)

    try:
        image_url, credit_name, credit_url = pipeline.fetch_unsplash_image(query, slug)
    except Exception as exc:  # noqa: BLE001 — одна неудачная статья не должна рушить весь проход
        print(f"[{slug}] ОШИБКА: {exc} — пропускаю, перезапуск скрипта повторит эту статью.", flush=True)
        return False

    text = LOCAL_WEBP_RE.sub(f"image: {pipeline.yaml_quote(image_url)}", text, count=1)

    if re.search(r'^image_credit:.*$', text, re.MULTILINE):
        text = re.sub(
            r'^image_credit:.*$',
            f"image_credit: {pipeline.yaml_quote(credit_name)}" if credit_name else "",
            text,
            count=1,
            flags=re.MULTILINE,
        )
    elif credit_name:
        text = text.replace(
            f"image: {pipeline.yaml_quote(image_url)}",
            f"image: {pipeline.yaml_quote(image_url)}\nimage_credit: {pipeline.yaml_quote(credit_name)}",
            1,
        )

    if re.search(r'^image_credit_url:.*$', text, re.MULTILINE):
        text = re.sub(
            r'^image_credit_url:.*$',
            f"image_credit_url: {pipeline.yaml_quote(credit_url)}" if credit_url else "",
            text,
            count=1,
            flags=re.MULTILINE,
        )
    elif credit_url:
        anchor = f"image_credit: {pipeline.yaml_quote(credit_name)}" if credit_name else f"image: {pipeline.yaml_quote(image_url)}"
        text = text.replace(anchor, f"{anchor}\nimage_credit_url: {pipeline.yaml_quote(credit_url)}", 1)

    # На случай, если credit_name/credit_url оказались пустыми и строку выше
    # заменили на "" — убираем получившуюся пустую строку внутри frontmatter
    # (только между двумя маркерами "---", тело статьи не трогаем).
    fm_match = re.match(r'^---\n(.*?)\n---\n', text, re.DOTALL)
    if fm_match:
        cleaned_fm = "\n".join(l for l in fm_match.group(1).split("\n") if l != "")
        text = f"---\n{cleaned_fm}\n---\n" + text[fm_match.end():]

    path.write_text(text, encoding="utf-8")
    print(f"[{slug}] OK -> {image_url}", flush=True)
    return True


def main() -> None:
    files = sorted(CONTENT_DIR.glob("*.md"))
    targets = [f for f in files if f.stem not in SKIP_SLUGS]

    pending = [f for f in targets if LOCAL_WEBP_RE.search(f.read_text(encoding="utf-8"))]
    print(f"Найдено {len(pending)} статей с локальными webp для миграции.", flush=True)

    counter: dict[str, int] = {}
    migrated = 0
    for i, path in enumerate(pending):
        ok = migrate_file(path, counter)
        if ok:
            migrated += 1
        if i < len(pending) - 1:
            time.sleep(SLEEP_SECONDS)

    print(f"Готово: смигрировано {migrated}/{len(pending)} статей.", flush=True)


if __name__ == "__main__":
    main()
