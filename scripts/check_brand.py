#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "requests>=2.31",
#   "beautifulsoup4>=4.12",
#   "python-whois>=0.9.4",
# ]
# ///
"""Проверка названий для медиа: домен, WHOIS, реестр СМИ РКН, реестр товарных знаков.

Запуск (изолированно, ничего не трогает в системном Python):

    uv run scripts/check_brand.py "Деловой Регистр" "Биржевой Срез" "Линия Рынка"

Без uv — обычный venv:

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r scripts/requirements-check_brand.txt
    python scripts/check_brand.py "Название 1" "Название 2"

Полезные флаги:
    --zone ru               зона домена (по умолчанию ru)
    --skip-online            не ходить в РКН/Роспатент, проверить только домен
    --timeout 5               таймаут сетевых запросов, сек
    --workers 4                параллельная обработка названий

ВАЖНО:
- Реестр СМИ РКН проверяется автоматически (форма поиска по названию на
  rkn.gov.ru) с сверкой на точное совпадение — это эвристический скрининг,
  а не юридическое заключение. Если сайт недоступен или поменял вёрстку,
  скрипт честно пишет "не проверено", а не делает вид, что имя свободно.
- Реестр товарных знаков Роспатента не имеет простого публичного API без
  капчи и авторизации (поиск на searchplatform.rospatent.gov.ru работает
  через сессионный API, рассчитанный на браузер) — эта проверка в скрипте
  всегда отдаётся на ручную проверку по ссылке, а не подделывается.
- Перед регистрацией СМИ и подачей заявки на товарный знак финальную
  проверку нужно делать вручную/у юриста.
"""

from __future__ import annotations

import argparse
import logging
import re
import socket
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

try:
    import requests
except ImportError:  # pragma: no cover - деградация без сети
    requests = None

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - деградация без парсера
    BeautifulSoup = None

try:
    import whois as pywhois
except ImportError:  # pragma: no cover - деградация без whois
    pywhois = None
else:
    # У python-whois при сетевых сбоях логгер сам печатает диагностику —
    # мы уже честно отражаем сбой в таблице (см. check_whois), поэтому
    # приглушаем дублирующий вывод библиотеки.
    logging.getLogger("whois.whois").setLevel(logging.CRITICAL)


USER_AGENT = (
    "Mozilla/5.0 (compatible; check_brand.py/1.0; "
    "brand-name due-diligence script; +local-use)"
)

# Стандартная практическая транслитерация (как у Яндекса для URL/доменов).
TRANSLIT_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "c", "ч": "ch", "ш": "sh", "щ": "sch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}


def transliterate(text: str) -> str:
    """Транслитерирует кириллицу в латиницу и убирает всё, кроме букв/цифр."""
    result = []
    for ch in text.lower():
        result.append(TRANSLIT_MAP.get(ch, ch))
    slug = "".join(result)
    return re.sub(r"[^a-z0-9]", "", slug)


def make_domain(name: str, zone: str) -> str:
    slug = transliterate(name)
    return f"{slug}.{zone}"


@dataclass
class BrandCheck:
    name: str
    domain: str
    domain_status: str = "не проверено"
    domain_free: Optional[bool] = None
    rkn_found: Optional[bool] = None
    tm_found: Optional[bool] = None
    notes: list = field(default_factory=list)

    @property
    def registries_summary(self) -> str:
        def fmt(label: str, value: Optional[bool]) -> str:
            if value is True:
                return f"{label}: ⚠ есть совпадение"
            if value is False:
                return f"{label}: нет"
            return f"{label}: не проверено"

        return "; ".join([fmt("РКН", self.rkn_found), fmt("ТЗ", self.tm_found)])

    @property
    def verdict(self) -> str:
        if self.domain_free is False or self.rkn_found or self.tm_found:
            return "⚠ ЕСТЬ РИСК — не рекомендуется без проверки юристом"
        if self.domain_free is None or self.rkn_found is None or self.tm_found is None:
            return "❓ Требуется ручная проверка (часть данных недоступна)"
        return "✅ Похоже, свободно (перепроверьте вручную перед регистрацией)"


def check_dns(domain: str) -> Optional[bool]:
    """True — домен занят (есть A-запись), False — свободен, None — не удалось проверить.

    socket.gethostbyname не принимает свой таймаут — используется глобальный
    socket.setdefaulttimeout, который main() выставляет один раз перед
    запуском пула потоков (мутировать его из каждого потока небезопасно:
    гонка потоков может навсегда испортить глобальное состояние сокетов).
    """
    try:
        socket.gethostbyname(domain)
        return True
    except socket.gaierror:
        return False
    except OSError:
        return None


_WHOIS_TRANSPORT_ERROR_MARKERS = (
    "error trying to connect to socket",
    "connection refused",
    "connection reset",
    "timed out",
    "network is unreachable",
)


def check_whois(domain: str, timeout: float) -> Optional[bool]:
    """True — домен зарегистрирован, False — похоже свободен, None — недоступно.

    python-whois при сбое сокета (нет сети, порт 43 заблокирован фаерволом)
    не бросает исключение, а молча возвращает объект с пустым текстом
    ошибки вместо ответа whois-сервера — такой случай нужно отличать от
    настоящего "домен свободен", иначе получим ложноположительный вердикт.
    """
    if pywhois is None:
        return None
    try:
        data = pywhois.whois(domain, timeout=int(timeout))
    except Exception:
        return None
    if not data:
        return None

    raw_text = (getattr(data, "text", "") or "").lower()
    if any(marker in raw_text for marker in _WHOIS_TRANSPORT_ERROR_MARKERS):
        return None

    registered_markers = (
        getattr(data, "domain_name", None),
        getattr(data, "creation_date", None),
        getattr(data, "registrar", None),
    )
    return any(registered_markers)


def resolve_domain_status(check: BrandCheck, timeout: float) -> None:
    dns_hit = check_dns(check.domain)
    if dns_hit is True:
        check.domain_free = False
        check.domain_status = "ЗАНЯТ (есть A-запись)"
        return

    if dns_hit is None:
        # Сам DNS-запрос не прошёл (нет сети/таймаут) — это не значит, что
        # домен свободен, поэтому WHOIS в этой ситуации не запускаем.
        check.domain_free = None
        check.domain_status = "не удалось проверить (нет сети/таймаут DNS)"
        return

    # dns_hit is False: A-записи нет — уточняем через WHOIS, т.к. домен
    # может быть занят, но без настроенных DNS-записей.
    whois_hit = check_whois(check.domain, timeout)
    if whois_hit is True:
        check.domain_free = False
        check.domain_status = "ЗАНЯТ (нет A-записи, но виден в WHOIS)"
    elif whois_hit is False:
        check.domain_free = True
        check.domain_status = "СВОБОДЕН"
    else:
        check.domain_free = True
        check.domain_status = "СВОБОДЕН (по DNS, WHOIS недоступен)"
        check.notes.append("WHOIS не проверен — установите python-whois или проверьте вручную")


_QUOTE_CHARS = "\"'«»„“”`"


def _normalize_media_name(name: str) -> str:
    stripped = name.strip(_QUOTE_CHARS + " \t")
    return re.sub(r"\s+", " ", stripped).lower()


def _extract_rkn_names(html: str) -> list[str]:
    """Достаёт названия СМИ из таблицы результатов (class="TblList")."""
    if BeautifulSoup is not None:
        try:
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table", class_="TblList")
            if table is None:
                return []
            names = []
            for row in table.find_all("tr")[1:]:  # первая tr — заголовок thead
                cell = row.find("td")
                if cell is None:
                    continue
                text = cell.get_text(strip=True)
                if text:
                    names.append(text)
            return names
        except Exception:
            pass
    # Фолбэк без beautifulsoup4: грубый разбор ссылок на карточки СМИ
    return re.findall(r'<a[^>]*href="\?id=\d+[^"]*"[^>]*>([^<]+)</a>', html)


def check_rkn_registry(check: BrandCheck, timeout: float, url: str) -> None:
    """Ищет точное совпадение названия в открытом реестре зарегистрированных СМИ РКН.

    Реестр сам делает поиск по подстроке (форма на rkn.gov.ru, поле
    smi_name), поэтому результат почти всегда — список похожих названий;
    здесь он сверяется на точное совпадение (без учёта кавычек/регистра/
    лишних пробелов), а не просто "нашлась хоть какая-то строка".
    check.rkn_found: True — есть точное совпадение, False — точных нет
    (возможно, есть похожие — см. check.notes), None — сайт недоступен
    или изменил вёрстку и результату нельзя доверять.
    """
    if requests is None:
        check.rkn_found = None
        return
    data = {
        "act": "search",
        "cert_num": "",
        "smi_name": check.name,
        "staff_address": "",
        "TERR_ID": "0",
        "STATUS_ID": "0",
    }
    try:
        resp = requests.post(url, data=data, timeout=timeout, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        html = resp.text
    except Exception:
        check.rkn_found = None
        check.notes.append("РКН: сайт недоступен/таймаут — проверьте вручную")
        return

    if "записей не найдено" in html.lower():
        check.rkn_found = False
        return

    names = _extract_rkn_names(html)
    if not names:
        # Ни маркера "не найдено", ни таблицы результатов — похоже, сайт
        # поменял вёрстку. Не делаем вид, что имя точно свободно.
        check.rkn_found = None
        check.notes.append("РКН: не удалось разобрать ответ сайта — проверьте вручную")
        return

    target = _normalize_media_name(check.name)
    if any(_normalize_media_name(n) == target for n in names):
        check.rkn_found = True
        return

    check.rkn_found = False
    similar = [n for n in names if _normalize_media_name(n) != target][:3]
    if similar:
        check.notes.append(
            "РКН: точных совпадений нет, но есть похожие названия: " + "; ".join(similar)
        )


def check_trademark_registry(check: BrandCheck, timeout: float, url: str) -> None:
    """Товарные знаки: Роспатент (searchplatform.rospatent.gov.ru) не отдаёт
    результаты поиска простым GET/POST — это SPA поверх сессионного API
    (создание сессии, привязка параметров запроса, опрос результата),
    рассчитанного на браузер, а не на разовый запрос скрипта. Реализовать
    это надёжно без риска сломаться при любом обновлении фронтенда не
    получится, поэтому здесь оставлена честная заглушка — check.tm_found
    всегда None, а не подделанное "совпадений нет".
    """
    check.tm_found = None
    check.notes.append(f"Товарные знаки: проверьте вручную — {url}")


def process_name(
    name: str,
    zone: str,
    timeout: float,
    skip_online: bool,
    rkn_url: str,
    tm_url: str,
) -> BrandCheck:
    domain = make_domain(name, zone)
    check = BrandCheck(name=name, domain=domain)

    resolve_domain_status(check, timeout)

    if skip_online:
        check.notes.append("проверка РКН/Роспатента пропущена (--skip-online)")
        return check

    check_rkn_registry(check, timeout, rkn_url)
    check_trademark_registry(check, timeout, tm_url)

    if requests is None:
        check.notes.append("модуль requests не установлен — онлайн-проверки недоступны")
    if BeautifulSoup is None:
        check.notes.append("модуль beautifulsoup4 не установлен — разбор HTML упрощён")

    return check


def render_table(results: list[BrandCheck]) -> str:
    headers = ["Название", "Домен .ru", "Статус домена", "Найдено в реестрах", "Вердикт"]
    rows = [
        [r.name, r.domain, r.domain_status, r.registries_summary, r.verdict]
        for r in results
    ]

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(cells: list[str]) -> str:
        return " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells))

    sep = "-+-".join("-" * w for w in widths)

    lines = [fmt_row(headers), sep]
    lines.extend(fmt_row(row) for row in rows)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Проверка названий для медиа: домен, WHOIS, реестр СМИ РКН, реестр товарных знаков.",
    )
    parser.add_argument("names", nargs="+", help='Названия для проверки, например: "Деловой Регистр"')
    parser.add_argument("--zone", default="ru", help="Доменная зона (по умолчанию: ru)")
    parser.add_argument("--timeout", type=float, default=5.0, help="Таймаут сетевых запросов, сек")
    parser.add_argument("--workers", type=int, default=4, help="Сколько названий проверять параллельно")
    parser.add_argument(
        "--skip-online",
        action="store_true",
        help="Не обращаться к РКН/Роспатенту, проверить только домен (DNS/WHOIS)",
    )
    parser.add_argument(
        "--rkn-url",
        default="https://rkn.gov.ru/activity/mass-media/for-founders/media/",
        help="URL формы поиска по реестру зарегистрированных СМИ (РКН)",
    )
    parser.add_argument(
        "--tm-url",
        default="https://searchplatform.rospatent.gov.ru/trademarks",
        help="Ссылка на открытый поиск по реестру товарных знаков (Роспатент) для ручной проверки",
    )
    args = parser.parse_args()

    # Выставляется один раз до старта пула потоков: socket.setdefaulttimeout
    # глобален и не потокобезопасен, мутировать его из воркеров нельзя.
    socket.setdefaulttimeout(args.timeout)

    if requests is None and not args.skip_online:
        print(
            "Внимание: модуль 'requests' не установлен — проверки РКН/Роспатента "
            "будут пропущены. Установите зависимости из scripts/requirements-check_brand.txt "
            "или запустите с --skip-online.\n",
            file=sys.stderr,
        )

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        results = list(
            pool.map(
                lambda name: process_name(
                    name, args.zone, args.timeout, args.skip_online, args.rkn_url, args.tm_url
                ),
                args.names,
            )
        )

    print(render_table(results))

    notes = [(r.name, note) for r in results for note in r.notes]
    if notes:
        print("\nПримечания:")
        for name, note in notes:
            print(f"  [{name}] {note}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
