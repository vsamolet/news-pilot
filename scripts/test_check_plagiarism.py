#!/usr/bin/env python3
"""Тесты для check_plagiarism.py: пара "похоже — не похоже".

Запуск:
    python scripts/test_check_plagiarism.py
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from check_plagiarism import check_plagiarism

ORIGINAL = (
    "Центральный банк повысил ключевую ставку до 21 процента годовых, "
    "сославшись на устойчиво высокую инфляцию и перегрев потребительского "
    "кредитования. Решение совета директоров вступает в силу немедленно."
)


class CheckPlagiarismTests(unittest.TestCase):
    def test_near_copy_fails(self) -> None:
        """Рерайт с косметическими правками (пара вставных слов) — почти
        дословное совпадение, риск претензии по авторскому праву реальный."""
        near_copy = (
            "Центральный банк повысил ключевую ставку до 21 процента годовых, "
            "сославшись на устойчиво высокую инфляцию и перегрев потребительского "
            "кредитования, при этом решение совета директоров вступает в силу немедленно."
        )
        result = check_plagiarism(ORIGINAL, near_copy)
        self.assertEqual(result.status, "FAIL")
        self.assertGreater(result.similarity, 0.2)

    def test_genuine_rewrite_passes(self) -> None:
        """Полноценный рерайт своими словами: те же факты, другие
        формулировки и порядок изложения — приемлемо для публикации."""
        genuine_rewrite = (
            "Регулятор поднял базовую процентную ставку до 21% годовых. По словам "
            "представителей совета директоров, причиной стали разогнавшиеся темпы "
            "роста цен и слишком быстрый рост займов населению. Новая ставка "
            "действует уже сегодня."
        )
        result = check_plagiarism(ORIGINAL, genuine_rewrite)
        self.assertEqual(result.status, "PASS")
        self.assertLessEqual(result.similarity, 0.2)


if __name__ == "__main__":
    unittest.main()
