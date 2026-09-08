#!/usr/bin/env python3
"""
Проверочный скрипт: делает по одному запросу к YandexGPT и к Unsplash
и печатает статус-код и краткий ответ каждого API.

Запуск:
    python test_apis.py
"""

from __future__ import annotations

import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

YANDEX_API_KEY = os.getenv("YANDEX_API_KEY")
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID")
UNSPLASH_ACCESS_KEY = os.getenv("UNSPLASH_ACCESS_KEY")

YANDEXGPT_COMPLETION_URL = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
UNSPLASH_RANDOM_PHOTO_URL = "https://api.unsplash.com/photos/random"

REQUEST_TIMEOUT = 30


def test_yandex_gpt() -> None:
    print("--- YandexGPT ---")
    if not YANDEX_API_KEY or not YANDEX_FOLDER_ID:
        print("Пропущено: YANDEX_API_KEY / YANDEX_FOLDER_ID не заданы в .env")
        return

    payload = {
        "modelUri": f"gpt://{YANDEX_FOLDER_ID}/yandexgpt/latest",
        "completionOptions": {"stream": False, "temperature": 0.3, "maxTokens": 50},
        "messages": [{"role": "user", "text": "Привет, это тестовый запрос."}],
    }
    headers = {
        "Authorization": f"Api-Key {YANDEX_API_KEY}",
        "x-folder-id": YANDEX_FOLDER_ID,
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            YANDEXGPT_COMPLETION_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as exc:
        print(f"Ошибка запроса: {exc}")
        return

    print(f"HTTP статус: {response.status_code}")
    try:
        data = response.json()
    except json.JSONDecodeError:
        print(f"Тело ответа (не JSON): {response.text[:300]}")
        return

    if response.ok:
        text = data["result"]["alternatives"][0]["message"]["text"]
        print(f"Ответ модели: {text!r}")
    else:
        print(f"Тело ответа: {json.dumps(data, ensure_ascii=False)}")


def test_unsplash() -> None:
    print("\n--- Unsplash ---")
    if not UNSPLASH_ACCESS_KEY:
        print("Пропущено: UNSPLASH_ACCESS_KEY не задан в .env")
        return

    headers = {"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"}
    params = {"query": "business", "orientation": "landscape"}

    try:
        response = requests.get(
            UNSPLASH_RANDOM_PHOTO_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as exc:
        print(f"Ошибка запроса: {exc}")
        return

    print(f"HTTP статус: {response.status_code}")
    try:
        data = response.json()
    except json.JSONDecodeError:
        print(f"Тело ответа (не JSON): {response.text[:300]}")
        return

    if response.ok:
        photo_id = data.get("id")
        image_url = data.get("urls", {}).get("regular")
        print(f"Фото найдено: id={photo_id}, url={image_url}")
    else:
        print(f"Тело ответа: {json.dumps(data, ensure_ascii=False)}")


if __name__ == "__main__":
    test_yandex_gpt()
    test_unsplash()
