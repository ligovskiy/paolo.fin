# Telegram-бот учёта финансов (MVP)

Основано на ваших правилах. Без фантазии.

## Что делает
- Принимает текст и голосовые.
- Голос → транскрипция через OpenAI (whisper-1).
- Текст → структурирование операций через OpenAI (gpt-4o-mini), поддерживает несколько операций в одном сообщении.
- Жёсткие правила категорий/знаков.
- Пишет в Google Sheets: `Дата | Тип операции | Категория | Описание/Получатель | Сумма | Комментарий(-)`.
- Квиток + кнопка «Отменить» (удаляет последнюю вставленную строку).
- Приватный доступ: по `username`.

## Переменные окружения
Главное:
- `ALLOWED_USERNAME=antigorevich`
- `TELEGRAM_TOKEN` — токен бота
- `OPENAI_API_KEY` — ключ OpenAI
- `GOOGLE_SHEET_ID` — ID таблицы
- `SHEET_NAME` — имя листа (по умолчанию `Лист1`)
- `GOOGLE_CREDENTIALS_JSON` — содержимое service account JSON (весь файл как строка) или положите файл рядом как `credentials.json`

## Права Google Sheets
1. Создайте сервисный аккаунт в Google Cloud Console.
2. Скачайте JSON ключ.
3. Откройте ваш Google Sheet и **расшарьте** доступ по email сервисного аккаунта с правом **Редактор**.

## Локальный запуск
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export TELEGRAM_TOKEN="..."
export OPENAI_API_KEY="..."
export GOOGLE_SHEET_ID="..."
export SHEET_NAME="Лист1"
export ALLOWED_USERNAME="antigorevich"
# Один из способов авторизации Google:
# 1) Через переменную (предпочтительно на хостинге)
# export GOOGLE_CREDENTIALS_JSON='{"type":"service_account", ...}'
# 2) Через локальный файл: положите `credentials.json` рядом с `main.py`

python main.py
```

## Render (или любой хостинг)
- Add Environment Variables из `.env.example`.
- Смонтируйте `service_account.json` или положите JSON в переменную `GOOGLE_SERVICE_ACCOUNT_JSON`.
- Команда запуска: `python main.py`.

## Примечания
- Дубль-защита по `message.id`.
- Если OpenAI-парсинг упадёт, есть упрощённый фоллбек по триггерам.
- Комментарии в таблицу всегда `-`.
- Дата — МСК (Europe/Moscow).
- Если укажете другие модели — замените `ASR_MODEL`/`NLP_MODEL`.
