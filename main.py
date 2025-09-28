import logging
import json
import os
import re
import sys
from datetime import datetime, timedelta
import pytz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
from tenacity import retry, stop_after_attempt, wait_exponential

# Загрузка .env
load_dotenv()

# Получаем переменные из .env
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
SHEET_NAME = os.getenv('SHEET_NAME', 'Sheet1')  # По умолчанию
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
ALLOWED_USERS = os.getenv('ALLOWED_USERS', '').split(',')  # Список ID пользователей, разделённых запятыми

# Проверяем импорты и файлы конфигурации
try:
    import gspread
    from google.oauth2.service_account import Credentials
    print("✅ Google Sheets модули загружены")
except ImportError as e:
    print(f"❌ Ошибка импорта gspread: {e}")
    print("Установите: pip install gspread google-auth")
    sys.exit(1)

try:
    from openai import OpenAI
    print("✅ OpenAI модуль загружен")
except ImportError as e:
    print(f"❌ Ошибка импорта OpenAI: {e}")
    print("Установите: pip install openai")
    sys.exit(1)

# Московское время
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

def get_moscow_time():
    """Возвращает текущее московское время"""
    return datetime.now(MOSCOW_TZ)

def format_moscow_date():
    """Возвращает дату в московском времени в формате ДД.ММ.ГГГГ"""
    return get_moscow_time().strftime('%d.%m.%Y')

# Настройка логирования с файлом
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Кэш для данных Sheets
SHEETS_CACHE = None
CACHE_TIMESTAMP = None
CACHE_TIMEOUT = timedelta(minutes=5)  # Кэшируем на 5 минут

# Инициализация с проверкой ошибок
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def initialize_services():
    """Инициализирует все внешние сервисы с проверкой ошибок и retry"""
    services = {}
    
    # Инициализация OpenAI
    try:
        if not OPENAI_API_KEY:
            raise ValueError("OpenAI API ключ не настроен в .env")
        services['openai'] = OpenAI(api_key=OPENAI_API_KEY)
        print("✅ OpenAI клиент инициализирован")
    except Exception as e:
        print(f"❌ Ошибка инициализации OpenAI: {e}")
        return None
    
    # Инициализация Google Sheets
    try:
        if not os.path.exists('credentials.json'):
            raise FileNotFoundError("Файл credentials.json не найден")
        
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
        gc = gspread.authorize(creds)
        
        if not GOOGLE_SHEET_ID:
            raise ValueError("Google Sheet ID не настроен в .env")
            
        services['sheets'] = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)
        print("✅ Google Sheets подключены")
        
        # Проверяем структуру таблицы
        try:
            headers = services['sheets'].row_values(1)
            if not headers:
                services['sheets'].append_row(["Дата", "Тип операции", "Категория", "Описание/Получатель", "Сумма", "Комментарий"])
                print("✅ Заголовки таблицы созданы")
            else:
                print(f"✅ Таблица существует, заголовки: {headers}")
        except Exception as e:
            print(f"⚠️ Предупреждение при проверке структуры таблицы: {e}")
            
    except Exception as e:
        print(f"❌ Ошибка инициализации Google Sheets: {e}")
        return None
    
    # Проверка Telegram токена
    try:
        if not TELEGRAM_TOKEN:
            raise ValueError("Telegram токен не настроен в .env")
        print("✅ Telegram токен найден")
    except Exception as e:
        print(f"❌ Ошибка проверки Telegram токена: {e}")
        return None
    
    return services

# Глобальные переменные для сервисов
SERVICES = initialize_services()
if not SERVICES:
    print("❌ Критическая ошибка: Не удалось инициализировать сервисы")
    sys.exit(1)

client = SERVICES['openai']
finance_sheet = SERVICES['sheets']

# Хранилище последних операций и контекста (с сохранением в файл)
USER_LAST_OPERATIONS = {}
USER_CONTEXT = {}
CONTEXT_FILE = 'user_context.json'

def load_context():
    """Загружает контекст из файла"""
    global USER_CONTEXT
    if os.path.exists(CONTEXT_FILE):
        with open(CONTEXT_FILE, 'r', encoding='utf-8') as f:
            USER_CONTEXT = json.load(f)
        print("✅ Контекст загружен из файла")

def save_context():
    """Сохраняет контекст в файл"""
    with open(CONTEXT_FILE, 'w', encoding='utf-8') as f:
        json.dump(USER_CONTEXT, f, ensure_ascii=False, indent=2)

load_context()  # Загружаем при старте

def normalize_name(name):
    """Нормализует имя, убирая падежные окончания"""
    name_lower = name.lower()
    if name_lower.endswith(('у', 'а', 'е', 'ом', 'ым')):
        base = name[:-1] if not (name_lower.endswith('ом') or name_lower.endswith('ым')) else name[:-2]
        return base.capitalize()
    # Известные имена
    mappings = {
        'интигаму': 'Интигам', 'интигама': 'Интигам',
        'балтики': 'Балтика', 'балтике': 'Балтика', 'балтику': 'Балтика',
        'петрову': 'Петров', 'петрова': 'Петров',
        'рустаму': 'Рустам', 'рустама': 'Рустам',
    }
    return mappings.get(name_lower, name.capitalize())

def analyze_message_with_ai(text, user_context=None):
    """Анализирует сообщение с помощью ИИ с учетом контекста"""
    try:
        # Сначала проверяем, не является ли это командным запросом
        command_result = parse_voice_command(text)
        if command_result:
            return command_result

        context_info = ""
        if user_context:
            recent_operations = user_context.get('recent_operations', [])
            if recent_operations:
                context_info = f"""
КОНТЕКСТ последних операций пользователя:
{chr(10).join(recent_operations[-5:])}

Используй этот контекст для более точного понимания. Например:
- Если говорит "такая же сумма" - ищи в контексте
- Если "тому же человеку" - используй имя из предыдущих операций
- Если просто "зарплата" без имени - предложи уточнить или используй контекст
"""

        prompt = f"""
Проанализируй сообщение пользователя и определи его тип и данные.

{context_info}

Сообщение: "{text}"

Верни JSON в следующем формате:

Для ФИНАНСОВЫХ операций:
{{
    "type": "finance",
    "operation_type": "Пополнение" или "Расход",
    "amount": число (положительное для пополнения, отрицательное для расхода),
    "category": одна из категорий: "Зарплаты сотрудникам", "Выплаты учредителям", "Оплата поставщику", "Процент", "Закупка товара", "Материалы", "Транспорт", "Связь", "Такси", "Общественные расходы", "Благотворительность", "Закупка Тула", "Закупка Москва", "-" (для пополнений),
    "description": "краткое описание с именами людей",
    "comment": "",
    "confidence": число от 0 до 1 (насколько уверен в распознавании)
}}

Если НЕЯСНО или нужно УТОЧНЕНИЕ:
{{
    "type": "clarification",
    "message": "Уточняющий вопрос пользователю",
    "suggestions": ["вариант 1", "вариант 2", "вариант 3"]
}}

ПРАВИЛА РАСПОЗНАВАНИЯ:

1. ФИНАНСЫ - точные индикаторы:
   - Пополнения: "пополнил", "снял", "взял наличку", "получил деньги" = Пополнение
   - Расходы: "заплатил", "потратил", "дал", "купил", "оплатил", "зарплата" = Расход

2. КАТЕГОРИИ - строгие правила с приоритетами:
   - Если "рынок тула" или "тула рынок" - "Закупка Тула" (приоритет над другими)
   - Если "рынок москва" или "москва рынок" - "Закупка Москва" (приоритет над другими)
   - Если "поставщику" или "оплата поставщику" - ВСЕГДА "Оплата поставщику", даже если есть имя (приоритет над зарплатами)
   - "дал/заплатил/зарплата + ИМЯ" = "Зарплаты сотрудникам" (только если нет "поставщику")
   - "Таня лично/Игорь лично/Антон лично" = "Выплаты учредителям"
   - "материалы/закупка/товары" = "Материалы"
   - "такси/убер/яндекс" = "Такси"
   - "транспорт/бензин/авто/Герасимов" = "Транспорт"
   - "связь/интернет/телефон" = "Связь"
   - "благотворительность/донат/помощь/СВО" = "Благотворительность"
   - "хоз расходы/хозяйственные/офис/канцелярия" = "Общественные расходы"

3. ОПИСАНИЕ - только суть, с заглавной буквы:
   - Убирай: "заплатил", "дал", "потратил", "купил", "оплатил", "лично"
   - Оставляй: имена, должности, назначение

4. ВСЕГДА ВЫСОКАЯ УВЕРЕННОСТЬ:
   - Если в сообщении есть ЧИСЛО - confidence = 0.9
   - НЕ задавай уточняющих вопросов если есть сумма
   - Лучше записать что-то чем спрашивать
   - Если confidence < 0.7 - уточни, но если есть сумма, confidence всегда >=0.9

5. ОБРАБОТКА ПАДЕЖНЫХ ОКОНЧАНИЙ:
   - "Балтики" → "Балтика", "Рустаму" → "Рустам", "Петрову" → "Петров"
   - "Интигаму" → "Интигам", "Сидорову" → "Сидоров"

6. КОНТЕКСТНЫЕ ФРАЗЫ:
   - "такая же сумма" = ищи последнюю сумму в контексте
   - "тому же" = используй последнего получателя
   - "как вчера" = анализируй контекст за вчера
   - "обычная зарплата Петрову" = если есть в контексте - используй, иначе уточни

7. ПРИМЕРЫ:
   - "оплата поставщику Шамилю 10000" → {"type": "finance", "operation_type": "Расход", "amount": -10000, "category": "Оплата поставщику", "description": "Шамиль", "confidence": 0.9}
   - "рынок тула 5000 за товары" → {"type": "finance", "operation_type": "Расход", "amount": -5000, "category": "Закупка Тула", "description": "За товары", "confidence": 0.9}
   - "зарплата Петрову 40000" → {"type": "finance", "operation_type": "Расход", "amount": -40000, "category": "Зарплаты сотрудникам", "description": "Петров", "confidence": 0.9}
   - "дал Тане лично 30000" → {"type": "finance", "operation_type": "Расход", "amount": -30000, "category": "Выплаты учредителям", "description": "Таня", "confidence": 0.9}

ВАЖНО: Приоритет ключевым фразам (поставщику > имя). Если ambiguously, используй контекст для best fit. Анализируй весь текст для точного определения!
"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты эксперт по анализу финансовых операций. Точность критически важна. При сомнениях - используй контекст и выбирай best fit."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2  # Лёгкий креатив для лучшего понимания
        )

        result = response.choices[0].message.content.strip()
        # Убираем markdown форматирование если есть
        if result.startswith("```json"):
            result = result[7:-3]
        elif result.startswith("```"):
            result = result[3:-3]

        try:
            return json.loads(result)
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON от ИИ: {e}")
            return {"type": "clarification", "message": "Ошибка анализа. Попробуйте переформулировать.", "suggestions": []}

    except Exception as e:
        logger.error(f"Ошибка ИИ анализа: {e}")
        return {"type": "clarification", "message": "Извините, произошла ошибка. Попробуйте переформулировать.", "suggestions": []}

def update_user_context(user_id, operation_data):
    """Обновляет контекст пользователя"""
    if user_id not in USER_CONTEXT:
        USER_CONTEXT[user_id] = {'recent_operations': []}

    # Формируем строку операции для контекста
    context_line = f"{operation_data['data']['description']}: {operation_data['data']['amount']:,.0f} ₽ ({operation_data['data']['category']})"

    USER_CONTEXT[user_id]['recent_operations'].append(context_line)

    # Храним только последние 10 операций
    if len(USER_CONTEXT[user_id]['recent_operations']) > 10:
        USER_CONTEXT[user_id]['recent_operations'] = USER_CONTEXT[user_id]['recent_operations'][-10:]

    save_context()  # Сохраняем после обновления

def add_finance_record(data, user_id):
    """Добавляет финансовую запись в таблицу"""
    try:
        row = [
            format_moscow_date(),  # Московское время
            data['operation_type'],
            data['category'],
            data['description'],
            data['amount'],
            data.get('comment', '')
        ]
        finance_sheet.append_row(row)
        invalidate_cache()  # Инвалидируем кэш после изменения

        # Сохраняем последнюю операцию
        USER_LAST_OPERATIONS[user_id] = {
            'type': 'finance',
            'data': data,
            'row': len(get_cached_records()),
            'timestamp': get_moscow_time()
        }

        # Обновляем контекст
        update_user_context(user_id, USER_LAST_OPERATIONS[user_id])

        return True
    except Exception as e:
        logger.error(f"Ошибка записи финансов: {e}")
        return False

def edit_finance_record(row_number, data):
    """Редактирует запись в таблице по номеру строки"""
    try:
        cells = finance_sheet.range(f'A{row_number}:F{row_number}')
        cells[0].value = data.get('date', format_moscow_date())
        cells[1].value = data['operation_type']
        cells[2].value = data['category']
        cells[3].value = data['description']
        cells[4].value = data['amount']
        cells[5].value = data.get('comment', '')
        finance_sheet.update_cells(cells)
        invalidate_cache()
        return True
    except Exception as e:
        logger.error(f"Ошибка редактирования записи: {e}")
        return False

def delete_finance_record(row_number):
    """Удаляет запись по номеру строки"""
    try:
        finance_sheet.delete_rows(row_number)
        invalidate_cache()
        return True
    except Exception as e:
        logger.error(f"Ошибка удаления записи: {e}")
        return False

def clear_table():
    """Очищает всю таблицу, кроме заголовков"""
    try:
        # Получаем все строки кроме первой (заголовки)
        records = finance_sheet.get_all_values()
        if len(records) > 1:
            finance_sheet.delete_rows(2, len(records) - 1)  # Удаляем с 2-й строки
        invalidate_cache()
        print("✅ Таблица очищена")
        return True
    except Exception as e:
        logger.error(f"Ошибка очистки таблицы: {e}")
        return False

def get_cached_records():
    """Получает записи из кэша или обновляет"""
    global SHEETS_CACHE, CACHE_TIMESTAMP
    now = datetime.now()
    if SHEETS_CACHE is None or CACHE_TIMESTAMP is None or now - CACHE_TIMESTAMP > CACHE_TIMEOUT:
        SHEETS_CACHE = finance_sheet.get_all_records()
        CACHE_TIMESTAMP = now
        print("✅ Кэш Sheets обновлён")
    return SHEETS_CACHE

def invalidate_cache():
    """Инвалидирует кэш"""
    global SHEETS_CACHE, CACHE_TIMESTAMP
    SHEETS_CACHE = None
    CACHE_TIMESTAMP = None

def parse_voice_command(text):
    """Парсит голосовые команды и возвращает соответствующую команду"""
    text_lower = text.lower()

    # Команды по получателям
    if any(phrase in text_lower for phrase in ['кому платили', 'анализ получателей', 'по получателям', 'кому больше', 'топ получателей']):
        return {"type": "voice_command", "command": "recipients", "params": text}

    # Команды по поставщикам
    if any(phrase in text_lower for phrase in ['анализ поставщика', 'по поставщику', 'история с', 'поставщик']):
        return {"type": "voice_command", "command": "suppliers", "params": text}

    # Команды по категориям
    if any(phrase in text_lower for phrase in ['по категориям', 'категории', 'расходы по']):
        return {"type": "voice_command", "command": "categories", "params": text}

    # Команды аналитики
    if any(phrase in text_lower for phrase in ['анализ', 'аналитика', 'отчет', 'покажи траты', 'сколько потратили']):
        return {"type": "voice_command", "command": "analytics", "params": text}

    # Команды поиска
    if any(phrase in text_lower for phrase in ['найди', 'найти', 'поиск', 'покажи операции', 'когда платили']):
        return {"type": "voice_command", "command": "search", "params": text}

    # Команды истории
    if any(phrase in text_lower for phrase in ['история', 'последние операции', 'что было']):
        return {"type": "voice_command", "command": "history", "params": text}

    # Команды бэкапа
    if any(phrase in text_lower for phrase in ['бэкап', 'резервная копия', 'сохрани', 'backup']):
        return {"type": "voice_command", "command": "backup", "params": text}

    return None

def extract_params_from_voice(text, command_type):
    """Извлекает параметры из голосового запроса"""
    text_lower = text.lower()
    params = {}

    # Извлекаем имена/компании
    names = re.findall(r'\b[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)?\b', text)
    if names:
        params['name'] = normalize_name(names[0])

    # Извлекаем периоды
    if any(word in text_lower for word in ['неделя', 'неделю']):
        params['period'] = 'неделя'
    elif any(word in text_lower for word in ['месяц']):
        params['period'] = 'месяц'
    elif any(word in text_lower for word in ['декабрь', 'январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь']):
        months = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь']
        for month in months:
            if month in text_lower:
                params['period'] = month
                break

    # Извлекаем категории
    if any(word in text_lower for word in ['зарплат', 'зарплаты']):
        params['category'] = 'зарплаты'
    elif any(word in text_lower for word in ['поставщик', 'поставщиков']):
        params['category'] = 'поставщик'
    elif any(word in text_lower for word in ['процент', 'проценты']):
        params['category'] = 'процент'

    return params

def create_quick_buttons():
    """Создает быстрые кнопки для частых действий"""
    keyboard = [
        [
            InlineKeyboardButton("📊 Отчет", callback_data="quick_analytics"),
            InlineKeyboardButton("🔍 Поиск", callback_data="quick_search")
        ],
        [
            InlineKeyboardButton("📋 История", callback_data="quick_history"),
            InlineKeyboardButton("💾 Бэкап", callback_data="quick_backup")
        ],
        [
            InlineKeyboardButton("📂 Категории", callback_data="quick_categories"),
            InlineKeyboardButton("👥 Получатели", callback_data="quick_recipients")
        ],
        [
            InlineKeyboardButton("🏭 Поставщики", callback_data="quick_suppliers"),
            InlineKeyboardButton("✏️ Редактировать", callback_data="quick_edit")
        ],
        [
            InlineKeyboardButton("🗑️ Удалить", callback_data="quick_delete")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_search_buttons():
    """Создает кнопки для популярных поисковых запросов (без Петров/Интигам)"""
    keyboard = [
        [
            InlineKeyboardButton("💰 Зарплаты", callback_data="search_зарплаты"),
            InlineKeyboardButton("📊 Процент", callback_data="search_процент")
        ],
        [
            InlineKeyboardButton("📅 За неделю", callback_data="search_неделя"),
            InlineKeyboardButton("💸 >50000", callback_data="search_>50000")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_confirmation_buttons(action_type):
    """Создаёт кнопки для подтверждения"""
    keyboard = [
        [
            InlineKeyboardButton("✅ Да", callback_data=f"confirm_{action_type}_yes"),
            InlineKeyboardButton("❌ Нет", callback_data=f"confirm_{action_type}_no")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик голосовых сообщений"""
    user_id = str(update.effective_user.id)
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("❌ Доступ запрещён.")
        return

    try:
        await update.message.reply_text("🎤 Распознаю голосовое сообщение...")

        voice_file = await context.bot.get_file(update.message.voice.file_id)
        voice_path = f"voice_{update.message.voice.file_id}.ogg"
        await voice_file.download_to_drive(voice_path)

        with open(voice_path, "rb") as audio_file:
            transcript = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                language="ru"
            )

        os.remove(voice_path)
        recognized_text = transcript.text

        await update.message.reply_text(f"📝 Распознал: \"{recognized_text}\"\nРаспознал правильно? Если нет, перефразируй.")

        user_context = USER_CONTEXT.get(user_id)
        analysis = analyze_message_with_ai(recognized_text, user_context)

        await process_analysis_result(update, analysis, user_id, f"🎤 \"{recognized_text}\"", context)

    except Exception as e:
        logger.error(f"Ошибка обработки голосового: {e}")
        await update.message.reply_text("❌ Ошибка при обработке голосового сообщения.")

async def handle_voice_command(update: Update, context: ContextTypes.DEFAULT_TYPE, analysis):
    """Обрабатывает голосовые команды"""
    command = analysis["command"]
    params_text = analysis["params"]
    params = extract_params_from_voice(params_text, command)
    
    message = update.message if update.message else update.callback_query.message

    if command == "analytics":
        await show_analytics(update, context, params.get('period'))
    elif command == "search":
        search_terms = []
        if 'name' in params:
            search_terms.append(params['name'])
        if 'period' in params:
            search_terms.append(params['period'])
        if 'category' in params:
            search_terms.append(params['category'])

        if search_terms:
            context.args = search_terms
            await advanced_search(update, context)
        else:
            await message.reply_text(
                "🔍 **Голосовой поиск**\n\nПопробуйте сказать:\n• 'Найди Петрова'\n• 'Покажи операции за неделю'\n• 'Когда платили Интигаму'",
                reply_markup=create_search_buttons()
            )
    elif command == "history":
        await show_context_history(update, context)
    elif command == "backup":
        await create_backup(update, context)
    elif command == "recipients":
        await show_recipients(update, context, params)
    elif command == "suppliers":
        await show_suppliers(update, context, params)
    elif command == "categories":
        await show_categories(update, context, params)

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатия на кнопки"""
    query = update.callback_query
    await query.answer()

    data = query.data
    message = query.message

    try:
        if data == "quick_analytics":
            await show_analytics(update, context)
        elif data == "quick_search":
            await message.edit_text(
                "🔍 **Быстрый поиск**\n\nВыберите категорию или скажите что ищете:",
                reply_markup=create_search_buttons()
            )
        elif data == "quick_history":
            await show_context_history(update, context)
        elif data == "quick_backup":
            await create_backup(update, context)
        elif data == "quick_categories":
            await show_categories(update, context)
        elif data == "quick_recipients":
            await show_recipients(update, context)
        elif data == "quick_suppliers":
            await show_suppliers(update, context)
        elif data == "quick_edit":
            await edit_last(update, context)
        elif data == "quick_delete":
            await delete_last(update, context)
        elif data.startswith("search_"):
            search_term = data.replace("search_", "")
            context.args = [search_term]
            await advanced_search(update, context)
        elif data.startswith("confirm_"):
            # Логика подтверждения (для finance, clear_table)
            parts = data.split("_")
            action = parts[1]
            choice = parts[2]
            if choice == "yes" and action == "clear":
                if clear_table():
                    await message.edit_text("🗑️ Таблица полностью очищена!")
                else:
                    await message.edit_text("❌ Ошибка очистки.")
            elif choice == "no":
                await message.edit_text("❌ Операция отменена.")
            else:
                await message.edit_text("❓ Неизвестное подтверждение.")
        else:
            await message.edit_text("⚠️ Команда не реализована.")
    except Exception as e:
        logger.error(f"Ошибка в callback: {e}")
        await message.edit_text("❌ Ошибка при обработке кнопки.")

async def process_analysis_result(update, analysis, user_id, source_info="", context=None):
    """Обрабатывает результат анализа ИИ"""
    if analysis["type"] == "voice_command":
        await handle_voice_command(update, context, analysis)
        return

    if analysis["type"] == "finance":
        confidence = analysis.get('confidence', 1.0)

        if confidence < 0.7:
            confirm_text = f"""
❓ **Проверьте правильность:**

{source_info}
🔄 Тип: {analysis['operation_type']}
📂 Категория: {analysis['category']}
📝 Описание: {analysis['description']}
💰 Сумма: {analysis['amount']:,.0f} ₽

✅ Записать? Или уточните что не так.
            """
            await update.message.reply_text(confirm_text, parse_mode='Markdown', reply_markup=create_confirmation_buttons("finance"))
            return

        if add_finance_record(analysis, user_id):
            emoji = "📈" if analysis["operation_type"] == "Пополнение" else "📉"
            response = f"""
{emoji} **Финансовая операция записана:**

{source_info}
📅 Дата: {format_moscow_date()}
🔄 Тип: {analysis['operation_type']}
📂 Категория: {analysis['category']}
📝 Описание: {analysis['description']}
💰 Сумма: {analysis['amount']:,.0f} ₽

✅ **Записано в Google Таблицу!**
            """

            await update.message.reply_text(
                response,
                parse_mode='Markdown',
                reply_markup=create_quick_buttons()
            )
        else:
            await update.message.reply_text("❌ Ошибка при записи в таблицу финансов.")
    else:
        suggestions = analysis.get('suggestions', [])
        response = f"❓ {analysis.get('message', 'Не понял ваше сообщение.')}"

        if suggestions:
            response += "\n\n💡 **Возможно, вы имели в виду:**\n"
            for i, suggestion in enumerate(suggestions[:3], 1):
                response += f"{i}. {suggestion}\n"

        await update.message.reply_text(response, parse_mode='Markdown')

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_id = str(update.effective_user.id)
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("❌ Доступ запрещён.")
        return

    welcome_text = """
💰 **Умный финансовый помощник с ИИ!**

🎤 **Новинка: Голосовое управление!**

💸 **Записывайте операции:**
• "Дал Петрову 40000 за работу"
• "Таня лично 30000"
• "Оплатил поставщику Интигаму 300000"
• "Рынок Тула 5000 за товары"
• "Рынок Москва 10000"

🗣️ **Управляйте голосом:**
• 🎤 "Покажи траты за неделю"
• 🎤 "Найди все операции с Петровым"
• 🎤 "Анализ по категориям за месяц"
• 🎤 "Когда платили Интигаму"

🏭 **13 категорий:**
• Зарплаты, Учредители, Поставщики
• Процент, Закупка товара, Материалы
• Транспорт, Связь, Такси, Общественные, СВО
• Закупка Тула, Закупка Москва

**Говорите естественно - бот всё поймет!**
    """

    await update.message.reply_text(
        welcome_text,
        parse_mode='Markdown',
        reply_markup=create_quick_buttons()
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений с контекстом"""
    user_id = str(update.effective_user.id)
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("❌ Доступ запрещён.")
        return

    user_message = update.message.text

    await update.message.reply_text("🤔 Анализирую с учетом контекста...")

    user_context = USER_CONTEXT.get(user_id)
    analysis = analyze_message_with_ai(user_message, user_context)

    await process_analysis_result(update, analysis, user_id, context=context)

async def show_context_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает историю с контекстом"""
    user_id = str(update.effective_user.id)
    message = update.message if update.message else update.callback_query.message

    try:
        await message.reply_text("📊 Получаю историю с контекстом...")

        user_context = USER_CONTEXT.get(user_id, {})
        recent_ops = user_context.get('recent_operations', [])

        if recent_ops:
            history = "🧠 **Контекст последних операций:**\n\n"
            for i, op in enumerate(reversed(recent_ops[-5:]), 1):
                history += f"{i}. {op}\n"
        else:
            history = "📊 **Контекст пуст** - начните добавлять операции!\n\n"

        finance_records = get_cached_records()
        recent_finance = finance_records[-3:] if len(finance_records) > 3 else finance_records

        if recent_finance:
            history += "\n💰 **Последние финансовые операции:**\n"
            for record in reversed(recent_finance):
                emoji = "📈" if record.get('Сумма', 0) > 0 else "📉"
                history += f"{emoji} {record.get('Описание/Получатель', '')}: {record.get('Сумма', 0):,.0f} ₽\n"

        await message.reply_text(history, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка истории: {e}")
        await message.reply_text("❌ Ошибка при получении истории.")

async def show_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE, period=None):
    """Умная аналитика трат с параметрами периода и графиками"""
    try:
        message = update.message if update.message else update.callback_query.message
        
        await message.reply_text("📊 Анализирую ваши финансы...")

        finance_records = pd.DataFrame(get_cached_records())

        if finance_records.empty:
            await message.reply_text("📊 Нет данных для аналитики. Добавьте операции!")
            return

        # Конвертируем даты в datetime (МСК), с обработкой ошибок
        try:
            finance_records['Дата'] = pd.to_datetime(finance_records['Дата'], format='%d.%m.%Y', errors='coerce').dt.tz_localize(MOSCOW_TZ)
            finance_records = finance_records.dropna(subset=['Дата'])  # Удаляем некорректные даты
        except Exception as date_e:
            logger.error(f"Ошибка парсинга дат: {date_e}")
            await message.reply_text("❌ Ошибка с датами в таблице. Проверьте формат ДД.ММ.ГГГГ.")
            return

        # Фильтр по периоду
        now = get_moscow_time()
        if period == 'неделя':
            start_date = now - timedelta(days=7)
        elif period == 'месяц':
            start_date = now - timedelta(days=30)
        elif period in ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь']:
            month_num = {'январь':1, 'февраль':2, 'март':3, 'апрель':4, 'май':5, 'июнь':6, 'июль':7, 'август':8, 'сентябрь':9, 'октябрь':10, 'ноябрь':11, 'декабрь':12}[period]
            start_date = datetime(now.year, month_num, 1, tzinfo=MOSCOW_TZ)
        else:
            start_date = now - timedelta(days=30)  # Default

        recent_records = finance_records[finance_records['Дата'] >= start_date]

        if recent_records.empty:
            await message.reply_text(f"📊 Нет данных за указанный период ({period or '30 дней'}).")
            return

        total_income = recent_records[recent_records['Сумма'] > 0]['Сумма'].sum()
        total_expense = recent_records[recent_records['Сумма'] < 0]['Сумма'].sum()

        categories = recent_records[recent_records['Сумма'] < 0].groupby('Категория')['Сумма'].sum()

        salaries = recent_records[recent_records['Категория'] == 'Зарплаты сотрудникам'].groupby('Описание/Получатель')['Сумма'].sum().abs()

        report = f"""
📊 **Умная аналитика за период {period or '30 дней'}**

💰 **Общие итоги:**
📈 Доходы: +{total_income:,.0f} ₽
📉 Расходы: {total_expense:,.0f} ₽
💼 Чистый результат: {total_income + total_expense:,.0f} ₽
📊 Операций: {len(recent_records)}

💸 **Расходы по категориям:**
"""

        for cat, amount in categories.sort_values().items():
            percent = abs(amount) / abs(total_expense) * 100 if total_expense != 0 else 0
            report += f"• {cat}: {amount:,.0f} ₽ ({percent:.1f}%)\n"

        if not salaries.empty:
            report += f"\n👥 **Зарплаты сотрудникам:**\n"
            for person, amount in salaries.sort_values(ascending=False).items():
                report += f"• {person}: {amount:,.0f} ₽\n"

        avg_daily = abs(total_expense) / ((now - start_date).days or 1)
        report += f"\n📈 **Средние траты в день:** {avg_daily:,.0f} ₽"

        if not categories.empty:
            top_category = categories.idxmin()
            report += f"\n🔝 **Больше всего тратите на:** {top_category}"

        await message.reply_text(report, parse_mode='Markdown')

        # Генерация графика
        if not categories.empty:
            fig, ax = plt.subplots()
            categories.abs().plot(kind='pie', ax=ax, autopct='%1.1f%%', title='Расходы по категориям')
            buf = BytesIO()
            fig.savefig(buf, format='png')
            buf.seek(0)
            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=buf)
            plt.close(fig)

    except Exception as e:
        logger.error(f"Ошибка аналитики: {e}")
        await message.reply_text("❌ Ошибка при создании аналитики. Проверьте логи.")

async def advanced_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Продвинутый поиск операций с использованием pandas"""
    args = context.args
    message = update.message if update.message else update.callback_query.message
    
    if not args:
        help_text = """
🔍 **Супер-поиск операций:**

**По имени/компании:**
• `/search Петров` - все операции с Петровым
• `/search Интигам` - все операции с Интигамом

**По категории:**
• `/search зарплаты` - все зарплаты
• `/search поставщик` - все оплаты поставщикам

**По периоду:**
• `/search декабрь` - операции за декабрь
• `/search неделя` - операции за неделю

**По сумме:**
• `/search >50000` - операции больше 50к
• `/search <10000` - операции меньше 10к
        """
        await message.reply_text(help_text, parse_mode='Markdown')
        return

    search_query = " ".join(args).lower()

    try:
        await message.reply_text(f"🔍 Ищу операции по запросу: '{search_query}'...")

        finance_records = pd.DataFrame(get_cached_records())
        if finance_records.empty:
            await message.reply_text("📊 Нет данных.")
            return

        # Конвертируем для фильтров
        finance_records['Сумма'] = pd.to_numeric(finance_records['Сумма'], errors='coerce')
        finance_records['Дата'] = pd.to_datetime(finance_records['Дата'], format='%d.%m.%Y', errors='coerce')

        # Фильтры
        mask = finance_records.apply(lambda row: search_query in str(row).lower(), axis=1)
        if '>' in search_query:
            thresh = int(search_query.split('>')[1])
            mask = finance_records['Сумма'] > thresh
        elif '<' in search_query:
            thresh = int(search_query.split('<')[1])
            mask = finance_records['Сумма'] < thresh

        found_records = finance_records[mask]

        if found_records.empty:
            await message.reply_text(f"❌ По запросу '{search_query}' ничего не найдено.")
            return

        found_records = found_records.sort_values('Дата', ascending=False)

        result = f"🔍 **Найдено: {len(found_records)} операций**\n\n"
        
        display_records = found_records.head(15)

        for _, record in display_records.iterrows():
            emoji = "📈" if record['Сумма'] > 0 else "📉"
            result += f"{emoji} {record['Дата'].strftime('%d.%m.%Y')}: {record['Описание/Получатель']} - {record['Сумма']:,.0f} ₽\n"

        if len(found_records) > 15:
            result += f"\n... и ещё {len(found_records) - 15} операций"

        total_amount = found_records['Сумма'].sum()
        result += f"\n\n📊 **Общая сумма:** {total_amount:,.0f} ₽"

        await message.reply_text(result, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Ошибка поиска: {e}")
        await message.reply_text("❌ Ошибка при поиске операций.")

async def create_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создает резервную копию данных"""
    message = update.message if update.message else update.callback_query.message
    
    try:
        await message.reply_text("💾 Создаю резервную копию...")

        finance_records = get_cached_records()

        backup_data = {
            'created': get_moscow_time().strftime('%d.%m.%Y %H:%M'),
            'finance_records': len(finance_records),
            'finance': finance_records
        }

        backup_filename = f"backup_{get_moscow_time().strftime('%Y%m%d_%H%M')}.json"
        with open(backup_filename, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2)

        with open(backup_filename, 'rb') as f:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=f,
                filename=backup_filename,
                caption=f"💾 **Резервная копия создана!**\n\n📊 Записей: {len(finance_records)}\n📅 Дата: {backup_data['created']}"
            )

        os.remove(backup_filename)

    except Exception as e:
        logger.error(f"Ошибка создания backup: {e}")
        await message.reply_text("❌ Ошибка при создании резервной копии.")

async def show_recipients(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Показывает анализ по получателям"""
    message = update.message if update.message else update.callback_query.message
    try:
        await message.reply_text("👥 Анализирую получателей...")
        finance_records = pd.DataFrame(get_cached_records())
        if finance_records.empty:
            await message.reply_text("👥 Нет данных для анализа получателей.")
            return
        recipients = finance_records[finance_records['Сумма'] < 0].groupby('Описание/Получатель')['Сумма'].sum().abs().sort_values(ascending=False)
        if recipients.empty:
            await message.reply_text("👥 Нет данных о получателях.")
            return
        report = "👥 **Топ получателей:**\n"
        for person, amount in recipients.head(10).items():
            report += f"• {person}: {amount:,.0f} ₽\n"
        await message.reply_text(report, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Ошибка анализа получателей: {e}")
        await message.reply_text("❌ Ошибка анализа.")

async def show_suppliers(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Показывает анализ по поставщикам"""
    message = update.message if update.message else update.callback_query.message
    try:
        await message.reply_text("🏭 Анализирую поставщиков...")
        finance_records = pd.DataFrame(get_cached_records())
        if finance_records.empty:
            await message.reply_text("🏭 Нет данных для анализа поставщиков.")
            return
        suppliers = finance_records[(finance_records['Категория'] == 'Оплата поставщику') & (finance_records['Сумма'] < 0)].groupby('Описание/Получатель')['Сумма'].sum().abs().sort_values(ascending=False)
        if suppliers.empty:
            await message.reply_text("🏭 Нет данных о поставщиках.")
            return
        report = "🏭 **Топ поставщиков:**\n"
        for supplier, amount in suppliers.head(10).items():
            report += f"• {supplier}: {amount:,.0f} ₽\n"
        await message.reply_text(report, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Ошибка анализа поставщиков: {e}")
        await message.reply_text("❌ Ошибка анализа.")

async def show_categories(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Показывает анализ по категориям"""
    message = update.message if update.message else update.callback_query.message
    try:
        await message.reply_text("📂 Анализирую категории...")
        finance_records = pd.DataFrame(get_cached_records())
        if finance_records.empty:
            await message.reply_text("📂 Нет данных для анализа категорий.")
            return
        categories = finance_records[finance_records['Сумма'] < 0].groupby('Категория')['Сумма'].sum().abs().sort_values(ascending=False)
        if categories.empty:
            await message.reply_text("📂 Нет данных о категориях.")
            return
        report = "📂 **Расходы по категориям:**\n"
        for cat, amount in categories.items():
            report += f"• {cat}: {amount:,.0f} ₽\n"
        await message.reply_text(report, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Ошибка анализа категорий: {e}")
        await message.reply_text("❌ Ошибка анализа.")

async def edit_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редактирует последнюю операцию"""
    message = update.message if update.message else update.callback_query.message
    await message.reply_text("✏️ Редактирование последней операции: Укажите изменения в сообщении (например, 'сумма 50000').")

async def delete_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет последнюю операцию"""
    user_id = str(update.effective_user.id)
    message = update.message if update.message else update.callback_query.message
    try:
        last_row = len(get_cached_records()) + 1  # 1-based, headers
        if delete_finance_record(last_row):
            await message.reply_text("🗑️ Последняя операция удалена.")
        else:
            await message.reply_text("❌ Ошибка удаления.")
    except Exception as e:
        logger.error(f"Ошибка удаления: {e}")
        await message.reply_text("❌ Ошибка.")

async def clear_table_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для полной очистки таблицы с подтверждением"""
    user_id = str(update.effective_user.id)
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("❌ Доступ запрещён.")
        return
    await update.message.reply_text(
        "⚠️ **Внимание!** Это удалит ВСЕ записи из таблицы (кроме заголовков).\n\nПодтвердите?",
        reply_markup=create_confirmation_buttons("clear")
    )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Ошибка: {context.error}")
    try:
        if update.message:
            await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")
    except:
        pass

def main():
    """Запуск продвинутого ИИ-бота"""
    print("🚀 Запускаю продвинутый ИИ финансовый бот...")

    # Проверяем токен
    try:
        if not TELEGRAM_TOKEN:
            print("❌ Telegram токен не настроен в .env")
            return
        
        application = Application.builder().token(TELEGRAM_TOKEN).build()
        print("✅ Telegram приложение создано")
    except Exception as e:
        print(f"❌ Ошибка создания Telegram приложения: {e}")
        return

    # Добавляем обработчики
    try:
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("search", advanced_search))
        application.add_handler(CommandHandler("history", show_context_history))
        application.add_handler(CommandHandler("analytics", show_analytics))
        application.add_handler(CommandHandler("backup", create_backup))
        application.add_handler(CommandHandler("clear_table", clear_table_command))  # Новая команда
        application.add_handler(CallbackQueryHandler(handle_callback_query))
        application.add_handler(MessageHandler(filters.VOICE, handle_voice))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        application.add_error_handler(error_handler)
        print("✅ Обработчики добавлены")
    except Exception as e:
        print(f"❌ Ошибка добавления обработчиков: {e}")
        return

    print("🧠 Продвинутый ИИ-бот готов к работе!")
    print("🎤 Поддержка голосовых сообщений включена!")
    print("🧠 Контекстное понимание активировано!")
    print("📊 Умная аналитика доступна!")
    print("")
    print("📊 Основные команды:")
    print("   /start - приветствие и обзор возможностей")
    print("   /history - история операций с контекстом")
    print("   /analytics - умная аналитика трат")
    print("   /search [текст] - поиск операций")
    print("   /backup - создать резервную копию")
    print("   /clear_table - полная очистка таблицы (с подтверждением)")
    print("")
    print("💡 Говорите естественно - бот понимает контекст!")

    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"❌ Критическая ошибка при запуске: {e}")
        print("Проверьте:")
        print("1. Корректность Telegram токена")
        print("2. Подключение к интернету")
        print("3. Права доступа к файлам")

if __name__ == '__main__':
    main()