# Система генерации коммерческих предложений для ООО "АРМАСЕТИ ИМПОРТ"

Система для автоматизации формирования коммерческих предложений на основе запросов клиентов с использованием ИИ.

## Описание проекта

Проект представляет собой ИИ-бота, который обрабатывает запросы клиентов на естественном языке, находит подходящие товары в базе данных и формирует коммерческие предложения в формате Excel.

### Основные функции:

- Загрузка и обработка прайс-листов (CSV/Excel)
- Извлечение характеристик из наименований товаров
- Обработка запросов на естественном языке
- Поиск товаров по семантическим признакам
- Формирование коммерческих предложений в Excel
- Отправка КП по email и через Telegram
- Кэширование типовых запросов

## Технический стек

- Python 3.10+
- FastAPI для API
- SQLite для хранения данных
- pandas для обработки данных
- openpyxl для работы с Excel
- LangChain и OpenAI API (GPT-4o) для обработки запросов
- python-telegram-bot для интеграции с Telegram
- smtplib для отправки email

## Установка и запуск

### Требования

- Python 3.10+
- Доступ к API OpenAI

### Установка

1. Клонировать репозиторий:

```bash
git clone <repository-url>
cd commercial-proposal-generator
```

2. Создать виртуальное окружение и активировать его:

```bash
python -m venv venv
source venv/bin/activate   # для Linux/Mac
venv\Scripts\activate      # для Windows
```

3. Установить зависимости:

```bash
pip install -r requirements.txt
```

4. Настроить переменные окружения (создать файл .env):

```
OPENAI_API_KEY=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
TELEGRAM_BOT_TOKEN=7610704072:AAHcbh_qvZ__8kYiWLI0XCOZ_eN1Z_WFnPw
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-email-password
```

### Запуск

```bash
uvicorn main:app --reload
```

Сервер будет запущен на http://localhost:8000

## API Endpoints

- `POST /generate-proposal` - Генерация коммерческого предложения
  ```json
  {
    "query": "фланец 25 мм сталь 20",
    "email": "client@example.com",  // опционально
    "telegram_id": "12345678"       // опционально
  }
  ```

- `POST /upload-price-list` - Загрузка прайс-листа
  ```
  file_path: путь к файлу прайс-листа (CSV или Excel)
  ```

## Структура проекта

- `main.py` - Точка входа FastAPI
- `data_loader.py` - Загрузка и обработка прайс-листов
- `query_processor.py` - Обработка запросов и поиск товаров
- `proposal_generator.py` - Генерация коммерческих предложений
- `sender.py` - Отправка КП через email и Telegram
- `cache.py` - Кэширование запросов
- `logger.py` - Настройка логирования

## Использование

1. Загрузите прайс-лист через API endpoint `/upload-price-list`
2. Отправьте запрос на `/generate-proposal` с текстом запроса
3. Получите коммерческое предложение в формате Excel
4. При необходимости, укажите email или Telegram ID для отправки КП

## Примеры запросов

- "фланец 25 мм сталь 20"
- "отводы стальные 90 градусов"
- "Фланцы плоские ст.20 исп.В Ду 25" 