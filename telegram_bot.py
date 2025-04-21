import os
import telebot
import json
import requests
from dotenv import load_dotenv
import pandas as pd
import sqlite3
import re
import uuid
from datetime import datetime
import traceback

# Загрузка переменных окружения
load_dotenv()

# Токен Telegram бота
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7610704072:AAHcbh_qvZ__8kYiWLI0XCOZ_eN1Z_WFnPw")

# Адрес API (будет использоваться как резервный вариант)
API_URL = "http://127.0.0.1:8000"

# Пути к файлам
DB_PATH = "products.db"
UPLOADS_DIR = os.path.abspath("uploads")
PROPOSALS_DIR = os.path.abspath("proposals")

# Создаем директории, если они не существуют
os.makedirs(UPLOADS_DIR, exist_ok=True)
os.makedirs(PROPOSALS_DIR, exist_ok=True)

# Создаем экземпляр бота
bot = telebot.TeleBot(TOKEN)

# Инициализация базы данных
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier TEXT,
        name TEXT,
        price REAL,
        stock INTEGER,
        category TEXT,
        diameter TEXT,
        material TEXT,
        pressure TEXT,
        execution TEXT,
        standard TEXT,
        additional_params TEXT
    )
    ''')
    conn.commit()
    conn.close()
    print("База данных инициализирована")

# Извлечение характеристик из наименования товара
def extract_characteristics(product_name: str):
    characteristics = {
        'category': None,
        'diameter': None,
        'material': None,
        'pressure': None,
        'execution': None,
        'standard': None,
        'additional_params': None
    }
    
    try:
        # Категория (например, "Фланцы", "Отводы")
        category_match = re.search(r'^(Фланцы|Отводы|Переходы|Заглушки|Тройники)(?:\s+|$)', product_name)
        if category_match:
            characteristics['category'] = category_match.group(1)
        
        # Диаметр (например, "Ду 25")
        diameter_match = re.search(r'Ду\s*(\d+)', product_name)
        if diameter_match:
            characteristics['diameter'] = diameter_match.group(1)
        
        # Материал (например, "ст.20")
        material_match = re.search(r'(?:ст\.|сталь)\s*(\d+|\w+)', product_name, re.IGNORECASE)
        if material_match:
            characteristics['material'] = material_match.group(0)
        
        # Давление
        pressure_match = re.search(r'-(\d+)-', product_name)
        if pressure_match:
            characteristics['pressure'] = pressure_match.group(1)
        
        # Исполнение (например, "исп.В")
        execution_match = re.search(r'исп\.(\w+)', product_name)
        if execution_match:
            characteristics['execution'] = f"исп.{execution_match.group(1)}"
        
        # Стандарт (например, "ГОСТ 33259-2015")
        standard_match = re.search(r'(ГОСТ\s+[\d\-]+)', product_name)
        if standard_match:
            characteristics['standard'] = standard_match.group(1)
        
        # Дополнительные параметры (например, "01-1-В")
        additional_match = re.search(r'\d+\-\d+\-\w+', product_name)
        if additional_match:
            characteristics['additional_params'] = additional_match.group(0)
    
    except Exception as e:
        print(f"Ошибка при извлечении характеристик из '{product_name}': {str(e)}")
    
    return characteristics

# Загрузка прайс-листа в базу данных
def load_price_list(file_path):
    try:
        # Определение типа файла и загрузка с помощью pandas
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Неподдерживаемый формат файла. Используйте CSV или Excel.")
        
        print(f"Заголовки файла: {list(df.columns)}")
        
        # Интеллектуальное определение столбцов
        column_mapping = {}
        
        # Поиск столбца с названием товара
        name_candidates = ['наименование товара', 'наименование', 'товар', 'продукт', 'артикул', 'описание', 'название']
        for column in df.columns:
            col_lower = str(column).lower()
            for candidate in name_candidates:
                if candidate in col_lower:
                    column_mapping['name'] = column
                    break
            if 'name' in column_mapping:
                break
                
        # Если не нашли столбец с названием, берем первый текстовый столбец
        if 'name' not in column_mapping:
            for column in df.columns:
                if df[column].dtype == 'object':
                    column_mapping['name'] = column
                    break
                    
        # Поиск столбца с ценой
        price_candidates = ['цена', 'руб', 'стоимость', 'price']
        for column in df.columns:
            col_lower = str(column).lower()
            for candidate in price_candidates:
                if candidate in col_lower:
                    column_mapping['price'] = column
                    break
            if 'price' in column_mapping:
                break
                
        # Если не нашли столбец с ценой, берем первый числовой столбец
        if 'price' not in column_mapping:
            for column in df.columns:
                if pd.api.types.is_numeric_dtype(df[column]):
                    column_mapping['price'] = column
                    break
        
        # Поиск столбца с остатком
        stock_candidates = ['остаток', 'кол-во', 'количество', 'запас', 'наличие', 'шт', 'stock']
        for column in df.columns:
            col_lower = str(column).lower()
            for candidate in stock_candidates:
                if candidate in col_lower:
                    column_mapping['stock'] = column
                    break
            if 'stock' in column_mapping:
                break
        
        # Если не нашли столбец с остатком, берем второй числовой столбец
        if 'stock' not in column_mapping:
            num_columns = []
            for column in df.columns:
                if pd.api.types.is_numeric_dtype(df[column]):
                    num_columns.append(column)
            if len(num_columns) > 1 and 'price' in column_mapping:
                num_columns.remove(column_mapping['price'])
                column_mapping['stock'] = num_columns[0]
            elif len(num_columns) > 0 and 'price' not in column_mapping:
                column_mapping['stock'] = num_columns[0]
        
        # Поиск столбца с поставщиком
        supplier_candidates = ['поставщик', 'производитель', 'контрагент', 'vendor', 'supplier']
        for column in df.columns:
            col_lower = str(column).lower()
            for candidate in supplier_candidates:
                if candidate in col_lower:
                    column_mapping['supplier'] = column
                    break
            if 'supplier' in column_mapping:
                break
        
        # Если не нашли поставщика, используем имя файла или значение "Не указан"
        if 'supplier' not in column_mapping:
            supplier_name = os.path.splitext(os.path.basename(file_path))[0]
            df['_supplier'] = supplier_name
            column_mapping['supplier'] = '_supplier'
            
        print(f"Найденные соответствия столбцов: {column_mapping}")
        
        # Проверка, что нашли хотя бы название товара и цену
        if 'name' not in column_mapping:
            raise ValueError("Не удалось определить столбец с наименованием товара")
        if 'price' not in column_mapping:
            raise ValueError("Не удалось определить столбец с ценой товара")
        
        # Если не нашли остаток, используем значение по умолчанию
        if 'stock' not in column_mapping:
            df['_stock'] = 100  # Значение по умолчанию
            column_mapping['stock'] = '_stock'
            
        # Очистка данных от NaN и пустых строк
        df = df.dropna(subset=[column_mapping['name']])
        df = df[df[column_mapping['name']].astype(str).str.strip() != '']
        
        # Преобразование всех цен в числа
        df[column_mapping['price']] = pd.to_numeric(df[column_mapping['price']], errors='coerce')
        df = df.dropna(subset=[column_mapping['price']])
        
        # Преобразование остатка в целое число
        if 'stock' in column_mapping:
            df[column_mapping['stock']] = pd.to_numeric(df[column_mapping['stock']], errors='coerce').fillna(1).astype(int)
        
        # Обработка данных и загрузка в базу
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Очистка существующих данных
        cursor.execute("DELETE FROM products")
        
        # Обработка каждой строки
        for _, row in df.iterrows():
            # Извлечение имени продукта
            product_name = str(row[column_mapping['name']])
            
            # Извлечение характеристик с помощью regex
            characteristics = extract_characteristics(product_name)
            
            # Получение цены и остатка
            price = float(row[column_mapping['price']])
            stock = int(row[column_mapping['stock']]) if 'stock' in column_mapping else 100
            supplier = str(row[column_mapping['supplier']]) if 'supplier' in column_mapping else "Не указан"
            
            # Создание записи продукта
            cursor.execute('''
            INSERT INTO products (
                supplier, name, price, stock, 
                category, diameter, material, pressure, 
                execution, standard, additional_params
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                supplier,
                product_name,
                price,
                stock,
                characteristics.get('category'),
                characteristics.get('diameter'),
                characteristics.get('material'),
                characteristics.get('pressure'),
                characteristics.get('execution'),
                characteristics.get('standard'),
                characteristics.get('additional_params')
            ))
        
        conn.commit()
        conn.close()
        
        return len(df)
    
    except Exception as e:
        print(f"Ошибка при загрузке прайс-листа: {str(e)}")
        print(traceback.format_exc())
        raise

# Поиск товаров по характеристикам
def search_products(query):
    try:
        # Извлечение характеристик из запроса
        characteristics = {}
        
        # Категория (например, "фланец", "отвод")
        category_match = re.search(r'\b(фланец|отвод|переход|заглушка|тройник)(?:\w*)\b', query, re.IGNORECASE)
        if category_match:
            category = category_match.group(1).lower()
            # Сопоставление с категориями в базе данных
            category_map = {
                'фланец': 'Фланцы',
                'отвод': 'Отводы',
                'переход': 'Переходы',
                'заглушка': 'Заглушки',
                'тройник': 'Тройники'
            }
            characteristics['category'] = category_map.get(category)
        
        # Диаметр (например, "25 мм", "Ду 25")
        diameter_match = re.search(r'\b(?:Ду|ду|диаметр)?\s*(\d+)(?:\s*мм)?\b', query)
        if diameter_match:
            characteristics['diameter'] = diameter_match.group(1)
        
        # Материал (например, "сталь 20", "ст.20")
        material_match = re.search(r'\b(?:ст\.|сталь)\s*(\d+|\w+)\b', query, re.IGNORECASE)
        if material_match:
            characteristics['material'] = material_match.group(0)
        
        # Стандарт (например, "ГОСТ 33259-2015")
        standard_match = re.search(r'\b(ГОСТ\s+[\d\-]+)\b', query, re.IGNORECASE)
        if standard_match:
            characteristics['standard'] = standard_match.group(1)
        
        # Построение SQL-запроса на основе характеристик
        sql_query = "SELECT * FROM products WHERE 1=1"
        params = []
        
        for key, value in characteristics.items():
            if value:
                sql_query += f" AND {key} LIKE ?"
                params.append(f"%{value}%")
        
        # Поиск по слову в наименовании, если не найдены другие характеристики
        if not characteristics:
            sql_query += " AND name LIKE ?"
            params.append(f"%{query}%")
        
        # Выполнение запроса
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql_query, params)
        
        # Преобразование результатов в список словарей
        products = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return products
    
    except Exception as e:
        print(f"Ошибка при поиске продуктов: {str(e)}")
        raise

# Генерация КП в Excel
def generate_proposal(products, quantity=10):
    try:
        # Генерация имени файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        uid = str(uuid.uuid4())[:8]
        filename = f"КП_{timestamp}_{uid}.xlsx"
        file_path = os.path.join(PROPOSALS_DIR, filename)
        
        # Создание DataFrame для Excel
        df = pd.DataFrame(columns=["№", "Наименование товара", "Цена (руб)", "Кол-во", "Сумма (руб)"])
        
        # Добавление товаров
        for i, product in enumerate(products, 1):
            qty = min(quantity, product.get("stock", quantity))
            price = product.get("price", 0)
            total = price * qty
            
            df.loc[i] = [i, product.get("name", ""), price, qty, total]
        
        # Добавление строки с итогом
        df.loc[len(df) + 1] = ["Итого:", "", "", "", df["Сумма (руб)"].sum()]
        
        # Сохранение в Excel
        writer = pd.ExcelWriter(file_path, engine='openpyxl')
        df.to_excel(writer, index=False, sheet_name="Коммерческое предложение")
        writer.close()
        
        return file_path
    
    except Exception as e:
        print(f"Ошибка при генерации КП: {str(e)}")
        raise

# Получение статистики о товарах в базе
def get_products_stats():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Общее количество товаров
        cursor.execute("SELECT COUNT(*) FROM products")
        total_count = cursor.fetchone()[0]
        
        # Количество по категориям
        cursor.execute("SELECT category, COUNT(*) FROM products WHERE category IS NOT NULL GROUP BY category")
        categories = cursor.fetchall()
        
        conn.close()
        
        stats = f"📊 Статистика базы данных:\n\n"
        stats += f"Всего товаров: {total_count}\n\n"
        
        if categories:
            stats += "По категориям:\n"
            for category, count in categories:
                stats += f"- {category}: {count}\n"
        
        return stats
    except Exception as e:
        print(f"Ошибка при получении статистики: {str(e)}")
        return f"Ошибка при получении статистики: {str(e)}"

# Обработчик команды /start
@bot.message_handler(commands=['start'])
def handle_start(message):
    bot.reply_to(message, "Привет! Я бот для создания коммерческих предложений.\n\n"
                         "Доступные команды:\n"
                         "/start - показать это сообщение\n"
                         "/status - проверить статус базы данных\n"
                         "/help - показать помощь\n\n"
                         "Для загрузки прайс-листа отправьте Excel или CSV файл.\n"
                         "Для создания КП просто напишите запрос, например: 'фланец 25 мм сталь 20'")

# Обработчик команды /help
@bot.message_handler(commands=['help'])
def handle_help(message):
    help_text = (
        "📋 Инструкция по использованию:\n\n"
        "1️⃣ Загрузите прайс-лист\n"
        "   - Отправьте Excel (.xlsx, .xls) или CSV файл\n"
        "   - Файл должен содержать столбцы: 'Наименование поставщика', 'Наименование товара', 'Цена (руб)', 'Остаток'\n\n"
        "2️⃣ Создайте коммерческое предложение\n"
        "   - Просто напишите текстовый запрос (например: 'фланец 25 мм сталь 20')\n"
        "   - Бот найдет подходящие товары и создаст КП\n\n"
        "3️⃣ Команды\n"
        "   /start - главное меню\n"
        "   /status - проверить статус базы данных\n"
        "   /help - показать эту инструкцию"
    )
    bot.reply_to(message, help_text)

# Обработчик команды /status
@bot.message_handler(commands=['status'])
def handle_status(message):
    try:
        # Проверяем наличие базы данных
        if os.path.exists(DB_PATH):
            # Получаем статистику
            stats = get_products_stats()
            bot.reply_to(message, stats)
        else:
            bot.reply_to(message, "❌ База данных еще не создана. Загрузите прайс-лист для создания базы.")
    except Exception as e:
        error_msg = f"❌ Ошибка при проверке статуса: {str(e)}"
        print(error_msg)
        bot.reply_to(message, error_msg)

# Обработчик файлов
@bot.message_handler(content_types=['document'])
def handle_document(message):
    try:
        file_info = bot.get_file(message.document.file_id)
        file_extension = os.path.splitext(message.document.file_name)[1].lower()
        
        # Проверка, что это Excel или CSV
        if file_extension not in ['.xlsx', '.xls', '.csv']:
            bot.reply_to(message, "❌ Пожалуйста, отправьте файл в формате Excel (.xlsx, .xls) или CSV (.csv)")
            return
        
        # Скачивание файла
        downloaded_file = bot.download_file(file_info.file_path)
        
        # Создаем абсолютный путь к файлу
        local_file_path = os.path.join(UPLOADS_DIR, message.document.file_name)
        
        # Сохранение файла
        with open(local_file_path, 'wb') as new_file:
            new_file.write(downloaded_file)
            
        bot.reply_to(message, f"✅ Файл {message.document.file_name} получен. Начинаю загрузку в базу данных...")
        print(f"Сохранен файл: {local_file_path}")
        
        # Инициализация базы данных
        init_db()
        
        # Загрузка прайс-листа напрямую в базу данных
        count = load_price_list(local_file_path)
        
        bot.reply_to(message, f"✅ Прайс-лист успешно загружен. Добавлено {count} позиций в базу данных.")
    
    except Exception as e:
        error_msg = f"❌ Произошла ошибка: {str(e)}"
        print(error_msg)
        print(traceback.format_exc())
        bot.reply_to(message, error_msg)

# Обработчик текстовых сообщений (не команд)
@bot.message_handler(func=lambda message: True)
def handle_query(message):
    try:
        query = message.text.strip()
        
        # Проверяем наличие базы данных
        if not os.path.exists(DB_PATH):
            bot.reply_to(message, "❌ База данных еще не создана. Загрузите прайс-лист для создания базы.")
            return
        
        # Поиск товаров
        products = search_products(query)
        
        if not products:
            bot.reply_to(message, "⚠️ По вашему запросу ничего не найдено")
            return
        
        # Генерация КП
        bot.reply_to(message, f"✅ Найдено товаров: {len(products)}\nГенерирую КП...")
        
        proposal_path = generate_proposal(products, 10)
        
        # Отправка файла КП
        with open(proposal_path, 'rb') as f:
            bot.send_document(message.chat.id, f, caption=f"Коммерческое предложение по запросу: {query}")
    
    except Exception as e:
        error_msg = f"❌ Произошла ошибка: {str(e)}"
        print(error_msg)
        print(traceback.format_exc())
        bot.reply_to(message, error_msg)

# Запуск бота
if __name__ == "__main__":
    print("Бот запущен...")
    
    # Создаем необходимые директории
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    os.makedirs(PROPOSALS_DIR, exist_ok=True)
    
    # Инициализируем базу данных при запуске
    init_db()
    
    # Запускаем бота
    bot.polling(none_stop=True) 