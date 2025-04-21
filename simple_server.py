import os
import pandas as pd
import json
import re
from typing import List, Dict, Optional, Any
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Body
from fastapi.responses import FileResponse
import uvicorn
from pydantic import BaseModel
import sqlite3
import uuid
from datetime import datetime

# Инициализация базы данных
DB_PATH = "products.db"

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

# Извлечение характеристик из наименования товара
def extract_characteristics(product_name: str) -> Dict[str, Optional[str]]:
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
def load_price_list(file_path: str) -> int:
    try:
        # Определение типа файла и загрузка с помощью pandas
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Неподдерживаемый формат файла. Используйте CSV или Excel.")
        
        # Проверка необходимых столбцов
        required_columns = ['Наименование поставщика', 'Наименование товара', 'Цена (руб)', 'Остаток']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Отсутствуют необходимые столбцы: {', '.join(missing_columns)}")
        
        # Удаление итоговых строк
        df = df[df['Цена (руб)'].notna() & df['Остаток'].notna()]
        
        # Обработка данных и загрузка в базу
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Очистка существующих данных
        cursor.execute("DELETE FROM products")
        
        # Обработка каждой строки
        for _, row in df.iterrows():
            # Извлечение имени продукта
            product_name = row['Наименование товара']
            
            # Извлечение характеристик с помощью regex
            characteristics = extract_characteristics(product_name)
            
            # Создание записи продукта
            cursor.execute('''
            INSERT INTO products (
                supplier, name, price, stock, 
                category, diameter, material, pressure, 
                execution, standard, additional_params
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['Наименование поставщика'],
                product_name,
                float(row['Цена (руб)']),
                int(row['Остаток']),
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
        raise

# Поиск товаров по характеристикам
def search_products(query: str) -> List[Dict]:
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
def generate_proposal(products: List[Dict], quantity: int = 10) -> str:
    try:
        # Создание каталога для КП, если он не существует
        if not os.path.exists("proposals"):
            os.makedirs("proposals")
        
        # Генерация имени файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        uid = str(uuid.uuid4())[:8]
        filename = f"КП_{timestamp}_{uid}.xlsx"
        file_path = os.path.join("proposals", filename)
        
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

# Модели данных для API
class QueryRequest(BaseModel):
    query: str
    quantity: int = 10

# Инициализация FastAPI
app = FastAPI(title="Генератор коммерческих предложений")

@app.get("/")
def read_root():
    return {"message": "API генератора коммерческих предложений работает"}

@app.post("/upload-price-list")
async def upload_price_list_api(file_path: str = Body(..., embed=True)):
    try:
        print(f"Получен запрос на загрузку файла: {file_path}")
        
        # Проверка существования файла
        file_exists = os.path.exists(file_path)
        print(f"Файл существует: {file_exists}")
        
        if not file_exists:
            # Проверяем относительный путь от текущей директории
            current_dir = os.getcwd()
            rel_path = os.path.join(current_dir, file_path)
            print(f"Относительный путь: {rel_path}")
            file_exists = os.path.exists(rel_path)
            print(f"Файл существует по относительному пути: {file_exists}")
            
            if file_exists:
                file_path = rel_path
            else:
                # Проверяем абсолютный путь
                abs_path = os.path.abspath(file_path)
                print(f"Абсолютный путь: {abs_path}")
                file_exists = os.path.exists(abs_path)
                print(f"Файл существует по абсолютному пути: {file_exists}")
                
                if file_exists:
                    file_path = abs_path
                else:
                    raise HTTPException(status_code=404, detail=f"Файл {file_path} не найден")
        
        # Загрузка прайс-листа
        count = load_price_list(file_path)
        
        return {
            "status": "success",
            "message": f"Прайс-лист успешно загружен. Добавлено {count} позиций"
        }
    except Exception as e:
        error_msg = f"Ошибка при загрузке прайс-листа: {str(e)}"
        print(error_msg)
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_msg)

@app.post("/generate-proposal")
async def generate_proposal_api(request: QueryRequest):
    try:
        # Поиск товаров
        products = search_products(request.query)
        
        if not products:
            return {
                "status": "warning",
                "message": "По вашему запросу ничего не найдено"
            }
        
        # Генерация КП
        proposal_path = generate_proposal(products, request.quantity)
        
        return {
            "status": "success",
            "products_found": len(products),
            "proposal_path": proposal_path
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download-proposal/{filename}")
async def download_proposal(filename: str):
    file_path = os.path.join("proposals", filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Файл не найден")
    
    return FileResponse(file_path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=filename)

@app.get("/list-products")
async def list_products(limit: int = 100, offset: int = 0):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT * FROM products LIMIT {limit} OFFSET {offset}")
    products = [dict(row) for row in cursor.fetchall()]
    
    cursor.execute("SELECT COUNT(*) as total FROM products")
    total = cursor.fetchone()["total"]
    
    conn.close()
    
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "products": products
    }

if __name__ == "__main__":
    # Инициализация базы данных
    init_db()
    
    # Запуск сервера
    uvicorn.run("simple_server:app", host="0.0.0.0", port=8000, reload=True) 