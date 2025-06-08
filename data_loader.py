import os
import pandas as pd
import sqlite3
import re
from typing import List, Dict, Optional
import logging

from logger import setup_logger

logger = setup_logger()

class DataLoader:
    def __init__(self, db_path: str = "products.db"):
        """
        Инициализация загрузчика данных с подключением к SQLite базе.
        
        Args:
            db_path: Путь к файлу базы данных SQLite
        """
        self.db_path = db_path
        self._initialize_db()
    
    def _initialize_db(self):
        """Инициализация базы данных нужна для создания таблиц при первом запуске."""
        try:
            with sqlite3.connect(self.db_path) as conn:
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
                
                # Индексы нужны для быстрого поиска по характеристикам
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_category ON products(category)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_diameter ON products(diameter)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_material ON products(material)')
                
                conn.commit()
                logger.info("База данных инициализирована успешно")
                
        except Exception as e:
            logger.error(f"Ошибка инициализации базы данных: {str(e)}")
            raise
    
    def load_price_list(self, file_path: str) -> int:
        """
        Загрузка прайс-листа из CSV или Excel файла.
        
        Args:
            file_path: Путь к файлу прайс-листа
            
        Returns:
            Количество загруженных товаров
        """
        try:
            logger.info(f"Загрузка прайс-листа из {file_path}")
            
            # Определение типа файла и загрузка через pandas
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError("Неподдерживаемый формат файла. Используйте CSV или Excel.")
            
            # Проверка необходимых колонок
            required_columns = ['Наименование поставщика', 'Наименование товара', 'Цена (руб)', 'Остаток']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Отсутствуют необходимые колонки: {', '.join(missing_columns)}")
            
            # Удаление итоговых строк без данных о товарах
            df = df[df['Цена (руб)'].notna() & df['Остаток'].notna()]
            
            # Обработка данных и загрузка в базу
            self._process_and_load_data(df)
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Ошибка загрузки прайс-листа: {str(e)}")
            raise
    
    def _process_and_load_data(self, df: pd.DataFrame):
        """
        Обработка DataFrame и загрузка в SQLite базу данных.
        
        Args:
            df: DataFrame с данными о товарах
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Очистка существующих данных
                cursor.execute("DELETE FROM products")
                
                # Обработка каждой строки
                products = []
                for _, row in df.iterrows():
                    # Извлечение названия товара
                    product_name = row['Наименование товара']
                    
                    # Извлечение характеристик через regex
                    characteristics = self._extract_characteristics(product_name)
                    
                    # Создание записи товара
                    product = (
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
                    )
                    products.append(product)
                
                # Пакетная вставка для производительности
                cursor.executemany('''
                INSERT INTO products (
                    supplier, name, price, stock, 
                    category, diameter, material, pressure, 
                    execution, standard, additional_params
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', products)
                
                conn.commit()
                logger.info(f"Загружено {len(products)} товаров в базу данных")
                
        except Exception as e:
            logger.error(f"Ошибка обработки и загрузки данных: {str(e)}")
            raise
    
    def _extract_characteristics(self, product_name: str) -> Dict[str, Optional[str]]:
        """
        Извлечение характеристик товара из названия через regex.
        
        Args:
            product_name: Текст названия товара
            
        Returns:
            Словарь извлеченных характеристик
        """
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
            # Категория товара (например, "Фланцы", "Отводы")
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
            logger.warning(f"Ошибка извлечения характеристик из '{product_name}': {str(e)}")
        
        return characteristics
    
    def get_products_by_characteristics(self, characteristics: Dict[str, str]) -> List[Dict]:
        """
        Получение товаров по заданным характеристикам.
        
        Args:
            characteristics: Словарь характеристик для поиска
            
        Returns:
            Список подходящих товаров
        """
        try:
            query = "SELECT * FROM products WHERE 1=1"
            params = []
            
            for key, value in characteristics.items():
                if value and key in ['category', 'diameter', 'material', 'pressure', 'execution', 'standard']:
                    query += f" AND {key} LIKE ?"
                    params.append(f"%{value}%")
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                # Преобразование в список словарей
                columns = [col[0] for col in cursor.description]
                products = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return products
            
        except Exception as e:
            logger.error(f"Ошибка получения товаров по характеристикам: {str(e)}")
            raise
    
    def close(self):
        """Закрытие соединения с базой данных."""
        logger.info("Соединение с базой данных закрыто") 