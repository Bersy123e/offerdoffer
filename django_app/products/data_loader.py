import pandas as pd
import sqlite3
import re
import logging
from typing import Dict, List, Optional, Tuple, Union
import os
from logger import setup_logger

logger = setup_logger()

class DataLoader:
    def __init__(self, db_path: str = "products.db"):
        """
        Initialize the DataLoader with database path.
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self._initialize_db()
    
    def _initialize_db(self):
        """Initialize SQLite database with required tables."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create products table
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
                
                # Create indices for faster searching
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_name ON products(name)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_category ON products(category)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_diameter ON products(diameter)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_material ON products(material)')
                
                conn.commit()
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
    
    def load_price_list(self, file_path: str) -> int:
        """
        Load price list from CSV or Excel file.
        
        Args:
            file_path: Path to price list file
            
        Returns:
            Number of products loaded
        """
        try:
            logger.info(f"Loading price list from {file_path}")
            
            # Determine file type and load with pandas
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError("Unsupported file format. Use CSV or Excel.")
            
            # Check required columns
            required_columns = ['Наименование поставщика', 'Наименование товара', 'Цена (руб)', 'Остаток']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
            # Remove summary rows (rows that don't represent products)
            # This is a simplified approach; actual implementation would need more specific rules
            df = df[df['Цена (руб)'].notna() & df['Остаток'].notna()]
            
            # Process data and load into database
            self._process_and_load_data(df)
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Error loading price list: {str(e)}")
            raise
    
    def _process_and_load_data(self, df: pd.DataFrame):
        """
        Process dataframe and load into SQLite database.
        
        Args:
            df: Dataframe with product data
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Clear existing data
                cursor.execute("DELETE FROM products")
                
                # Process each row
                products = []
                for _, row in df.iterrows():
                    # Extract product name
                    product_name = row['Наименование товара']
                    
                    # Extract characteristics using regex
                    characteristics = self._extract_characteristics(product_name)
                    
                    # Create product record
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
                
                # Batch insert
                cursor.executemany('''
                INSERT INTO products (
                    supplier, name, price, stock, 
                    category, diameter, material, pressure, 
                    execution, standard, additional_params
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', products)
                
                conn.commit()
            logger.info(f"Loaded {len(products)} products into database")
            
        except Exception as e:
            logger.error(f"Error processing and loading data: {str(e)}")
            raise
    
    def _extract_characteristics(self, product_name: str) -> Dict[str, Optional[str]]:
        """
        Extract product characteristics from name using regex.
        
        Args:
            product_name: Product name text
            
        Returns:
            Dictionary of extracted characteristics
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
            # Example regex patterns (to be expanded based on actual data)
            
            # Category (e.g., "Фланцы", "Отводы")
            category_match = re.search(r'^(Фланцы|Отводы|Переходы|Заглушки|Тройники)(?:\s+|$)', product_name)
            if category_match:
                characteristics['category'] = category_match.group(1)
            
            # Diameter (e.g., "Ду 25")
            diameter_match = re.search(r'Ду\s*(\d+)', product_name)
            if diameter_match:
                characteristics['diameter'] = diameter_match.group(1)
            
            # Material (e.g., "ст.20")
            material_match = re.search(r'(?:ст\.|сталь)\s*(\d+|\w+)', product_name, re.IGNORECASE)
            if material_match:
                characteristics['material'] = material_match.group(0)
            
            # Pressure
            pressure_match = re.search(r'-(\d+)-', product_name)
            if pressure_match:
                characteristics['pressure'] = pressure_match.group(1)
            
            # Execution (e.g., "исп.В")
            execution_match = re.search(r'исп\.(\w+)', product_name)
            if execution_match:
                characteristics['execution'] = f"исп.{execution_match.group(1)}"
            
            # Standard (e.g., "ГОСТ 33259-2015")
            standard_match = re.search(r'(ГОСТ\s+[\d\-]+)', product_name)
            if standard_match:
                characteristics['standard'] = standard_match.group(1)
            
            # Additional params (e.g., "01-1-В")
            additional_match = re.search(r'\d+\-\d+\-\w+', product_name)
            if additional_match:
                characteristics['additional_params'] = additional_match.group(0)
            
        except Exception as e:
            logger.warning(f"Error extracting characteristics from '{product_name}': {str(e)}")
        
        return characteristics
    
    def get_products_by_characteristics(self, characteristics: Dict[str, str]) -> List[Dict]:
        """
        Get products matching given characteristics.
        
        Args:
            characteristics: Dictionary of characteristics to match
            
        Returns:
            List of matching products
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
                
                # Convert to list of dictionaries
                columns = [col[0] for col in cursor.description]
                products = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return products
            
        except Exception as e:
            logger.error(f"Error getting products by characteristics: {str(e)}")
            raise
    
    def close(self):
        """Close database connection."""
        logger.info("Database connection closed") 