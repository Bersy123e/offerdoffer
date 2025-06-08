import pandas as pd
import re
import logging
from typing import Dict, List, Optional, Tuple
import itertools

logger = logging.getLogger(__name__)

class BruteforceProcessor:
    """
    УРОВЕНЬ 2: Брутфорс-процессор для сложных файлов
    Пробует все возможные комбинации заголовков, листов, структур
    """
    
    def __init__(self, llm=None):
        self.llm = llm
        
    def process_complex_file(self, file_path: str, file_name: str = "") -> Dict:
        """
        Брутфорс обработка сложного файла
        """
        logger.info(f"УРОВЕНЬ 2: Брутфорс анализ {file_name}")
        
        result = {
            'success': False,
            'method': 'bruteforce',
            'attempts': [],
            'products': [],
            'best_attempt': None
        }
        
        try:
            # ПОПЫТКА 1: Анализ всех листов
            attempts = []
            
            # Получаем все листы
            xl_file = pd.ExcelFile(file_path)
            for sheet_name in xl_file.sheet_names:
                logger.info(f"Анализируем лист: {sheet_name}")
                sheet_attempts = self._analyze_sheet(file_path, sheet_name)
                attempts.extend(sheet_attempts)
            
            result['attempts'] = attempts
            
            # Выбираем лучшую попытку
            best_attempt = self._select_best_attempt(attempts)
            if best_attempt:
                result['best_attempt'] = best_attempt
                result['products'] = best_attempt['products']
                result['success'] = len(best_attempt['products']) > 0
                
                logger.info(f"Лучшая попытка: {best_attempt['description']} - {len(best_attempt['products'])} товаров")
            
        except Exception as e:
            logger.error(f"Ошибка брутфорс обработки: {e}")
            result['error'] = str(e)
        
        return result
    
    def _analyze_sheet(self, file_path: str, sheet_name: str) -> List[Dict]:
        """Анализ конкретного листа с множественными попытками"""
        attempts = []
        
        try:
            # Читаем лист
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=str)
            
            # ПОПЫТКА 1: Ищем все возможные строки заголовков (первые 30 строк)
            for header_row in range(min(30, len(df))):
                attempt = self._try_header_row(df, header_row, f"{sheet_name}_row_{header_row}")
                if attempt:
                    attempts.append(attempt)
            
            # ПОПЫТКА 2: Анализ по содержимому без заголовков
            attempt = self._try_content_analysis(df, f"{sheet_name}_content_analysis")
            if attempt:
                attempts.append(attempt)
                
        except Exception as e:
            logger.error(f"Ошибка анализа листа {sheet_name}: {e}")
        
        return attempts
    
    def _try_header_row(self, df: pd.DataFrame, header_row: int, description: str) -> Optional[Dict]:
        """Попытка обработки с конкретной строкой заголовков"""
        try:
            if header_row >= len(df):
                return None
                
            # Получаем заголовки
            headers = df.iloc[header_row].tolist()
            
            # Фильтруем пустые заголовки
            clean_headers = []
            header_mapping = {}
            
            for i, header in enumerate(headers):
                if pd.notna(header) and str(header).strip():
                    clean_name = str(header).strip()
                    clean_headers.append(clean_name)
                    header_mapping[clean_name] = i
            
            # Нужно минимум 2 колонки
            if len(clean_headers) < 2:
                return None
            
            # Пробуем найти маппинг
            mapping = self._bruteforce_mapping(clean_headers, header_mapping)
            if not mapping:
                return None
            
            # Извлекаем товары
            products = self._extract_with_mapping(df, mapping, header_row)
            
            return {
                'description': description,
                'header_row': header_row,
                'headers': clean_headers,
                'mapping': mapping,
                'products': products,
                'score': len(products)
            }
            
        except Exception as e:
            logger.debug(f"Ошибка попытки {description}: {e}")
            return None
    
    def _bruteforce_mapping(self, headers: List[str], header_mapping: Dict[str, int]) -> Optional[Dict]:
        """Брутфорс поиск маппинга колонок"""
        
        mapping = {
            'name_column_idx': None,
            'price_column_idx': None,  
            'stock_column_idx': None,
            'article_column_idx': None
        }
        
        headers_lower = [h.lower() for h in headers]
        
        # Ищем название товара
        name_keywords = ['наимен', 'товар', 'номенклатур', 'продукц', 'изделие', 'описание']
        for i, header in enumerate(headers_lower):
            if any(keyword in header for keyword in name_keywords):
                mapping['name_column_idx'] = header_mapping[headers[i]]
                break
        
        # Если не нашли по ключевым словам - берем самую длинную колонку (исключая служебные)
        if mapping['name_column_idx'] is None:
            max_len = 0
            best_idx = None
            for i, header in enumerate(headers_lower):
                if not any(word in header for word in ['п/п', 'номер', '№', 'цена', 'кол-во', 'остат']):
                    if len(header) > max_len:
                        max_len = len(header)
                        best_idx = i
            if best_idx is not None:
                mapping['name_column_idx'] = header_mapping[headers[best_idx]]
        
        # Ищем цену
        price_keywords = ['цена', 'стоимост', 'руб', 'price', 'сумма']
        for i, header in enumerate(headers_lower):
            if any(keyword in header for keyword in price_keywords):
                mapping['price_column_idx'] = header_mapping[headers[i]]
                break
        
        # Ищем остаток/количество
        stock_keywords = ['остат', 'кол-во', 'налич', 'склад', 'количеств', 'доступ']
        for i, header in enumerate(headers_lower):
            if any(keyword in header for keyword in stock_keywords):
                mapping['stock_column_idx'] = header_mapping[headers[i]]
                break
        
        # Ищем артикул
        article_keywords = ['артикул', 'код', 'sku', 'art']
        for i, header in enumerate(headers_lower):
            if any(keyword in header for keyword in article_keywords):
                mapping['article_column_idx'] = header_mapping[headers[i]]
                break
        
        # Должна быть хотя бы колонка с названиями
        if mapping['name_column_idx'] is not None:
            return mapping
        
        return None
    
    def _try_content_analysis(self, df: pd.DataFrame, description: str) -> Optional[Dict]:
        """Анализ файла по содержимому без опоры на заголовки"""
        try:
            # Анализируем каждую колонку
            column_analysis = {}
            
            for col_idx in range(df.shape[1]):
                col_data = df.iloc[:, col_idx].dropna()
                if len(col_data) < 5:
                    continue
                    
                str_data = [str(cell).lower().strip() for cell in col_data if str(cell).strip()]
                
                # Подсчитываем признаки
                product_matches = sum(
                    1 for cell in str_data 
                    if re.search(r'отвод|фланец|тройник|ду\s*\d+|гост|ст\.\s*\d+|задвижка', cell)
                )
                
                numeric_matches = sum(
                    1 for cell in str_data
                    if re.match(r'^\d+([.,]\d+)?$', cell.strip())
                )
                
                article_matches = sum(
                    1 for cell in str_data
                    if re.match(r'^[a-z0-9_\*\-\.]+$', cell) and len(cell) > 3
                )
                
                # Средняя длина строк
                avg_length = sum(len(cell) for cell in str_data) / len(str_data) if str_data else 0
                
                column_analysis[col_idx] = {
                    'product_score': product_matches,
                    'numeric_score': numeric_matches,
                    'article_score': article_matches,
                    'avg_length': avg_length,
                    'total_rows': len(str_data)
                }
            
            # Определяем роли колонок
            mapping = self._assign_column_roles(column_analysis)
            if not mapping or mapping['name_column_idx'] is None:
                return None
            
            # Извлекаем товары (предполагаем что данные начинаются с строки 1)
            products = self._extract_with_mapping(df, mapping, 0)
            
            return {
                'description': description,
                'header_row': -1,  # Без заголовков
                'headers': [],
                'mapping': mapping,
                'products': products,
                'score': len(products),
                'column_analysis': column_analysis
            }
            
        except Exception as e:
            logger.debug(f"Ошибка контент-анализа: {e}")
            return None
    
    def _assign_column_roles(self, column_analysis: Dict) -> Optional[Dict]:
        """Определение ролей колонок на основе анализа"""
        mapping = {
            'name_column_idx': None,
            'price_column_idx': None,
            'stock_column_idx': None,
            'article_column_idx': None
        }
        
        # Ищем колонку с товарами (больше всего product_score и длинные строки)
        best_name_score = 0
        for col_idx, analysis in column_analysis.items():
            score = analysis['product_score'] + (analysis['avg_length'] / 20)  # Бонус за длину
            if score > best_name_score:
                best_name_score = score
                mapping['name_column_idx'] = col_idx
        
        # Ищем колонку с числами (цены или остатки)
        numeric_columns = [
            (col_idx, analysis['numeric_score']) 
            for col_idx, analysis in column_analysis.items()
            if analysis['numeric_score'] > 5
        ]
        
        # Сортируем по количеству чисел
        numeric_columns.sort(key=lambda x: x[1], reverse=True)
        
        # Первая числовая колонка - цены, вторая - остатки
        if len(numeric_columns) >= 1:
            mapping['price_column_idx'] = numeric_columns[0][0]
        if len(numeric_columns) >= 2:
            mapping['stock_column_idx'] = numeric_columns[1][0]
        
        # Ищем артикулы
        best_article_score = 0
        for col_idx, analysis in column_analysis.items():
            if col_idx != mapping['name_column_idx'] and analysis['article_score'] > best_article_score:
                best_article_score = analysis['article_score']
                mapping['article_column_idx'] = col_idx
        
        return mapping if mapping['name_column_idx'] is not None else None
    
    def _extract_with_mapping(self, df: pd.DataFrame, mapping: Dict, header_row: int) -> List[Dict]:
        """Извлечение товаров с использованием маппинга"""
        products = []
        
        name_col = mapping.get('name_column_idx')
        price_col = mapping.get('price_column_idx')
        stock_col = mapping.get('stock_column_idx')
        article_col = mapping.get('article_column_idx')
        
        if name_col is None:
            return products
        
        # Начинаем после заголовков
        start_row = header_row + 1 if header_row >= 0 else 0
        
        for idx in range(start_row, len(df)):
            row = df.iloc[idx]
            
            # Название товара
            name_value = str(row.iloc[name_col] if name_col < len(row) else '').strip()
            if not name_value or name_value == 'nan' or len(name_value) < 5:
                continue
            
            # Фильтрация мусора
            if self._is_junk_row(name_value):
                continue
            
            # Цена
            price = 0.0
            if price_col is not None and price_col < len(row):
                price_raw = str(row.iloc[price_col]).strip()
                if price_raw and price_raw != 'nan':
                    price_match = re.search(r'[\d\s\,\.]+', price_raw.replace(' ', ''))
                    if price_match:
                        try:
                            price = float(price_match.group().replace(',', '.'))
                        except:
                            price = 0.0
            
            # Остаток
            stock = 100
            if stock_col is not None and stock_col < len(row):
                stock_raw = str(row.iloc[stock_col]).strip()
                if stock_raw and stock_raw != 'nan':
                    stock_match = re.search(r'\d+', stock_raw)
                    if stock_match:
                        try:
                            stock = int(stock_match.group())
                        except:
                            stock = 100
            
            # Артикул
            article = ""
            if article_col is not None and article_col < len(row):
                article = str(row.iloc[article_col]).strip()
                if article == 'nan':
                    article = ""
            
            products.append({
                'name': name_value,
                'price': price,
                'stock': stock,
                'article': article,
                'supplier': ''
            })
        
        return products
    
    def _is_junk_row(self, name_value: str) -> bool:
        """Проверка что строка - мусор"""
        name_lower = name_value.lower().strip()
        
        junk_patterns = [
            r'^[\d\s\-\.\,\(\)]+$',  # Только цифры
            r'^[а-я\s]{1,4}$',  # Слишком короткие
            r'итого|всего|сумма|подпись|печать',
            r'тел\.|факс|@|www\.',
            r'наименование|товар|цена|остаток|артикул',  # Заголовки
            r'остатки и доступность|параметры|количество товаров',  # Служебные
        ]
        
        return any(re.search(pattern, name_lower) for pattern in junk_patterns)
    
    def _select_best_attempt(self, attempts: List[Dict]) -> Optional[Dict]:
        """Выбор лучшей попытки из всех"""
        if not attempts:
            return None
        
        # Сортируем по количеству найденных товаров
        attempts_with_products = [a for a in attempts if len(a['products']) > 0]
        
        if not attempts_with_products:
            return None
        
        # Выбираем попытку с наибольшим количеством товаров
        best_attempt = max(attempts_with_products, key=lambda x: x['score'])
        
        return best_attempt 