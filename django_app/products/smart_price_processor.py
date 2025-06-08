import pandas as pd
import re
import logging
from typing import Dict, List, Optional, Tuple
import json

logger = logging.getLogger(__name__)

class SmartPriceListProcessor:
    """
    Умная обработка прайс-листов с двухэтапной проверкой:
    1. Структурный анализ (поиск заголовков, данных)
    2. LLM + Regex валидация
    """
    
    def __init__(self, llm=None):
        self.llm = llm
        
    def process_price_list(self, df: pd.DataFrame, file_name: str = "") -> Dict:
        """
        Основной метод обработки прайс-листа
        
        Returns:
            Dict с результатами: {
                'success': bool,
                'mapping': dict,
                'header_row': int,
                'data_rows': list,
                'products': list,
                'errors': list
            }
        """
        logger.info(f"Начинаю умную обработку прайс-листа: {file_name}")
        
        result = {
            'success': False,
            'mapping': {},
            'header_row': -1,
            'data_rows': [],
            'products': [],
            'errors': []
        }
        
        try:
            # ЭТАП 1: Поиск структуры таблицы
            header_row = self._find_header_row_smart(df)
            if header_row == -1:
                result['errors'].append("Не удалось найти строку заголовков")
                return result
                
            result['header_row'] = header_row
            logger.info(f"Найдена строка заголовков: {header_row}")
            
            # Получаем заголовки и образцы данных
            headers = df.iloc[header_row].tolist()
            sample_rows = self._get_sample_data_rows(df, header_row)
            
            # Создаем новый DataFrame с правильными заголовками
            df_with_headers = df.copy()
            df_with_headers.columns = [f'col_{i}' for i in range(len(df.columns))]
            header_data = df.iloc[header_row].tolist()
            
            # Создаем маппинг индексов к названиям колонок
            col_name_mapping = {}
            for i, header_val in enumerate(header_data):
                if pd.notna(header_val) and str(header_val).strip():
                    col_name_mapping[f'col_{i}'] = str(header_val).strip()
            
            # ЭТАП 2: LLM + Regex маппинг колонок
            mapping = self._get_column_mapping_v2(headers, sample_rows, file_name)
            if not mapping:
                result['errors'].append("Не удалось определить маппинг колонок")
                return result
                
            # Преобразуем маппинг к индексам колонок
            index_mapping = {}
            for key, col_name in mapping.items():
                if col_name:
                    for col_idx, mapped_name in col_name_mapping.items():
                        if mapped_name == col_name:
                            index_mapping[key] = col_idx
                            break
                    
            result['mapping'] = mapping
            result['index_mapping'] = index_mapping
            logger.info(f"Получен маппинг: {mapping}")
            logger.info(f"Индексный маппинг: {index_mapping}")
            
            # ЭТАП 3: Валидация маппинга
            validation = self._validate_mapping_with_regex(df_with_headers, index_mapping, header_row)
            if not validation.get('name_valid', False):
                result['errors'].append("Колонка с названиями не прошла валидацию")
                
            if not validation.get('price_valid', False):
                result['errors'].append("Колонка с ценами не прошла валидацию")
                
            # ЭТАП 4: Извлечение товаров (продолжаем даже при ошибках валидации)
            products = self._extract_products_smart(df_with_headers, index_mapping, header_row)
            result['products'] = products
            result['success'] = len(products) > 0
            
            # Если есть товары - считаем успехом даже при ошибках валидации
            if result['success'] and result['errors']:
                result['errors'] = []  # Очищаем ошибки если товары найдены
            
            logger.info(f"Извлечено {len(products)} товаров")
            
        except Exception as e:
            logger.exception(f"Ошибка при обработке прайс-листа: {e}")
            result['errors'].append(f"Общая ошибка: {str(e)}")
            
        return result
    
    def _find_header_row_smart(self, df: pd.DataFrame) -> int:
        """Умный поиск строки заголовков"""
        header_keywords = ['наимен', 'товар', 'цена', 'кол-во', 'остаток', 'артикул', 'п/п']
        
        best_row = -1
        best_score = 0
        
        # Ищем в первых 20 строках
        search_rows = min(20, len(df))
        
        for idx in range(search_rows):
            row = df.iloc[idx]
            row_strs = [str(cell).lower() for cell in row if pd.notna(cell)]
            
            score = 0
            for cell in row_strs:
                for keyword in header_keywords:
                    if keyword in cell:
                        score += 1
                        break  # Один балл за ячейку максимум
            
            if score > best_score and score >= 2:  # Минимум 2 совпадения
                best_score = score
                best_row = idx
                
        logger.info(f"Лучшая строка заголовков: {best_row} (счет: {best_score})")
        return best_row
    
    def _get_sample_data_rows(self, df: pd.DataFrame, header_row: int, count: int = 5) -> List[List]:
        """Получение образцов строк с данными"""
        start_row = header_row + 1
        end_row = min(start_row + count, len(df))
        
        sample_rows = []
        for idx in range(start_row, end_row):
            if idx < len(df):
                row_data = df.iloc[idx].tolist()
                sample_rows.append(row_data)
                
        return sample_rows
    
    def _get_column_mapping_v2(self, headers: List, sample_rows: List[List], file_name: str = "") -> Optional[Dict]:
        """Улучшенный LLM-маппинг колонок"""
        
        if not self.llm:
            logger.warning("LLM не доступен, используем fallback маппинг")
            return self._fallback_mapping(headers)
            
        try:
            # Создаем детальный контекст
            context = f"ФАЙЛ: {file_name}\n"
            context += f"ЗАГОЛОВКИ: {headers}\n\n"
            context += "ОБРАЗЦЫ ДАННЫХ:\n"
            
            for i, row in enumerate(sample_rows):
                context += f"Строка {i+1}: {row}\n"
            
            prompt = f"""
            Проанализируй структуру прайс-листа и определи маппинг колонок.
            
            {context}
            
            ВАЖНО:
            1. Игнорируй пустые колонки и колонки с номерами п/п
            2. Ищи колонку с полными названиями товаров (не артикулами)
            3. Ищи колонку с ценами (числовые значения)
            4. Ищи колонку с остатками/количеством
            5. Если видишь несколько похожих колонок - выбери самую подходящую
            
            Верни ТОЛЬКО JSON в таком формате:
            {{
                "name_column": "точное название колонки с товарами",
                "price_column": "точное название колонки с ценами", 
                "stock_column": "точное название колонки с остатками",
                "confidence": "high/medium/low"
            }}
            
            Если колонка не найдена - используй null.
            """
            
            response = self.llm.invoke(prompt)
            response_text = response.content.strip()
            logger.info(f"LLM ответ для маппинга: {response_text}")
            
            # Извлекаем JSON
            json_str = self._extract_json_from_response(response_text)
            if json_str:
                mapping = json.loads(json_str)
                
                # Валидируем что колонки существуют
                validated_mapping = {}
                header_set = set(str(h) for h in headers if pd.notna(h))
                
                for key, value in mapping.items():
                    if value and str(value) in header_set:
                        validated_mapping[key] = value
                    else:
                        validated_mapping[key] = None
                        
                logger.info(f"Валидированный маппинг: {validated_mapping}")
                return validated_mapping
                
        except Exception as e:
            logger.error(f"Ошибка LLM маппинга: {e}")
            
        # Fallback
        return self._fallback_mapping(headers)
    
    def _extract_json_from_response(self, response_text: str) -> Optional[str]:
        """Извлечение JSON из ответа LLM"""
        # Ищем JSON в блоках ```json
        match_block = re.search(r"```json\s*([\s\S]*?)\s*```", response_text, re.IGNORECASE)
        if match_block:
            return match_block.group(1).strip()
            
        # Ищем JSON без блоков
        match_plain = re.search(r"^\s*{\s*[\s\S]*?\s*}\s*$", response_text)
        if match_plain:
            return response_text.strip()
            
        # Ищем любой JSON объект
        start = response_text.find('{')
        end = response_text.rfind('}')
        if start != -1 and end != -1 and end > start:
            return response_text[start:end+1]
            
        return None
    
    def _fallback_mapping(self, headers: List) -> Dict:
        """Улучшенный fallback маппинг по ключевым словам"""
        mapping = {
            'name_column': None,
            'price_column': None,
            'stock_column': None,
            'confidence': 'low'
        }
        
        # Фильтруем пустые заголовки и приводим к нижнему регистру
        clean_headers = []
        original_headers = []
        
        for h in headers:
            if pd.notna(h) and str(h).strip():
                clean_headers.append(str(h).lower().strip())
                original_headers.append(str(h).strip())
        
        logger.info(f"Заголовки для fallback: {original_headers}")
        
        # Ищем название товара - более широкий поиск
        name_keywords = ['наимен', 'товар', 'номенклатур', 'продукц', 'изделие']
        for i, header in enumerate(clean_headers):
            # Исключаем колонки с номерами
            if any(word in header for word in ['п/п', 'номер', '№']):
                continue
            if any(word in header for word in name_keywords):
                mapping['name_column'] = original_headers[i]
                break
        
        # Если не нашли по ключевым словам, ищем самую длинную непустую колонку
        if not mapping['name_column']:
            logger.info("Ищем колонку с названиями по длине заголовков...")
            max_len = 0
            best_idx = -1
            for i, header in enumerate(clean_headers):
                # Исключаем служебные колонки
                if any(word in header for word in ['п/п', 'номер', '№', 'цена', 'кол-во', 'остат']):
                    continue
                if len(header) > max_len:
                    max_len = len(header)
                    best_idx = i
            if best_idx >= 0:
                mapping['name_column'] = original_headers[best_idx]
                logger.info(f"Выбрана колонка по длине: {mapping['name_column']}")
                
        # Ищем цену
        price_keywords = ['цена', 'стоимост', 'руб', 'price']
        for i, header in enumerate(clean_headers):
            if any(word in header for word in price_keywords):
                mapping['price_column'] = original_headers[i]
                break
                
        # Ищем остаток
        stock_keywords = ['остат', 'кол-во', 'налич', 'склад', 'количеств']
        for i, header in enumerate(clean_headers):
            if any(word in header for word in stock_keywords):
                mapping['stock_column'] = original_headers[i]
                break
                
        logger.info(f"Fallback маппинг: {mapping}")
        return mapping
    
    def _validate_mapping_with_regex(self, df: pd.DataFrame, mapping: Dict, header_row_idx: int) -> Dict:
        """Валидация маппинга через regex анализ данных"""
        validation_results = {}
        
        try:
            # Проверяем колонку с названиями
            if mapping.get('name_column'):
                name_col = mapping['name_column']
                # Проверяем что колонка существует в DataFrame
                if name_col not in df.columns:
                    logger.error(f"Колонка '{name_col}' не найдена в DataFrame. Доступные: {list(df.columns)}")
                    validation_results['name_valid'] = False
                else:
                    sample_data = df[name_col].iloc[header_row_idx+1:header_row_idx+20]
                    
                    product_patterns = [
                        r'фланец|отвод|тройник|задвижка|редуктор|клапан',
                        r'ду\s*\d+|ру\s*\d+|дн\s*\d+',
                        r'гост\s*\d+|ост\s*\d+',
                        r'ст\.\s*\d+|сталь',
                    ]
                    
                    product_matches = 0
                    total_valid = 0
                    
                    for value in sample_data.dropna():
                        value_str = str(value).lower()
                        if len(value_str.strip()) > 5:  # Минимальная длина
                            total_valid += 1
                            if any(re.search(pattern, value_str) for pattern in product_patterns):
                                product_matches += 1
                    
                    validation_results['name_valid'] = (
                        total_valid > 0 and 
                        (product_matches / total_valid > 0.2 or product_matches >= 2)
                    )
                    validation_results['name_score'] = product_matches / total_valid if total_valid > 0 else 0
                
            # Проверяем колонку с ценами
            if mapping.get('price_column'):
                price_col = mapping['price_column']
                sample_data = df[price_col].iloc[header_row_idx+1:header_row_idx+20]
                
                numeric_count = 0
                total_count = 0
                
                for value in sample_data.dropna():
                    value_str = str(value).strip()
                    if value_str:
                        total_count += 1
                        # Ищем числа в строке
                        price_match = re.search(r'[\d\s\,\.]+', value_str)
                        if price_match:
                            try:
                                price_num = float(price_match.group().replace(',', '.').replace(' ', ''))
                                if price_num > 0:
                                    numeric_count += 1
                            except:
                                pass
                
                validation_results['price_valid'] = (
                    total_count > 0 and 
                    (numeric_count / total_count > 0.3 or numeric_count >= 3)
                )
                validation_results['price_score'] = numeric_count / total_count if total_count > 0 else 0
                
        except Exception as e:
            logger.error(f"Ошибка валидации маппинга: {e}")
            validation_results = {'name_valid': False, 'price_valid': False}
            
        logger.info(f"Результаты валидации: {validation_results}")
        return validation_results
    
    def _extract_products_smart(self, df: pd.DataFrame, mapping: Dict, header_row: int) -> List[Dict]:
        """Умное извлечение товаров из таблицы"""
        products = []
        
        if not mapping.get('name_column'):
            logger.error("Нет маппинга для колонки с названиями")
            return products
            
        name_col = mapping['name_column']
        price_col = mapping.get('price_column')
        stock_col = mapping.get('stock_column')
        
        # Обрабатываем строки после заголовков
        start_row = header_row + 1
        
        for idx in range(start_row, len(df)):
            row = df.iloc[idx]
            
            # Получаем название
            name_value = str(row.get(name_col, '')).strip()
            if not name_value or name_value == 'nan':
                continue
                
            # Фильтрация мусора
            if self._is_junk_row(name_value):
                continue
                
            # Проверка на товарную строку
            if not self._is_product_row(name_value):
                continue
                
            # Извлекаем цену
            price = 0.0
            if price_col:
                price_raw = str(row.get(price_col, '')).strip()
                if price_raw and price_raw != 'nan':
                    price_match = re.search(r'[\d\s\,\.]+', price_raw.replace(' ', ''))
                    if price_match:
                        try:
                            price = float(price_match.group().replace(',', '.'))
                        except:
                            price = 0.0
                            
            # Извлекаем остаток
            stock = 100  # по умолчанию
            if stock_col:
                stock_raw = str(row.get(stock_col, '')).strip()
                if stock_raw and stock_raw != 'nan':
                    stock_match = re.search(r'\d+', stock_raw)
                    if stock_match:
                        try:
                            stock = int(stock_match.group())
                        except:
                            stock = 100
                            
            products.append({
                'name': name_value,
                'price': price,
                'stock': stock,
                'supplier': ''  # Заполнится в views
            })
            
        return products
    
    def _is_product_row(self, name_value: str) -> bool:
        """Проверка что строка содержит товар"""
        name_lower = name_value.lower()
        
        # Минимальная длина
        if len(name_value.strip()) < 5:
            return False
            
        # Позитивные признаки товара
        product_indicators = [
            r'фланец|отвод|тройник|задвижка|редуктор|клапан|муфта|переход',
            r'ду\s*\d+|ру\s*\d+|дн\s*\d+',
            r'гост\s*\d+|ост\s*\d+|тм\s*\d+',
            r'ст\.\s*\d+|сталь',
            r'\d+\*\d+|\d+х\d+',  # Размеры
            r'исп\.\s*[а-я]',  # Исполнение
        ]
        
        return any(re.search(pattern, name_lower) for pattern in product_indicators)
    
    def _is_junk_row(self, name_value: str) -> bool:
        """Проверка что строка - мусор"""
        name_lower = name_value.lower().strip()
        
        # Негативные признаки
        junk_patterns = [
            r'^[\d\s\-\.\,\(\)]+$',  # Только цифры и знаки
            r'^[а-я\s]{1,3}$',  # Слишком короткие слова
            r'итого|всего|сумма|подпись|печать|директор|менеджер',
            r'тел\.|факс|@|www\.|http',
            r'ул\.|пр\.|д\.|кв\.|офис|этаж',
            r'наименование|товар|цена|остаток|артикул',  # Заголовки
        ]
        
        return any(re.search(pattern, name_lower) for pattern in junk_patterns) 