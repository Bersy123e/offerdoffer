import re
import json
from typing import Dict, List, Optional, Any
import logging
import httpx
import time
from langchain_openai import ChatOpenAI
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
import os
print("HTTP_PROXY:", os.environ.get("HTTP_PROXY"))
print("HTTPS_PROXY:", os.environ.get("HTTPS_PROXY"))
import sqlite3
from functools import reduce
import pandas as pd

from cache import QueryCache
from logger import setup_logger

logger = setup_logger()

def extract_quantity(text: Optional[str]) -> Optional[int]:
    """Извлекает количество из текста (например, '5 штук')"""
    if not text:
        return None
    # Ищем число, за которым опционально идет пробел и "шт"/"штук"/"компл"
    match = re.search(r'(\d+)\s*(?:шт|штук|компл)\b', text, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    # Если не нашли с "шт", ищем просто последнее число в строке
    numbers = re.findall(r'\d+', text)
    if numbers:
        try:
            return int(numbers[-1])
        except ValueError:
            return None
    return None

def normalize_dimensions(text: str) -> str:
    """
    Приводит размеры вида '57х5', '57 х 5', '57*5', '57 x 5', '57X5' к единому виду '57x5'.
    Заменяет все варианты 'x', 'х', '*', с пробелами и без, на 'x' без пробелов.
    """
    if not text:
        return text
    # Заменяем кириллическую 'х' и латинскую 'x' и '*' на 'x', убираем пробелы вокруг
    return re.sub(r'(\d+)\s*[xх*]\s*(\d+)', r'\1x\2', text, flags=re.IGNORECASE)

class QueryProcessor:
    def __init__(self, data_loader, query_cache: QueryCache):
        """
        Initialize QueryProcessor with data loader and cache.
        Args:
            data_loader: DataLoader instance for database access
            query_cache: QueryCache instance for caching query results
        """
        self.data_loader = data_loader
        self.query_cache = query_cache
        # Используем gpt-4o-mini для экономии и скорости
        self.llm_model_name = "gpt-4o-mini"
        self._initialize_llm()
    
    def _initialize_llm(self):
        """Initialize LangChain with OpenAI."""
        try:
            api_key = os.environ.get("OPENAI_API_KEY", "")
            if not api_key:
                logger.error("OPENAI_API_KEY not found in environment variables.")
                raise ValueError("OPENAI_API_KEY is not set")
                
            proxy_url = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")
            http_client_args = {}
            if proxy_url:
                logger.info(f"Using proxy: {proxy_url}")
                proxies = {"http://": proxy_url, "https://": proxy_url}
                http_client_args['proxies'] = proxies
            
            # Создаем httpx.Client с настройками прокси, если они есть
            http_client = httpx.Client(**http_client_args)
            
            # Передаем http_client в ChatOpenAI
            self.llm = ChatOpenAI(
                model_name=self.llm_model_name, 
                openai_api_key=api_key, 
                temperature=0, # Низкая температура для точности
                http_client=http_client
            )
            logger.info(f"LLM ({self.llm_model_name}) initialized successfully.")
            
            # Оставляем split_chain и prompt для разделения запросов
            # (хотя возможно стоит переделать на новые RunnableSequence)
            self.split_prompt = PromptTemplate(
                input_variables=["query"],
                template="""
                ЗАДАЧА: Разделить общий запрос клиента на отдельные товарные позиции.
                ПРАВИЛА:
                1. Для КАЖДОЙ позиции извлечь описание товара (item_query) и запрошенное количество (quantity).
                2. Игнорировать общие фразы, приветствия, предлоги.
                3. Если количество не указано явно, использовать null или 1.
                4. Формат ответа - ТОЛЬКО валидный JSON-массив объектов. Каждый объект должен иметь ключи "item_query" (строка) и "quantity" (число или null).
                
                ПРИМЕРЫ:
                Запрос: "Добрый день! Нужен редуктор тип В 5 штук и еще задвижка ДУ500 10 шт"
                Ответ: [{{"item_query": "редуктор тип В", "quantity": 5}}, {{"item_query": "задвижка ДУ500", "quantity": 10}}]
                Запрос: "отводы стальные 90 градусов гост 17375 10 штук"
                Ответ: [{{"item_query": "отводы стальные 90 градусов гост 17375", "quantity": 10}}]
                Запрос: "фланец плоский ст.20"
                Ответ: [{{"item_query": "фланец плоский ст.20", "quantity": null}}]
                
                Запрос: {{query}}
                Ответ:
                """ # Используем двойные фигурные скобки для literal braces
            )
            # TODO: Переделать LLMChain на новый синтаксис prompt | llm
            self.split_chain = LLMChain(llm=self.llm, prompt=self.split_prompt)
            
        except ValueError as ve:
             logger.error(f"Value error during LLM initialization: {ve}")
             self.llm = None # Устанавливаем в None, чтобы проверить позже
        except Exception as e:
            logger.exception(f"Failed to initialize LLM: {e}")
            self.llm = None # Устанавливаем в None, чтобы проверить позже
            
    def _fallback_keyword_mapping(self, headers: list) -> Optional[dict]:
        """
        Пытается определить маппинг по ключевым словам в заголовках.
        """
        mapping = {
            "name": None,
            "price": None,
            "stock": None,
            "article": None
        }
        
        header_keywords = {
            "name": ['наимен', 'товар', 'продукт', 'описан', 'позиц', 'name', 'product', 'item', 'description'],
            "price": ['цена', 'стоим', 'прайс', 'price', 'cost', 'value'],
            "stock": ['кол-во', 'остат', 'наличие', 'склад', 'баланс', 'stock', 'quantity', 'qty', 'amount', 'balance', 'available'],
            "article": ['артикул', 'код', 'sku', 'id', 'номер', 'article', 'code']
        }
        
        assigned_headers = set()

        # Сначала ищем точные совпадения или приоритетные слова
        for standard_name, keywords in header_keywords.items():
            best_match = None
            for header in headers:
                if header is None or header in assigned_headers:
                    continue
                header_lower = str(header).lower().strip()
                # Ищем полное совпадение или слово целиком
                if any(f'\b{kw}\b' in header_lower for kw in keywords) or header_lower in keywords:
                     # Простое совпадение по приоритетным словам
                     if standard_name == "price" and "цена" in header_lower:
                          best_match = header
                          break # Нашли "цена", больше не ищем цену
                     if standard_name == "name" and ("наимен" in header_lower or "товар" in header_lower):
                          best_match = header
                          break # Нашли "наименование" или "товар"
                     if standard_name == "stock" and ("кол-во" in header_lower or "остат" in header_lower or "наличи" in header_lower):
                           best_match = header
                           break # Нашли количество/остаток/наличие
                     if standard_name == "article" and ("артикул" in header_lower or "код" in header_lower or "\bsku\b" in header_lower):
                           best_match = header
                           break # Нашли артикул/код/sku
                     # Если нет приоритетных, запоминаем первое совпадение
                     if best_match is None:
                         best_match = header

            if best_match:
                mapping[standard_name] = best_match
                assigned_headers.add(best_match)
                
        # Если что-то не нашли, пробуем найти по частичному совпадению (менее надежно)
        for standard_name, keywords in header_keywords.items():
             if mapping[standard_name] is None: # Ищем только для ненайденных
                 best_match = None
                 for header in headers:
                     if header is None or header in assigned_headers:
                         continue
                     header_lower = str(header).lower().strip()
                     if any(kw in header_lower for kw in keywords):
                         best_match = header # Берем первое попавшееся не занятое
                         break
                 if best_match:
                     mapping[standard_name] = best_match
                     assigned_headers.add(best_match)

        # Проверяем, найдены ли хотя бы имя и цена
        if not mapping.get("name") or not mapping.get("price"):
            logger.warning(f"Fallback mapping failed to find essential fields 'name' or 'price'. Headers: {headers}")
            return None
            
        logger.info(f"Fallback keyword mapping successful: {mapping}")
        return mapping

    def get_column_mapping(self, header_row: list, sample_rows: list[list]) -> Optional[dict]:
        """
        Uses LLM or fallback keyword matching to determine the column mapping.
        Returns mapping dict or None if both methods fail.
        """
        # --- Попытка 1: LLM ---
        if self.llm:
            # Преобразуем данные в строку для промпта
            header_str = "\t".join(map(str, header_row))
            sample_rows_str = "\n".join(["\t".join(map(str, row)) for row in sample_rows])
            file_fragment = f"Headers:\n{header_str}\n\nSample data:\n{sample_rows_str}"
            
            # Определяем стандартные поля, которые ищем
            standard_fields = { # Ключ: Описание для LLM
                "name": "Наименование товара (полное название, марка, тип, ГОСТ, характеристики)",
                "price": "Цена (розничная, оптовая, с НДС или без - любая цена)",
                "stock": "Остаток на складе (количество, наличие, 'в наличии', 'под заказ')",
                "article": "Артикул (код товара, SKU, номенклатурный номер)",
                 # Добавь сюда другие важные поля, если они есть
                 # "unit": "Единица измерения (шт, кг, м, т)"
            }
            
            field_descriptions = "\n".join([f"- {k}: {v}" for k, v in standard_fields.items()])
            
            prompt = f"""
            ЗАДАЧА: Проанализируй фрагмент прайс-листа (заголовки и первые строки с данными) и определи, какие колонки соответствуют нужным нам стандартным полям.
            Стандартные поля, которые мы ищем:
            {field_descriptions}

            ФРАГМЕНТ ПРАЙС-ЛИСТА:
            {file_fragment}

            ТРЕБОВАНИЯ К ОТВЕТУ:
            1. Верни ТОЛЬКО валидный JSON-объект.
            2. Ключи JSON-объекта - это наши стандартные имена полей ({', '.join(standard_fields.keys())}).
            3. Значения JSON-объекта - это ТОЧНЫЕ названия колонок из ФРАГМЕНТА ПРАЙС-ЛИСТА.
            4. Если для стандартного поля не нашлось подходящей колонки, используй значение null (не строку "null").
            5. Если для одного стандартного поля подходят НЕСКОЛЬКО колонок, выбери САМУЮ ПОДХОДЯЩУЮ (например, для 'price' выбери розничную цену, если есть и оптовая).
            6. НЕ ПРИДУМЫВАЙ колонки, которых нет в заголовках.

            ПРИМЕР ОТВЕТА:
            {{
              "name": "Наименование товара",
              "price": "Цена розн.",
              "stock": "Остаток",
              "article": "Артикул"
            }}
            ИЛИ (если что-то не найдено):
            {{
              "name": "Номенклатура",
              "price": "Стоимость",
              "stock": null,
              "article": "Код"
            }}

            JSON-ОТВЕТ:
            """
            
            logger.info(f"Sending request to LLM ({self.llm_model_name}) for column mapping. Header: {header_str}")
            response = self.llm.invoke(prompt)
            response_text = response.content.strip()
            logger.info(f"LLM response for mapping: {response_text}")
            
            # Пытаемся извлечь JSON из ответа
            json_str = None
            match_block = re.search(r"```json\s*([\s\S]*?)\s*```", response_text, re.IGNORECASE)
            match_plain = re.search(r"^\s*{\s*[\s\S]*?\s*}\s*$", response_text) # Если JSON без ```
            
            if match_block:
                json_str = match_block.group(1).strip()
            elif match_plain:
                 json_str = response_text
            else:
                 logger.warning("Could not find JSON block in LLM response for mapping.")
                 # Попытка найти JSON хоть как-то (менее надежно)
                 start_index = response_text.find('{')
                 end_index = response_text.rfind('}')
                 if start_index != -1 and end_index != -1 and end_index > start_index:
                      json_str = response_text[start_index:end_index+1]
            
            if json_str:
                try:
                    mapping = json.loads(json_str)
                    validated_mapping = {}
                    valid_headers = set(str(h) for h in header_row if h is not None) # Приводим к строке
                    
                    # Сначала заполняем тем, что вернул LLM
                    for standard_name, file_header in mapping.items():
                         if standard_name in standard_fields:
                             file_header_str = str(file_header) if file_header is not None else None
                             if file_header_str is None:
                                 validated_mapping[standard_name] = None
                             elif file_header_str in valid_headers:
                                 validated_mapping[standard_name] = file_header # Сохраняем исходное значение
                             else:
                                 logger.warning(f"LLM returned header '{file_header}' for '{standard_name}' which is not in the original headers {list(valid_headers)}. Ignoring.")
                                 validated_mapping[standard_name] = None
                         else:
                             logger.warning(f"LLM returned unknown standard field '{standard_name}'. Ignoring.")
                             
                    # Добавляем стандартные поля, которые LLM мог пропустить
                    for field in standard_fields:
                        if field not in validated_mapping:
                            validated_mapping[field] = None
                            
                    # Проверяем, найдены ли хотя бы имя и цена
                    if validated_mapping.get("name") and validated_mapping.get("price"):
                        logger.info(f"Successfully obtained and validated column mapping from LLM: {validated_mapping}")
                        return validated_mapping
                    else:
                        logger.warning("LLM failed to map essential fields 'name' or 'price'. Trying fallback.")
                        # LLM не справился, дальше попробуем fallback

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode JSON from LLM response for mapping: {e}. Trying fallback.")
                    # Ошибка JSON, дальше попробуем fallback
            else:
                logger.error("Could not extract JSON string from LLM response for mapping. Trying fallback.")
                # Нет JSON, дальше попробуем fallback
            
        # --- Попытка 2: Fallback на ключевые слова ---
        logger.info("Attempting fallback keyword mapping.")
        fallback_mapping = self._fallback_keyword_mapping(header_row)
        
        if fallback_mapping:
            return fallback_mapping
        else:
            # Обе попытки провалились
            logger.error("Both LLM and fallback keyword mapping failed.")
            return None

    def process_query(self, query: str) -> List[Dict]:
        """
        Process a natural language query and return ALL matching products (OR search, без скоринга).
        """
        try:
            logger.info(f"Processing query: {query}")
            query = normalize_dimensions(query)
            prompt = f"""
            ЗАДАЧА: Извлечь из текста ТОЧНЫЕ ключевые слова для поиска товара в базе по полю 'name'.
            ПРАВИЛА:
            1. Извлекай характеристики МАКСИМАЛЬНО ПОЛНО и ТОЧНО как они есть в тексте (например, "тип В", "ст.20", "ГОСТ 17375-2001", "108*6", "ДУ400", "РУ16"). НЕ разбивай их на части (НЕ надо "тип" и "В" отдельно).
            2. НЕ включай количество (штук, шт, компл и т.д.) и связанные с ним числа.
            3. НЕ включай единицы измерения (мм, кг, гр и т.п.), если они не являются частью ГОСТа или маркировки.
            4. НЕ включай общие слова и предлоги ("нужен", "в", "количестве", "для", "под", "и", "с", "еще").
            5. Формат ответа - ТОЛЬКО валидный JSON-массив строк. Никакого лишнего текста.
            
            ПРИМЕРЫ:
            Запрос: "Отвод ГОСТ17375-2001 108*6 ст.20 90гр. 2000 штук"
            Ответ: ["Отвод", "ГОСТ 17375-2001", "108*6", "ст.20", "90гр"]
            Запрос: "редуктор тип В нужен в количестве 5 штук"
            Ответ: ["редуктор", "тип В"]
            Запрос: "Задвижка под привод 30с964нж ДУ300 РУ25 (тип Б)"
            Ответ: ["Задвижка", "под привод", "30с964нж", "ДУ300", "РУ25", "тип Б"]
            Запрос: "Фланцы плоские ст.20 исп.В Ду 25"
            Ответ: ["Фланцы", "плоские", "ст.20", "исп.В", "Ду 25"]
            Запрос: "Редуктор Г 10 шт"
            Ответ: ["Редуктор", "Г"]
            
            Запрос: {query}
            Ответ:
            """
            response = self.llm.invoke(prompt)
            text = response.content.strip()
            keywords = []
            try:
                keywords = json.loads(text)
            except Exception:
                match = re.search(r'\[.*\]', text, re.DOTALL)
                if match:
                    try:
                        keywords = json.loads(match.group(0))
                    except Exception:
                         logger.warning(f"Could not parse JSON from LLM response: {text}")
                         keywords = [kw.strip() for kw in query.split() if kw.strip()]
                else:
                    logger.warning(f"Could not find JSON in LLM response: {text}")
                    keywords = [kw.strip() for kw in query.split() if kw.strip()]
            keywords = [kw for kw in keywords if kw and isinstance(kw, str)]
            if not keywords:
                logger.warning(f"LLM returned empty keywords for query '{query}'. Falling back to splitting query text.")
                cleaned_query = re.sub(r'\d+\s*(?:шт|штук|компл)\b', '', query, flags=re.IGNORECASE).strip()
                cleaned_query = re.sub(r'\b\d+\s*$', '', cleaned_query).strip()
                keywords = [kw.strip() for kw in cleaned_query.split() if kw.strip() and kw.lower() not in ["нужен", "в", "количестве", "для", "под", "и", "с", "еще", "шт", "штук", "компл"]]
                if not keywords:
                    keywords = [query] if query else []
            logger.info(f"Using keywords: {keywords}")
            if keywords:
                # Создаем SQL запрос для поиска по ключевым словам
                query = "SELECT * FROM products WHERE "
                conditions = []
                params = []
                
                for keyword in keywords:
                    conditions.append("name LIKE ?")
                    params.append(f"%{keyword}%")
                
                query += " OR ".join(conditions)
                
                import sqlite3
                with sqlite3.connect(self.data_loader.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    columns = [col[0] for col in cursor.description]
                    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                logger.info(f"Generated SQL query (OR search): {query}")
                logger.info(f"Found {len(results)} products by OR search. Returning all.")
                return results
            else:
                logger.info("No keywords found, returning empty list")
                return []
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            raise
    
    def split_query_into_items(self, full_query: str) -> List[Dict[str, Any]]:
        """
        Splits a full query text into individual item queries and quantities using LLM.
        """
        full_query = normalize_dimensions(full_query)
        items = []
        try:
            logger.info(f"Attempting to split query using LLM: {full_query[:100]}...")
            response = self.split_chain.invoke({"query": full_query})
            raw_llm_response = response['text'].strip()
            logger.info(f"RAW LLM response for splitting: {raw_llm_response}") # Логируем сырой ответ
            json_match = re.search(r'\[\s\S*\]', raw_llm_response)
            if json_match:
                json_str = json_match.group(0)
                logger.info(f"Found JSON block: {json_str[:200]}...")
                try:
                    items = json.loads(json_str)
                    if not isinstance(items, list):
                         logger.error("LLM split response is not a list.")
                         items = []
                    else:
                         valid_items = []
                         for item in items:
                              if isinstance(item, dict) and 'item_query' in item and 'quantity' in item:
                                   valid_items.append(item)
                              else:
                                   logger.warning(f"Invalid item structure ignored: {item}")
                         items = valid_items
                         if items:
                              logger.info(f"Successfully parsed {len(items)} items from LLM JSON.")
                         else:
                              logger.warning("Parsed JSON was list, but contained no valid items.")
                except json.JSONDecodeError as json_err:
                    logger.error(f"JSONDecodeError parsing LLM split response: {json_err}. JSON string: {json_str[:200]}...")
                    items = []
            else:
                logger.warning("JSON array block not found in LLM split response.")
                items = []
        except Exception as e:
            logger.exception(f"Error during LLM split query execution: {e}")
            items = []
        if not items:
            logger.warning("LLM split failed or returned no valid items. Trying fallback splitting by lines/separators.")
            lines = []
            if '---' in full_query:
                lines = [line.strip() for line in full_query.split('---') if line.strip()]
            elif '\n' in full_query: # Prefer newline splitting if available
                 lines = [line.strip() for line in full_query.splitlines() if line.strip()]
            else:
                # If no clear separators, treat as single item (or potentially split by common phrases if needed later)
                lines = [full_query.strip()] if full_query.strip() else []

            if len(lines) > 0: # Process lines if any exist
                 logger.info(f"Fallback: Split query into {len(lines)} potential items based on separators/lines.")
                 processed_items = []
                 for line in lines:
                      quantity = extract_quantity(line) 
                      # --- Improved quantity removal for item_query ---
                      # 1. Remove the quantity pattern first
                      item_query_text = re.sub(r'\d+\s*(?:шт|штук|компл)\b', '', line, flags=re.IGNORECASE).strip()
                      # 2. Remove any remaining standalone number at the end (likely quantity if pattern missed)
                      item_query_text = re.sub(r'\s+\d+\s*$', '', item_query_text).strip() 
                      # --- End Improved removal ---
                      if item_query_text:
                           processed_items.append({"item_query": item_query_text, "quantity": quantity})
                      else:
                           logger.warning(f"Fallback: Line '{line}' became empty after removing quantity, skipping.")
                 
                 if processed_items:
                      items = processed_items
                      logger.info(f"Fallback generated items: {items}")
                 else:
                      logger.error("Fallback: No valid items could be generated from lines.")
                      items = [] # Ensure items is empty list if nothing generated
            else:
                 logger.error("Fallback: Query was empty or contained no processable lines.")
                 items = [] # Ensure items is empty list

        return items

    def extract_products_from_table(self, table_rows: list) -> list:
        logger.info("extract_products_from_table CALLED")
        prompt = """
            ЗАДАЧА: Извлечь данные о товарах из строк таблицы прайс-листа. КАЖДАЯ строка (даже если не похожа на товар) должна быть отражена в результате!
            ПРАВИЛА:
            1. Для КАЖДОЙ строки вернуть JSON-объект с полями: 'supplier', 'name', 'price', 'stock'. Если поле не найдено — ставь null.
            2. supplier: Наименование поставщика (если есть столбец, иначе пусто).
            3. name: ПОЛНОЕ наименование товара из соответствующего столбца. Если был заголовок — добавь его в начало. Если не найдено — null.
            4. price: Цена товара. Ищи любые столбцы с ценой ('Цена', 'Price', 'Стоимость', 'Цена руб', 'Цена с НДС'). Извлекай только число (убирай валюту, 'руб', 'тг' и т.д.). Если не найдено — null.
            5. stock: Остаток товара на складе. Если не найдено — ставь 100.
            6. ВКЛЮЧАЙ даже строки без цены и названия (пусть будут с null).
            7. ФОРМАТ: Верни ТОЛЬКО валидный JSON-массив объектов. Без текста до или после, без ```json ... ```.
            ПРИМЕР СТРОКИ ИЗ ТАБЛИЦЫ:
            {'Наименование изделия': 'Редуктор тип Б', 'Ду (мм)': '50', 'Цена руб. Ру16': '17000', 'Остаток шт': 4}
            ОЖИДАЕМЫЙ JSON ОБЪЕКТ (если не было заголовка):
            {\"supplier\": \"\", \"name\": \"Редуктор тип Б\", \"price\": 17000, \"stock\": 4}
            Таблица строк (JSON): {row}
            Ответ (ТОЛЬКО JSON-массив):
        """
        results = []
        batch_size = 50 # Можно уменьшить для сложных таблиц
        current_header = "" # Для хранения последнего заголовка
        for i in range(0, len(table_rows), batch_size):
            batch = table_rows[i:i+batch_size]
            processed_batch = []
            for row_dict in batch:
                filled_values = [v for v in row_dict.values() if pd.notna(v) and str(v).strip()]
                potential_price = str(row_dict.get('price', '') or row_dict.get('Цена', '') or row_dict.get('Цена руб', '')).strip()
                potential_stock = str(row_dict.get('stock', '') or row_dict.get('Остаток', '') or row_dict.get('Кол-во', '')).strip()
                is_likely_header = len(filled_values) < 3 and not potential_price and not potential_stock and len(filled_values) > 0
                if is_likely_header:
                    current_header = str(filled_values[0]).strip()
                    processed_batch.append({"is_header": True, "header_text": current_header})
                else:
                    row_dict["_context_header"] = current_header
                    processed_batch.append(row_dict)
            batch_text = json.dumps(processed_batch, ensure_ascii=False)
            full_prompt = prompt.replace('{row}', batch_text)
            logger.info(f"LLM PROMPT (extract_products_from_table): {full_prompt}")
            response = self.llm.invoke(full_prompt)
            text = response.content
            logger.info(f"LLM RAW RESPONSE (extract_products_from_table): {text}")
            json_str = None
            match = re.search(r"```json\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
            if match:
                json_str = match.group(1).strip()
            else:
                first_bracket = text.find('[')
                if first_bracket != -1:
                   brace_level = 0
                   end_index = -1
                   in_string = False
                   for idx, char in enumerate(text[first_bracket:]):
                       if char == '"' and (idx == 0 or text[first_bracket+idx-1] != '\\'):
                           in_string = not in_string
                       elif not in_string:
                           if char == '[' or char == '{':
                               brace_level += 1
                           elif char == ']' or char == '}':
                               brace_level -= 1
                       if brace_level == 0 and char == ']':
                           end_index = first_bracket + idx + 1
                           break
                   if end_index != -1:
                       json_str = text[first_bracket:end_index]
                   else:
                       json_str = text[first_bracket:]
                else: 
                    json_str = None
            if json_str:
                try:
                    batch_result = json.loads(json_str)
                    if not batch_result or not isinstance(batch_result, list):
                        logger.error(f"LLM batch_result is empty or not a list! batch_result={batch_result}, batch={batch_text}")
                    if isinstance(batch_result, list):
                        cleaned_batch_result = []
                        for item in batch_result:
                            if isinstance(item, dict):
                                item.pop('_context_header', None)
                                name = item.get('name')
                                price = item.get('price')
                                if price is not None and not isinstance(price, (int, float)):
                                    price_str = str(price)
                                    price_num = re.findall(r"[\d\.\,]+", price_str)
                                    if price_num:
                                        try:
                                            price = float(price_num[0].replace(',', '.'))
                                        except Exception:
                                            price = 0
                                    else:
                                        price = 0
                                if name is None and price is None:
                                    continue # Совсем пустая строка — пропускаем
                                if price is None:
                                    price = 0
                                stock = item.get('stock')
                                try:
                                    stock_val = int(stock) if stock is not None else 100
                                except Exception:
                                    stock_val = 100
                                item['name'] = name if name is not None else ''
                                item['price'] = price
                                item['stock'] = stock_val
                                cleaned_batch_result.append(item)
                        results.extend(cleaned_batch_result)
                    else:
                        logger.warning(f"LLM returned non-list JSON: {json_str[:500]}...")
                except json.JSONDecodeError as json_err:
                    logger.error(f"JSONDecodeError parsing LLM response: {json_err}. String: {json_str[:500]}... Batch: {batch_text}")
            else:
                logger.error(f"Could not extract JSON block from LLM response: {text[:500]}... Batch: {batch_text}")
            time.sleep(1)
        return results 