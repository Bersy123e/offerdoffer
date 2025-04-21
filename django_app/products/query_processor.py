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
from django.db.models import Q
import operator
from functools import reduce
import pandas as pd

from .cache import QueryCache
from logger import setup_logger
from .models import Product

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

class QueryProcessor:
    def __init__(self, query_cache: QueryCache):
        """
        Initialize QueryProcessor with cache.
        Args:
            query_cache: QueryCache instance for caching query results
        """
        self.query_cache = query_cache
        self._initialize_llm()
    
    def _initialize_llm(self):
        """Initialize LangChain with OpenAI."""
        try:
            api_key = os.environ.get("OPENAI_API_KEY", "")
            proxy_url = "http://niM1Bv1s:tbrA9EWJ@172.120.17.109:64192"
            proxies = {"http://": proxy_url, "https://": proxy_url}
            http_client = httpx.Client(proxies=proxies)
            
            # ОСТАВЛЯЕМ ТОЛЬКО ПРОМПТ ДЛЯ РАЗДЕЛЕНИЯ
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
            
            # Инициализируем LLM
            self.llm = ChatOpenAI(
                temperature=0, 
                model_name="gpt-4o-mini",
                openai_api_key=api_key,
                http_client=http_client
            )
            
            # ОСТАВЛЯЕМ ТОЛЬКО ЦЕПОЧКУ ДЛЯ РАЗДЕЛЕНИЯ
            self.split_chain = LLMChain(llm=self.llm, prompt=self.split_prompt)
            
            logger.info("LLM initialized successfully (only split_chain is active)")
            
        except Exception as e:
            logger.error(f"Error initializing LLM: {str(e)}")
            raise
    
    def process_query(self, query: str) -> List[Product]:
        """
        Process a natural language query and return matching products.
        1. Search for products containing ANY keywords (OR search).
        2. Score each found product based on keyword matches (specific keywords get more weight).
        3. Give a large BONUS score if ALL specific keywords are present.
        4. Filter results: prioritize bonus score, otherwise take max normal score.
        CACHE IS TEMPORARILY DISABLED FOR DEBUGGING.
        """
        try:
            logger.info(f"Processing query: {query}")
            # КЭШ ВРЕМЕННО ОТКЛЮЧЕН
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
                         # Fallback if JSON parsing fails after finding brackets
                         keywords = [kw.strip() for kw in query.split() if kw.strip()]
                else:
                    logger.warning(f"Could not find JSON in LLM response: {text}")
                    # Fallback if no JSON detected
                    keywords = [kw.strip() for kw in query.split() if kw.strip()]
            
            keywords = [kw for kw in keywords if kw and isinstance(kw, str)]

            # --- Fallback if LLM returns empty keywords ---
            if not keywords:
                logger.warning(f"LLM returned empty keywords for query '{query}'. Falling back to splitting query text.")
                # Attempt to remove quantity/units again, just in case
                cleaned_query = re.sub(r'\d+\s*(?:шт|штук|компл)\b', '', query, flags=re.IGNORECASE).strip()
                cleaned_query = re.sub(r'\b\d+\s*$', '', cleaned_query).strip() # Remove trailing number if it's likely quantity
                # Split remaining text into words as keywords
                keywords = [kw.strip() for kw in cleaned_query.split() if kw.strip() and kw.lower() not in ["нужен", "в", "количестве", "для", "под", "и", "с", "еще", "шт", "штук", "компл"]]
                if keywords:
                     logger.info(f"Using fallback keywords: {keywords}")
                else:
                     logger.warning("Fallback keyword generation also resulted in empty list.")
                     # As a last resort, maybe use the original query? Or return empty? Let's use original for now.
                     keywords = [query] if query else []
                     logger.info(f"Using original query as last resort keyword: {keywords}")
            # --- End Fallback ---

            logger.info(f"Using keywords: {keywords}")
            generic_keywords_set = {'редуктор', 'отвод', 'задвижка', 'фланец', 'переход', 'тройник', 'заглушка'}
            specific_keywords = [kw for kw in keywords if kw.lower() not in generic_keywords_set]
            logger.info(f"Specific keywords: {specific_keywords}")
            if keywords:
                escaped_keywords = [re.escape(kw) for kw in keywords]
                q_objects = [Q(name__iregex=kw) for kw in escaped_keywords]
                combined_q = reduce(operator.or_, q_objects)
                qs = Product.objects.filter(combined_q)
                try:
                    logger.info(f"Generated SQL query (OR search): {qs.query}")
                except Exception as sql_err:
                    logger.error(f"Could not log SQL query: {sql_err}")
            else:
                qs = Product.objects.none()
            initial_results = list(qs)
            logger.info(f"Found {len(initial_results)} initial products by OR search.")
            scored_results = []
            if initial_results and keywords:
                logger.info("--- Start Scoring --- ")
                bonus_score_value = 100
                for product in initial_results:
                    score = 0
                    bonus_achieved = False
                    product_name_lower = product.name.lower()
                    matching_kws_details = []
                    specific_matches_count = 0
                    for kw in keywords:
                        kw_lower = kw.lower()
                        if re.search(re.escape(kw_lower), product_name_lower):
                            weight = 3 if kw_lower not in generic_keywords_set else 1
                            score += weight
                            matching_kws_details.append(f"'{kw}'(w:{weight})")
                            if kw_lower not in generic_keywords_set:
                                specific_matches_count += 1
                    if specific_keywords and specific_matches_count == len(specific_keywords):
                         all_specific_present_for_bonus = True
                         for spec_kw in specific_keywords:
                              if not re.search(re.escape(spec_kw.lower()), product_name_lower):
                                  all_specific_present_for_bonus = False
                                  break
                         if all_specific_present_for_bonus:
                              score += bonus_score_value
                              bonus_achieved = True
                              logger.info(f"Product ID {product.id} got BONUS score (+{bonus_score_value})")
                    if score > 0:
                      scored_results.append({"product": product, "score": score, "bonus": bonus_achieved})
                      logger.info(f"Product ID {product.id} Name: '{product.name[:50]}...' Score: {score} (Bonus: {bonus_achieved}) (Matches: {', '.join(matching_kws_details)})")
                    else:
                       logger.info(f"Product ID {product.id} Name: '{product.name[:50]}...' Score: 0")
                logger.info("--- End Scoring --- ")
            final_results = []
            if scored_results:
                scored_results.sort(key=lambda item: item["score"], reverse=True)
                bonus_items = [item for item in scored_results if item["bonus"]]
                if bonus_items:
                    logger.info(f"Found {len(bonus_items)} products with BONUS score. Selecting the highest score among them.")
                    max_bonus_score = bonus_items[0]["score"]
                    final_results = [item["product"] for item in bonus_items if item["score"] == max_bonus_score]
                    logger.info(f"Returning {len(final_results)} products with max bonus score ({max_bonus_score}).")
                else:
                    max_normal_score = scored_results[0]["score"]
                    logger.info(f"No bonus products. Max normal score is {max_normal_score}. Filtering by this score.")
                    final_results = [item["product"] for item in scored_results if item["score"] == max_normal_score]
                    logger.info(f"Returning {len(final_results)} products with max normal score.")
            else:
                 logger.info("No products scored > 0. Returning empty list.")
            logger.info(f"Returning {len(final_results)} final products.")
            return final_results
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            raise
    
    def split_query_into_items(self, full_query: str) -> List[Dict[str, Any]]:
        """Splits a full query text into individual item queries and quantities using LLM."""
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
        prompt = (
            """
            ЗАДАЧА: Извлечь данные о товарах из строк таблицы прайс-листа. Каждая строка - потенциальный товар. Иногда над товарами есть строки-заголовки.
            ПРАВИЛА:
            1. Для каждой строки, которая является товаром, вернуть JSON-объект ТОЛЬКО с полями: "supplier", "name", "price", "stock".
            2. supplier: Наименование поставщика (если есть столбец, иначе пусто).
            3. name: ПОЛНОЕ наименование товара из соответствующего столбца. ЕСЛИ над строкой товара была строка-заголовок (например, 'Редукторы', 'Задвижки под привод'), ДОБАВЬ этот заголовок в НАЧАЛО наименования товара (например, "Редукторы Редуктор тип Б").
            4. price: Цена товара. Ищи столбцы с названиями типа 'Цена', 'Price', 'Стоимость', 'Цена руб', 'Цена с НДС'. Извлекай ТОЛЬКО число (убирай валюту, 'руб', 'тг' и т.д.). Должно быть числом (целым или десятичным).
            5. stock: Остаток товара на складе. Ищи столбцы типа 'Остаток', 'Кол-во', 'Наличие', 'Stock', 'Qty'. Извлекай ТОЛЬКО число (обычно целое). Если не найдено, ставь null или 100.
            6. ТОЧНОСТЬ: Цена - это обычно число с копейками или без, остаток - обычно целое. Не путай их!
            7. ИСКЛЮЧИТЬ: Пустые строки, строки с 'Итого', 'Всего', строки, где нет ни наименования, ни цены - это не товары.
            8. ФОРМАТ: Верни ТОЛЬКО валидный JSON-массив объектов. Без текста до или после, без ```json ... ```.
            
            ПРИМЕР СТРОКИ ИЗ ТАБЛИЦЫ:
            {'Наименование изделия': 'Редуктор тип Б', 'Ду (мм)': '50', 'Цена руб. Ру16': '17000', 'Остаток шт': 4}
            ОЖИДАЕМЫЙ JSON ОБЪЕКТ (если не было заголовка):
            {{"supplier": "", "name": "Редуктор тип Б", "price": 17000, "stock": 4}}
            
            Таблица строк (JSON): {row}
            Ответ (ТОЛЬКО JSON-массив):
            """
        )
        results = []
        batch_size = 50 # Можно уменьшить для сложных таблиц
        current_header = "" # Для хранения последнего заголовка
        for i in range(0, len(table_rows), batch_size):
            batch = table_rows[i:i+batch_size]
            # Определение заголовков внутри батча (упрощенное)
            processed_batch = []
            for row_dict in batch:
                # Простая проверка на заголовок: мало колонок заполнено, нет цены/остатка?
                filled_values = [v for v in row_dict.values() if pd.notna(v) and str(v).strip()]
                potential_price = str(row_dict.get('price', '') or row_dict.get('Цена', '') or row_dict.get('Цена руб', '')).strip()
                potential_stock = str(row_dict.get('stock', '') or row_dict.get('Остаток', '') or row_dict.get('Кол-во', '')).strip()
                is_likely_header = len(filled_values) < 3 and not potential_price and not potential_stock and len(filled_values) > 0
                
                if is_likely_header:
                    current_header = str(filled_values[0]).strip() # Берем первое непустое значение как заголовок
                    logger.info(f"Detected header: {current_header}")
                    processed_batch.append({"is_header": True, "header_text": current_header})
                else:
                    # Добавляем текущий заголовок к строке для контекста LLM
                    row_dict["_context_header"] = current_header
                    processed_batch.append(row_dict)
            
            batch_text = json.dumps(processed_batch, ensure_ascii=False)
            full_prompt = prompt.replace('{row}', batch_text)
            try:
                response = self.llm.invoke(full_prompt)
                text = response.content
                logger.info(f"LLM response content (price list parse): {text[:500]}...")
                json_str = None
                match = re.search(r"```json\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
                if match:
                    json_str = match.group(1).strip()
                else:
                    first_bracket = text.find('[')
                    if first_bracket != -1:
                       # Пытаемся найти закрывающую скобку
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
                           if brace_level == 0 and char == ']': # Только когда нашли парную ] для первого [
                               end_index = first_bracket + idx + 1
                               break
                       if end_index != -1:
                           json_str = text[first_bracket:end_index]
                       else: # Не нашли парную скобку
                           json_str = text[first_bracket:] # Берем до конца, как раньше
                    else: 
                        json_str = None # Если не нашли даже [
                
                if json_str:
                    try:
                        batch_result = json.loads(json_str)
                        if isinstance(batch_result, list):
                            # Убираем поле _context_header перед добавлением
                            cleaned_batch_result = []
                            for item in batch_result:
                                if isinstance(item, dict):
                                   item.pop('_context_header', None)
                                   cleaned_batch_result.append(item)
                            results.extend(cleaned_batch_result)
                        else:
                            logger.warning(f"LLM returned non-list JSON: {json_str[:500]}...")
                    except json.JSONDecodeError as json_err:
                        logger.error(f"JSONDecodeError parsing LLM response: {json_err}. String: {json_str[:500]}...")
                else:
                    logger.error(f"Could not extract JSON block from LLM response: {text[:500]}...")
            except Exception as e:
                logger.error(f"LLM batch parse error: {e}")
            time.sleep(1) # Пауза между батчами
        return results 