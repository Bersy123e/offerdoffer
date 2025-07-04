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
from django.db.models import Q
import operator
from functools import reduce
import pandas as pd
import statistics  # Для вычисления статистики релевантности

from .cache import QueryCache
from logger import setup_logger
from .models import Product

# Отключаем DEBUG логи от OpenAI и httpx чтобы не засорять вывод
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

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
    Приводит размеры к единому виду:
    '57х5', '57 х 5', '57*5', '57 x 5', '57X5', '57 5' -> '57x5'
    ВАЖНО: нормализует ВСЕ возможные варианты написания размеров
    """
    if not text:
        return text
    
    result = text
    
    # 1. Заменяем все варианты разделителей размеров на 'x'
    # Кириллическая 'х', латинская 'x', '*', любые пробелы вокруг
    result = re.sub(r'(\d+)\s*[xх*×X]\s*(\d+)', r'\1x\2', result, flags=re.IGNORECASE)
    
    # 2. КРИТИЧНО: "число пробел число" тоже размер (57 5 -> 57x5) 
    # Но только если это явно размеры (не в середине длинного числа)
    result = re.sub(r'(\d+)\s+(\d+)(?=\s|$|[^\d.])', r'\1x\2', result)
    
    # 3. Дополнительные варианты размеров
    # "Ду57/5" или "57/5" тоже размеры 
    result = re.sub(r'(\d+)/(\d+)', r'\1x\2', result)
    
    return result

class QueryProcessor:
    def __init__(self, query_cache: QueryCache):
        """
        Initialize QueryProcessor with cache.
        Args:
            query_cache: QueryCache instance for caching query results
        """
        self.query_cache = query_cache
        # Используем gpt-4o для максимальной точности анализа сложных структур
        self.llm_model_name = "gpt-4o"
        self._initialize_llm()
    
    def _initialize_llm(self):
        """Initialize LangChain with OpenAI."""
        try:
            api_key = os.environ.get("OPENAI_API_KEY", "")
            if not api_key:
                logger.error("OPENAI_API_KEY not found in environment variables. Проверьте переменную окружения или .env файл.")
                raise ValueError("OPENAI_API_KEY is not set")
            else:
                masked = api_key[:4] + "***" + api_key[-4:]
                logger.info(f"OPENAI_API_KEY найден (длина={len(api_key)}): {masked}")
                
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
            logger.exception(f"Failed to initialize LLM: {type(e).__name__}: {e}")
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

    def process_query(self, query: str) -> List[Product]:
        """
        Process a natural language query and return ALL matching products (OR search, без скоринга).
        """
        try:
            logger.info(f"Processing query: {query}")
            query = normalize_dimensions(query)
            prompt = f"""ЗАДАЧА: Извлечь ключевые слова для поиска товара.

ПРАВИЛА:
1. Извлекай характеристики ТОЧНО как в тексте: "тип В", "ст.20", "ГОСТ 17375-2001", "108*6", "ДУ400".
2. НЕ включай количество (штук, шт, компл) и числа количества.
3. НЕ включай слова: нужен, для, под, и, с, еще, в, количестве.
4. ВСЕГДА возвращай JSON-массив, даже для простых слов.

ПРИМЕРЫ:
"редуктор" → ["редуктор"]
"фланец" → ["фланец"] 
"отвод 57х5" → ["отвод", "57х5"]
"задвижка ДУ300" → ["задвижка", "ДУ300"]
"Отвод ГОСТ17375-2001 108*6 ст.20 90гр. 2000 штук" → ["Отвод", "ГОСТ17375-2001", "108*6", "ст.20", "90гр"]

Запрос: {query}
Ответ:"""
            keywords = []
            if self.llm is not None:
                try:
                    response = self.llm.invoke(prompt)
                    text = response.content.strip()
                    try:
                        keywords = json.loads(text)
                    except Exception:
                        match = re.search(r'\[.*\]', text, re.DOTALL)
                        if match:
                            try:
                                keywords = json.loads(match.group(0))
                            except Exception:
                                 logger.warning(f"Could not parse JSON from LLM response: {text}")
                                 keywords = []
                        else:
                            logger.warning(f"Could not find JSON in LLM response: {text}")
                            keywords = []
                except Exception as llm_err:
                    logger.error(f"LLM invoke failed: {llm_err}. Falling back to simple keyword split.")
            # Если LLM не используется или не вернул ключевые слова
            if not keywords:
                keywords = [kw.strip() for kw in query.split() if kw.strip()]
            # Финальная очистка ключевых слов: удаляем пробелы, пустые строки, приводим к нижнему регистру
            keywords = [kw.strip() for kw in keywords if isinstance(kw, str) and kw.strip()]
            if not keywords:
                logger.warning(f"LLM returned empty keywords for query '{query}'. Falling back to splitting query text.")
                cleaned_query = re.sub(r'\d+\s*(?:шт|штук|компл)\b', '', query, flags=re.IGNORECASE).strip()
                cleaned_query = re.sub(r'\b\d+\s*$', '', cleaned_query).strip()
                keywords = [kw.strip() for kw in cleaned_query.split() if kw.strip() and kw.lower() not in ["нужен", "в", "количестве", "для", "под", "и", "с", "еще", "шт", "штук", "компл"]]
                if not keywords:
                    keywords = [query] if query else []
            logger.info(f"Using keywords: {keywords}")
            if keywords:
                # ---- ШАГ 1. СТРОГИЙ AND-ПОИСК ----
                all_products = list(Product.objects.all())
                # Нормализуем ключевые слова и названия товаров (чтобы 108*6 == 108х6 == 108x6)
                keywords_norm = [normalize_dimensions(kw.lower()) for kw in keywords]

                def product_matches(p, required_ratio: float = 1.0) -> bool:
                    name_norm = normalize_dimensions(p.name.lower())
                    hits = sum(1 for kw in keywords_norm if kw in name_norm)
                    return hits / len(keywords_norm) >= required_ratio

                # СТРОГИЙ поиск: все ключевые слова должны присутствовать
                strict_products = [p for p in all_products if product_matches(p, required_ratio=1.0)]

                if strict_products:
                    logger.info(
                        f"STRICT-поиск: найдено {len(strict_products)} товар(ов), удовлетворяющих 100% из {len(keywords_norm)} ключевых слов."
                    )
                    return strict_products

                # МЯГКИЙ AND-поиск (>=80% совпадений) – спасает, если ключевые слова содержат редкие детали
                soft_products = [p for p in all_products if product_matches(p, required_ratio=0.8)]

                if soft_products:
                    logger.info(
                        f"SOFT-поиск: найдено {len(soft_products)} товар(ов), удовлетворяющих ≥80% ключевых слов."
                    )
                    # Переходим к скорингу, но уже по уменьшенному набору
                    all_products = soft_products

                # ---- ШАГ 2. ГИБКИЙ ПОИСК С ОЦЕНКОЙ РЕЛЕВАНТНОСТИ ----
                scored_products = []
                
                for product in all_products:
                    score = self._calculate_relevance_score(product.name.lower(), keywords, query.lower())
                    if score > 0:
                        scored_products.append((product, score))
                
                # Сортируем по релевантности (убывание)
                scored_products.sort(key=lambda x: x[1], reverse=True)
                
                if not scored_products:
                    logger.info("No products found with positive relevance score.")
                    return []
                
                # --- ДОПОЛНИТЕЛЬНЫЕ ЛОГИ ДЛЯ ДИАГНОСТИКИ ---
                if scored_products:
                    scores_only = [s for _, s in scored_products]
                    max_score = scores_only[0]
                    avg_score = statistics.mean(scores_only)
                    median_score = statistics.median(scores_only)
                    logger.info(
                        f"Статистика релевантности: макс={max_score:.1f}, среднее={avg_score:.1f}, медиана={median_score:.1f}, всего_оценено={len(scores_only)}"
                    )

                    # Логируем топ-10 товаров по релевантности
                    top_samples = [
                        (p.id, p.name[:60], f"{s:.1f}") for p, s in scored_products[:10]
                    ]
                    logger.debug(f"Топ-10 по релевантности: {top_samples}")
                # --- КОНЕЦ ДОПОЛНИТЕЛЬНЫХ ЛОГОВ ---
                
                # АДАПТИВНЫЙ ПОРОГ РЕЛЕВАНТНОСТИ (ИСКОННАЯ ЛОГИКА)
                if max_score >= 1000:  # Точное совпадение
                    threshold = 50   # Снижаем порог
                elif max_score >= 500:  # Название начинается с запроса  
                    threshold = 40   # Снижаем порог
                elif max_score >= 300:  # Запрос содержится в названии
                    threshold = 30   # Снижаем порог
                elif max_score >= 150:  # Хорошие совпадения ключевых слов
                    threshold = 20   # Снижаем порог
                else:  # Слабые совпадения
                    threshold = 15   # Снижаем порог
                
                # Фильтруем по порогу
                filtered_products = [(p, s) for p, s in scored_products if s >= threshold]
                
                # Логи о количестве прошедших/отсеянных товаров
                logger.info(
                    f"Прошло фильтр: {len(filtered_products)} из {len(scored_products)} (порог={threshold})"
                )
                
                # НЕ ОГРАНИЧИВАЕМ количество результатов - могут быть разные поставщики
                # Пользователь хочет видеть все релевантные товары от всех поставщиков
                
                results = [product for product, score in filtered_products]
                
                logger.info(f"Found {len(results)} relevant products (threshold={threshold}, max_score={max_score:.1f}).")
                if filtered_products:
                    logger.info(f"Top 3 matches: {[(p.id, p.name[:50], f'{score:.1f}') for p, score in filtered_products[:3]]}")
                
                return results
            else:
                logger.info("No keywords extracted, returning empty results.")
                return []
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            raise
    
    def _calculate_relevance_score(self, product_name: str, keywords: List[str], original_query: str) -> float:
        """
        Рассчитывает релевантность товара запросу.
        Чем выше балл, тем более релевантен товар.
        """
        score = 0.0
        product_name = product_name.lower().strip()
        original_query = original_query.lower().strip()
        
        # 0. ПРЕДВАРИТЕЛЬНАЯ ФИЛЬТРАЦИЯ - исключаем заведомо нерелевантные товары
        if len(original_query) >= 3:  # Только для запросов длиннее 3 символов
            # Если нет ни одного общего значимого слова - сразу отсекаем
            query_words = set(re.findall(r'\b\w{2,}\b', original_query))  # Слова от 2 букв
            product_words = set(re.findall(r'\b\w{2,}\b', product_name))
            
            # НО! Не отсекаем если есть размеры - они могут быть записаны по-разному
            has_dimensions = bool(re.search(r'\d+[x*х]\d+', original_query, re.IGNORECASE))
            
            if not has_dimensions and not query_words.intersection(product_words) and original_query not in product_name:
                return 0  # Нет пересечений - нерелевантно
        
        # 1. ТОЧНОЕ СОВПАДЕНИЕ названия (высший приоритет)
        if product_name == original_query:
            score += 1000
            return score
        
        # 2. НАЗВАНИЕ НАЧИНАЕТСЯ С ЗАПРОСА (очень высокий приоритет)
        if product_name.startswith(original_query):
            score += 500
            return score
        
        # 3. ЗАПРОС СОДЕРЖИТСЯ В НАЗВАНИИ ЦЕЛИКОМ
        if original_query in product_name:
            score += 300
        
        # 4. СТРОГАЯ ПРОВЕРКА КЛЮЧЕВЫХ СЛОВ
        keywords_lower = [kw.lower().strip() for kw in keywords if kw and len(kw.strip()) >= 2]
        exact_matches = 0
        important_keywords_found = 0
        
        # Важные ключевые слова (типы товаров, характеристики)
        important_patterns = ['редуктор', 'задвижка', 'фланец', 'отвод', 'переход', 'тройник', 
                             'заглушка', 'клапан', 'кран', 'муфта', 'патрубок',
                             r'ду\s*\d+', r'ру\s*\d+', r'гост\s*\d+', r'ст\.\d+', r'тип\s*[а-я]']
        
        for keyword in keywords_lower:
            if keyword in product_name:
                # Базовый бонус за вхождение
                base_bonus = 30
                
                # Проверяем важность ключевого слова
                is_important = any(re.search(pattern, keyword, re.IGNORECASE) for pattern in important_patterns) or \
                              any(pattern in keyword for pattern in ['редуктор', 'задвижка', 'фланец', 'отвод'])
                
                if is_important:
                    base_bonus = 80  # Повышенный бонус для важных слов
                    important_keywords_found += 1
                    
                score += base_bonus
                exact_matches += 1
                
                # Дополнительный бонус за границы слов (более точное совпадение)
                if f" {keyword} " in f" {product_name} " or \
                   product_name.startswith(keyword + " ") or \
                   product_name.endswith(" " + keyword) or \
                   len(keyword) >= 4:  # Длинные ключевые слова менее требовательны к границам
                    score += 20
        
        # 5. СТРОГИЕ ТРЕБОВАНИЯ К ПОКРЫТИЮ
        if len(keywords_lower) > 0:
            coverage_ratio = exact_matches / len(keywords_lower)
            
            if coverage_ratio >= 0.8:  # 80%+ ключевых слов найдено
                score += 100
            elif coverage_ratio >= 0.6:  # 60%+ ключевых слов найдено  
                score += 50
            elif coverage_ratio < 0.4:  # Менее 40% - штраф
                score -= 30
        
        # 6. ШТРАФ ЗА ОТСУТСТВИЕ ВАЖНЫХ КЛЮЧЕВЫХ СЛОВ
        if important_keywords_found == 0 and len(keywords_lower) > 2:
            score -= 50  # Штраф если нет важных ключевых слов в длинном запросе
        
        # 6. СПЕЦИАЛЬНЫЕ БОНУСЫ ДЛЯ ЧАСТЫХ ТИПОВ ТОВАРОВ
        special_bonuses = {
            'редуктор': ['редуктор'],
            'задвижка': ['задвижка', 'клапан'],
            'фланец': ['фланец', 'фланцы'],
            'отвод': ['отвод', 'отводы'],
            'переход': ['переход', 'переходы'],
            'тройник': ['тройник', 'тройники'],
            'заглушка': ['заглушка', 'заглушки']
        }
        
        for product_type, synonyms in special_bonuses.items():
            if any(syn in original_query for syn in synonyms):
                if product_type in product_name:
                    score += 30  # Бонус за соответствие типа товара
        
        # 7. ШТРАФ ЗА СЛИШКОМ ДЛИННЫЕ НАЗВАНИЯ (если запрос короткий)
        if len(original_query.split()) <= 2 and len(product_name.split()) > 5:
            score -= 10
        
        # 8. КРИТИЧЕСКИ ВАЖНЫЙ ПОИСК ПО РАЗМЕРАМ
        
        # Нормализуем и запрос, и название товара для точного сравнения размеров
        normalized_query = normalize_dimensions(original_query)
        normalized_product = normalize_dimensions(product_name)
        
        # Извлекаем ВСЕ размеры из нормализованных строк
        query_dimensions = re.findall(r'\d+x\d+', normalized_query, re.IGNORECASE)
        product_dimensions = re.findall(r'\d+x\d+', normalized_product, re.IGNORECASE)
        
        # УЛУЧШЕННАЯ система размеров - точные совпадения И частичные
        if query_dimensions:
            exact_dimension_matches = set(query_dimensions) & set(product_dimensions)
            
            if exact_dimension_matches:
                # ВЫСОКИЙ бонус за точное совпадение размеров
                score += len(exact_dimension_matches) * 150  
                # logger.debug(f"Exact dimension match: query={query_dimensions}, product={product_dimensions}, matches={exact_dimension_matches}")
            elif product_dimensions:
                # МАЛЫЙ штраф за неточные размеры - позволяем видеть близкие варианты
                score -= 20  # Очень малый штраф
                # logger.debug(f"Dimension mismatch penalty: query={query_dimensions}, product={product_dimensions}")
            # Если в товаре нет размеров, не штрафуем (может быть общее название)
        
        # Другие КРИТИЧНЫЕ характеристики (ДУ, РУ, тип, ГОСТ, сталь)
        critical_patterns = [
            (r'ду\s*(\d+)', 150),      # ДУ - важная характеристика
            (r'ру\s*(\d+)', 100),      # РУ - важная характеристика  
            (r'тип\s*([абвг])', 80),   # Тип - важная характеристика
            (r'гост\s*(\d+(?:[-\s]*\d+)?)', 120),  # ГОСТ - стандарт
            (r'ст\.?\s*(\d+)', 80),    # Сталь - материал
            (r'исп\.?\s*([а-я])', 60), # Исполнение
            (r'09г2с', 100),           # Конкретная сталь
            (r'ст20', 80),             # Конкретная сталь
            (r'ст45', 80),             # Конкретная сталь
        ]
        
        for pattern, bonus in critical_patterns:
            query_matches = set(re.findall(pattern, original_query, re.IGNORECASE))
            product_matches = set(re.findall(pattern, product_name, re.IGNORECASE))
            
            if query_matches and product_matches:
                # Бонус за совпадающие критичные характеристики
                common_matches = query_matches & product_matches
                if common_matches:
                    score += len(common_matches) * bonus
                    # logger.debug(f"Critical pattern match: {pattern} - {common_matches}")  # Убираем избыточные логи
                else:
                    # Штраф за несовпадение критичных характеристик
                    score -= bonus // 2
                    # logger.debug(f"Critical pattern mismatch: query={query_matches}, product={product_matches}")  # Убираем избыточные логи
            elif query_matches and not product_matches:
                # Если в запросе есть критичная характеристика, а в товаре нет - штраф
                score -= bonus // 3
        
        return max(score, 0)  # Минимум 0
    
    def split_query_into_items(self, full_query: str) -> List[Dict[str, Any]]:
        """
        ПРОСТОЕ разделение запроса на товары:
        - Если есть переносы строк → каждая строка = товар
        - Если нет переносов → весь запрос = один товар
        """
        full_query = normalize_dimensions(full_query).strip()
        
        if not full_query:
            logger.warning("Empty query provided")
            return []
            
        # ПРОСТАЯ ЛОГИКА: разделяем по строкам
        if '\n' in full_query:
            lines = [line.strip() for line in full_query.splitlines() if line.strip()]
            
            # УМНАЯ ФИЛЬТРАЦИЯ через LLM - определяем товарные строки
            if self.llm and len(lines) > 1:
                clean_lines = self._filter_product_lines_with_llm(lines)
                logger.info(f"LLM filtered {len(clean_lines)} product lines from {len(lines)} total lines")
            else:
                # Простая фильтрация без LLM
                clean_lines = []
                for line in lines:
                    line_lower = line.lower()
                    # Пропускаем заголовки и мусор
                    if any(junk in line_lower for junk in ['№', 'наименование', 'количество', 'цена', 'стоимость', 'итог']):
                        continue
                    if len(line) < 5:  # Слишком короткие строки
                        continue
                    if re.match(r'^\d+$', line):  # Только цифры
                        continue
                    clean_lines.append(line)
                logger.info(f"Simple filter: {len(clean_lines)} lines from {len(lines)} total lines")
        else:
            # Один товар
            clean_lines = [full_query]
            logger.info("Treating entire query as single item")
        
        # Создаем список товаров
        items = []
        for line in clean_lines:
            quantity = extract_quantity(line)
            # Убираем количество из названия товара
            item_name = re.sub(r'\d+\s*(?:шт|штук|компл)\b', '', line, flags=re.IGNORECASE).strip()
            item_name = re.sub(r'\s+\d+\s*$', '', item_name).strip()  # Убираем число в конце
            
            if item_name:
                items.append({
                    "item_query": item_name,
                    "quantity": quantity or 1
                })
                logger.info(f"Added item: '{item_name}', qty: {quantity or 1}")
        
        return items
    
    def _filter_product_lines_with_llm(self, lines: List[str]) -> List[str]:
        """
        Использует LLM для определения какие строки содержат товары, а какие - мусор
        """
        try:
            # Создаем промпт для фильтрации
            lines_text = '\n'.join([f"{i+1}. {line}" for i, line in enumerate(lines)])
            
            prompt = f"""
Из списка строк выбери ТОЛЬКО те, которые содержат названия товаров/изделий.
ИСКЛЮЧИ: заголовки таблиц, номера, итоги, пустые строки, служебную информацию.

СТРОКИ:
{lines_text}

Ответ в формате: только номера строк через запятую (например: 1,3,5)
"""
            
            response = self.llm.invoke(prompt)
            result_text = response.content if hasattr(response, 'content') else str(response)
            
            # Извлекаем номера строк
            numbers = re.findall(r'\d+', result_text)
            selected_indices = [int(n)-1 for n in numbers if int(n) <= len(lines)]
            
            # Возвращаем отфильтрованные строки
            filtered_lines = [lines[i] for i in selected_indices if 0 <= i < len(lines)]
            
            if filtered_lines:
                logger.info(f"LLM selected {len(filtered_lines)} product lines: {[f'{i+1}' for i in selected_indices]}")
                return filtered_lines
            else:
                logger.warning("LLM returned no valid line numbers, using simple filter")
                return lines  # Fallback to all lines
                
        except Exception as e:
            logger.exception(f"Error in LLM filtering: {e}")
            return lines  # Fallback to all lines

    def extract_products_from_table(self, table_rows: list) -> list:
        """
        ПРОСТАЯ И НАДЁЖНАЯ обработка прайс-листов.
        Берет первые колонки как: название, цена, остаток
        """
        logger.info(f"extract_products_from_table CALLED with {len(table_rows)} rows")
        
        if not table_rows:
            logger.warning("No table rows to process")
            return []
            
        # --- ШАГ 1. Определяем колонки по ключевым словам в заголовке ---
        first_row = table_rows[0]
        headers = list(first_row.keys())
        logger.info(f"Available headers: {headers}")

        # Наборы ключевых слов
        price_kw = ['цена', 'price', 'стоим', 'cost', 'value']
        stock_kw = ['остат', 'кол-во', 'налич', 'stock', 'qty', 'amount', 'balance']
        name_kw = ['наимен', 'товар', 'product', 'item', 'описан', 'nomenkl', 'назв']

        def find_header(keywords):
            for h in headers:
                if h is None:
                    continue
                hl = str(h).lower()
                if any(k in hl for k in keywords):
                    return h
            return None

        name_col = find_header(name_kw) or headers[0]
        price_col = find_header(price_kw)
        stock_col = find_header(stock_kw)

        # Если всё равно не нашли цену/остаток – используем позицию 1/2 как раньше
        if price_col is None and len(headers) > 1:
            price_col = headers[1]
        if stock_col is None and len(headers) > 2:
            stock_col = headers[2]
        
        # --- ШАГ 1.2. Fallback: анализ данных, ищем числовые столбцы ---
        sample_limit = min(30, len(table_rows))
        def detect_numeric_column(candidate_headers, allow_zero=True):
            best_header = None
            best_score = 0
            for h in candidate_headers:
                numeric_hits = 0
                non_empty = 0
                for i in range(sample_limit):
                    val = table_rows[i].get(h, '')
                    if val is None:
                        continue
                    val_str = str(val).strip()
                    if not val_str:
                        continue
                    non_empty += 1
                    if re.search(r"\d", val_str):
                        numeric_hits += 1
                if non_empty == 0:
                    continue
                ratio = numeric_hits / non_empty
                if ratio > best_score:
                    best_score = ratio
                    best_header = h
            return best_header if best_score >= 0.6 else None

        # Переопределяем price_col/stock_col, если не нашли по заголовку
        if price_col is None:
            price_col = detect_numeric_column(headers)
            logger.info(f"Fallback numeric detection выбрал price_col='{price_col}' (score>=0.6)")
        if stock_col is None:
            # Для остатка допускаем большее количество нулей, поэтому allow_zero True
            stock_col = detect_numeric_column(headers)
            logger.info(f"Fallback numeric detection выбрал stock_col='{stock_col}' (score>=0.6)")

        logger.info(
            f"Итоговый маппинг колонок: name='{name_col}', price='{price_col}', stock='{stock_col}'"
        )

        # --- ШАГ 1.3. Проверяем, не перепутаны ли цена и остаток по содержимому ---
        def looks_like_price(v: str) -> bool:
            v = str(v).lower().strip()
            if not v:
                return False
            # наличие валютного символа или запятой как десятичного
            if any(sym in v for sym in [',', '₽', 'руб', '$', 'eur', '€', 'тг']):
                return True
            # большое число
            digits = re.sub(r'[^0-9]', '', v)
            if digits and len(digits) >= 4:
                return True  # >= 1000
            return False

        def looks_like_stock(v: str) -> bool:
            v = str(v).lower().strip()
            if not v:
                return False
            if any(word in v for word in ['нет', 'под заказ', 'ожид', 'отсут', 'в наличии', 'есть']):
                return True
            digits = re.sub(r'[^0-9]', '', v)
            if digits and len(digits) <= 4:  # до 9999 шт
                return True
            return False

        swap_needed = False
        if price_col and stock_col:
            price_like_in_price = 0
            stock_like_in_price = 0
            price_like_in_stock = 0
            stock_like_in_stock = 0
            sample_n = min(25, len(table_rows))
            for i in range(sample_n):
                price_val = table_rows[i].get(price_col, '')
                stock_val = table_rows[i].get(stock_col, '')
                if looks_like_price(price_val):
                    price_like_in_price += 1
                if looks_like_stock(price_val):
                    stock_like_in_price += 1
                if looks_like_price(stock_val):
                    price_like_in_stock += 1
                if looks_like_stock(stock_val):
                    stock_like_in_stock += 1

            # если price_col больше похож на stock, а stock_col похож на price
            if stock_like_in_price > price_like_in_price and price_like_in_stock > stock_like_in_stock:
                swap_needed = True

        if swap_needed:
            price_col, stock_col = stock_col, price_col
            logger.warning(
                f"SWAP DETECTED: переопределяем price_col -> '{price_col}', stock_col -> '{stock_col}' по анализу содержимого."
            )

        results = []
        
        for row_idx, row_dict in enumerate(table_rows):
            try:
                # Извлекаем имя
                name = str(row_dict.get(name_col, '')).strip() if name_col else ''
                
                # Извлекаем цену
                price = 0
                price_raw = ""
                if price_col:
                    price_raw = str(row_dict.get(price_col, '')).strip()
                    if price_raw:
                        # Зачистка: убираем валюту, пробелы, «руб/₽/tг/eur» и т. д.
                        clean_price = re.sub(r'[^0-9,\.]', '', price_raw.replace('\xa0', ''))
                        price_numbers = re.findall(r'[\d\,\.]+', clean_price)
                        if price_numbers:
                            try:
                                price = float(price_numbers[0].replace(',', '.'))
                            except:
                                price = 0
                        else:
                            logger.debug(
                                f"Row {row_idx}: цена не распознана из '{price_raw}'. price_numbers пустой."
                            )
                
                # Логируем первые несколько строк для диагностики
                if row_idx < 5:
                    logger.info(f"Row {row_idx}: name='{name}', price_raw='{price_raw}', price={price}")
                
                # Извлекаем остаток
                stock = 100  # значение по умолчанию
                if stock_col:
                    stock_raw = str(row_dict.get(stock_col, '')).strip()
                    if stock_raw:
                        stock_lower = stock_raw.lower()
                        # текстовые варианты
                        if any(w in stock_lower for w in ['нет', '0', 'под заказ', 'ожид', 'отсут']):
                            stock = 0
                        elif any(w in stock_lower for w in ['есть', 'в наличии', 'налич', 'много']):
                            stock = 100
                        else:
                            stock_numbers = re.findall(r'\d+', stock_raw)
                            if stock_numbers:
                                try:
                                    stock = int(stock_numbers[0])
                                except:
                                    stock = 100
                            elif stock_numbers == []:
                                logger.debug(f"Row {row_idx}: остаток не распознан из '{stock_raw}'.")
                else:
                    logger.debug(
                        f"Row {row_idx}: stock_col is None, raw_row={row_dict}"
                    )
                
                # Поставщик = пустая строка (не важно для простой логики)
                supplier = ""
                
                # МИНИМАЛЬНАЯ фильтрация - берём почти всё
                if not name or len(name) < 2:
                    if row_idx < 5:
                        logger.info(f"Row {row_idx}: Skipped - name too short: '{name}'")
                    continue
                    
                # Если нет цены - ставим 0 (цена неизвестна, но товар есть)
                if price <= 0:
                    price = 0.0
                    if row_idx < 5:
                        logger.info(f"Row {row_idx}: Set price=0 (no price found) for '{name}'")
                    
                # УЛУЧШЕННАЯ фильтрация мусора
                name_lower = name.lower().strip()
                
                # 1. Заголовки таблиц (расширено)
                if any(k in name_lower for k in ['наимен', 'товар', 'цена', 'назв', 'артикул', 'код', 'остаток', 'количество', 'сумма', 'стоим']) and len(name_lower.split())<=3:
                    if row_idx < 5:
                        logger.info(f"Row {row_idx}: Skipped - table header: '{name}'")
                    continue
                
                # 2. Телефоны и факсы
                if re.search(r'[\+\-\(\)\s]*[\d\-\(\)\s]{7,}', name) and ('тел' in name_lower or 'факс' in name_lower or '+' in name):
                    if row_idx < 5:
                        logger.info(f"Row {row_idx}: Skipped - phone/fax detected: '{name}'")
                    continue
                
                # 3. Email адреса
                if '@' in name and '.' in name:
                    if row_idx < 5:
                        logger.info(f"Row {row_idx}: Skipped - email detected: '{name}'")
                    continue
                
                # 4. Адреса
                if any(addr_word in name_lower for addr_word in ['ул.', 'пр.', 'д.', 'кв.', 'офис', 'этаж']):
                    if row_idx < 5:
                        logger.info(f"Row {row_idx}: Skipped - address detected: '{name}'")
                    continue
                
                # 5. Только цифры или знаки препинания
                if re.match(r'^[\d\s\-\.\,\(\)]+$', name):
                    if row_idx < 5:
                        logger.info(f"Row {row_idx}: Skipped - only numbers/punctuation: '{name}'")
                    continue
                
                # 6. Общие фразы и мусор
                junk_phrases = ['итого', 'всего', 'сумма', 'подпись', 'печать', 'директор', 'менеджер', 'контакты', 'реквизиты']
                if any(junk in name_lower for junk in junk_phrases):
                    if row_idx < 5:
                        logger.info(f"Row {row_idx}: Skipped - junk phrase detected: '{name}'")
                    continue
                    
                results.append({
                    'supplier': supplier,
                    'name': name,
                    'price': price,
                    'stock': stock
                })
                
                if row_idx < 5:
                    logger.info(f"Row {row_idx}: ADDED product: '{name}', price={price}")
                
            except Exception as e:
                logger.warning(f"Error processing row {row_idx}: {e}")
                continue
        
        logger.info(f"Successfully extracted {len(results)} products from {len(table_rows)} rows")
        
        # Показываем примеры извлеченных товаров
        if results:
            logger.info(f"Sample extracted products: {results[:3]}")
            
        return results 