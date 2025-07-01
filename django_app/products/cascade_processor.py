import pandas as pd
import re
import logging
import numpy as np
from typing import List, Dict, Optional, Any, Tuple
import os

logger = logging.getLogger('commercial_proposal')

class CascadeProcessor:
    """
    Каскадный процессор для извлечения данных из прайс-листов.
    Пробует несколько методов по очереди:
    1. Эвристический (быстрый, для простых форматов).
    2. LLM (медленный, для сложных форматов) - пока не реализован.
    """

    def __init__(self, llm: Optional[Any] = None):
        self.llm = llm
        self.cascade_log = []
        # Удаляем ссылки на несуществующие процессоры
        # self.level1_processor и self.level2_processor не определены

    def process_file_cascade(self, file_path: str, file_name: str) -> Dict:
        logger.info(f"--- ЗАПУСК КАСКАДНОЙ ОБРАБОТКИ для файла: {file_name} ---")
        self.cascade_log = [f"Начало обработки файла: {file_name}"]
        error_message = "Не удалось извлечь данные. Неизвестная ошибка."

        try:
            heuristic_result = self._process_with_heuristics(file_path)
            if heuristic_result.get('success'):
                logger.info("Уровень 1 (Эвристика) успешно извлек товары.")
                self.cascade_log.append("Уровень 1 (Эвристика) завершился успешно.")
                heuristic_result['final_method'] = 'Эвристика'
                heuristic_result['cascade_log'] = self.cascade_log
                return heuristic_result
            else:
                self.cascade_log.extend(heuristic_result.get('log', []))
                self.cascade_log.append("Уровень 1 (Эвристика) не дал результата.")
                logger.warning("Уровень 1 (Эвристика) не дал результата.")
                error_message = heuristic_result.get('error', error_message)
        except Exception as e:
            logger.error(f"Критическая ошибка на Уровне 1 (Эвристика): {e}", exc_info=True)
            self.cascade_log.append(f"Критическая ошибка на Уровне 1: {e}")
            error_message = f"Критическая ошибка: {e}"

        return {"success": False, "products": [], "error": error_message, "cascade_log": self.cascade_log}

    def _process_with_heuristics(self, file_path: str) -> Dict:
        log = []
        try:
            # Проверяем, есть ли несколько листов в Excel
            file_ext = os.path.splitext(file_path)[1].lower()
            all_products = []
            
            if file_ext in ['.xlsx', '.xls']:
                excel_file = pd.ExcelFile(file_path)
                sheet_names = excel_file.sheet_names
                log.append(f"Файл содержит {len(sheet_names)} лист(ов): {sheet_names}")
                
                # Обрабатываем каждый лист
                for sheet_name in sheet_names:
                    log.append(f"\n--- Обработка листа '{sheet_name}' ---")
                    try:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                        log.append(f"Лист '{sheet_name}' прочитан: {df.shape[0]} строк, {df.shape[1]} колонок")
                        
                        # Обрабатываем лист теми же методами
                        products = self._process_single_sheet(df, sheet_name, log)
                        all_products.extend(products)
                        
                    except Exception as sheet_error:
                        log.append(f"Ошибка обработки листа '{sheet_name}': {sheet_error}")
                        continue
                
                if all_products:
                    log.append(f"\nВсего извлечено товаров со всех листов: {len(all_products)}")
                    return {"success": True, "products": all_products, "log": log}
                else:
                    return {"success": False, "products": [], "log": log, "error": "Не найдено товаров ни на одном листе"}
            else:
                # Для других форматов читаем как обычно
                df = pd.read_excel(file_path, header=None)
                log.append(f"Файл {file_path} успешно прочитан в DataFrame.")
                products = self._process_single_sheet(df, "main", log)
                return {"success": True, "products": products, "log": log} if products else {"success": False, "products": [], "log": log, "error": "Не найдено товаров"}
                
        except Exception as e:
            log.append(f"Не удалось прочитать файл: {e}")
            return {"success": False, "products": [], "log": log, "error": f"Ошибка чтения: {e}"}
    
    def _process_single_sheet(self, df: pd.DataFrame, sheet_name: str, log: List[str]) -> List[Dict]:
        """Обработка одного листа Excel"""
        header_row_index, header = self._find_header_row(df, log)
        if header_row_index is None:
            log.append(f"Лист '{sheet_name}': Не удалось найти строку заголовка.")
            return []  # Возвращаем пустой список, а не словарь
        
        df.columns = [f"col_{i}" for i in range(len(df.columns))]
        header_map = {f"col_{i}": str(h) for i, h in enumerate(header)}
        
        data_df = df.iloc[header_row_index + 1:].reset_index(drop=True)
        log.append(f"Лист '{sheet_name}': Данные для обработки подготовлены, начиная со строки {header_row_index + 1}.")

        name_col, price_col, stock_col = self._map_columns(header_map, data_df, log)

        if not name_col or not price_col:
            log.append(f"Лист '{sheet_name}': Не удалось определить обязательные колонки 'Наименование' или 'Цена'.")
            return []  # Возвращаем пустой список

        products = self._extract_products_with_subheaders(data_df, name_col, price_col, stock_col, log, header_map)

        if not products:
            log.append(f"Лист '{sheet_name}': Не удалось извлечь ни одного товара.")
            return []
        
        log.append(f"Лист '{sheet_name}': Извлечено {len(products)} товаров.")
        return products

    def _find_header_row(self, df: pd.DataFrame, log: List[str]) -> Tuple[Optional[int], Optional[List[str]]]:
        header_keywords = ['наимен', 'товар', 'цена', 'кол-во', 'остат', 'артикул', 'руб', 'гост', 'н-ра', 'описание']
        best_row_index = -1
        max_matches = 0

        for i, row in df.head(20).iterrows():
            row_str = ' '.join(str(x) for x in row.dropna() if x).lower()
            if not row_str: continue
            
            matches = sum(1 for kw in header_keywords if kw in row_str)
            
            if matches > 1 and matches > max_matches:
                max_matches = matches
                best_row_index = i
        
        if best_row_index != -1:
            log.append(f"Найдена строка заголовка (индекс {best_row_index}) с {max_matches} совпадениями.")
            return best_row_index, list(df.iloc[best_row_index])
        
        log.append("Строка заголовка не найдена, используется первая строка.")
        return 0, list(df.iloc[0])

    def _map_columns(self, header_map: Dict, df: pd.DataFrame, log: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        price_kw = ['цена', 'price', 'стоим', 'cost', 'value', 'руб', 'rub', 'сумма']
        stock_kw = ['остат', 'кол-во', 'налич', 'stock', 'qty', 'amount', 'balance', 'количество', 'склад']
        name_kw = ['наимен', 'товар', 'product', 'item', 'описан', 'nomenkl', 'назв', 'продукт']

        def find_header(keywords):
            for col_idx, header_name in header_map.items():
                if any(k in str(header_name).lower() for k in keywords):
                    return col_idx
            return None

        name_col = find_header(name_kw) or 'col_0'
        price_col = find_header(price_kw)
        stock_col = find_header(stock_kw)
        
        # Если не нашли колонку с ценой по ключевым словам, попробуем найти по данным
        if not price_col:
            log.append("Не найдена колонка цены по заголовкам. Пробуем определить по содержимому...")
            # Анализируем первые 10 строк данных
            for col_idx in header_map.keys():
                if col_idx == name_col:  # Пропускаем колонку с названием
                    continue
                    
                price_like_count = 0
                for i in range(min(10, len(df))):
                    val = str(df.iloc[i].get(col_idx, '')).strip()
                    # Проверяем, похоже ли на цену
                    if re.search(r'\d+[\s,\.]*\d*', val) and not re.search(r'[а-яА-Яa-zA-Z]{3,}', val):
                        price_like_count += 1
                
                if price_like_count >= 5:  # Если больше половины значений похожи на цены
                    price_col = col_idx
                    log.append(f"Вероятная колонка цены найдена по содержимому: {col_idx}")
                    break
        
        log.append(f"Первичный маппинг: Name='{header_map.get(name_col)}' ({name_col}), Price='{header_map.get(price_col)}' ({price_col}), Stock='{header_map.get(stock_col)}' ({stock_col})")
        
        return name_col, price_col, stock_col

    def _extract_products_with_subheaders(self, df: pd.DataFrame, name_col: str, price_col: str, stock_col: Optional[str], log: List[str], header_map: Dict) -> List[Dict]:
        products = []
        current_subheader = ""
        for index, row in df.iterrows():
            name = str(row.get(name_col, "")).strip()
            
            is_subheader = name and all(pd.isna(v) or str(v).strip() == "" for k, v in row.items() if k != name_col)
            
            if is_subheader:
                current_subheader = name
                log.append(f"Обнаружен подзаголовок: '{current_subheader}'")
                continue

            if not name: continue

            full_name = f"{current_subheader} {name}".strip()
            
            price = 0
            price_raw = str(row.get(price_col, "")).strip()
            if price_raw and price_raw not in ['nan', 'None', '-', '']:
                # Логируем исходное значение цены для отладки
                logger.debug(f"Обрабатываем цену: '{price_raw}'")
                
                # Удаляем все пробельные символы включая неразрывные пробелы
                price_raw = price_raw.replace('\xa0', ' ').replace('\u00a0', ' ')
                
                # Если цена содержит дробь через пробел (например "1 234,56")
                # или через точку как разделитель тысяч (например "1.234,56")
                price_raw = price_raw.replace(' ', '')
                
                # Заменяем запятую на точку для десятичных
                if ',' in price_raw and '.' in price_raw:
                    # Если есть и точка и запятая, предполагаем что точка - разделитель тысяч
                    price_raw = price_raw.replace('.', '').replace(',', '.')
                else:
                    price_raw = price_raw.replace(',', '.')
                
                # Извлекаем только числа и точку
                clean_price = re.sub(r'[^\d\.]', '', price_raw)
                
                if clean_price:
                    try:
                        price = float(clean_price)
                        logger.debug(f"Успешно извлечена цена: {price}")
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Не удалось преобразовать цену '{clean_price}': {e}")
            
            stock = 100
            if stock_col and pd.notna(row.get(stock_col)):
                stock_raw = str(row[stock_col]).lower().strip()
                if any(w in stock_raw for w in ['нет', '0', 'под заказ', 'ожид', 'отсут']): stock = 0
                elif any(w in stock_raw for w in ['есть', 'в наличии', 'налич', 'много']): stock = 100
                else:
                    stock_numbers = re.findall(r'\d+', stock_raw)
                    if stock_numbers: stock = int(stock_numbers[0])

            # Добавляем товар даже если цена = 0 (возможно, это специальная цена)
            # Но пропускаем строки без названия
            if full_name and full_name.strip():
                products.append({"name": full_name, "price": price, "stock": stock})
                if price == 0:
                    logger.warning(f"Товар с нулевой ценой: {full_name[:50]}...")
        
        log.append(f"Извлечено {len(products)} товаров.")
        return products
    
    def get_cascade_summary(self, result: Dict) -> str:
        summary_lines = [
            f"Финальный успешный метод: {result.get('final_method', 'Неизвестен')}",
            f"Извлечено товаров: {len(result.get('products', []))}",
            "--- Лог выполнения ---",
            f"Статус: {'Успех' if result.get('success') else 'Неудача'}"
        ]
        summary_lines.extend(result.get('cascade_log', ['Лог пуст.']))
        summary_lines.extend(result.get('log', []))
        return "\n".join(summary_lines)

    def _read_file_safely(self, file_path: str) -> Optional[pd.DataFrame]:
        """Безопасное чтение файла"""
        try:
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.csv':
                # Для CSV пробуем разные кодировки
                for encoding in ['utf-8', 'cp1251', 'latin1']:
                    try:
                        return pd.read_csv(file_path, encoding=encoding, dtype=str)
                    except:
                        continue
                        
            elif file_ext in ['.xlsx', '.xls']:
                # Для Excel пробуем разные движки
                if file_ext == '.xls':
                    try:
                        return pd.read_excel(file_path, header=None, dtype=str, engine='xlrd')
                    except:
                        try:
                            return pd.read_excel(file_path, header=None, dtype=str, engine='openpyxl')
                        except:
                            return pd.read_excel(file_path, header=None, dtype=str)
                else:
                    return pd.read_excel(file_path, header=None, dtype=str)
                    
            return None
            
        except Exception as e:
            logger.error(f"Ошибка чтения файла {file_path}: {e}")
            return None
    
    def _select_best_overall_result(self, level1_result: Dict, level2_result: Dict) -> Optional[Dict]:
        """Выбор лучшего результата из всех уровней"""
        candidates = []
        
        if level1_result.get('success') and level1_result.get('products'):
            candidates.append({
                'method': 'level1_smart',
                'products': level1_result['products'],
                'count': len(level1_result['products'])
            })
            
        if level2_result.get('success') and level2_result.get('products'):
            candidates.append({
                'method': 'level2_bruteforce', 
                'products': level2_result['products'],
                'count': len(level2_result['products'])
            })
        
        if not candidates:
            return None
            
        # Выбираем кандидата с наибольшим количеством товаров
        best = max(candidates, key=lambda x: x['count'])
        return best
        
    # Этот метод закомментирован, так как он использует несуществующие level1_processor и level2_processor
    # def process_file_cascade(self, file_path: str, file_name: str = "") -> Dict:
    #     """
    #     Каскадная обработка файла с тремя уровнями сложности
    #     """
    #     logger.info(f"🎯 КАСКАДНАЯ ОБРАБОТКА: {file_name}")
    #     
    #     result = {
    #         'success': False,
    #         'final_method': None,
    #         'products': [],
    #         'cascade_log': [],
    #         'all_attempts': {}
    #     }
    #     
    #     try:
    #         # Читаем файл для передачи в процессоры
    #         df = self._read_file_safely(file_path)
    #         if df is None:
    #             result['cascade_log'].append("❌ Ошибка чтения файла")
    #             return result
    #         
    #         # УРОВЕНЬ 1: УМНАЯ СИСТЕМА
    #         logger.info("🔥 УРОВЕНЬ 1: Умная система")
    #         level1_result = self.level1_processor.process_price_list(df, file_name)
    #         result['all_attempts']['level1'] = level1_result
    #         
    #         if level1_result['success'] and len(level1_result['products']) >= 5:
    #             # Успех на уровне 1!
    #             result['success'] = True
    #             result['final_method'] = 'level1_smart'
    #             result['products'] = level1_result['products']
    #             result['cascade_log'].append(f"✅ УРОВЕНЬ 1 УСПЕХ: {len(level1_result['products'])} товаров")
    #             logger.info(f"✅ Уровень 1 успешен: {len(level1_result['products'])} товаров")
    #             return result
    #         else:
    #             result['cascade_log'].append(f"⚠️ УРОВЕНЬ 1: {len(level1_result.get('products', []))} товаров (недостаточно)")
    #             
    #         # УРОВЕНЬ 2: БРУТФОРС АНАЛИЗ
    #         logger.info("🔥 УРОВЕНЬ 2: Брутфорс анализ")
    #         level2_result = self.level2_processor.process_complex_file(file_path, file_name)
    #         result['all_attempts']['level2'] = level2_result
    #         
    #         if level2_result['success'] and len(level2_result['products']) > 0:
    #             # Сравниваем результаты уровня 1 и 2
    #             level1_count = len(level1_result.get('products', []))
    #             level2_count = len(level2_result['products'])
    #             
    #             if level2_count > level1_count:
    #                 # Уровень 2 лучше
    #                 result['success'] = True
    #                 result['final_method'] = 'level2_bruteforce'
    #                 result['products'] = level2_result['products']
    #                 result['cascade_log'].append(f"✅ УРОВЕНЬ 2 ЛУЧШЕ: {level2_count} товаров (vs {level1_count})")
    #                 logger.info(f"✅ Уровень 2 лучше: {level2_count} vs {level1_count} товаров")
    #                 return result
    #             elif level1_count > 0:
    #                 # Уровень 1 все же лучше
    #                 result['success'] = True  
    #                 result['final_method'] = 'level1_smart'
    #                 result['products'] = level1_result['products']
    #                 result['cascade_log'].append(f"✅ УРОВЕНЬ 1 ЛУЧШЕ: {level1_count} товаров (vs {level2_count})")
    #                 return result
    #         else:
    #             result['cascade_log'].append(f"⚠️ УРОВЕНЬ 2: {len(level2_result.get('products', []))} товаров")
    #             
    #         # УРОВЕНЬ 3: ПОЛНЫЙ LLM АНАЛИЗ (пока заглушка)
    #         logger.info("🔥 УРОВЕНЬ 3: Полный LLM анализ")
    #         result['cascade_log'].append("⏳ УРОВЕНЬ 3: Требует реализации")
    #         
    #         # Если дошли сюда - берем лучший результат из уровней 1-2
    #         best_result = self._select_best_overall_result(level1_result, level2_result)
    #         if best_result:
    #             result['success'] = True
    #             result['final_method'] = best_result['method']
    #             result['products'] = best_result['products']
    #             result['cascade_log'].append(f"✅ ЛУЧШИЙ РЕЗУЛЬТАТ: {best_result['method']} - {len(best_result['products'])} товаров")
    #         else:
    #             result['cascade_log'].append("❌ ВСЕ УРОВНИ НЕУСПЕШНЫ")
    #             
    #     except Exception as e:
    #         logger.error(f"Ошибка каскадной обработки: {e}")
    #         result['cascade_log'].append(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
    #         
    #     return result 