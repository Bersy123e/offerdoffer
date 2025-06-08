import pandas as pd
import logging
from typing import Dict, List, Optional
import os

from .smart_price_processor import SmartPriceListProcessor
from .bruteforce_processor import BruteforceProcessor

logger = logging.getLogger(__name__)

class CascadeProcessor:
    """
    КАСКАДНАЯ СИСТЕМА ОБРАБОТКИ ПРАЙС-ЛИСТОВ
    
    Уровень 1: Умная система (SmartPriceListProcessor)
    Уровень 2: Брутфорс анализ (BruteforceProcessor) 
    Уровень 3: LLM с полным контекстом (будет реализован позже)
    """
    
    def __init__(self, llm=None):
        self.llm = llm
        self.level1_processor = SmartPriceListProcessor(llm=llm)
        self.level2_processor = BruteforceProcessor(llm=llm)
        
    def process_file_cascade(self, file_path: str, file_name: str = "") -> Dict:
        """
        Каскадная обработка файла с тремя уровнями сложности
        """
        logger.info(f"🎯 КАСКАДНАЯ ОБРАБОТКА: {file_name}")
        
        result = {
            'success': False,
            'final_method': None,
            'products': [],
            'cascade_log': [],
            'all_attempts': {}
        }
        
        try:
            # Читаем файл для передачи в процессоры
            df = self._read_file_safely(file_path)
            if df is None:
                result['cascade_log'].append("❌ Ошибка чтения файла")
                return result
            
            # УРОВЕНЬ 1: УМНАЯ СИСТЕМА
            logger.info("🔥 УРОВЕНЬ 1: Умная система")
            level1_result = self.level1_processor.process_price_list(df, file_name)
            result['all_attempts']['level1'] = level1_result
            
            if level1_result['success'] and len(level1_result['products']) >= 5:
                # Успех на уровне 1!
                result['success'] = True
                result['final_method'] = 'level1_smart'
                result['products'] = level1_result['products']
                result['cascade_log'].append(f"✅ УРОВЕНЬ 1 УСПЕХ: {len(level1_result['products'])} товаров")
                logger.info(f"✅ Уровень 1 успешен: {len(level1_result['products'])} товаров")
                return result
            else:
                result['cascade_log'].append(f"⚠️ УРОВЕНЬ 1: {len(level1_result.get('products', []))} товаров (недостаточно)")
                
            # УРОВЕНЬ 2: БРУТФОРС АНАЛИЗ
            logger.info("🔥 УРОВЕНЬ 2: Брутфорс анализ")
            level2_result = self.level2_processor.process_complex_file(file_path, file_name)
            result['all_attempts']['level2'] = level2_result
            
            if level2_result['success'] and len(level2_result['products']) > 0:
                # Сравниваем результаты уровня 1 и 2
                level1_count = len(level1_result.get('products', []))
                level2_count = len(level2_result['products'])
                
                if level2_count > level1_count:
                    # Уровень 2 лучше
                    result['success'] = True
                    result['final_method'] = 'level2_bruteforce'
                    result['products'] = level2_result['products']
                    result['cascade_log'].append(f"✅ УРОВЕНЬ 2 ЛУЧШЕ: {level2_count} товаров (vs {level1_count})")
                    logger.info(f"✅ Уровень 2 лучше: {level2_count} vs {level1_count} товаров")
                    return result
                elif level1_count > 0:
                    # Уровень 1 все же лучше
                    result['success'] = True  
                    result['final_method'] = 'level1_smart'
                    result['products'] = level1_result['products']
                    result['cascade_log'].append(f"✅ УРОВЕНЬ 1 ЛУЧШЕ: {level1_count} товаров (vs {level2_count})")
                    return result
            else:
                result['cascade_log'].append(f"⚠️ УРОВЕНЬ 2: {len(level2_result.get('products', []))} товаров")
                
            # УРОВЕНЬ 3: ПОЛНЫЙ LLM АНАЛИЗ (пока заглушка)
            logger.info("🔥 УРОВЕНЬ 3: Полный LLM анализ")
            result['cascade_log'].append("⏳ УРОВЕНЬ 3: Требует реализации")
            
            # Если дошли сюда - берем лучший результат из уровней 1-2
            best_result = self._select_best_overall_result(level1_result, level2_result)
            if best_result:
                result['success'] = True
                result['final_method'] = best_result['method']
                result['products'] = best_result['products']
                result['cascade_log'].append(f"✅ ЛУЧШИЙ РЕЗУЛЬТАТ: {best_result['method']} - {len(best_result['products'])} товаров")
            else:
                result['cascade_log'].append("❌ ВСЕ УРОВНИ НЕУСПЕШНЫ")
                
        except Exception as e:
            logger.error(f"Ошибка каскадной обработки: {e}")
            result['cascade_log'].append(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
            
        return result
    
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
        
    def get_cascade_summary(self, result: Dict) -> str:
        """Генерация сводки по каскадной обработке"""
        if not result:
            return "❌ Нет результатов обработки"
            
        summary = []
        summary.append(f"🎯 КАСКАДНАЯ ОБРАБОТКА")
        summary.append(f"Финальный метод: {result.get('final_method', 'неизвестен')}")
        summary.append(f"Товаров найдено: {len(result.get('products', []))}")
        summary.append(f"Статус: {'✅ УСПЕХ' if result['success'] else '❌ НЕУДАЧА'}")
        summary.append("")
        
        summary.append("Лог обработки:")
        for log_entry in result.get('cascade_log', []):
            summary.append(f"  {log_entry}")
            
        return "\n".join(summary) 