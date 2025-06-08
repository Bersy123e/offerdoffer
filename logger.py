import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional

def setup_logger(
    name: str = "commercial_proposal", 
    level: int = logging.INFO, 
    log_file: str = "app.log",
    max_bytes: int = 10485760,  # 10МБ
    backup_count: int = 5,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) -> logging.Logger:
    """
    Настройка логгера нужна для отслеживания работы системы и ошибок.
    
    Args:
        name: Имя логгера
        level: Уровень логирования
        log_file: Путь к файлу логов
        max_bytes: Максимальный размер файла лога в байтах
        backup_count: Количество резервных файлов
        log_format: Формат сообщений лога
        
    Returns:
        Настроенный логгер
    """
    # Создание папки логов нужно для избежания ошибок записи
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Получение или создание логгера
    logger = logging.getLogger(name)
    
    # Проверка конфигурации предотвращает дублирование обработчиков
    if not logger.handlers:
        logger.setLevel(level)
        
        # Форматтер нужен для единообразного вида сообщений
        formatter = logging.Formatter(log_format)
        
        # Консольный вывод нужен для мониторинга в реальном времени
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Файловый вывод с UTF-8 нужен для сохранения истории и русских символов
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=max_bytes, 
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"Логгер {name} инициализирован")
    
    return logger

class LoggerManager:
    """Менеджер для создания и получения логгеров с централизованным управлением."""
    
    _loggers: Dict[str, logging.Logger] = {}
    
    @classmethod
    def get_logger(
        cls,
        name: str = "commercial_proposal",
        level: int = logging.INFO,
        log_file: Optional[str] = None
    ) -> logging.Logger:
        """
        Получение или создание логгера с кэшированием для производительности.
        
        Args:
            name: Имя логгера
            level: Уровень логирования
            log_file: Путь к файлу логов (по умолчанию: logs/{name}.log)
            
        Returns:
            Настроенный логгер
        """
        if name not in cls._loggers:
            log_file = log_file or f"logs/{name}.log"
            cls._loggers[name] = setup_logger(
                name=name,
                level=level,
                log_file=log_file
            )
        
        return cls._loggers[name] 