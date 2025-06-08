import logging
import os
from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
import uvicorn
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла нужна для API ключей и настроек
load_dotenv()

from data_loader import DataLoader
from query_processor import QueryProcessor
from proposal_generator import ProposalGenerator
from sender import EmailSender
from cache import QueryCache
from logger import setup_logger

# Настройка логирования нужна для отслеживания ошибок в продакшене
logger = setup_logger()

app = FastAPI(title="Commercial Proposal Generator", 
              description="AI-based commercial proposal generator for ARMASETI IMPORT LLC")

# Проверка API ключа нужна для защиты от несанкционированного доступа
def verify_api_key(authorization: str = Header(...)):
    api_key = "your-api-key"  # В продакшене должен храниться в переменных окружения
    if authorization != f"Bearer {api_key}":
        logger.warning(f"Попытка несанкционированного доступа")
        raise HTTPException(status_code=401, detail="Неверный API ключ")
    return True

# Модель запроса определяет структуру входных данных для валидации
class QueryRequest(BaseModel):
    query: str
    email: Optional[str] = None

# Глобальная инициализация сервисов позволяет переиспользовать соединения и кэш
data_loader = DataLoader()
query_cache = QueryCache()
query_processor = QueryProcessor(data_loader, query_cache)
proposal_generator = ProposalGenerator()
email_sender = EmailSender()

@app.post("/generate-proposal")
async def generate_proposal(request: QueryRequest, authorized: bool = Depends(verify_api_key)):
    try:
        logger.info(f"Получен запрос: {request.query}")
        
        # Обработка запроса через ИИ для поиска подходящих товаров
        products = query_processor.process_query(request.query)
        
        # Генерация Excel файла с коммерческим предложением
        proposal_path = proposal_generator.generate(products)
        
        # Отправка КП клиенту через email при указании адреса
        if request.email:
            email_sender.send(proposal_path, request.email)
        
        return {"status": "success", "products_found": len(products), "proposal_path": proposal_path}
    
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса: {str(e)}")
        # HTTPException нужен для корректного возврата ошибки клиенту
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")

@app.post("/upload-price-list")
async def upload_price_list(file_path: str, authorized: bool = Depends(verify_api_key)):
    try:
        logger.info(f"Загрузка прайс-листа из: {file_path}")
        data_loader.load_price_list(file_path)
        # Очистка кэша нужна для актуализации результатов поиска
        query_cache.clear()
        return {"status": "success", "message": "Прайс-лист успешно загружен"}
    
    except Exception as e:
        logger.error(f"Ошибка при загрузке прайс-листа: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки: {str(e)}")

if __name__ == "__main__":
    # Запуск сервера в debug режиме для разработки
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 