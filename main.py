import logging
from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
import uvicorn
from typing import Optional, List, Dict, Any

from data_loader import DataLoader
from query_processor import QueryProcessor
from proposal_generator import ProposalGenerator
from sender import EmailSender, TelegramSender
from cache import QueryCache
from logger import setup_logger

# Initialize logging
logger = setup_logger()

app = FastAPI(title="Commercial Proposal Generator", 
              description="AI-based commercial proposal generator for ARMASETI IMPORT LLC")

# API key validation
def verify_api_key(authorization: str = Header(...)):
    api_key = "your-api-key"  # In production, store this securely
    if authorization != f"Bearer {api_key}":
        logger.warning(f"Unauthorized access attempt")
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# Request model
class QueryRequest(BaseModel):
    query: str
    email: Optional[str] = None
    telegram_id: Optional[str] = None

# Initialize services
data_loader = DataLoader()
query_cache = QueryCache()
query_processor = QueryProcessor(data_loader, query_cache)
proposal_generator = ProposalGenerator()
email_sender = EmailSender()
telegram_sender = TelegramSender()

@app.post("/generate-proposal")
async def generate_proposal(request: QueryRequest, authorized: bool = Depends(verify_api_key)):
    try:
        logger.info(f"Received query: {request.query}")
        
        # Process query and find products
        products = query_processor.process_query(request.query)
        
        # Generate proposal
        proposal_path = proposal_generator.generate(products)
        
        # Send proposal if needed
        if request.email:
            email_sender.send(proposal_path, request.email)
        
        if request.telegram_id:
            telegram_sender.send(proposal_path, request.telegram_id)
        
        return {"status": "success", "products_found": len(products), "proposal_path": proposal_path}
    
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload-price-list")
async def upload_price_list(file_path: str, authorized: bool = Depends(verify_api_key)):
    try:
        logger.info(f"Uploading price list from: {file_path}")
        data_loader.load_price_list(file_path)
        query_cache.clear()
        return {"status": "success", "message": "Price list uploaded successfully"}
    
    except Exception as e:
        logger.error(f"Error uploading price list: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 