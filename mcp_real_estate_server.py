"""
MCP Server do wyszukiwania nieruchomości z wykorzystaniem FastMCP
Integruje się z systemem hybrid_search.py
"""

from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
from typing import Dict, List
import os
import logging
import json
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
import asyncio

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from hybrid_search import hybrid_search
from real_estate_vector_db import RealEstateVectorDB

# Ustawienia serwera
PORT = os.environ.get("PORT", 10000)

# Tworzymy FastAPI app для HTTP endpoints
app = FastAPI(title="Real Estate MCP Server")

# Tworzymy serwer MCP
mcp = FastMCP("real-estate-search", host="0.0.0.0", port=PORT)

# Inicjalizujemy bazę danych wektorową
vector_db = RealEstateVectorDB()

@mcp.tool()
def search_real_estate(query: str, max_results: int = 10) -> List[Dict]:
    """
    Wyszukiwanie nieruchomości w języku naturalnym.
    
    Obsługuje wyszukiwanie mieszkań, domów, pokoi do wynajęcia i sprzedaży w Polsce.
    Używa wyszukiwania hybrydowego (MongoDB + wyszukiwanie semantyczne).
    
    Args:
        query: Zapytanie w języku naturalnym, np.: 
               'chcę kupić 2-pokojowe mieszkanie w Warszawie do 500000 zł' 
               lub 'szukam mieszkania do wynajęcia na Mokotowie'
        
    Returns:
        Lista znalezionych ogłoszeń ze szczegółowymi informacjami
    """
    try:
        logger.info(f"Wykonywanie wyszukiwania: {query}")
        
        # Wykonujemy wyszukiwanie hybrydowe
        search_results = hybrid_search(query)
        
        if not search_results or search_results is None:
            return [{
                "error": "Nie znaleziono wyników dla Twojego zapytania",
                "message": "Spróbuj zmienić kryteria wyszukiwania"
            }]
        
        # Pobieramy końcowe wyniki z hybrid_search
        results = search_results.get("final_results", [])
        
        if not results:
            return [{
                "error": "Nie znaleziono wyników dla Twojego zapytania",
                "message": "Spróbuj zmienić kryteria wyszukiwania"
            }]
        
        # Ograniczamy liczbę wyników
        
        
        # Formatujemy wyniki dla MCP
        formatted_results = []
        for result in results:
            formatted_result = {
                "title": result.get('title', 'Bez tytułu'),
                "price": result.get('price', 'Nie podana'),
                "room_count": result.get('room_count', 'Nie podane'),
                "space_sm": result.get('space_sm', 'Nie podana'),
                "city": result.get('city', 'Nie podane'),
                "district": result.get('district', 'Nie podane'),
                "link": result.get('link', ''),
                "score": result.get('score', 0),
                "description": result.get('description', '')[:200] + "..." if result.get('description') else ""
            }
            formatted_results.append(formatted_result)
        
        return formatted_results
        
    except Exception as e:
        logger.error(f"Błąd podczas wyszukiwania: {e}")
        return [{
            "error": "Wystąpił błąd podczas wyszukiwania",
            "message": str(e)
        }]

@mcp.tool()
def get_database_stats() -> Dict:
    """
    Pobierz statystyki bazy danych nieruchomości.
    
    Returns:
        Słownik ze statystykami bazy danych
    """
    try:
        stats = vector_db.get_stats()
        
        return {
            "total_listings": stats.get('total', 0),
            "rent_listings": stats.get('rent', 0),
            "sale_listings": stats.get('sale', 0),
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Błąd podczas pobierania statystyk: {e}")
        return {
            "error": "Wystąpił błąd podczas pobierania statystyk",
            "message": str(e),
            "status": "error"
        }

# HTTP endpoints для Cursor
@app.get("/")
async def root():
    return {"message": "Real Estate MCP Server", "status": "running"}

@app.get("/sse")
async def sse_endpoint():
    """SSE endpoint для Cursor MCP"""
    async def event_generator():
        # Отправляем heartbeat
        yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': '2025-01-01T00:00:00Z'})}\n\n"
        
        # Ждем запросы
        while True:
            await asyncio.sleep(30)  # Heartbeat каждые 30 секунд
            yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': '2025-01-01T00:00:00Z'})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
        }
    )

@app.post("/messages")
async def handle_mcp_message(request: Request):
    """Обработка MCP сообщений (безопасная для ошибок парсинга JSON)."""
    try:
        # Прочитаем сырой запрос для логов и стабильного парсинга
        raw_bytes = await request.body()
        try:
            body = await request.json()
        except Exception as parse_err:
            logger.error(
                f"Invalid JSON in /messages: {parse_err}; raw={raw_bytes!r}"
            )
            return {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": "Parse error"},
            }

        method = body.get("method")

        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": body.get("id"),
                "result": {
                    "tools": [
                        {
                            "name": "search_real_estate",
                            "description": "Wyszukiwanie nieruchomości w języku naturalnym",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "query": {
                                        "type": "string",
                                        "description": "Zapytanie w języku naturalnym",
                                    }
                                },
                                "required": ["query"],
                            },
                        },
                        {
                            "name": "get_database_stats",
                            "description": "Pobierz statystyki bazy danych",
                            "parameters": {"type": "object", "properties": {}},
                        },
                    ]
                },
            }

        elif method == "tool/call":
            tool_name = body.get("params", {}).get("toolName")
            tool_args = body.get("params", {}).get("args", {})

            if tool_name == "search_real_estate":
                result = search_real_estate(**tool_args)
            elif tool_name == "get_database_stats":
                result = get_database_stats()
            else:
                result = {"error": f"Unknown tool: {tool_name}"}

            return {
                "jsonrpc": "2.0",
                "id": body.get("id"),
                "result": {"output": result},
            }

        else:
            return {
                "jsonrpc": "2.0",
                "id": body.get("id"),
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            }

    except Exception as e:
        logger.error(f"Error handling MCP message: {e}", exc_info=True)
        # Аккуратно пытаемся вернуть id, если тело всё же было распознано ранее
        safe_id = None
        try:
            safe_id = body.get("id")  # type: ignore[name-defined]
        except Exception:
            pass
        return {
            "jsonrpc": "2.0",
            "id": safe_id,
            "error": {"code": -32603, "message": str(e)},
        }

# Uruchamiamy serwer
if __name__ == "__main__":
    import uvicorn
    logger.info(f"Uruchamianie serwera MCP do wyszukiwania nieruchomości na porcie {PORT}")
    
    # Запускаем FastAPI сервер
    uvicorn.run(app, host="0.0.0.0", port=PORT)
