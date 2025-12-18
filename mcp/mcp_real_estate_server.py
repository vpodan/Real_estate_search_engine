"""
MCP Server do wyszukiwania nieruchomości z wykorzystaniem FastMCP
Integruje się z systemem hybrid_search.py
"""

from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
from typing import Dict, List
import os
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from src.hybrid_search import hybrid_search
from src.real_estate_vector_db import RealEstateVectorDB

# Check required environment variables
if "OPENAI_API_KEY" not in os.environ:
    raise Exception("OPENAI_API_KEY environment variable not set")

if "MONGODB_URI" not in os.environ:
    logger.warning("MONGODB_URI not set, using default: mongodb://mongo:27017")

# Ustawienia serwera
PORT = os.environ.get("PORT", 10000)

# Tworzymy serwer MCP
mcp = FastMCP("real-estate-search", host="0.0.0.0", port=PORT)

# Inicjalizujemy bazę danych wektorową
vector_db = RealEstateVectorDB()

# Add search tool
@mcp.tool()
def search_real_estate(query: str) -> List[Dict]:
    """
    Search for real estate listings in Warsaw using natural language.
    
    Supports searching for apartments, houses, rooms for rent and sale.
    Uses hybrid search (MongoDB + semantic vector search).
    
    Args:
        query: Natural language search query, e.g.:
               'two bedroom apartment in Warsaw under 3000 PLN'
               'apartment for rent in Mokotów district'
    
    Returns:
        List of real estate listings with detailed information.
    """
    try:
        logger.info(f"Searching for: {query}")
        
        # Perform hybrid search
        search_results = hybrid_search(query)
        
        if not search_results or not search_results.get("final_results"):
            return [{
                "error": "No results found",
                "message": "Try different search criteria"
            }]
        
        # Format results for MCP
        results = search_results["final_results"][:10]  # Limit to 10 results
        formatted_results = []
        
        for result in results:
            formatted_results.append({
                "title": result.get('title', 'No title'),
                "price": result.get('price', 'Not specified'),
                "room_count": result.get('room_count', 'Not specified'),
                "space_sm": result.get('space_sm', 'Not specified'),
                "city": result.get('city', 'Not specified'),
                "district": result.get('district', 'Not specified'),
                "link": result.get('link', ''),
                "score": result.get('score', 0),
                "description": (result.get('description', '')[:200] + "...") if result.get('description') else ""
            })
        
        return formatted_results
        
    except Exception as e:
        logger.error(f"Search error: {e}", exc_info=True)
        return [{"error": "Search failed", "message": str(e)}]

# Add stats tool
@mcp.tool()
def get_database_stats() -> Dict:
    """
    Get statistics about the real estate database.
    
    Returns:
        Dictionary with database statistics including total listings,
        rent listings count, and sale listings count.
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
        logger.error(f"Stats error: {e}", exc_info=True)
        return {
            "error": "Failed to get stats",
            "message": str(e),
            "status": "error"
        }

# Run the server
if __name__ == "__main__":
    logger.info(f"Starting Real Estate MCP Server on port {PORT}")
    mcp.run(transport="streamable-http")
