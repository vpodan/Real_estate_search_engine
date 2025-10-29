"""
MCP Server для поиска недвижимости - STDIO версия для Cursor
"""
from mcp.server.stdio import stdio_server
from mcp.server import Server
from mcp.types import Tool, TextContent
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from hybrid_search import hybrid_search
from real_estate_vector_db import RealEstateVectorDB

# Создаём MCP сервер
server = Server("real-estate-search")

# Инициализируем базу данных
vector_db = RealEstateVectorDB()

@server.list_tools()
async def list_tools() -> list[Tool]:
    """Список доступных инструментов"""
    return [
        Tool(
            name="search_real_estate",
            description="Поиск недвижимости в Варшаве. Поддерживает запросы на естественном языке на русском, польском и английском.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Запрос на естественном языке, например: 'двухкомнатная квартира в центре до 3000 злотых'"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_database_stats",
            description="Получить статистику базы данных объявлений",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Вызов инструмента"""
    try:
        if name == "search_real_estate":
            query = arguments.get("query", "")
            logger.info(f"Поиск: {query}")
            
            # Выполняем поиск
            results = hybrid_search(query)
            
            if not results or not results.get("final_results"):
                return [TextContent(
                    type="text",
                    text="Не найдено объявлений по вашему запросу. Попробуйте изменить критерии поиска."
                )]
            
            # Форматируем результаты
            formatted_results = []
            for i, result in enumerate(results["final_results"][:5], 1):
                text = f"""
{i}. {result.get('title', 'Без названия')}
   💰 Цена: {result.get('price', 'н/д')} zł
   🏠 Комнат: {result.get('room_count', 'н/д')}
   📐 Площадь: {result.get('space_sm', 'н/д')} м²
   📍 Адрес: {result.get('city', '')}, {result.get('district', '')}
   🔗 Ссылка: {result.get('link', '')}
   📊 Релевантность: {result.get('score', 0):.2f}
"""
                formatted_results.append(text)
            
            final_text = f"Найдено {len(results['final_results'])} объявлений:\n" + "\n".join(formatted_results)
            
            return [TextContent(type="text", text=final_text)]
            
        elif name == "get_database_stats":
            stats = vector_db.get_stats()
            text = f"""
📊 Статистика базы данных:
   Всего объявлений: {stats.get('total', 0)}
   На аренду: {stats.get('rent', 0)}
   На продажу: {stats.get('sale', 0)}
"""
            return [TextContent(type="text", text=text)]
            
        else:
            return [TextContent(
                type="text",
                text=f"Неизвестный инструмент: {name}"
            )]
            
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return [TextContent(
            type="text",
            text=f"Ошибка при выполнении: {str(e)}"
        )]

async def main():
    """Запуск сервера через stdio"""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

