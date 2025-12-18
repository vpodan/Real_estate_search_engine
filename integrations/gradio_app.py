"""
Gradio приложение для поиска недвижимости в Варшаве
Работает БЕЗ MongoDB - использует JSON файлы + ChromaDB
"""
import gradio as gr
import json
import os
from typing import List, Dict
import pandas as pd

# Попытка импорта (для локального запуска с ChromaDB)
try:
    from src.real_estate_vector_db import RealEstateVectorDB
    USE_VECTOR_DB = True
except:
    USE_VECTOR_DB = False

# Загрузка данных из JSON файлов
def load_data():
    """Загружает данные из JSON файлов"""
    rent_data = []
    sale_data = []
    
    if os.path.exists("rent_listings.json"):
        with open("rent_listings.json", "r", encoding="utf-8") as f:
            for line in f:
                try:
                    rent_data.append(json.loads(line))
                except:
                    pass
    
    if os.path.exists("sale_listings.json"):
        with open("sale_listings.json", "r", encoding="utf-8") as f:
            for line in f:
                try:
                    sale_data.append(json.loads(line))
                except:
                    pass
    
    return rent_data + sale_data

# Загружаем данные при старте
ALL_LISTINGS = load_data()

# Инициализация vector DB (если доступна)
if USE_VECTOR_DB:
    try:
        vector_db = RealEstateVectorDB()
    except:
        vector_db = None
else:
    vector_db = None

def simple_search(query: str, listings: List[Dict], max_results: int = 10) -> List[Dict]:
    """
    Простой поиск по ключевым словам
    """
    query_lower = query.lower()
    keywords = query_lower.split()
    
    scored_results = []
    
    for listing in listings:
        score = 0
        text = f"{listing.get('title', '')} {listing.get('description', '')} {listing.get('district', '')}".lower()
        
        # Подсчет совпадений ключевых слов
        for keyword in keywords:
            if keyword in text:
                score += text.count(keyword)
        
        if score > 0:
            listing['score'] = score
            scored_results.append(listing)
    
    # Сортировка по релевантности
    scored_results.sort(key=lambda x: x['score'], reverse=True)
    
    return scored_results[:max_results]

def extract_filters(query: str) -> Dict:
    """
    Простое извлечение фильтров из запроса
    """
    query_lower = query.lower()
    filters = {}
    
    # Тип транзакции
    if "wynajem" in query_lower or "аренд" in query_lower or "rent" in query_lower:
        filters['type'] = 'rent'
    elif "sprzedaż" in query_lower or "купи" in query_lower or "sale" in query_lower:
        filters['type'] = 'sale'
    
    # Количество комнат
    if "2 pok" in query_lower or "2 комн" in query_lower or "2 room" in query_lower or "dwupokojowe" in query_lower:
        filters['rooms'] = 2
    elif "3 pok" in query_lower or "3 комн" in query_lower or "3 room" in query_lower:
        filters['rooms'] = 3
    elif "kawalerka" in query_lower or "studio" in query_lower:
        filters['rooms'] = 1
    
    # Цена (очень упрощенно)
    import re
    price_match = re.search(r'do\s+(\d+)', query_lower)
    if price_match:
        filters['max_price'] = int(price_match.group(1))
    
    # Район
    districts = ["mokotów", "praga", "bielany", "wilanów", "wola", "ursynów", "śródmieście", "centrum"]
    for district in districts:
        if district in query_lower:
            filters['district'] = district.capitalize()
            break
    
    return filters

def filter_listings(listings: List[Dict], filters: Dict) -> List[Dict]:
    """
    Фильтрация по критериям
    """
    filtered = listings
    
    if 'type' in filters:
        # Определяем по source_collection или по цене
        if filters['type'] == 'rent':
            filtered = [l for l in filtered if 
                       l.get('source_collection') == 'rent_listings' or 
                       (l.get('price', 999999) < 10000)]
        else:
            filtered = [l for l in filtered if 
                       l.get('source_collection') == 'sale_listings' or 
                       (l.get('price', 0) > 10000)]
    
    if 'rooms' in filters:
        filtered = [l for l in filtered if l.get('room_count') == filters['rooms']]
    
    if 'max_price' in filters:
        filtered = [l for l in filtered if l.get('price', 999999999) <= filters['max_price']]
    
    if 'district' in filters:
        district_lower = filters['district'].lower()
        filtered = [l for l in filtered if district_lower in l.get('district', '').lower()]
    
    return filtered

def search_real_estate(query: str, max_results: int = 5):
    """
    Главная функция поиска
    """
    if not query or not query.strip():
        return "Введите запрос для поиска", None
    
    try:
        # 1. Извлекаем фильтры
        filters = extract_filters(query)
        
        # 2. Фильтруем данные
        filtered_listings = filter_listings(ALL_LISTINGS, filters)
        
        # 3. Поиск по ключевым словам
        if vector_db:
            # Используем vector search если доступен
            try:
                results = vector_db.semantic_search(query, top_k=max_results)
            except:
                results = simple_search(query, filtered_listings, max_results)
        else:
            results = simple_search(query, filtered_listings, max_results)
        
        if not results:
            return "Ничего не найдено. Попробуйте изменить запрос.", None
        
        # Форматируем вывод
        output_text = f"**Найдено: {len(filtered_listings)} после фильтрации**\n\n"
        output_text += f"**Показаны топ-{len(results)} результатов:**\n\n"
        
        table_data = []
        
        for i, result in enumerate(results[:max_results], 1):
            title = result.get('title', 'Без названия')
            price = result.get('price', 0)
            rooms = result.get('room_count', 'N/A')
            space = result.get('space_sm', 'N/A')
            district = result.get('district', 'N/A')
            city = result.get('city', 'Warszawa')
            link = result.get('link', 'N/A')
            
            output_text += f"### {i}. {title}\n"
            output_text += f"- **Цена:** {price:,.0f} zł\n"
            output_text += f"- **Комнаты:** {rooms}, **Площадь:** {space} m²\n"
            output_text += f"- **Район:** {district}, {city}\n"
            output_text += f"- **Ссылка:** [{link}]({link})\n\n"
            
            table_data.append({
                "Название": title[:50] + "..." if len(title) > 50 else title,
                "Цена (zł)": f"{price:,.0f}",
                "Комнаты": rooms,
                "Площадь (m²)": space,
                "Район": district,
            })
        
        df = pd.DataFrame(table_data)
        return output_text, df
        
    except Exception as e:
        return f"Ошибка поиска: {str(e)}", None

def get_stats():
    """Статистика базы данных"""
    total = len(ALL_LISTINGS)
    rent = sum(1 for l in ALL_LISTINGS if l.get('source_collection') == 'rent_listings' or l.get('price', 999999) < 10000)
    sale = total - rent
    
    stats_text = f"""
## Статистика базы данных

- **Всего объявлений:** {total:,}
- **Аренда:** {rent:,}
- **Продажа:** {sale:,}
- **Источник:** Otodom.pl
"""
    return stats_text

# Примеры
examples = [
    ["2 pokoje Mokotów do 5000 zł wynajem"],
    ["mieszkanie na sprzedaż Praga-Południe do 850000 zł"],
    ["kawalerka wynajem centrum do 3000 zł"],
]

# Gradio UI
with gr.Blocks(title="🏠 Поиск недвижимости в Варшаве", theme=gr.themes.Soft()) as demo:
    gr.Markdown("""
    # 🏠 Поиск недвижимости в Варшаве
    
    **Keyword Search + Filters** (или Vector Search если ChromaDB доступна)
    
    Введите запрос для поиска квартир в Варшаве.
    """)
    
    with gr.Tab("🔍 Поиск"):
        with gr.Row():
            with gr.Column(scale=3):
                query_input = gr.Textbox(
                    label="Запрос",
                    placeholder="Например: 2 pokoje Mokotów do 5000 zł wynajem",
                    lines=2
                )
            with gr.Column(scale=1):
                max_results = gr.Slider(1, 10, 5, step=1, label="Макс. результатов")
        
        search_btn = gr.Button("🔍 Искать", variant="primary", size="lg")
        
        output_text = gr.Markdown()
        output_table = gr.DataFrame(label="Результаты")
        
        gr.Examples(examples=examples, inputs=[query_input])
        
        search_btn.click(
            fn=search_real_estate,
            inputs=[query_input, max_results],
            outputs=[output_text, output_table]
        )
    
    with gr.Tab("📊 Статистика"):
        stats_output = gr.Markdown()
        stats_btn = gr.Button("Обновить")
        stats_btn.click(fn=get_stats, outputs=[stats_output])
        demo.load(fn=get_stats, outputs=[stats_output])
    
    gr.Markdown("""
    ---
    **Tech:** Python • JSON Data • Keyword Search • Gradio
    """)

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)














