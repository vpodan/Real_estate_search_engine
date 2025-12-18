"""
Полная версия для Hugging Face Spaces с MongoDB Atlas + ChromaDB
Гибридный поиск: MongoDB фильтрация + Semantic Search
Использует OpenAI Function Calling для извлечения критериев
"""
import gradio as gr
import json
import os
from typing import List, Dict, Optional
import pandas as pd
import logging
import ssl
import certifi
from openai import OpenAI

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("🚀 Инициализация приложения (Full Version)...")

# === КОНФИГУРАЦИЯ ===
USE_MONGODB = False
USE_CHROMADB = False
mongo_db = None
vector_db = None

# === ПОДКЛЮЧЕНИЕ К MONGODB ATLAS ===
if os.getenv("MONGODB_URI"):
    try:
        print("🔗 Подключение к MongoDB Atlas...")
        from pymongo import MongoClient
        from pymongo.server_api import ServerApi
        
        # SSL настройки для MongoDB Atlas
        mongodb_uri = os.getenv("MONGODB_URI")
        
        # Добавляем SSL параметры если их нет
        if "tls=true" not in mongodb_uri and "ssl=true" not in mongodb_uri:
            separator = "&" if "?" in mongodb_uri else "?"
            mongodb_uri = f"{mongodb_uri}{separator}tls=true&tlsAllowInvalidCertificates=false"
        
        client = MongoClient(
            mongodb_uri,
            server_api=ServerApi('1'),
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            socketTimeoutMS=10000,
            tls=True,
            tlsCAFile=certifi.where(),
            tlsAllowInvalidCertificates=False
        )
        
        # Проверка подключения
        client.admin.command('ping')
        
        # Получаем базу данных
        db_name = os.getenv("MONGODB_DB", "real_estate")
        mongo_db = client[db_name]
        
        # Проверяем коллекции
        rent_count = mongo_db.rent_listings.count_documents({})
        sale_count = mongo_db.sale_listings.count_documents({})
        
        USE_MONGODB = True
        print(f"✅ MongoDB Atlas подключена!")
        print(f"   - База: {db_name}")
        print(f"   - Аренда: {rent_count:,} объявлений")
        print(f"   - Продажа: {sale_count:,} объявлений")
        
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к MongoDB: {e}")
        print(f"⚠️ MongoDB недоступна: {e}")
        USE_MONGODB = False

# === FALLBACK: ЗАГРУЗКА JSON ФАЙЛОВ ===
ALL_LISTINGS = []

if not USE_MONGODB:
    print("📂 Загрузка данных из JSON файлов...")
    
    # Debug информация
    current_dir = os.getcwd()
    print(f"📁 Текущая директория: {current_dir}")
    
    try:
        files_in_dir = os.listdir(current_dir)
        print(f"📂 Файлы в директории: {files_in_dir}")
    except Exception as e:
        print(f"⚠️ Не удалось получить список файлов: {e}")
    
    for filename in ["rent_listings.json", "sale_listings.json"]:
        filepath = os.path.join(current_dir, filename)
        print(f"🔍 Проверяем: {filepath}")
        
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    
                    if not content:
                        print(f"⚠️ Файл {filename} пустой")
                        continue
                    
                    # Определяем формат: JSON Array или JSON Lines
                    if content.startswith('['):
                        # JSON Array формат: [{...}, {...}]
                        print(f"📋 Формат: JSON Array")
                        data = json.loads(content)
                        count = 0
                        for doc in data:
                            # Добавляем source_collection для определения типа
                            if "rent" in filename:
                                doc['source_collection'] = 'rent_listings'
                            else:
                                doc['source_collection'] = 'sale_listings'
                            ALL_LISTINGS.append(doc)
                            count += 1
                    else:
                        # JSON Lines формат: {...}\n{...}\n
                        print(f"📋 Формат: JSON Lines")
                        count = 0
                        for line in content.split('\n'):
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                doc = json.loads(line)
                                # Добавляем source_collection для определения типа
                                if "rent" in filename:
                                    doc['source_collection'] = 'rent_listings'
                                else:
                                    doc['source_collection'] = 'sale_listings'
                                ALL_LISTINGS.append(doc)
                                count += 1
                            except json.JSONDecodeError as e:
                                print(f"⚠️ Ошибка парсинга строки: {e}")
                                continue
                    
                print(f"✅ Загружено {count} объявлений из {filename}")
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки {filename}: {e}")
                print(f"❌ Детали ошибки: {e}")
        else:
            print(f"⚠️ Файл не найден: {filepath}")

print(f"📊 Всего загружено: {len(ALL_LISTINGS)} объявлений")

# === ИНИЦИАЛИЗАЦИЯ CHROMADB ===
try:
    print("🔍 Инициализация ChromaDB...")
    
    # Распаковка архива если есть
    import tarfile
    if not os.path.exists("chroma_real_estate") and os.path.exists("chroma_real_estate.tar.gz"):
        print("📦 Распаковка ChromaDB архива...")
        with tarfile.open("chroma_real_estate.tar.gz", "r:gz") as tar:
            tar.extractall(".")
        print("✅ ChromaDB распакована")
    
    from real_estate_vector_db import RealEstateVectorDB
    vector_db = RealEstateVectorDB()
    
    # Проверяем данные
    stats = vector_db.get_stats()
    if stats.get('total', 0) > 0:
        USE_CHROMADB = True
        print(f"✅ ChromaDB инициализирована: {stats.get('total', 0)} документов")
    else:
        print("⚠️ ChromaDB пуста")
        vector_db = None
        
except Exception as e:
    logger.error(f"❌ Ошибка инициализации ChromaDB: {e}")
    print(f"⚠️ ChromaDB недоступна: {e}")
    USE_CHROMADB = False
    vector_db = None

# === OPENAI CLIENT ===
openai_client = None
if os.getenv("OPENAI_API_KEY"):
    try:
        openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        print("✅ OpenAI client инициализирован")
    except Exception as e:
        print(f"⚠️ Ошибка инициализации OpenAI: {e}")

# === ФУНКЦИИ ИЗВЛЕЧЕНИЯ КРИТЕРИЕВ С OPENAI ===

def get_openai_function_schema():
    """Схема функции для OpenAI Function Calling"""
    return {
        "name": "extract_search_criteria",
        "description": (
            "Извлекает ключевые критерии поиска недвижимости из запроса пользователя. "
            "Всегда возвращай все ключи, даже если информация не найдена - установи null. "
            "Определи, хочет ли пользователь купить или арендовать; если не указано - null."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "city": {
                    "type": ["string", "null"],
                    "description": "Город, например 'Warszawa'. Если не указан - null."
                },
                "district": {
                    "type": ["string", "null"],
                    "description": "Район/Osiedle, например 'Mokotów'. Если пользователь называет несколько районов (например 'Bemowo или Mokotów'), верни первый. Если не указан - null."
                },
                "districts": {
                    "type": ["array", "null"],
                    "items": {"type": "string"},
                    "description": "Список районов, если пользователь называет несколько (например ['Bemowo', 'Mokotów'] для 'Bemowo или Mokotów'). Если только один район или не указано - null."
                },
                "room_count": {
                    "type": ["integer", "null"],
                    "description": "Количество комнат (целое число), например 2. Если не указано - null."
                },
                "max_price": {
                    "type": ["integer", "null"],
                    "description": "Максимальная цена в zł (без 'zł'), например 850000. Если не указано - null."
                },
                "min_price": {
                    "type": ["integer", "null"],
                    "description": "Минимальная цена в zł. Если не указано - null."
                },
                "transaction_type": {
                    "type": ["string", "null"],
                    "description": (
                        "Тип транзакции: 'rent' (аренда/wynajem) или 'sale' (продажа/sprzedaż/kupno). "
                        "Если не указано - null."
                    )
                },
                "has_balcony": {
                    "type": ["boolean", "null"],
                    "description": "Есть ли балкон. true если упоминается 'balkon', 'loggia', 'taras'. false если явно сказано, что нет. Если не указано - null."
                },
                "has_parking": {
                    "type": ["boolean", "null"],
                    "description": "Есть ли парковка. true если упоминается 'parking', 'miejsce parkingowe'. false если явно сказано, что нет. Если не указано - null."
                },
                "has_garage": {
                    "type": ["boolean", "null"],
                    "description": "Есть ли гараж. true если упоминается 'garaż', 'garage', 'miejsce w garażu'. false если явно сказано, что нет. Если не указано - null."
                },
                "has_elevator": {
                    "type": ["boolean", "null"],
                    "description": "Есть ли лифт. true если упоминается 'winda', 'elevator'. false если явно сказано, что нет. Если не указано - null."
                },
                "floor": {
                    "type": ["integer", "null"],
                    "description": "Этаж, например 0=parter, 1, 2, ..., 10. Если не указано - null. ВНИМАНИЕ: Не интерпретируй семантические описания как 'высокий этаж', 'na górze' - это для semantic search, а не конкретные номера этажей."
                },
                "space_sm": {
                    "type": ["number", "null"],
                    "description": "Площадь в квадратных метрах, например 45.0. Если не указано - null."
                },
                "market_type": {
                    "type": ["string", "null"],
                    "description": "Тип рынка: 'PRIMARY' (первичный/pierwotny) или 'SECONDARY' (вторичный/wtórny). Если не указано - null."
                },
                "stan_wykonczenia": {
                    "type": ["string", "null"],
                    "description": "Состояние отделки: 'to_completion' (под отделку/do wykończenia) или 'ready_to_use' (готово к использованию/gotowe do użytku). Если не указано - null."
                },
                "min_build_year": {
                    "type": ["integer", "null"],
                    "description": "Минимальный год постройки (не старше чем X год), например 2010. Если не указано - null."
                },
                "max_build_year": {
                    "type": ["integer", "null"],
                    "description": "Максимальный год постройки (не новее чем X год), например 2020. Если не указано - null."
                },
                "building_material": {
                    "type": ["string", "null"],
                    "description": "Материал здания: 'breezeblock', 'brick' (cegła), 'concrete_plate', 'silikat', 'reinforced_concrete', 'wood'. Если не указано - null."
                },
                "building_type": {
                    "type": ["string", "null"],
                    "description": "Тип здания: 'block' (blok), 'apartment', 'tenement' (kamienica), 'infill'. Если не указано - null."
                },
                "ogrzewanie": {
                    "type": ["string", "null"],
                    "description": "Тип отопления: 'urban' (miejskie), 'gas' (gazowe), 'electrical' (elektryczne), 'boiler_room' (kotłownia). Если не указано - null."
                },
                "max_czynsz": {
                    "type": ["integer", "null"],
                    "description": "Максимальный czynsz в zł (только для аренды), например 500. Если не указано - null."
                },
                "has_air_conditioning": {
                    "type": ["boolean", "null"],
                    "description": "Есть ли кондиционер/klimatyzacja. true если упоминается 'klimatyzacja', 'air conditioning', 'klima'. false если явно сказано, что нет. Если не указано - null."
                },
                "pets_allowed": {
                    "type": ["boolean", "null"],
                    "description": "Разрешены ли животные. true если упоминается 'zwierzęta', 'pets', 'psy', 'koty'. false если явно сказано, что нет. Если не указано - null."
                },
                "furnished": {
                    "type": ["boolean", "null"],
                    "description": "Меблированная ли квартира. true если упоминается 'umeblowane', 'furnished', 'z meblami', 'меблирован'. false если 'nieumeblowane', 'bez mebli'. Если не указано - null."
                }
            },
            "required": []
        }
    }

def extract_criteria_from_query(query: str) -> Dict:
    """
    Извлекает критерии поиска из естественного запроса используя OpenAI Function Calling
    Аналогично main.py extract_criteria_from_prompt
    """
    # Если OpenAI недоступен - используем fallback (простой парсинг)
    if not openai_client:
        return extract_criteria_fallback(query)
    
    try:
        completion = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": query}],
            tools=[{
                "type": "function",
                "function": get_openai_function_schema()
            }],
            tool_choice="auto"
        )
        
        message = completion.choices[0].message
        
        # Если OpenAI вернул function call
        if message.tool_calls:
            tool_call = message.tool_calls[0]
            raw_args = tool_call.function.arguments
            
            try:
                args_dict = json.loads(raw_args)
            except json.JSONDecodeError:
                logger.error("Не удалось декодировать JSON из function call")
                return extract_criteria_fallback(query)
            
            # Формируем критерии (все поля как в main.py)
            criteria = {
                "city": args_dict.get("city"),
                "district": args_dict.get("district"),
                "districts": args_dict.get("districts"),
                "room_count": args_dict.get("room_count"),
                "max_price": args_dict.get("max_price"),
                "min_price": args_dict.get("min_price"),
                "transaction_type": args_dict.get("transaction_type"),
                "has_balcony": args_dict.get("has_balcony"),
                "has_parking": args_dict.get("has_parking"),
                "has_garage": args_dict.get("has_garage"),
                "has_elevator": args_dict.get("has_elevator"),
                "floor": args_dict.get("floor"),
                "space_sm": args_dict.get("space_sm"),
                # Новые критерии
                "market_type": args_dict.get("market_type"),
                "stan_wykonczenia": args_dict.get("stan_wykonczenia"),
                "min_build_year": args_dict.get("min_build_year"),
                "max_build_year": args_dict.get("max_build_year"),
                "building_material": args_dict.get("building_material"),
                "building_type": args_dict.get("building_type"),
                "ogrzewanie": args_dict.get("ogrzewanie"),
                "max_czynsz": args_dict.get("max_czynsz"),
                "has_air_conditioning": args_dict.get("has_air_conditioning"),
                "pets_allowed": args_dict.get("pets_allowed"),
                "furnished": args_dict.get("furnished"),
            }
            
            logger.info(f"✅ OpenAI извлек критерии: {criteria}")
            return criteria
        else:
            # OpenAI не вернул function call - используем fallback
            logger.warning("OpenAI не вернул function call, используем fallback")
            return extract_criteria_fallback(query)
            
    except Exception as e:
        logger.error(f"Ошибка OpenAI Function Calling: {e}")
        return extract_criteria_fallback(query)

def extract_criteria_fallback(query: str) -> Dict:
    """
    Fallback функция для извлечения критериев без OpenAI
    Простой парсинг по ключевым словам (как было раньше)
    """
    query_lower = query.lower()
    criteria = {}
    
    # Тип транзакции
    rent_keywords = ["wynajem", "wynająć", "rent", "аренд", "арендова"]
    if any(word in query_lower for word in rent_keywords):
        criteria['transaction_type'] = 'rent'
    
    sale_keywords = [
        "sprzedaż", "sprzedaz", "sprzedać", "sprzedac",
        "kupić", "kupic", "kupit", "kup",
        "chcę kupić", "chce kupic", "chochu kupit",
        "sale", "buy", "purchase",
        "продаж", "купи", "купит", "купить"
    ]
    if any(word in query_lower for word in sale_keywords):
        criteria['transaction_type'] = 'sale'
    
    # Количество комнат
    import re
    room_patterns = [
        (r'\b1\s*pok', 1), (r'kawalerka', 1), (r'studio', 1),
        (r'\b2\s*pok', 2), (r'dwupokojowe', 2),
        (r'\b3\s*pok', 3), (r'trzypokojowe', 3),
        (r'\b4\s*pok', 4), (r'czteropokojowe', 4),
    ]
    
    for pattern, rooms in room_patterns:
        if re.search(pattern, query_lower):
            criteria['room_count'] = rooms
            break
    
    # Цена
    price_match = re.search(r'do\s+(\d+)', query_lower)
    if price_match:
        criteria['max_price'] = int(price_match.group(1))
    
    price_match = re.search(r'od\s+(\d+)', query_lower)
    if price_match:
        criteria['min_price'] = int(price_match.group(1))
    
    # Район
    districts = [
        "mokotów", "praga", "bielany", "wilanów", "wola", 
        "ursynów", "śródmieście", "centrum", "ochota", "żoliborz",
        "bemowo", "włochy", "targówek", "rembertów", "wesoła",
        "białołęka", "ursus", "wawer"
    ]
    
    for district in districts:
        if district in query_lower:
            criteria['district'] = district.capitalize()
            break
    
    logger.info(f"⚠️ Fallback извлек критерии: {criteria}")
    return criteria

# === ПОИСК В MONGODB ===

def search_in_mongodb(criteria: Dict) -> List[Dict]:
    """Поиск в MongoDB по критериям"""
    if not USE_MONGODB or not mongo_db:
        return []
    
    try:
        # Определяем коллекцию
        transaction_type = criteria.get('transaction_type', 'rent')
        collection = mongo_db.rent_listings if transaction_type == 'rent' else mongo_db.sale_listings
        
        # Строим MongoDB query
        mongo_query = {}
        
        if 'room_count' in criteria:
            mongo_query['room_count'] = criteria['room_count']
        
        if 'max_price' in criteria:
            mongo_query['price'] = {'$lte': criteria['max_price']}
        
        if 'min_price' in criteria:
            if 'price' not in mongo_query:
                mongo_query['price'] = {}
            mongo_query['price']['$gte'] = criteria['min_price']
        
        if 'district' in criteria:
            # Регистронезависимый поиск по району
            mongo_query['district'] = {'$regex': criteria['district'], '$options': 'i'}
        
        # Выполняем поиск
        results = list(collection.find(mongo_query).limit(100))
        
        # Добавляем source_collection
        for result in results:
            result['source_collection'] = collection.name
        
        return results
        
    except Exception as e:
        logger.error(f"Ошибка поиска в MongoDB: {e}")
        return []

# === ПОИСК В JSON ===

def search_in_json(criteria: Dict) -> List[Dict]:
    """Поиск в загруженных JSON данных"""
    if not ALL_LISTINGS:
        return []
    
    filtered = ALL_LISTINGS.copy()
    
    # Фильтр по типу транзакции
    if 'transaction_type' in criteria:
        if criteria['transaction_type'] == 'rent':
            filtered = [l for l in filtered if 
                       l.get('source_collection') == 'rent_listings' or 
                       (l.get('price') is not None and l.get('price') < 15000)]
        else:
            filtered = [l for l in filtered if 
                       l.get('source_collection') == 'sale_listings' or 
                       (l.get('price') is not None and l.get('price') >= 15000)]
    
    # Фильтр по комнатам
    if criteria.get('room_count') is not None:
        filtered = [l for l in filtered if l.get('room_count') == criteria['room_count']]
    
    # Фильтр по цене (пропускаем если price = None)
    if criteria.get('max_price') is not None:
        filtered = [l for l in filtered if 
                   l.get('price') is not None and l.get('price') <= criteria['max_price']]
    
    if criteria.get('min_price') is not None:
        filtered = [l for l in filtered if 
                   l.get('price') is not None and l.get('price') >= criteria['min_price']]
    
    # Фильтр по району (поддержка множественных районов)
    if criteria.get('districts') and len(criteria['districts']) > 0:
        # Если указано несколько районов
        districts_lower = [d.lower() for d in criteria['districts']]
        filtered = [l for l in filtered if 
                   any(dist in str(l.get('district', '')).lower() for dist in districts_lower)]
    elif criteria.get('district'):
        # Если указан один район
        district_lower = criteria['district'].lower()
        filtered = [l for l in filtered if district_lower in str(l.get('district', '')).lower()]
    
    # Фильтр по балкону
    if criteria.get('has_balcony') is not None:
        filtered = [l for l in filtered if l.get('has_balcony') == criteria['has_balcony']]
    
    # Фильтр по парковке
    if criteria.get('has_parking') is not None:
        filtered = [l for l in filtered if l.get('has_parking') == criteria['has_parking']]
    
    # Фильтр по гаражу
    if criteria.get('has_garage') is not None:
        filtered = [l for l in filtered if l.get('has_garage') == criteria['has_garage']]
    
    # Фильтр по лифту
    if criteria.get('has_elevator') is not None:
        filtered = [l for l in filtered if l.get('has_elevator') == criteria['has_elevator']]
    
    # Фильтр по этажу
    if criteria.get('floor') is not None:
        filtered = [l for l in filtered if l.get('floor') == criteria['floor']]
    
    # Фильтр по площади
    if criteria.get('space_sm') is not None:
        filtered = [l for l in filtered if 
                   l.get('space_sm') is not None and l.get('space_sm') >= criteria['space_sm']]
    
    # Фильтр по типу рынка
    if criteria.get('market_type') is not None:
        filtered = [l for l in filtered if l.get('market_type') == criteria['market_type']]
    
    # Фильтр по состоянию отделки
    if criteria.get('stan_wykonczenia') is not None:
        filtered = [l for l in filtered if l.get('stan_wykonczenia') == criteria['stan_wykonczenia']]
    
    # Фильтр по году постройки
    if criteria.get('min_build_year') is not None or criteria.get('max_build_year') is not None:
        if criteria.get('min_build_year') is not None:
            filtered = [l for l in filtered if 
                       l.get('build_year') and int(l.get('build_year', 0)) >= criteria['min_build_year']]
        if criteria.get('max_build_year') is not None:
            filtered = [l for l in filtered if 
                       l.get('build_year') and int(l.get('build_year', 9999)) <= criteria['max_build_year']]
    
    # Фильтр по материалу здания
    if criteria.get('building_material') is not None:
        filtered = [l for l in filtered if l.get('building_material') == criteria['building_material']]
    
    # Фильтр по типу здания
    if criteria.get('building_type') is not None:
        filtered = [l for l in filtered if l.get('building_type') == criteria['building_type']]
    
    # Фильтр по типу отопления
    if criteria.get('ogrzewanie') is not None:
        filtered = [l for l in filtered if l.get('ogrzewanie') == criteria['ogrzewanie']]
    
    # Фильтр по czynsz (только для аренды)
    if criteria.get('max_czynsz') is not None:
        filtered = [l for l in filtered if 
                   l.get('czynsz') is not None and l.get('czynsz') <= criteria['max_czynsz']]
    
    # Фильтр по кондиционеру
    if criteria.get('has_air_conditioning') is not None:
        filtered = [l for l in filtered if l.get('has_air_conditioning') == criteria['has_air_conditioning']]
    
    # Фильтр по животным
    if criteria.get('pets_allowed') is not None:
        filtered = [l for l in filtered if l.get('pets_allowed') == criteria['pets_allowed']]
    
    # Фильтр по меблировке
    if criteria.get('furnished') is not None:
        filtered = [l for l in filtered if l.get('furnished') == criteria['furnished']]
    
    return filtered[:100]  # Ограничиваем 100 результатами

# === SEMANTIC SEARCH ===

def semantic_search_in_subset(query: str, filtered_listings: List[Dict], max_results: int = 10) -> List[Dict]:
    """Семантический поиск в отфильтрованных результатах"""
    if not USE_CHROMADB or not vector_db:
        return []
    
    try:
        # Получаем ID отфильтрованных объявлений
        filtered_ids = [str(l.get('_id')) for l in filtered_listings]
        
        if not filtered_ids:
            return []
        
        # Выполняем semantic search
        semantic_results = vector_db.semantic_search_in_subset(
            query=query,
            subset_ids=filtered_ids,
            top_k=max_results
        )
        
        # Объединяем с полными данными
        full_results = []
        for result in semantic_results:
            for listing in filtered_listings:
                if str(listing.get('_id')) == result['id']:
                    listing_copy = listing.copy()
                    listing_copy['semantic_score'] = result['score']
                    listing_copy['similarity'] = result.get('similarity', result['score'])
                    full_results.append(listing_copy)
                    break
        
        return full_results
        
    except Exception as e:
        logger.error(f"Ошибка semantic search: {e}")
        return []

# === KEYWORD SEARCH (FALLBACK) ===

def keyword_search(query: str, filtered_listings: List[Dict], max_results: int = 10) -> List[Dict]:
    """Простой поиск по ключевым словам"""
    query_lower = query.lower()
    keywords = query_lower.split()
    
    scored = []
    for listing in filtered_listings:
        score = 0
        text = f"{listing.get('title', '')} {listing.get('description', '')} {listing.get('district', '')}".lower()
        
        for keyword in keywords:
            if keyword in text:
                score += text.count(keyword)
        
        if score > 0:
            listing_copy = listing.copy()
            listing_copy['keyword_score'] = score
            scored.append(listing_copy)
    
    scored.sort(key=lambda x: x.get('keyword_score', 0), reverse=True)
    return scored[:max_results]

# === ГЛАВНАЯ ФУНКЦИЯ ПОИСКА ===

def hybrid_search_real_estate(query: str, max_results: int = 5):
    """
    Гибридный поиск:
    1. Извлекаем критерии из запроса
    2. Фильтруем по MongoDB/JSON
    3. Semantic search в отфильтрованных результатах
    4. Fallback на keyword search если нужно
    """
    if not query or not query.strip():
        return "❌ Введите запрос для поиска", None
    
    try:
        # 1. Извлекаем критерии
        criteria = extract_criteria_from_query(query)
        print(f"🔍 Запрос: {query}")
        print(f"📋 Критерии: {criteria}")
        
        # 2. Фильтрация (MongoDB или JSON)
        if USE_MONGODB:
            filtered_listings = search_in_mongodb(criteria)
            data_source = "MongoDB Atlas"
        else:
            filtered_listings = search_in_json(criteria)
            data_source = "JSON файлы"
        
        print(f"✅ Отфильтровано: {len(filtered_listings)} объявлений из {data_source}")
        
        if not filtered_listings:
            return f"❌ Ничего не найдено по критериям:\n{criteria}", None
        
        # 3. Semantic search (если доступен ChromaDB)
        if USE_CHROMADB and vector_db:
            final_results = semantic_search_in_subset(query, filtered_listings, max_results)
            search_method = "🟢 Hybrid Search (Filters + Semantic)"
        else:
            final_results = []
            search_method = "🟡 Keyword Search (Filters + Keywords)"
        
        # 4. Fallback на keyword search
        if not final_results:
            final_results = keyword_search(query, filtered_listings, max_results)
        
        if not final_results:
            return f"❌ Не удалось найти релевантные результаты среди {len(filtered_listings)} отфильтрованных объявлений", None
        
        # 5. Форматирование результатов
        output_text = f"## 🔍 Результаты поиска\n\n"
        output_text += f"**Запрос:** {query}\n"
        output_text += f"**Критерии:** {criteria}\n"
        output_text += f"**Метод поиска:** {search_method}\n"
        output_text += f"**Источник данных:** {data_source}\n"
        output_text += f"**Найдено после фильтрации:** {len(filtered_listings)}\n"
        output_text += f"**Показано:** {len(final_results)} лучших результатов\n\n"
        output_text += "---\n\n"
        
        table_data = []
        
        for i, result in enumerate(final_results, 1):
            title = result.get('title', 'Без названия')
            price = result.get('price')
            rooms = result.get('room_count', 'N/A')
            space = result.get('space_sm', 'N/A')
            district = result.get('district', 'N/A')
            city = result.get('city', 'Warszawa')
            link = result.get('link', 'N/A')
            
            # Форматирование цены
            if price is not None:
                price_str = f"{price:,.0f} zł"
                price_table = f"{price:,.0f}"
            else:
                price_str = "Цена не указана"
                price_table = "N/A"
            
            # Релевантность
            score_info = ""
            if 'semantic_score' in result:
                score_info = f"🎯 **Релевантность:** {result['semantic_score']:.3f}\n"
            elif 'keyword_score' in result:
                score_info = f"📝 **Совпадений:** {result['keyword_score']}\n"
            
            output_text += f"### {i}. {title}\n\n"
            output_text += f"- 💰 **Цена:** {price_str}\n"
            output_text += f"- 🏠 **Комнаты:** {rooms} | **Площадь:** {space} m²\n"
            output_text += f"- 📍 **Район:** {district}, {city}\n"
            output_text += score_info
            output_text += f"- [🔗 Открыть на Otodom.pl]({link})\n\n"
            output_text += "---\n\n"
            
            table_data.append({
                "№": i,
                "Название": title[:40] + "..." if len(title) > 40 else title,
                "Цена (zł)": price_table,
                "Комнаты": rooms,
                "Площадь (m²)": space,
                "Район": district,
            })
        
        df = pd.DataFrame(table_data)
        return output_text, df
        
    except Exception as e:
        import traceback
        error_msg = f"❌ Ошибка поиска: {str(e)}\n\n```\n{traceback.format_exc()}\n```"
        logger.error(error_msg)
        return error_msg, None

# === СТАТИСТИКА ===

def get_system_stats():
    """Статистика системы"""
    stats_text = "## 📊 Статистика системы\n\n"
    
    if USE_MONGODB:
        try:
            rent_count = mongo_db.rent_listings.count_documents({})
            sale_count = mongo_db.sale_listings.count_documents({})
            total = rent_count + sale_count
            
            stats_text += f"### 🗄️ MongoDB Atlas\n"
            stats_text += f"- **Всего объявлений:** {total:,}\n"
            stats_text += f"- **Аренда:** {rent_count:,}\n"
            stats_text += f"- **Продажа:** {sale_count:,}\n"
            stats_text += f"- **База данных:** `{mongo_db.name}`\n\n"
        except Exception as e:
            stats_text += f"### ⚠️ MongoDB Atlas\n"
            stats_text += f"- **Статус:** Ошибка подключения\n"
            stats_text += f"- **Ошибка:** {e}\n\n"
    else:
        total = len(ALL_LISTINGS)
        rent = sum(1 for l in ALL_LISTINGS if l.get('source_collection') == 'rent_listings')
        sale = total - rent
        
        stats_text += f"### 📂 JSON файлы\n"
        stats_text += f"- **Всего объявлений:** {total:,}\n"
        stats_text += f"- **Аренда:** {rent:,}\n"
        stats_text += f"- **Продажа:** {sale:,}\n\n"
    
    if USE_CHROMADB and vector_db:
        try:
            vector_stats = vector_db.get_stats()
            stats_text += f"### 🔍 ChromaDB\n"
            stats_text += f"- **Документов в векторной БД:** {vector_stats.get('total', 0):,}\n"
            stats_text += f"- **Коллекция:** `{vector_stats.get('collection_name', 'N/A')}`\n\n"
        except Exception as e:
            stats_text += f"### ⚠️ ChromaDB\n"
            stats_text += f"- **Статус:** Ошибка\n"
            stats_text += f"- **Ошибка:** {e}\n\n"
    else:
        stats_text += f"### ⚠️ ChromaDB\n"
        stats_text += f"- **Статус:** Недоступна\n\n"
    
    # Режим работы
    stats_text += f"### 🎯 Режим работы\n"
    if USE_MONGODB and USE_CHROMADB:
        stats_text += f"- **Режим:** 🟢 **Full Hybrid Search** (MongoDB + Semantic)\n"
    elif USE_MONGODB:
        stats_text += f"- **Режим:** 🟡 **MongoDB + Keyword Search**\n"
    elif USE_CHROMADB:
        stats_text += f"- **Режим:** 🟡 **JSON + Semantic Search**\n"
    else:
        stats_text += f"- **Режим:** 🔴 **Только Keyword Search**\n"
    
    stats_text += f"\n### 📍 Источник данных\n"
    stats_text += f"- **Сайт:** Otodom.pl\n"
    stats_text += f"- **Город:** Warszawa\n"
    
    return stats_text

# === ПРИМЕРЫ ЗАПРОСОВ ===

EXAMPLE_QUERIES = [
    "Szukam 2-pokojowego mieszkania na wynajem w Mokotowie do 3000 zł",
    "Kawalerka umeblowana z balkonem do 2500 zł",
    "3 pokoje na sprzedaż Wilanów gotowe do użytku",
    "Mieszkanie z parkingiem i windą w Bemowo lub Wola",
    "Nowe mieszkanie na rynku pierwotnym do 500000 zł",
]

# === GRADIO ИНТЕРФЕЙС ===

print("\n" + "="*60)
print("🚀 Запуск Gradio приложения...")
print(f"📊 Источник данных: {'MongoDB Atlas' if USE_MONGODB else 'JSON файлы'}")
print(f"🔍 ChromaDB: {'✅ Доступна' if USE_CHROMADB else '❌ Недоступна'}")

# Определяем режим работы
if USE_MONGODB and USE_CHROMADB:
    mode = "Full Hybrid Search (MongoDB + Semantic)"
elif USE_CHROMADB:
    mode = "Hybrid Search (JSON + Semantic)"
elif USE_MONGODB:
    mode = "MongoDB + Keyword Search"
else:
    mode = "Keyword Search Only"

print(f"🎯 Режим: {mode}")
print("="*60 + "\n")

with gr.Blocks(title="🏠 Real Estate Warsaw Search", theme=gr.themes.Soft()) as app:
    gr.Markdown("# 🏠 Поиск недвижимости в Варшаве")
    gr.Markdown("### Гибридный поиск: MongoDB Atlas + Semantic Search (ChromaDB)")
    
    with gr.Tab("🔍 Поиск"):
        with gr.Row():
            with gr.Column(scale=3):
                query_input = gr.Textbox(
                    label="Введите запрос на польском языке",
                    placeholder="Например: Szukam 2-pokojowego mieszkania na wynajem w Mokotowie do 3000 zł",
                    lines=2
                )
            with gr.Column(scale=1):
                max_results = gr.Slider(
                    minimum=1,
                    maximum=20,
                    value=5,
                    step=1,
                    label="Макс. результатов"
                )
        
        search_btn = gr.Button("🔍 Искать", variant="primary", size="lg")
        
        gr.Markdown("### 💡 Примеры запросов:")
        gr.Examples(
            examples=[[q, 5] for q in EXAMPLE_QUERIES],
            inputs=[query_input, max_results],
        )
        
        with gr.Row():
            output_text = gr.Markdown(label="Результаты")
        
        with gr.Row():
            output_table = gr.Dataframe(
                label="Таблица результатов",
                wrap=True
            )
        
        search_btn.click(
            fn=hybrid_search_real_estate,
            inputs=[query_input, max_results],
            outputs=[output_text, output_table]
        )
    
    with gr.Tab("📊 Статистика"):
        stats_display = gr.Markdown(value=get_system_stats())
        refresh_btn = gr.Button("🔄 Обновить статистику")
        refresh_btn.click(fn=get_system_stats, outputs=stats_display)
    
    with gr.Tab("ℹ️ О проекте"):
        gr.Markdown("""
        ## 🏠 Real Estate Warsaw Search
        
        ### 🎯 Возможности:
        - 🔍 **Гибридный поиск**: Комбинирует фильтрацию MongoDB и семантический поиск
        - 🤖 **NLP**: Автоматическое извлечение критериев из естественного запроса
        - 📊 **Большая база данных**: Тысячи актуальных объявлений из Otodom.pl
        - ⚡ **Быстрый поиск**: Оптимизированные запросы и индексы
        
        ### 🛠️ Технологии:
        - **Backend**: Python, MongoDB Atlas, ChromaDB
        - **NLP**: OpenAI Embeddings, Semantic Search
        - **Frontend**: Gradio
        - **Data Source**: Web scraping (Scrapy) с Otodom.pl
        
        ### 📝 Формат запроса:
        Пишите на польском языке естественным образом:
        - "Szukam 2-pokojowego mieszkania na wynajem w Mokotowie do 3000 zł"
        - "Kawalerka blisko metra"
        - "3 pokoje na sprzedaż Wilanów"
        
        Система автоматически извлечет:
        - Тип транзакции (аренда/продажа)
        - Количество комнат
        - Ценовой диапазон
        - Район
        - И другие параметры
        """)

if __name__ == "__main__":
    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False
    )

