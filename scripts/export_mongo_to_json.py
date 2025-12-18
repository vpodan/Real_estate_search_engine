"""
Экспорт данных из MongoDB в JSON файлы для Hugging Face Spaces
"""
import json
import os
from pymongo import MongoClient
from bson import ObjectId

# Подключение к MongoDB
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGODB_DB", "real_estate")

print("🔌 Подключение к MongoDB...")
print(f"   URI: {MONGO_URI}")
print(f"   Database: {DB_NAME}")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.server_info()  # Проверка подключения
    print("✅ Подключение успешно")
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")
    exit(1)

db = client[DB_NAME]

def convert_objectid(obj):
    """Конвертирует ObjectId в строку для JSON сериализации"""
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: convert_objectid(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid(item) for item in obj]
    return obj

# Экспорт rent_listings
print("\n📥 Экспорт rent_listings...")
rent_count = 0
with open("rent_listings.json", "w", encoding="utf-8") as f:
    for doc in db.rent_listings.find():
        # Конвертируем ObjectId в строку
        doc = convert_objectid(doc)
        # Добавляем source_collection для идентификации
        doc['source_collection'] = 'rent_listings'
        f.write(json.dumps(doc, ensure_ascii=False) + "\n")
        rent_count += 1

print(f"✅ Экспортировано {rent_count} объявлений аренды")
print(f"   Файл: rent_listings.json")

# Экспорт sale_listings
print("\n📥 Экспорт sale_listings...")
sale_count = 0
with open("sale_listings.json", "w", encoding="utf-8") as f:
    for doc in db.sale_listings.find():
        doc = convert_objectid(doc)
        doc['source_collection'] = 'sale_listings'
        f.write(json.dumps(doc, ensure_ascii=False) + "\n")
        sale_count += 1

print(f"✅ Экспортировано {sale_count} объявлений продажи")
print(f"   Файл: sale_listings.json")

# Статистика
print(f"\n📊 Общая статистика:")
print(f"   Всего объявлений: {rent_count + sale_count}")
print(f"   Аренда: {rent_count}")
print(f"   Продажа: {sale_count}")

# Проверка размера файлов
import os
rent_size = os.path.getsize("rent_listings.json") / (1024 * 1024)  # MB
sale_size = os.path.getsize("sale_listings.json") / (1024 * 1024)  # MB

print(f"\n💾 Размер файлов:")
print(f"   rent_listings.json: {rent_size:.2f} MB")
print(f"   sale_listings.json: {sale_size:.2f} MB")
print(f"   Общий размер: {rent_size + sale_size:.2f} MB")

if rent_size + sale_size > 100:
    print(f"\n⚠️ ВНИМАНИЕ: Файлы большие (>{100}MB)")
    print(f"   Рекомендуется использовать Git LFS для загрузки на HF Spaces")
else:
    print(f"\n✅ Размер файлов в норме для HF Spaces")

print("\n✅ Экспорт завершен!")
print("\n📝 Следующий шаг:")
print("   1. Загрузите эти файлы в ваш Hugging Face Space")
print("   2. Или используйте Git LFS: git lfs track '*.json'")

