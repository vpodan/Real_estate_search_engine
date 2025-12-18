#!/usr/bin/env python3
"""
Sprawdzanie danych w MongoDB
"""

from pymongo import MongoClient

def check_mongo_data():
    """Sprawdza obecność danych w MongoDB"""
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["real_estate"]
        
        rent_count = db["rent_listings"].count_documents({})
        sale_count = db["sale_listings"].count_documents({})
        
        print(f"Dane w MongoDB:")
        print(f"   Ogłoszeń wynajmu: {rent_count}")
        print(f"   Ogłoszeń sprzedaży: {sale_count}")
        print(f"   Wszystkich ogłoszeń: {rent_count + sale_count}")
        
        
        
    except Exception as e:
        print(f"Błąd połączenia z MongoDB: {e}")
        return False

if __name__ == "__main__":
    check_mongo_data()
