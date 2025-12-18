
import os
import sys
from typing import Optional

# Добавить корень проекта в путь для импортов
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.real_estate_vector_db import RealEstateVectorDB


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Wypełnianie wektorowej bazy danych nieruchomości")
    parser.add_argument("--reset", action="store_true", help="Wyczyść wektorową bazę danych przed wypełnieniem")
    parser.add_argument("--limit", type=int, help="Ogranicz liczbę rekordów do testowania")
    parser.add_argument("--stats", action="store_true", help="Pokaż statystyki bazy danych")
    parser.add_argument("--test-search", type=str, help="Przetestuj wyszukiwanie z podanym zapytaniem")
    
    args = parser.parse_args()
    
    print("System wyszukiwania semantycznego nieruchomości")
    print("=" * 50)
    
    # Inicjalizujemy wektorową bazę danych
    vector_db = RealEstateVectorDB()
    
    # Czyszczenie bazy danych jeśli potrzeba
    if args.reset:
        print("Czyszczenie wektorowej bazy danych...")
        vector_db.clear_database()
        vector_db = RealEstateVectorDB()  # Przecreujemy po czyszczeniu
    
    # Pokazujemy aktualną statystykę
    print("\nAktualna statystyka:")
    stats = vector_db.get_stats()
    
    # Wypełniamy bazę danych
    print(f"\nWypełnianie wektorowej bazy danych...")
    if args.limit:
        print(f"Ograniczenie: {args.limit} rekordów do testowania")
    
    vector_db.populate_from_mongo(limit=args.limit)
    
    # Pokazujemy końcową statystykę
    print("\nKońcowa statystyka:")
    vector_db.get_stats()
    
    # Testujemy wyszukiwanie jeśli podano zapytanie
    if args.test_search:
        print(f"\nTestujemy wyszukiwanie: '{args.test_search}'")
        results = vector_db.semantic_search(args.test_search, top_k=5)
        
        if results:
            print(f"\nZnaleziono {len(results)} wyników:")
            for i, result in enumerate(results, 1):
                print(f"\n{i}. ID: {result['id']} (Wynik: {result['score']:.3f})")
                print(f"   Treść: {result['content'][:150]}...")
                
                
                from pymongo import MongoClient
                client = MongoClient("mongodb://localhost:27017/")
                db = client["real_estate"]
                
                # Określamy kolekcję
                collection = db["rent_listings"] if result['metadata'].get('collection_type') == 'rent' else db["sale_listings"]
                full_data = collection.find_one({"_id": result['id']})
                
                if full_data:
                    print(f"   Typ: {'Wynajem' if result['metadata'].get('collection_type') == 'rent' else 'Sprzedaż'}")
                    print(f"   Cena: {full_data.get('price', 'N/A')} zł")
                    print(f"   Pokoje: {full_data.get('room_count', 'N/A')}")
                    print(f"   Powierzchnia: {full_data.get('space_sm', 'N/A')} m²")
                    if full_data.get('city'):
                        print(f"   Miasto: {full_data['city']}")
                    if full_data.get('district'):
                        print(f"   Dzielnica: {full_data['district']}")
                    if full_data.get('link'):
                        print(f"   Link: {full_data['link']}")
        else:
            print("Nie znaleziono wyników")
    
    print("\nGotowe!")


if __name__ == "__main__":
    main()
