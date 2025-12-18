import logging

logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("chromadb").setLevel(logging.WARNING)
logging.getLogger("chromadb.telemetry").setLevel(logging.WARNING)
logging.getLogger("chromadb.config").setLevel(logging.WARNING)

from src.main import extract_criteria_from_prompt, search_listings
from src.real_estate_vector_db import RealEstateVectorDB

def hybrid_search(query: str):
    
    results = {
        "query": query,
        "mongo_filtered_count": 0,
        "semantic_results": [],
        "final_results": []
    }
    
    print(f"🔍 HIBRIDOWE WYSZUKIWANIE: '{query}'")
    print("-" * 60)
    
    # KROK 1: Filtrowanie MongoDB po podstawowych kryteriach
    try:
        print("Krok 1: Filtrowanie MongoDB po podstawowych kryteriach")
        criteria = extract_criteria_from_prompt(query)
        
        # Używamy funkcji z main.py
        mongo_search_result = search_listings(criteria)
        mongo_listings = mongo_search_result["listings"]
        
        
        results["mongo_filtered_count"] = len(mongo_listings)
        print(f"   Znaleziono {results['mongo_filtered_count']} wyników w MongoDB")
        
        if results["mongo_filtered_count"] == 0:
            print(" Brak wyników w MongoDB - koniec wyszukiwania")
            return results
            
    except Exception as e:
        print(f"  Błąd filtrowania MongoDB: {e}")
        return results
    
    # KROK 2: Jeśli są wyniki w MongoDB, to wykonujemy semantyczne wyszukiwanie
    if results["mongo_filtered_count"] > 0:
        try:
            print("Krok 2: Wyszukiwanie semantyczne w przefiltrowanych wynikach")
            
            # Przygotowujemy listę ID do semantycznego wyszukiwania
            filtered_ids = []
            for listing in mongo_listings:
                filtered_ids.append(listing.get('_id'))
            
            print(f"   Przeszukujemy semantycznie {len(filtered_ids)} wyników...")
            
            # Inicjalizujemy wektorową bazę danych
            vector_db = RealEstateVectorDB()
            
            # Wykonujemy semantyczne wyszukiwanie tylko w przefiltrowanych wynikach
            semantic_results = vector_db.semantic_search_in_subset(
                query, 
                filtered_ids, 
                top_k=10
            )
            
            # Dodajemy pełne dane z mongo_listings
            for result in semantic_results:
                # Znajdujemy pełne dane w mongo_listings po ID
                full_data = None
                for listing in mongo_listings:
                    if listing.get('_id') == result['id']:
                        full_data = listing
                        break
                
                if full_data:
                    semantic_result = {
                        "id": result['id'],
                        "title": full_data.get('title', 'Bez tytułu'),
                        "price": full_data.get('price'),
                        "room_count": full_data.get('room_count'),
                        "space_sm": full_data.get('space_sm'),
                        "city": full_data.get('city'),
                        "district": full_data.get('district'),
                        "link": full_data.get('link'),
                        "semantic_score": result['score'],
                        "type": "hybrid",
                        "source_collection": full_data.get('source_collection')
                    }
                    results["semantic_results"].append(semantic_result)
            
            print(f"   Znaleziono {len(results['semantic_results'])} semantycznych wyników")
            
        except Exception as e:
            print(f" Błąd wyszukiwania semantycznego: {e}")
            # Jeśli semantyczne wyszukiwanie nie działa, zwracamy wyniki z MongoDB
            for listing in mongo_listings[:10]:
                mongo_result = {
                    "id": listing.get('_id'),
                    "title": listing.get('title', 'Bez tytułu'),
                    "price": listing.get('price'),
                    "room_count": listing.get('room_count'),
                    "space_sm": listing.get('space_sm'),
                    "city": listing.get('city'),
                    "district": listing.get('district'),
                    "link": listing.get('link'),
                    "semantic_score": None,
                    "type": "mongo_only",
                    "source_collection": listing.get('source_collection')
                }
                results["semantic_results"].append(mongo_result)
    
    # KROK 3: Przygotowanie finalnych wyników
    results["final_results"] = results["semantic_results"][:5]  # Top 5 wyników
    
    return results

def display_hybrid_results(results):
    
    
    print(f"WYNIKI HIBRYDOWEGO WYSZUKIWANIA:")
    print("=" * 60)
    print(f"Zapytanie: '{results['query']}'")
    print(f"Wyników po filtrowaniu MongoDB: {results['mongo_filtered_count']}")
    print(f"Semantycznych wyników: {len(results['semantic_results'])}")
    print(f"Finalnych wyników: {len(results['final_results'])}")
    
    if results["final_results"]:
        print(f"NAJLEPSZE WYNIKI:")
        print("-" * 50)
        
        for i, result in enumerate(results["final_results"], 1):
            print(f"\n{i}. {result['title'][:60]}...")
            print(f"Cena: {result['price']} zł")
            print(f"Pokoje: {result['room_count']}, Powierzchnia: {result['space_sm']} m2")
            print(f"{result['city']}, {result['district']}")
            
            if result['semantic_score']:
                print(f"Wynik semantyczny: {result['semantic_score']:.3f}")
            
            print(f" Link: {result['link'][:200]}...")
            print(f" Typ: {result['type']}")
    else:
        print("\nNie znaleziono wyników")

def test_hybrid_search():
    
    test_queries = [
        "Chce kupić wykończone dwupokojowe mieszkanie w warszawie w dzielnicy Mokotów lub wola lub Praga-Poludnie do 850000 zlotych  na wysokim piętrze z widokiem na zielień lub park, powyżej 2010 roku"
    ]
    
    print("TEST HIBRYDOWEGO WYSZUKIWANIA")
    print("=" * 70)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'='*20} TEST {i} {'='*20}")
        results = hybrid_search(query)
        display_hybrid_results(results)
        
        if i < len(test_queries):
            input("\nNaciśnij Enter aby kontynuować...")
    
    print(f"\n{'='*70}")
    print("\nTEST HIBRYDOWEGO WYSZUKIWANIA ZAKOŃCZONY")

if __name__ == "__main__":
    test_hybrid_search()
