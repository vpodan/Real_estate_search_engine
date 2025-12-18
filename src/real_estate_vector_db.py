import os
import argparse
import shutil
from typing import List, Dict, Optional, Tuple
from tqdm import tqdm
import logging

from langchain_core.documents import Document
from langchain_chroma import Chroma
from pymongo import MongoClient

from src.real_estate_embedding_function import (
    get_embedding_function,
    create_listing_text_for_embedding,
    create_listing_chunks_for_embedding,
    create_query_optimized_text,
    extract_listing_metadata,
    validate_listing_data
)

# Konfiguracja logowania
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)




CHROMA_PATH = "chroma_real_estate"
BATCH_SIZE = 50  
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGODB_DB", "real_estate")

# Połączenie z MongoDB (опционально)
db = None
collection_rent = None
collection_sale = None

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.server_info()  
    db = client[DB_NAME]
    collection_rent = db["rent_listings"]
    collection_sale = db["sale_listings"]
    logger.info("Połączenie z MongoDB udane")
except Exception as e:
    logger.error(f"Błąd połączenia z MongoDB: {e}")
    logger.info("MongoDB недоступна - работаем только с ChromaDB")


class RealEstateVectorDB:
    
    def __init__(self, persist_directory: str = CHROMA_PATH):
      
        self.persist_directory = persist_directory
        self.embedding_function = get_embedding_function()
        
        try:
            self.db = Chroma(
                persist_directory=persist_directory,
                embedding_function=self.embedding_function,
                collection_metadata={"hnsw:space": "cosine"}
            )
            logger.info(f"Wektorowa baza danych zainicjalizowana: {persist_directory}")
        except Exception as e:
            logger.error(f"Błąd inicjalizacji wektorowej bazy danych: {e}")
            raise
    
    def add_listing_to_vector_db(
        self, 
        listing_data: Dict, 
        collection_type: str,
        use_chunks: bool = False
    ) -> bool:
      
        # Walidacja danych
        if not validate_listing_data(listing_data):
            logger.warning(f"Pomijanie nieprawidłowego ogłoszenia")
            return False
        
        listing_id = str(listing_data.get("_id"))
        if not listing_id:
            logger.warning("Pomijanie ogłoszenia bez ID")
            return False
        
        # Sprawdzenie istnienia
        existing = self.db.get(ids=[listing_id], include=[])
        if existing['ids']:
            logger.debug(f"Ogłoszenie {listing_id} już istnieje")
            return False
        
        try:
            if use_chunks:
                return self._add_with_chunks(listing_data, collection_type, listing_id)
            else:
                return self._add_single(listing_data, collection_type, listing_id)
        except Exception as e:
            logger.error(f"Błąd dodawania {listing_id}: {e}")
            return False
    
    def _add_single(
        self, 
        listing_data: Dict, 
        collection_type: str, 
        listing_id: str
    ) -> bool:
        # Tworzymy tekst
        text_content = create_listing_text_for_embedding(
            listing_data,
            include_description=True,
            prioritize_search_fields=True
        )
        
        # Wyciągamy metadane
        metadata = extract_listing_metadata(listing_data)
        metadata["collection_type"] = collection_type
        
        # Tworzymy dokument
        document = Document(
            page_content=text_content,
            metadata=metadata
        )
        
        # Dodajemy do bazy danych
        self.db.add_documents([document], ids=[listing_id])
        logger.debug(f"Dodano: {listing_id}")
        return True
    
    def _add_with_chunks(
        self, 
        listing_data: Dict, 
        collection_type: str, 
        listing_id: str
    ) -> bool:
        # Tworzymy chunki z metadanymi
        chunks_with_metadata = create_listing_chunks_for_embedding(listing_data)
        
        if not chunks_with_metadata:
            logger.warning(f"Nie udało się utworzyć chunków dla {listing_id}")
            return False
        

        documents = []
        chunk_ids = []
        
        for i, (chunk_text, chunk_metadata) in enumerate(chunks_with_metadata):
            chunk_id = f"{listing_id}_chunk_{i}"
            chunk_metadata["collection_type"] = collection_type
            
            document = Document(
                page_content=chunk_text,
                metadata=chunk_metadata
            )
            
            documents.append(document)
            chunk_ids.append(chunk_id)
        
        # Dodajemy do bazy danych
        self.db.add_documents(documents, ids=chunk_ids)
        logger.debug(f"Dodano {len(documents)} chunków: {listing_id}")
        return True
    
    def _process_listings_batch(self, listings: List[Dict], collection_type: str, use_chunks: bool = False) -> Dict[str, int]:
        
        stats = {"processed": 0, "errors": 0}
        
        # Przetwarzamy w pakietach
        for i in range(0, len(listings), BATCH_SIZE):
            batch = listings[i:i + BATCH_SIZE]
            logger.info(f"Przetwarzanie pakietu {i//BATCH_SIZE + 1}/{(len(listings) + BATCH_SIZE - 1)//BATCH_SIZE} ({len(batch)} ogłoszeń)")
            
            for listing in batch:
                try:
                    if self.add_listing_to_vector_db(listing, collection_type, use_chunks):
                        stats["processed"] += 1
                    else:
                        stats["errors"] += 1
                except Exception as e:
                    logger.error(f"Błąd przetwarzania ogłoszenia {listing.get('_id', 'unknown')}: {e}")
                    stats["errors"] += 1
        
        return stats

    def populate_from_mongo(
        self, 
        limit: Optional[int] = None,
        use_chunks: bool = False,
        include_rent: bool = True,
        include_sale: bool = True
    ) -> Dict[str, int]:
       
        stats = {"rent": 0, "sale": 0, "errors": 0}
        
        # Ładujemy wynajem
        if include_rent:
            logger.info("Ładowanie ogłoszeń wynajmu...")
            try:
                query = {}
                cursor = collection_rent.find(query)
                if limit:
                    cursor = cursor.limit(limit)
                
                rent_listings = list(cursor)
                logger.info(f"Znaleziono ogłoszeń wynajmu: {len(rent_listings)}")
                
                # Przetwarzamy w pakietach
                rent_stats = self._process_listings_batch(rent_listings, "rent", use_chunks)
                stats["rent"] = rent_stats["processed"]
                stats["errors"] += rent_stats["errors"]
                        
            except Exception as e:
                logger.error(f"Błąd ładowania wynajmu: {e}")
        
        # Ładujemy sprzedaż
        if include_sale:
            logger.info("\nŁadowanie ogłoszeń sprzedaży...")
            try:
                query = {}
                cursor = collection_sale.find(query)
                if limit:
                    cursor = cursor.limit(limit)
                
                sale_listings = list(cursor)
                logger.info(f"Znaleziono ogłoszeń sprzedaży: {len(sale_listings)}")
                
                # Przetwarzamy w pakietach
                sale_stats = self._process_listings_batch(sale_listings, "sale", use_chunks)
                stats["sale"] = sale_stats["processed"]
                stats["errors"] += sale_stats["errors"]
                        
            except Exception as e:
                logger.error(f"Błąd ładowania sprzedaży: {e}")
        
        logger.info(f"\nŁadowanie zakończone!")
        logger.info(f"   Wynajem: {stats['rent']}")
        logger.info(f"   Sprzedaż: {stats['sale']}")
        logger.info(f"   Błędy: {stats['errors']}")
        
        return stats
    
    def semantic_search(
        self,
        query: str,
        collection_type: Optional[str] = None,
        filters: Optional[Dict] = None,
        top_k: int = 10,
        optimize_query: bool = True
    ) -> List[Dict]:
        
        # Optymalizacja zapytania
        if optimize_query:
            optimized = create_query_optimized_text(query, expand_synonyms=True)
            logger.info(f"Zapytanie: '{query}'")
            if optimized != query:
                logger.info(f"   Zoptymalizowane: '{optimized}'")
            search_query = optimized
        else:
            search_query = query
        
        
        filter_dict = filters.copy() if filters else {}
        if collection_type:
            filter_dict["collection_type"] = collection_type
        
        try:
            
            results = self.db.similarity_search_with_score(
                query=search_query,
                k=top_k,
                filter=filter_dict if filter_dict else None
            )
            
          
            formatted_results = []
            for doc, distance in results:
                # Odległość cosinusowa: 0 = identyczne, 2 = przeciwne
                # Przekształcamy w similarity score: 1 - (distance / 2)
                similarity_score = 1 - (distance / 2)
                
                result = {
                    "id": doc.metadata.get("listing_id", doc.metadata.get("id")),
                    "score": similarity_score,
                    "distance": distance,
                    "content_preview": doc.page_content[:200] + "...",
                    "metadata": doc.metadata
                }
                formatted_results.append(result)
            
            logger.info(f"Znaleziono wyników: {len(formatted_results)}")
            return formatted_results
            
        except Exception as e:
            logger.error(f"Błąd wyszukiwania: {e}")
            return []
    
    def search_and_get_full_data(
        self,
        query: str,
        collection_type: Optional[str] = None,
        top_k: int = 10
    ) -> List[Dict]:
        
        search_results = self.semantic_search(
            query=query,
            collection_type=collection_type,
            top_k=top_k
        )
        
        if not search_results:
            return []
        
        
        full_results = []
        
        for result in search_results:
            listing_id = result["id"]
            result_collection_type = result["metadata"].get("collection_type")
            
            
            collection = (
                collection_rent if result_collection_type == "rent" 
                else collection_sale
            )
            
            
            try:
                full_data = collection.find_one({"_id": listing_id})
                if full_data:
                    full_data["search_score"] = result["score"]
                    full_data["search_distance"] = result["distance"]
                    full_results.append(full_data)
            except Exception as e:
                logger.error(f"Błąd pobierania danych dla {listing_id}: {e}")
        
        return full_results
    
    def get_stats(self) -> Dict:
      
        try:
            all_docs = self.db.get(include=['metadatas'])
            total_count = len(all_docs['ids'])
            
            if total_count == 0:
                logger.info("Wektorowa baza danych jest pusta")
                return {"total": 0, "rent": 0, "sale": 0}
            
            # Liczenie według typów
            rent_count = sum(
                1 for meta in all_docs['metadatas'] 
                if meta.get('collection_type') == 'rent'
            )
            sale_count = sum(
                1 for meta in all_docs['metadatas'] 
                if meta.get('collection_type') == 'sale'
            )
            
            stats = {
                "total": total_count,
                "rent": rent_count,
                "sale": sale_count
            }
            
            logger.info("Statystyka wektorowej bazy danych:")
            logger.info(f"   Wszystkich ogłoszeń: {total_count}")
            logger.info(f"   Wynajem: {rent_count}")
            logger.info(f"   Sprzedaż: {sale_count}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Błąd pobierania statystyk: {e}")
            return {}
    
    def semantic_search_in_subset(
        self,
        query: str,
        subset_ids: List[str],
        top_k: int = 10,
        optimize_query: bool = True
    ) -> List[Dict]:
       
        try:
            if optimize_query:
                from src.real_estate_embedding_function import create_query_optimized_text
                optimized = create_query_optimized_text(query, expand_synonyms=True)
                logger.info(f"Semantyczne wyszukiwanie w podzbiorze: '{query}'")
                if optimized != query:
                    logger.info(f"   Zoptymalizowane: '{optimized}'")
                search_query = optimized
            else:
                search_query = query
                logger.info(f"Semantyczne wyszukiwanie w podzbiorze: '{query}'")
            
            # Pobieramy wszystkie dokumenty z wektorowej bazy danych
            all_docs = self.db.get(include=['metadatas', 'documents'])
            
            if not all_docs['ids']:
                logger.warning("Wektorowa baza danych jest pusta")
                return []
            
            # Filtrujemy tylko te dokumenty, które są w subset_ids
            filtered_indices = []
            filtered_metadatas = []
            filtered_documents = []
            
            for i, doc_id in enumerate(all_docs['ids']):
                if doc_id in subset_ids:
                    filtered_indices.append(i)
                    filtered_metadatas.append(all_docs['metadatas'][i])
                    filtered_documents.append(all_docs['documents'][i])
            
            if not filtered_indices:
                logger.warning(f"Nie znaleziono żadnych dokumentów z podanej listy ID")
                return []
            
            logger.info(f"   Znaleziono {len(filtered_indices)} dokumentów w podzbiorze")
            
            # Tworzymy tymczasową kolekcję z przefiltrowanymi dokumentami
            from chromadb import Collection
            
            # Pobieramy embedding dla zapytania
            query_embedding = self.embedding_function.embed_query(search_query)
            
            # Obliczamy podobieństwo dla każdego dokumentu w podzbiorze
            similarities = []
            
            for i, metadata in enumerate(filtered_metadatas):
                # Pobieramy embedding dokumentu 
                doc_text = filtered_documents[i]
                doc_embedding = self.embedding_function.embed_query(doc_text)
                
                # Obliczamy podobieństwo cosinusowe
                import numpy as np
                similarity = np.dot(query_embedding, doc_embedding) / (
                    np.linalg.norm(query_embedding) * np.linalg.norm(doc_embedding)
                )
                
                similarities.append({
                    "id": all_docs['ids'][filtered_indices[i]],
                    "score": float(similarity),
                    "distance": float(1 - similarity),
                    "metadata": metadata
                })
            
            # Sortujemy według podobieństwa (malejąco)
            similarities.sort(key=lambda x: x["score"], reverse=True)
            
            # Bierzemy top_k wyników
            results = similarities[:top_k]
            
            logger.info(f"Znaleziono wyników: {len(results)}")
            
            return results
            
        except Exception as e:
            logger.error(f"Błąd semantycznego wyszukiwania w podzbiorze: {e}")
            return []

    def clear_database(self):
       
        if os.path.exists(self.persist_directory):
            try:
                shutil.rmtree(self.persist_directory)
                logger.info("Wektorowa baza danych wyczyszczona")
            except Exception as e:
                logger.error(f"Błąd czyszczenia bazy danych: {e}")
        else:
            logger.warning("Wektorowa baza danych nie istnieje")


def print_search_results(results: List[Dict], show_full: bool = False):

    if not results:
        logger.info("Nie znaleziono wyników")
        return
    
    logger.info(f"\n{'='*80}")
    logger.info(f"Znaleziono wyników: {len(results)}")
    logger.info(f"{'='*80}\n")
    
    for i, result in enumerate(results, 1):
        logger.info(f"{i}. ID: {result['id']}")
        logger.info(f"   Podobieństwo: {result['score']:.2%}")
        logger.info(f"   Typ: {result['metadata'].get('collection_type', 'N/A')}")
        
        # Podstawowe informacje
        meta = result['metadata']
        if meta.get('city'):
            logger.info(f"   Lokalizacja: {meta['city']}, {meta.get('district', '')}")
        if meta.get('room_count'):
            logger.info(f"   Pokoje: {meta['room_count']}")
        if meta.get('price'):
            logger.info(f"   Cena: {meta['price']} zł")
        if meta.get('space_sm'):
            logger.info(f"   Powierzchnia: {meta['space_sm']} m²")
        
        if show_full:
            logger.info(f"   Podgląd: {result['content_preview']}")
        
        # Pobieramy link z MongoDB
        listing_id = result['id']
        collection_type = meta.get('collection_type')
        collection = (
            collection_rent if collection_type == 'rent' 
            else collection_sale
        )
        
        try:
            full_data = collection.find_one({"_id": listing_id})
            if full_data and full_data.get('link'):
                logger.info(f"   Link: {full_data['link']}")
        except:
            pass
        
        logger.info("")


def main():
    parser = argparse.ArgumentParser(
        description="Zarządzanie wektorową bazą danych nieruchomości"
    )
    parser.add_argument(
        "--reset", 
        action="store_true", 
        help="Wyczyścić bazę danych"
    )
    parser.add_argument(
        "--populate", 
        action="store_true", 
        help="Wypełnić bazę danych z MongoDB"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        help="Ograniczyć liczbę rekordów (do testu)"
    )
    parser.add_argument(
        "--chunks", 
        action="store_true", 
        help="Używać podziału na chunki"
    )
    parser.add_argument(
        "--stats", 
        action="store_true", 
        help="Pokaż statystyki bazy danych"
    )
    parser.add_argument(
        "--search", 
        type=str, 
        help="Wykonać zapytanie wyszukiwania"
    )
    parser.add_argument(
        "--type", 
        choices=['rent', 'sale'], 
        help="Typ ogłoszeń (rent/sale)"
    )
    parser.add_argument(
        "--top-k", 
        type=int, 
        default=10, 
        help="Liczba wyników wyszukiwania (domyślnie 10)"
    )
    parser.add_argument(
        "--verbose", 
        action="store_true", 
        help="Szczegółowe wyświetlanie"
    )
    
    args = parser.parse_args()
    
    # Ustawienie poziomu logowania
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Inicjalizacja bazy danych
    vector_db = RealEstateVectorDB()
    
    # Czyszczenie
    if args.reset:
        vector_db.clear_database()
        vector_db = RealEstateVectorDB()  # Przecreujemy
    
    # Wypełnianie
    if args.populate:
        vector_db.populate_from_mongo(
            limit=args.limit,
            use_chunks=args.chunks
        )
    
    # Statystyki
    if args.stats:
        vector_db.get_stats()
    
    # Wyszukiwanie
    if args.search:
        results = vector_db.semantic_search(
            query=args.search,
            collection_type=args.type,
            top_k=args.top_k
        )
        print_search_results(results, show_full=args.verbose)


if __name__ == "__main__":
    main()