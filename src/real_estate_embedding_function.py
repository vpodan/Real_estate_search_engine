# real_estate_embedding_function.py
import os
from typing import List, Dict, Optional, Tuple
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
import re
import logging
from functools import lru_cache

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Stałe
DEFAULT_CHUNK_SIZE = 1000
DEFAULT_CHUNK_OVERLAP = 200
OPENAI_MODEL = "text-embedding-3-large"
FALLBACK_MODEL = "all-MiniLM-L6-v2"


@lru_cache(maxsize=1)
def get_embedding_function(model_name: str = OPENAI_MODEL):
   
    try:
        openai_key = os.getenv("OPENAI_API_KEY")
        if openai_key and openai_key != "your_openai_api_key_here":
            logger.info(f"Używamy OpenAI API z modelem: {model_name}")
            return OpenAIEmbeddings(
                model=model_name,
                openai_api_key=openai_key,
                max_retries=3,
                request_timeout=30
            )
        else:
            logger.warning("Nie znaleziono klucza OpenAI API, używamy sentence-transformers")
            return SentenceTransformerEmbedding(model_name=FALLBACK_MODEL)
    except Exception as e:
        logger.error(f"Błąd podczas tworzenia funkcji embedding: {e}")
        return SentenceTransformerEmbedding(model_name=FALLBACK_MODEL)


class SentenceTransformerEmbedding:
    
    def __init__(self, model_name: str = FALLBACK_MODEL):
        logger.info(f"Ładowanie modelu: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.model_name = model_name
    
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        try:
            return self.model.encode(
                texts, 
                batch_size=32,
                show_progress_bar=len(texts) > 100
            ).tolist()
        except Exception as e:
            logger.error(f"Błąd podczas tworzenia embeddings: {e}")
            return []
    
    def embed_query(self, text: str) -> List[float]:
        
        try:
            return self.model.encode([text])[0].tolist()
        except Exception as e:
            logger.error(f"Błąd podczas tworzenia embedding zapytania: {e}")
            return []


def clean_text(text: str) -> str:
    
    if not text:
        return ""
    # Usuwamy wielokrotne spacje
    text = re.sub(r'\s+', ' ', text)
    # Usuwamy symbole specjalne, które nie niosą znaczenia
    text = re.sub(r'[^\w\s.,!?;:()\-–—€£$%°№]+', ' ', text)
    # Usuwamy nadmiarowe znaki interpunkcyjne
    text = re.sub(r'([.,!?;:])\1+', r'\1', text)
    
    return text.strip()

def normalize_price(price) -> Optional[float]:
    if price is None:
        return None
    try:
        # Usuwamy spacje i konwertujemy na float
        if isinstance(price, str):
            price = price.replace(' ', '').replace(',', '.')
        return float(price)
    except (ValueError, TypeError):
        return None

def create_price_text(listing_data: Dict) -> str:
    """Tworzy tekst z informacją o cenie"""
    price = listing_data.get('price')
    if price is None:
        return ""
    
    # Normalizujemy cenę
    normalized_price = normalize_price(price)
    if normalized_price is None:
        return str(price) + " zł"
    
    # Formatujemy cenę
    return f"{normalized_price:,.0f} zł"

def create_listing_text_for_embedding(
    listing_data: Dict,
    include_description: bool = True,
    prioritize_search_fields: bool = True
) -> str:
    """
    Tworzy zoptymalizowany tekst dla embedding.
    Args:
        listing_data: Dane ogłoszenia z MongoDB
        include_description: Czy uwzględnić pełny opis
        prioritize_search_fields: Czy priorytetyzować pola do wyszukiwania     
    Returns:
        Zoptymalizowany tekst dla embedding
    """
    sections = []
    
    # 1. NAGŁÓWEK 
    if listing_data.get('title'):
        title = clean_text(listing_data['title'])
        sections.append(f"OGŁOSZENIE: {title}")
    
    # 2. KLUCZOWE CHARAKTERYSTYKI 
    specs = []
    if listing_data.get('room_count'):
        room_variants = get_room_text_variants(listing_data['room_count'])
        specs.append(room_variants)
    
    if listing_data.get('space_sm'):
        space = listing_data['space_sm']
        specs.append(f"{space} m² powierzchnia {space} metrów kwadratowych")
    
    if listing_data.get('floor') is not None:
        floor = listing_data['floor']
        if floor == 0:
            specs.append("parter pierwsze piętro ground floor")
        else:
            if prioritize_search_fields:
                specs.append(f"{floor} piętro floor {floor} poziom {floor} piętro")
            else:
                specs.append(f"{floor} piętro floor {floor}")
    
    if specs:
        sections.append(f"CHARAKTERYSTYKI: {' | '.join(specs)}")
    
    # 3.LOKALIZACJA 
    location = create_location_text(listing_data)
    if location:
        sections.append(f"ADRES: {location}")
    
    # 4.CENA 
    price_text = create_price_text(listing_data)
    if price_text:
        sections.append(f"CENA: {price_text}")
    
    # 5.OPIS 
    if include_description and listing_data.get('description'):
        description = clean_text(listing_data['description'])
        max_length = 700 if prioritize_search_fields else 1000
        if len(description) > max_length:
            description = description[:max_length] + "..."
        sections.append(f"OPIS: {description}")
    
    # 6.BUDYNEK I STAN
    building_text = create_building_text(listing_data)
    if building_text:
        sections.append(f"BUDYNEK: {building_text}")
    
    # 7.UDOGODNIENIA
    amenities_text = create_amenities_text(listing_data)
    if amenities_text:
        sections.append(f"UDOGODNIENIA: {amenities_text}")
    
    # 8.DODATKOWE CECHY
    if listing_data.get('features_by_category'):
        features = clean_text(listing_data['features_by_category'])
        sections.append(f"SPECJALNE: {features}")
    
    # 9.METADANE
    metadata = []
    if listing_data.get('market_type'):
        metadata.append(f"rynek {listing_data['market_type']}")
    if listing_data.get('forma_wlasnosci'):
        metadata.append(f"własność {listing_data['forma_wlasnosci']}")
    
    if metadata:
        sections.append(f"DODATKOWO: {' | '.join(metadata)}")
    
    return " || ".join(sections)


def get_room_text_variants(room_count: int) -> str:

    variants = [f"{room_count} pokoi"]
    
    room_names = {
        1: ["jednopokojowe", "jeden pokój", "1-pokojowe", "kawalerka"],
        2: ["dwupokojowe", "dwa pokoje", "2-pokojowe"],
        3: ["trzypokojowe", "trzy pokoje", "3-pokojowe"],
        4: ["czteropokojowe", "cztery pokoje", "4-pokojowe"],
        5: ["pięciopokojowe", "pięć pokoi", "5-pokojowe"]
    }
    
    if room_count in room_names:
        variants.extend(room_names[room_count])
    else:
        variants.append(f"{room_count}-pokojowe")
    
    return " ".join(variants)


def create_location_text(listing_data: Dict) -> str:

    parts = []
    
    if listing_data.get('city'):
        city = listing_data['city']
        parts.append(city)
        if city.lower() == 'warszawa':
            parts.append("Warsaw Варшава")
        elif city.lower() == 'kraków':
            parts.append("Krakow Краков")
    
    if listing_data.get('district'):
        district = listing_data['district']
        parts.append(f"dzielnica {district} rejon {district}")
    
    if listing_data.get('neighbourhood'):
        neighbourhood = listing_data['neighbourhood']
        parts.append(f"osiedle {neighbourhood} dzielnica {neighbourhood}")
    
    if listing_data.get('street'):
        street = listing_data['street']
        street_text = f"ulica {street} ul. {street}"
        if listing_data.get('house_number'):
            street_text += f" {listing_data['house_number']}"
        parts.append(street_text)
    
    return " | ".join(parts)



def create_building_text(listing_data: Dict) -> str:
  
    parts = []
    
    if listing_data.get('build_year'):
        year = listing_data['build_year']
        parts.append(f"zbudowane w {year} roku {year}")
        
        try:
            year_int = int(year)
            if year_int >= 2020:
                parts.append("nowy budynek nowy budynek")
            elif year_int >= 2010:
                parts.append("nowoczesny nowoczesny")
            elif year_int >= 2000:
                parts.append("współczesny")
            else:
                parts.append("stary stary kamienica")
        except (ValueError, TypeError):
          
            pass
    
    if listing_data.get('stan_wykonczenia'):
        condition = listing_data['stan_wykonczenia']
        parts.append(f"stan {condition} stan {condition}")
    
    return " | ".join(parts)


def create_amenities_text(listing_data: Dict) -> str:

    amenities = []
    
    # Rozszerzone mapowanie z synonimami i kontekstowymi słowami
    amenity_mapping = {
        'has_garage': 'garaż miejsce na samochód parking podziemny',
        'has_parking': 'parking miejsce postojowe postój samochód',
        'has_balcony': 'balkon loggia taras zewnętrzna przestrzeń',
        'has_loggia': 'loggia balkon zadaszony taras',
        'has_terrace': 'taras duży balkon przestrzeń zewnętrzna',
        'has_elevator': 'winda elevator dźwig transport pionowy',
        'has_air_conditioning': 'klimatyzacja AC chłodzenie wentylacja',
        'has_internet': 'internet wifi sieć połączenie',
        'has_fiber_internet': 'światłowód szybki internet szerokopasmowy',
        'has_security': 'ochrona security bezpieczeństwo monitoring',
        'has_intercom': 'domofon intercom wideodomofon kontrola dostępu',
        'has_gym': 'siłownia gym fitness trening sport',
        'has_pool': 'basen swimming pool pływanie woda',
        'has_sauna': 'sauna spa relaks parowa',
        'has_storage': 'komórka miejsce storage room przechowywanie',
        'has_basement': 'piwnica basement przechowywanie podziemie',
        'has_garden': 'ogród garden zieleń natura rośliny',
        'pets_allowed': 'zwierzęta dozwolone pets allowed pies kot',
        'furnished': 'umeblowane furnished meble wyposażone',
        'partially_furnished': 'częściowo umeblowane partial furnished'
    }
    
    for key, description in amenity_mapping.items():
        if listing_data.get(key):
            amenities.append(description)
    
    # Widoki
    views = []
    if listing_data.get('sea_view'):
        views.append('widok na morze morze sea view')
    if listing_data.get('mountain_view'):
        views.append('widok na góry góry mountain view')
    if listing_data.get('park_view'):
        views.append('widok na park park zieleń nature')
    
    if views:
        amenities.extend(views)
    
    return " | ".join(amenities)


def create_listing_chunks_for_embedding(
    listing_data: Dict,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP
) -> List[Tuple[str, Dict]]:
   
  
    full_text = create_listing_text_for_embedding(
        listing_data,
        include_description=True,
        prioritize_search_fields=True
    )
    
    # Jeśli tekst jest krótki, zwracamy jako jeden chunk
    if len(full_text) <= chunk_size:
        metadata = extract_listing_metadata(listing_data)
        return [(full_text, metadata)]
    
    # Dzielimy na chunki z inteligentnymi separatorami
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
        separators=[
            " || ",  # Główny separator sekcji
            " | ",   # Separator podsekcji
            "\n\n",
            "\n",
            ". ",
            " ",
            ""
        ]
    )
    
    chunks = text_splitter.split_text(full_text)
    
    # Tworzymy kontekst dla każdego chunka
    base_metadata = extract_listing_metadata(listing_data)
    context_prefix = create_context_prefix(listing_data)
    
    result = []
    for i, chunk in enumerate(chunks):
        # Dodajemy kontekst do każdego chunka
        enhanced_chunk = f"{context_prefix} || {chunk}"
        
        # Kopiujemy metadane i dodajemy informację o chunka
        chunk_metadata = base_metadata.copy()
        chunk_metadata['chunk_index'] = i
        chunk_metadata['total_chunks'] = len(chunks)
        
        result.append((enhanced_chunk, chunk_metadata))
    
    return result


def create_context_prefix(listing_data: Dict) -> str:
   
    context_parts = []
    
    if listing_data.get('_id'):
        context_parts.append(f"ID_{listing_data['_id']}")
    
    if listing_data.get('city'):
        context_parts.append(listing_data['city'])
    
    if listing_data.get('room_count'):
        context_parts.append(f"{listing_data['room_count']}pok")
    
    price = normalize_price(listing_data.get('price'))
    if price:
        context_parts.append(f"{int(price)}zł")
    
    return " ".join(context_parts)


def extract_listing_metadata(listing_data: Dict) -> Dict:

    metadata = {}
    
    if listing_data.get('_id'):
        metadata['listing_id'] = str(listing_data['_id'])
    numeric_fields = [
        'price', 'czynsz', 'room_count', 'space_sm', 
        'floor', 'build_year', 'cena_za_metr'
    ]
    
    for field in numeric_fields:
        value = listing_data.get(field)
        if value is not None:
            try:
                metadata[field] = float(value)
            except (ValueError, TypeError):
                pass
    
    # Pola kategoryczne
    categorical_fields = [
        'city', 'district', 'neighbourhood', 'building_type',
        'building_material', 'ogrzewanie', 'stan_wykonczenia',
        'forma_wlasnosci', 'market_type'
    ]
    
    for field in categorical_fields:
        value = listing_data.get(field)
        if value:
            metadata[field] = str(value)
    
    
    boolean_fields = [
        'has_garage', 'has_parking', 'has_balcony', 'has_elevator',
        'has_air_conditioning', 'pets_allowed', 'furnished'
    ]
    
    for field in boolean_fields:
        if listing_data.get(field):
            metadata[field] = True
    
    return metadata


def create_query_optimized_text(query: str, expand_synonyms: bool = True) -> str:
    
    query = clean_text(query.lower())
    
    if not expand_synonyms:
        return query
    
    synonyms = {
        'mieszkanie': ['mieszkanie', 'kwatera', 'apartamenty', 'flat', 'dom'],
        'pokój': ['pokój', 'room', 'pomieszczenie'],
        '1 pokój': ['jednopokojowe', 'kawalerka', 'studio', '1-pokojowe'],
        '2 pokoje': ['dwupokojowe', '2-pokojowe', 'two bedroom'],
        '3 pokoje': ['trzypokojowe', '3-pokojowe', 'three bedroom'],
        'cena': ['cena', 'koszt', 'price', 'kosztorys'],
        'tanie': ['tanie', 'niedrogo', 'tanio', 'cheap', 'dostępne'],
        'drogie': ['drogie', 'drogo', 'expensive', 'premium', 'luksczne'],
        'centrum': ['centrum', 'center', 'centralna część', 'śródmieście'],
        'blisko': ['blisko', 'obok', 'niedaleko', 'near', 'w pobliżu'],
        'warszawa': ['warszawa', 'warsaw', 'stolica'],
        'kraków': ['kraków', 'krakow', 'królewskie miasto'],
        'balkon': ['balkon', 'taras', 'zewnętrzna przestrzeń'],
        'winda': ['winda', 'elevator', 'dźwig'],
        'parking': ['parking', 'garaż', 'miejsce postojowe'],
        'zwierzęta': ['zwierzęta', 'pets', 'pies', 'kot', 'zwierzęta domowe'],
        'umeblowane': ['umeblowane', 'furnished', 'meble', 'wyposażone']
    }
    
    words = query.split()
    expanded_terms = []
    
    for word in words:
        expanded_terms.append(word)
        
        # Wyszukiwanie synonimów
        for key, values in synonyms.items():
            if word in values or word == key:
                expanded_terms.extend([v for v in values if v != word])
                break
    
    # Usuwamy duplikaty, zachowując kolejność
    unique_terms = []
    seen = set()
    for term in expanded_terms:
        if term not in seen:
            unique_terms.append(term)
            seen.add(term)
    
    return " ".join(unique_terms)


def validate_listing_data(listing_data: Dict) -> bool:
    
    required_fields = ['title', 'city']
    
    for field in required_fields:
        if not listing_data.get(field):
            logger.warning(f"Ogłoszenie nie zawiera wymaganego pola: {field}")
            return False
    
    return True


# Funkcja testowa
def test_embedding_creation():
    test_listing = {
        '_id': 'test_123',
        'title': 'Przytulne dwupokojowe mieszkanie w centrum',
        'description': 'Doskonałe mieszkanie z remontem, blisko metra',
        'city': 'Warszawa',
        'district': 'Śródmieście',
        'room_count': 2,
        'space_sm': 45,
        'floor': 3,
        'price': 3500,
        'czynsz': 500,
        'has_balcony': True,
        'has_elevator': True,
        'furnished': True
    }
    
    print("=" * 80)
    print("TEST: Tworzenie tekstu dla embedding")
    print("=" * 80)
    
    text = create_listing_text_for_embedding(test_listing)
    print(f"\nWygenerowany tekst ({len(text)} znaków):")
    print(text)
    
    print("\n" + "=" * 80)
    print("TEST: Tworzenie chunków")
    print("=" * 80)
    
    chunks = create_listing_chunks_for_embedding(test_listing, chunk_size=300)
    print(f"\nLiczba chunków: {len(chunks)}")
    for i, (chunk_text, metadata) in enumerate(chunks):
        print(f"\nChunk {i + 1}:")
        print(f"Długość: {len(chunk_text)} znaków")
        print(f"Metadane: {metadata}")
        print(f"Tekst: {chunk_text[:200]}...")


if __name__ == "__main__":
    test_embedding_creation()