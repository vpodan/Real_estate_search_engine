from fastapi import FastAPI
from pydantic import BaseModel
import os
import json
import openai
from dotenv import load_dotenv
import pymongo
import logging


load_dotenv()
openai.api_key = os.environ.get("OPENAI_API_KEY")

MONGO_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.environ.get("MONGODB_DB", "real_estate")

mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_db     = mongo_client[MONGO_DB_NAME]

app = FastAPI()
logging.basicConfig(level=logging.DEBUG)

class PromptRequest(BaseModel):
    prompt: str

@app.post("/chat")
async def chat(request: PromptRequest):
    try:
        # Wywołanie OpenAI do wydobycia kryteriów
        completion = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": request.prompt}],
            functions=[_get_openai_function_schema()],
            function_call="auto"
        )

        message = completion.choices[0].message

        # Jeśli OpenAI zwróciło wywołanie funkcji
        if message.function_call:
            raw_args = message.function_call.arguments
            try:
                args_dict = json.loads(raw_args)
            except json.JSONDecodeError:
                return {"error": "Nie udało się zdekodować JSON-a z function_call.arguments"}

            # Zbieranie pełnego zestawu kryteriów
            criteria = {
                "province":         args_dict.get("province"),
                "city":             args_dict.get("city"),
                "district":         args_dict.get("district"),
                "neighbourhood":    args_dict.get("neighbourhood"),
                "street":           args_dict.get("street"),
                "house_number":     args_dict.get("house_number"),
                "room_count":       args_dict.get("room_count"),
                "space_sm":         args_dict.get("space_sm"),
                "floor":            args_dict.get("floor"),
                "max_price":        args_dict.get("max_price"),
                "transaction_type": args_dict.get("transaction_type"),
                "market_type":      args_dict.get("market_type"),
                "stan_wykonczenia": args_dict.get("stan_wykonczenia"),
                "min_build_year":   args_dict.get("min_build_year"),
                "max_build_year":   args_dict.get("max_build_year"),
                "building_material": args_dict.get("building_material"),
                "building_type":    args_dict.get("building_type"),
                "ogrzewanie":       args_dict.get("ogrzewanie"),
                "max_czynsz":       args_dict.get("max_czynsz"),
                "has_garage":       args_dict.get("has_garage"),
                "has_parking":      args_dict.get("has_parking"),
                "has_balcony":      args_dict.get("has_balcony"),
                "has_elevator":     args_dict.get("has_elevator"),
                "has_air_conditioning": args_dict.get("has_air_conditioning"),
                "pets_allowed":     args_dict.get("pets_allowed"),
                "furnished":        args_dict.get("furnished"),
            }
            logging.debug(f"Wydobyte kryteria: {criteria}")

            search_result = search_listings(criteria)
            
            return {
                "criteria": criteria,
                "listings": search_result["listings"]
            }

        # Jeśli OpenAI nie zwróciło function_call to zwykła odpowiedź tekstowa
        else:
            return {"response": message.content}

    except Exception as ex:
        logging.error(f"Błąd w /chat: {ex}")
        return {"error": str(ex)}


def _get_openai_function_schema():
    return {
        "name": "extract_search_criteria",
        "description": (
            "Wydobywa kluczowe kryteria wyszukiwania mieszkania z wiadomości użytkownika. "
            "Zawsze zwracaj wszystkie klucze nawet jeśli nie uda się znaleźć danej informacji "
            "wtedy ustaw wartość na null. Również zwróć uwagę, czy użytkownik chce kupić, "
            "czy wynająć mieszkanie; jeżeli nie podano takiej informacji, zwróć null."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "province": {
                    "type": ["string", "null"],
                    "description": "Województwo po polsku, np. 'pomorskie'. Jeśli nie podano null."
                },
                "city": {
                    "type": ["string", "null"],
                    "description": "Miasto, np. 'Gdańsk'. Jeśli nie podano null."
                },
                "district": {
                    "type": ["string", "null"],
                    "description": "Dzielnica/Osiedle, np. 'Wrzeszcz'. Jeśli użytkownik wymienia kilka dzielnic (np. 'Bemowo lub Mokotów'), zwróć pierwszą wymienioną. Jeśli nie podano null."
                },
                "districts": {
                    "type": ["array", "null"],
                    "items": {"type": "string"},
                    "description": "Lista dzielnic jeśli użytkownik wymienia kilka (np. ['Bemowo', 'Mokotów'] dla 'Bemowo lub Mokotów'). Jeśli tylko jedna dzielnica lub nie podano - null."
                },
                "neighbourhood": {
                    "type": ["string", "null"],
                    "description": "Mniejsze osidle wewnątrz osiedla, np. Wrzeszcz - Wrzeszcz Dolny Jeśli nie podano null."
                },
                "street": {
                    "type": ["string", "null"],
                    "description": "Ulica, np. 'Grunwaldzka'. Jeśli nie podano null."
                },
                "house_number": {
                    "type": ["integer", "null"],
                    "description": "Numer domu (liczba całkowita). Jeśli nie podano null."
                },
                            "room_count": {
                                "type": ["integer", "null"],
                                "description": "Liczba pokoi jako liczba całkowita, np. 2. Jeśli nie podano null."
                            },
                            "space_sm": {
                                "type": ["number", "null"],
                                "description": "Powierzchnia w metrach kwadratowych, np. 45.0. Jeśli nie podano null."
                            },
                "floor": {
                    "type": ["integer", "null"],
                    "description": "Piętro, np. 0=parter, 1, 2, …, 10. Jeśli nie podano – null. UWAGA: Nie interpretuj semantycznych opisów jak 'wysokie piętro', 'na górze' - to są opisy dla wyszukiwania semantycznego, nie konkretne numery pięter."
                },
                "max_price": {
                    "type": "integer",
                    "description": "Maksymalna cena w zł (bez 'zł'), np. 3000."
                },
                "transaction_type": {
                    "type": ["string", "null"],
                    "description": (
                        "Typ transakcji: 'kupno' lub 'wynajem'. "
                        "Jeśli nie określono -> null."
                    )
                },
                "market_type": {
                    "type": ["string", "null"],
                    "description": "Typ rynku: 'PRIMARY' (pierwotny) lub 'SECONDARY' (wtórny). Jeśli nie podano null."
                },
                "stan_wykonczenia": {
                    "type": ["string", "null"],
                    "description": "Stan wykończenia: 'to_completion' (do wykończenia) lub 'ready_to_use' (gotowe do użytku). Jeśli nie podano null."
                },
                "min_build_year": {
                    "type": ["integer", "null"],
                    "description": "Minimalny rok budowy (nie starsze niż X rok = min_build_year X), np. 2008. Jeśli nie podano null."
                },
                "max_build_year": {
                    "type": ["integer", "null"],
                    "description": "Maksymalny rok budowy (nie nowsze niż X rok = max_build_year X), np. 2020. Jeśli nie podano null."
                },
                "building_material": {
                    "type": ["string", "null"],
                    "description": "Materiał budynku: 'breezeblock', 'brick', 'concrete_plate', 'silikat', 'reinforced_concrete', 'wood'. Jeśli nie podano null."
                },
                "building_type": {
                    "type": ["string", "null"],
                    "description": "Typ budynku: 'block', 'apartment', 'tenement', 'infill'. Jeśli nie podano null."
                },
                "ogrzewanie": {
                    "type": ["string", "null"],
                    "description": "Typ ogrzewania: 'urban', 'gas', 'electrical', 'boiler_room'. Jeśli nie podano null."
                },
                "max_czynsz": {
                    "type": ["integer", "null"],
                    "description": "Maksymalny czynsz w zł (tylko dla wynajmu), np. 500. Jeśli nie podano null."
                },
                "has_garage": {
                    "type": ["boolean", "null"],
                    "description": "Czy mieszkanie ma garaż. Ustaw true jeśli użytkownik wspomina 'garaż', 'garage', 'miejsce w garażu'. Ustaw false jeśli wyraźnie mówi że nie ma. Jeśli nie podano null."
                },
                "has_parking": {
                    "type": ["boolean", "null"],
                    "description": "Czy mieszkanie ma miejsce parkingowe. Ustaw true jeśli użytkownik wspomina 'parking', 'miejsce parkingowe', 'parking podziemny'. Ustaw false jeśli wyraźnie mówi że nie ma. Jeśli nie podano null."
                },
                "has_balcony": {
                    "type": ["boolean", "null"],
                    "description": "Czy mieszkanie ma balkon. Ustaw true jeśli użytkownik wspomina 'balkon', 'loggia', 'taras'. Ustaw false jeśli wyraźnie mówi że nie ma. Jeśli nie podano null."
                },
                "has_elevator": {
                    "type": ["boolean", "null"],
                    "description": "Czy budynek ma windę. Ustaw true jeśli użytkownik wspomina 'winda', 'elevator', 'winda osobowa'. Ustaw false jeśli wyraźnie mówi że nie ma. Jeśli nie podano null."
                },
                "has_air_conditioning": {
                    "type": ["boolean", "null"],
                    "description": "Czy mieszkanie ma klimatyzację. Ustaw true jeśli użytkownik wspomina 'klimatyzacja', 'air conditioning', 'klima'. Ustaw false jeśli wyraźnie mówi że nie ma. Jeśli nie podano null."
                },
                "pets_allowed": {
                    "type": ["boolean", "null"],
                    "description": "Czy zwierzęta są dozwolone. Ustaw true jeśli użytkownik wspomina 'zwierzęta', 'pets', 'psy', 'koty'. Ustaw false jeśli wyraźnie mówi że nie ma. Jeśli nie podano null."
                },
                "furnished": {
                    "type": ["boolean", "null"],
                    "description": "Czy mieszkanie jest umeblowane. Ustaw true jeśli użytkownik wspomina 'umeblowane', 'furnished', 'z meblami'. Ustaw false jeśli wyraźnie mówi że nie ma. Jeśli nie podano null."
                }
            },
                        "required": ["max_price"]
        }
    }


def extract_criteria_from_prompt(prompt_text: str) -> dict:
    try:
        completion = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt_text}],
            functions=[_get_openai_function_schema()],
            function_call="auto"
        )

        message = completion.choices[0].message

        if message.function_call:
            raw_args = message.function_call.arguments
            try:
                args_dict = json.loads(raw_args)
            except json.JSONDecodeError:
                raise ValueError("Nie udało się zdekodować JSON-a z function_call.arguments")

            criteria = {
                "province":         args_dict.get("province"),
                "city":             args_dict.get("city"),
                "district":         args_dict.get("district"),
                "districts":        args_dict.get("districts"),
                "neighbourhood":    args_dict.get("neighbourhood"),
                "street":           args_dict.get("street"),
                "house_number":     args_dict.get("house_number"),
                "room_count":       args_dict.get("room_count"),
                "space_sm":         args_dict.get("space_sm"),
                "floor":            args_dict.get("floor"),
                "max_price":        args_dict.get("max_price"),
                "transaction_type": args_dict.get("transaction_type"),
                "market_type":      args_dict.get("market_type"),
                "stan_wykonczenia": args_dict.get("stan_wykonczenia"),
                "min_build_year":   args_dict.get("min_build_year"),
                "max_build_year":   args_dict.get("max_build_year"),
                "building_material": args_dict.get("building_material"),
                "building_type":    args_dict.get("building_type"),
                "ogrzewanie":       args_dict.get("ogrzewanie"),
                "max_czynsz":       args_dict.get("max_czynsz"),
                "has_garage":       args_dict.get("has_garage"),
                "has_parking":      args_dict.get("has_parking"),
                "has_balcony":      args_dict.get("has_balcony"),
                "has_elevator":     args_dict.get("has_elevator"),
                "has_air_conditioning": args_dict.get("has_air_conditioning"),
                "pets_allowed":     args_dict.get("pets_allowed"),
                "furnished":        args_dict.get("furnished"),
            }
            
            logging.debug(f"Wydobyte kryteria: {criteria}")
            return criteria

        else:
            raise ValueError("OpenAI nie zwróciło function_call")

    except Exception as ex:
        logging.error(f"Błąd podczas wydobywania kryteriów: {ex}")
        raise


def search_listings(criteria: dict) -> dict:
   
    try:
        # Tworzymy filtr MongoDB, uwzględniając tylko pola nie-null
        query_filter: dict = {}

        # Pola tekstowe: jeśli nie None i niepuste szukamy dokładnego dopasowania
        if criteria.get("province"):
            query_filter["province"] = criteria["province"]
        if criteria.get("city"):
            query_filter["city"] = criteria["city"]
        
        # Dzielnice: obsługujemy zarówno jedną dzielnicę, jak i kilka
        if criteria.get("districts") and len(criteria["districts"]) > 0:
            query_filter["district"] = {"$in": criteria["districts"]}
        elif criteria.get("district"):
            query_filter["district"] = criteria["district"]
            
        if criteria.get("neighbourhood"):
            query_filter["neighbourhood"] = criteria["neighbourhood"]    
        if criteria.get("street"):
            query_filter["street"] = criteria["street"]

        # Pola numeryczne: jeśli nie None i nie 0 dodajemy warunek
        if criteria.get("house_number") is not None:
            query_filter["house_number"] = criteria["house_number"]
        if criteria.get("room_count") is not None and criteria.get("room_count") != 0:
            query_filter["room_count"] = criteria["room_count"]
        if criteria.get("space_sm") is not None and criteria.get("space_sm") != 0:
            query_filter["space_sm"] = {"$gte": criteria["space_sm"]}
        if criteria.get("floor") is not None:
            query_filter["floor"] = criteria["floor"]
        if criteria.get("max_price") is not None:
            query_filter["price"] = {"$lte": criteria["max_price"]}
        
        # Nowe filtry dla sprzedaży
        if criteria.get("market_type") is not None:
            query_filter["market_type"] = criteria["market_type"]
        if criteria.get("stan_wykonczenia") is not None:
            query_filter["stan_wykonczenia"] = criteria["stan_wykonczenia"]
        if criteria.get("building_material") is not None:
            query_filter["building_material"] = criteria["building_material"]
        if criteria.get("building_type") is not None:
            query_filter["building_type"] = criteria["building_type"]
        if criteria.get("ogrzewanie") is not None:
            query_filter["ogrzewanie"] = criteria["ogrzewanie"]
        
        # Filtr roku budowy
        if criteria.get("min_build_year") is not None or criteria.get("max_build_year") is not None:
            build_year_filter = {}
            if criteria.get("min_build_year") is not None:
                build_year_filter["$gte"] = str(criteria["min_build_year"])
            if criteria.get("max_build_year") is not None:
                build_year_filter["$lte"] = str(criteria["max_build_year"])
            query_filter["build_year"] = build_year_filter
        
        # Filtr czynszu (tylko dla wynajmu)
        if criteria.get("max_czynsz") is not None:
            query_filter["czynsz"] = {"$lte": criteria["max_czynsz"]}

        # Filtry boolean dla dodatkowych cech
        if criteria.get("has_garage") is not None:
            query_filter["has_garage"] = criteria["has_garage"]
        if criteria.get("has_parking") is not None:
            query_filter["has_parking"] = criteria["has_parking"]
        if criteria.get("has_balcony") is not None:
            query_filter["has_balcony"] = criteria["has_balcony"]
        if criteria.get("has_elevator") is not None:
            query_filter["has_elevator"] = criteria["has_elevator"]
        if criteria.get("has_air_conditioning") is not None:
            query_filter["has_air_conditioning"] = criteria["has_air_conditioning"]
        if criteria.get("pets_allowed") is not None:
            query_filter["pets_allowed"] = criteria["pets_allowed"]
        if criteria.get("furnished") is not None:
            query_filter["furnished"] = criteria["furnished"]

        logging.debug(f"Utworzony filtr Mongo: {query_filter}")

        # Określamy, w jakiej kolekcji szukać: sale_listings, rent_listings lub obie
        listings = []
        to_search_collections = []

        if criteria.get("transaction_type") == "kupno":
            to_search_collections = ["sale_listings"]
        elif criteria.get("transaction_type") == "wynajem":
            to_search_collections = ["rent_listings"]
        else:
            to_search_collections = ["sale_listings", "rent_listings"]

        # Wykonujemy zapytania do odpowiednich kolekcji
        for coll_name in to_search_collections:
            coll = mongo_db[coll_name]
            cursor = coll.find(query_filter)
            for doc in cursor:
                raw_link = doc.get("link", "")
                # Sprawdzamy czy link już jest pełny
                if raw_link.startswith("http"):
                    full_link = raw_link
                else:
                    full_link = f"https://www.otodom.pl{raw_link}"
                listings.append({
                    "source_collection": coll_name,
                    "link": full_link,
                    "title": doc.get("title"),
                    "_id": doc.get("_id"),
                    "price": doc.get("price"),
                    "room_count": doc.get("room_count"),
                    "space_sm": doc.get("space_sm"),
                    "city": doc.get("city"),
                    "district": doc.get("district"),
                    "market_type": doc.get("market_type"),
                    "stan_wykonczenia": doc.get("stan_wykonczenia"),
                    "build_year": doc.get("build_year"),
                    "building_material": doc.get("building_material"),
                    "ogrzewanie": doc.get("ogrzewanie"),
                    "czynsz": doc.get("czynsz"),
                    "has_garage": doc.get("has_garage"),
                    "has_parking": doc.get("has_parking"),
                    "has_balcony": doc.get("has_balcony"),
                    "has_elevator": doc.get("has_elevator"),
                    "has_air_conditioning": doc.get("has_air_conditioning"),
                    "pets_allowed": doc.get("pets_allowed"),
                    "furnished": doc.get("furnished"),
                    "description": doc.get("description")
                })

        return {
            "total": len(listings),
            "listings": listings
        }

    except Exception as ex:
        logging.error(f"Błąd podczas wyszukiwania ogłoszeń: {ex}")
        return {"total": 0, "listings": []}

















