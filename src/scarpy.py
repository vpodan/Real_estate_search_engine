from pymongo import MongoClient
import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
import json
import re
import numpy as np
import hashlib
import html 


client = MongoClient("mongodb://localhost:27017/")  
db = client["real_estate"]  
collection_rent = db["rent_listings"]  
collection_sale = db["sale_listings"]  


def extract_offer_id(link: str):
    
    if not isinstance(link, str) or not link:
        return None
    path = link.split('?')[0].rstrip('/')
    m = re.search(r'(ID[0-9A-Za-z]+)$', path)
    return m.group(1) if m else None


def data_localisation(localisation_str):
    def get(parts, i):
        return parts[i].strip() if i < len(parts) else None

    if not isinstance(localisation_str, str) or not localisation_str.strip():
        return (None, None, None, None, None, None)

    parts = [p.strip() for p in localisation_str.split(',') if p.strip()]

    # Sprawdzamy, czy jest ulica
    street_name, house_number = None, None
    if parts and re.match(r'^(ul\.?|al\.?|pl\.?|os\.?)\s+', parts[0], flags=re.I):
        street_full = re.sub(r'^\s*(ul\.?|al\.?|pl\.?|os\.?)\s+', '', parts[0], flags=re.I)
        m = re.search(r'(\d+[A-Za-z]?)$', street_full)
        if m:
            house_number = m.group(1)
            street_name = street_full[:m.start()].strip()
        else:
            street_name = street_full.strip()
        parts = parts[1:]  

    
    if len(parts) == 4:
        neighbourhood, district, city, province = parts
    elif len(parts) == 3:
        neighbourhood, city, province = parts
        district = None
    elif len(parts) == 2:
        city, province = parts
        neighbourhood, district = None, None
    else:
        neighbourhood = get(parts, 0)
        district     = get(parts, 1)
        city         = get(parts, 2)
        province     = get(parts, 3)

    return (street_name, house_number, neighbourhood, district, city, province)

def parse_czynsz(value):
    if not isinstance(value, str):
        return None
    s = value.replace('\xa0', ' ')
    m = re.search(r'(\d[\d\s]*)', s)          
    if not m:
        return None
    digits = re.sub(r'\D', '', m.group(1))   
    return int(digits) if digits else None



def extract_room_and_space(listing):
    room_count = 'N/A'
    space_sm = 'N/A'
    parent_element = listing.css('dl.css-1k6eezo')
    for dt, dd in zip(parent_element.css('dt::text'), parent_element.css('dd span::text')):
        label = dt.get().strip()
        value = dd.get().strip()
        if label == 'Liczba pokoi':
            room_count = value
        elif label == 'Cena za metr kwadratowy':
            space_sm = value
    return room_count, space_sm

def extract_representative(listing):
    # Szukamy wszystkich bloków z tekstami wewnątrz SellerInfoWrapper
    reps = listing.css('div[data-sentry-element="SellerInfoWrapper"] span::text').getall()

    if reps:
        # Łączymy wszystkie kawałki tekstu (np. "PARTNERZY..." + "Biuro nieruchomości")
        rep = " ".join([r.strip() for r in reps if r.strip()])
        return rep
    else:
        return "Oferta prywatna"

def extract_room_count(value):
    match = re.search(r'(\d+)', str(value))
    if match:
        return int(match.group(1))
    return None

def extract_floor(value):
    if isinstance(value, str):
        if "parter" in value.lower():
            return 0
        elif "10+" in value:
            return 10
        match = re.search(r'(\d+)', value)
        if match:
            return int(match.group(1))
    return None

def extract_space(value):
    match = re.search(r'([\d.]+)', str(value).replace(',', '.'))
    if match:
        return float(match.group(1))
    return None

def parse_house_number(value):
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None

def clean_html_description(description):
    if not description:
        return description
    
    # Usuwamy HTML tagi
    clean_text = re.sub(r'<[^>]+>', '', description)
    
    # Dekodujemy HTML entities (&nbsp;, &amp; i inne)
    clean_text = html.unescape(clean_text)
    
    # Usuwamy dodatkowe spacje i nowe linie
    clean_text = re.sub(r'\s+', ' ', clean_text)
    
    # Usuwamy spacje na początku i końcu
    clean_text = clean_text.strip()
    
    # Zastępujemy wielokrotne spacje pojedynczymi
    clean_text = re.sub(r' {2,}', ' ', clean_text)
    
    return clean_text  

def parse_features_to_individual_fields(item, features_by_category):
 
    # Słownik dla mapowania polskich nazw na angielskie klucze
    feature_mapping = {
        # Parkowanie i garaż
        'garaż/miejsce parkingowe': 'has_garage',
        'miejsce parkingowe': 'has_parking',
        'garaż': 'has_garage',
        'parking': 'has_parking',
        
        # Balkony i tarasy
        'balkon': 'has_balcony',
        'loggia': 'has_loggia',
        'taras': 'has_terrace',
        'balkon/taras': 'has_balcony',
        
        # Winda
        'winda': 'has_elevator',
        'winda osobowa': 'has_elevator',
        
        # Klimatyzacja
        'klimatyzacja': 'has_air_conditioning',
        
        # Internet
        'internet': 'has_internet',
        'internet światłowodowy': 'has_fiber_internet',
        
        # Bezpieczeństwo
        'monitoring': 'has_security',
        'ochrona': 'has_security',
        'domofon': 'has_intercom',
        
        # Sport i rekreacja
        'siłownia': 'has_gym',
        'basen': 'has_pool',
        'sauna': 'has_sauna',
        
        # Dodatkowe udogodnienia
        'pralnia': 'has_laundry',
        'przechowalnia': 'has_storage',
        'piwnica': 'has_basement',
        'strych': 'has_attic',
        
        # Zwierzęta
        'zwierzęta dozwolone': 'pets_allowed',
        
        # Meble
        'umeblowane': 'furnished',
        'częściowo umeblowane': 'partially_furnished',
        
        # Ogród i zieleń
        'ogród': 'has_garden',
        'balkon z ogrodem': 'has_garden_balcony',
        
        # Widok
        'widok na morze': 'sea_view',
        'widok na góry': 'mountain_view',
        'widok na park': 'park_view',
    }
    
    # Inicjalizujemy wszystkie pola jako False
    for feature_key in feature_mapping.values():
        item[feature_key] = False
    
    # Parsujemy features_by_category
    for category in features_by_category:
        values = category.get('values', [])
        for value in values:
            # Szukamy dokładnego dopasowania
            if value in feature_mapping:
                item[feature_mapping[value]] = True
            else:
                # Szukamy częściowego dopasowania (dla bardziej elastycznego wyszukiwania)      
                for polish_name, english_key in feature_mapping.items():
                    if polish_name.lower() in value.lower() or value.lower() in polish_name.lower():
                        item[english_key] = True
                        break


def extract_additional_info_from_json(item, data_json, spider_type='rent'):
    
    if not data_json:
        return
        
    try:
        data = json.loads(data_json)
        ad_data = _find_ad_data(data)
        
        if ad_data:
            # Features by category (grupowane cechy)
            features_by_category = ad_data.get('featuresByCategory', [])
            category_features = []
            for category in features_by_category:
                label = category.get('label', '')
                values = category.get('values', [])
                if label and values:
                    category_features.append(f"{label}: {', '.join(values)}")
            item['features_by_category'] = ' | '.join(category_features) if category_features else None
            
            # Parsujemy features_by_category w oddzielne pola dla wygodnego wyszukiwania
            parse_features_to_individual_fields(item, features_by_category)
            
            # Rok budowy
            build_year = None
            target_data = ad_data.get('target', {})
            characteristics = ad_data.get('characteristics', [])
            
            if target_data and 'Build_year' in target_data:
                build_year = target_data['Build_year']
            
            # Alternatywny wyszukiwanie w characteristics
            if not build_year:
                for char in characteristics:
                    if char.get('key') == 'build_year':
                        build_year = char.get('value')
                        break
            
            item['build_year'] = build_year
            
                
            building_type = None
            if target_data and 'Building_type' in target_data:
                building_type_list = target_data['Building_type']
                if isinstance(building_type_list, list) and building_type_list:
                    building_type = building_type_list[0]
            
            # Alternatywny wyszukiwanie w characteristics
            if not building_type:
                for char in characteristics:
                    if char.get('key') == 'building_type':
                        building_type = char.get('localizedValue', char.get('value'))
                        break
            
            item['building_type'] = building_type
            
            # Materiał budynku
            building_material = None
            if target_data and 'Building_material' in target_data:
                building_material_list = target_data['Building_material']
                if isinstance(building_material_list, list) and building_material_list:
                    building_material = building_material_list[0]
            
            # Alternatywny wyszukiwanie w characteristics
            if not building_material:
                for char in characteristics:
                    if char.get('key') == 'building_material':
                        building_material = char.get('localizedValue', char.get('value'))
                        break
            
            item['building_material'] = building_material
            
            # Ogrzewanie
            ogrzewanie = None
            # Szukamy w target
            if target_data and 'Heating' in target_data:
                heating_list = target_data['Heating']
                if isinstance(heating_list, list) and heating_list:
                    ogrzewanie = heating_list[0]
            
            # Alternatywny wyszukiwanie w characteristics
            if not ogrzewanie:
                for char in characteristics:
                    if char.get('key') == 'heating':
                        ogrzewanie = char.get('localizedValue', char.get('value'))
                        break
            
            # Wyszukiwanie w additionalInformation
            if not ogrzewanie:
                additional_info = ad_data.get('additionalInformation', [])
                for info in additional_info:
                    if info.get('label') == 'heating':
                        values = info.get('values', [])
                        if values:
                        
                            heating_value = values[0]
                            if '::' in heating_value:
                                ogrzewanie = heating_value.split('::')[1]
                            else:
                                ogrzewanie = heating_value
                        break
            
            item['ogrzewanie'] = ogrzewanie
            
            # Stan wykończenia
            stan_wykonczenia = None
            # Szukamy w target
            if target_data and 'Construction_status' in target_data:
                status_list = target_data['Construction_status']
                if isinstance(status_list, list) and status_list:
                    stan_wykonczenia = status_list[0]
            
            if not stan_wykonczenia:
                for char in characteristics:
                    if char.get('key') == 'construction_status':
                        stan_wykonczenia = char.get('localizedValue', char.get('value'))
                        break
            
            item['stan_wykonczenia'] = stan_wykonczenia
            
            # Dodatkowe pola dla sprzedaży
            if spider_type == 'sale':
                
                forma_wlasnosci = None
                
                if target_data and 'Building_ownership' in target_data:
                    ownership_list = target_data['Building_ownership']
                    if isinstance(ownership_list, list) and ownership_list:
                        forma_wlasnosci = ownership_list[0]
                
                if not forma_wlasnosci:
                    for char in characteristics:
                        if char.get('key') == 'building_ownership':
                            forma_wlasnosci = char.get('localizedValue', char.get('value'))
                            break
                
                item['forma_wlasnosci'] = forma_wlasnosci
                
                
                market_type = None
                
                if 'market' in ad_data:
                    market_type = ad_data['market']
                
                
                if not market_type and target_data and 'MarketType' in target_data:
                    market_type = target_data['MarketType']
                
               
                if not market_type:
                    for char in characteristics:
                        if char.get('key') == 'market':
                            market_type = char.get('localizedValue', char.get('value'))
                            break
                
                item['market_type'] = market_type
                
                
                cena_za_metr = None
               
                if target_data and 'Price_per_m' in target_data:
                    cena_za_metr = target_data['Price_per_m']
                
                
                if not cena_za_metr:
                    for char in characteristics:
                        if char.get('key') == 'price_per_m':
                           
                            cena_za_metr = char.get('value')
                            if cena_za_metr:
                                try:
                                    cena_za_metr = float(cena_za_metr)
                                except:
                                    cena_za_metr = None
                            break
                
                item['cena_za_metr'] = cena_za_metr
            
    except Exception as e:
        print(f"Additional info extraction failed: {e}")

def extract_description_from_response(response):
    
    description_texts = []
    
    # Najpierw próbujemy dokładne selektory
    try:
        # Szukamy bloku z opisem po data-sentry-element="AdDescriptionBase"
        desc_container = response.css('div[data-sentry-element="AdDescriptionBase"]')
        if desc_container:
            # Extract text from <p> tags inside the description container
            paragraphs = desc_container.css('p::text').getall()
            for p in paragraphs:
                cleaned = re.sub(r'\s+', ' ', p).strip()
                if cleaned and len(cleaned) > 10:  
                    description_texts.append(cleaned)
                    
    except Exception as e:
        print(f"Description extraction error: {e}")
    
    if not description_texts:
        try:
            data_json = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
            if data_json:
                data = json.loads(data_json)
                json_desc = _extract_description_from_next_json(data)
                if json_desc and len(json_desc) > 20:
                    description_texts.append(json_desc)
        except Exception as e:
            print(f"JSON extraction failed: {e}")
    
    # Tworzymy finalne opisywanie
    description = '\n'.join(description_texts).strip() if description_texts else None
    
    # Usuwamy opisywanie od HTML tagów
    description = clean_html_description(description)
    
    return description

# Pipeline do zapisywania danych w MongoDB
class MongoDBPipeline:
    def open_spider(self, spider):
        self.collection = collection_rent if spider.name == 'RentSpider' else collection_sale
        # _id to już unikalny klucz, dodatkowy indeks nie jest potrzebny

    def process_item(self, item, spider):
        # jeśli z jakiegoś powodu nie znaleźliśmy ID, zabezpieczmy się za pomocą skrótu linku
        if not item.get('_id'):
            item['_id'] = hashlib.md5((item.get('link') or '').encode('utf-8')).hexdigest()

        # upsert: aktualizujemy istniejący dokument z tym samym _id lub wstawiamy nowy
        self.collection.update_one({'_id': item['_id']}, {'$set': dict(item)}, upsert=True)
        return item


# Pająk Scrapy do zbierania ofert wynajmu mieszkań
class RentSpider(scrapy.Spider):
    name = 'RentSpider'
    start_urls = [f'https://www.otodom.pl/pl/wyniki/wynajem/mieszkanie/mazowieckie/warszawa/warszawa/warszawa?limit=36&by=DEFAULT&direction=DESC&page={i}' for i in range(50, 100)]

    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'ITEM_PIPELINES': {'__main__.MongoDBPipeline': 1},  
    }

    def parse(self, response):
      listings = response.css('article[data-sentry-component="AdvertCard"]')
      self.logger.info(f"Znaleziono ogłoszeń na stronie: {len(listings)}")

      for listing in listings:
        item = self._parse_listing(listing)      
        href = item.get('link')
        if href and href != 'N/A':
            
            href = response.urljoin(href)
            item['link'] = href
            
            yield response.follow(
                href,
                callback=self.parse_detail,
                cb_kwargs={'item': item},
            )
        else:
            yield item

    

    def _parse_listing(self, listing):
        item = {
            'link': listing.css('a[data-cy="listing-item-link"]::attr(href)').get(default='N/A'),
            'title': listing.css('p[data-cy="listing-item-title"]::text').get(default='N/A'),
            'localisation': listing.css('p.css-oxb2ca.e1cuc5p50::text').get(default='N/A'),
            'price': listing.css('span.css-ussjv3.eanmlll1::text').get(default='N/A').replace('\u00a0', ' '),
            'czynsz': listing.css('span.css-u0t81v.eanmlll2::text').get(default='N/A').replace('\u00a0', ' '),
            'floor': listing.css('dl.css-1k6eezo.e1am572w0 dt:contains("Piętro") + dd span::text').get(default='N/A'),
        }

        item['_id'] = extract_offer_id(item['link'])

        item['czynsz'] = parse_czynsz(item['czynsz'])


        # Pobieranie informacji o liczbie pokoi i powierzchni
        item['room_count'], item['space_sm'] = extract_room_and_space(listing)
        item['representative'] = extract_representative(listing)

        price_raw = item['price'].replace('zł', '').replace(',', '.').replace('\u00a0', '').replace(' ', '').strip()
        try:
            item['price'] = float(price_raw)
        except:
            item['price'] = None 

        item['room_count'] = extract_room_count(item['room_count'])
        item['floor'] = extract_floor(item['floor'])

        (item['street'], item['house_number'], item['neighbourhood'], item['district'], item['city'], item['province']) = data_localisation(item['localisation'])
         
        item['space_sm'] = extract_space(item['space_sm'])
        item['house_number'] = parse_house_number(item['house_number'])

        return item

    def parse_detail(self, response, item):
        description = extract_description_from_response(response)
        
        item['description'] = description
        if not description:
            self.logger.warning(f"Opis nie znaleziony dla: {response.url}")
        else:
            self.logger.info(f"Opis znaleziony, długość: {len(description)} znaków")
        
        data_json = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        extract_additional_info_from_json(item, data_json, spider_type='rent')
        
        yield item
    



def _find_ad_data(data):
    
    from collections.abc import Mapping, Sequence
    
    def walk(o):
        if isinstance(o, Mapping):
            # Sprawdzamy, czy obecny obiekt zawiera dane ogłoszenia
            if 'features' in o and 'featuresByCategory' in o and 'target' in o:
                return o
            # Szukamy w priorytetowych węzłach
            for k in ('ad', 'advert', 'listing', 'item', 'props', 'pageProps'):
                if k in o:
                    r = walk(o[k])
                    if r: return r
            # Szukamy we wszystkich wartościach
            for v in o.values():
                r = walk(v)
                if r: return r
        elif isinstance(o, Sequence) and not isinstance(o, (str, bytes)):
            for v in o:
                r = walk(v)
                if r: return r
        return None
    
    return walk(data)


def _extract_description_from_next_json(data):
   
    from collections.abc import Mapping, Sequence
    def walk(o):
        if isinstance(o, Mapping):
            for k in ('ad', 'advert', 'listing', 'item', 'data', 'payload', 'props', 'pageProps'):
                if k in o:
                    r = walk(o[k])
                    if r: return r
            if 'description' in o and isinstance(o['description'], str):
                s = o['description'].strip()
                if len(s) > 50:    
                    s = clean_html_description(s)
                    return s
            for v in o.values():
                r = walk(v)
                if r: return r
        elif isinstance(o, Sequence) and not isinstance(o, (str, bytes)):
            for v in o:
                r = walk(v)
                if r: return r
        return None
    return walk(data)



# Pająk Scrapy do zbierania ofert sprzedaży mieszkań
class SaleSpider(scrapy.Spider):
    name = 'SaleSpider'
    start_urls = [f'https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/mazowieckie/warszawa/warszawa/warszawa?ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&page={i}' for i in range(50, 100)]

    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'ITEM_PIPELINES': {'__main__.MongoDBPipeline': 1},  
    }
    def parse(self, response):
        listings = response.css('article[data-sentry-component="AdvertCard"]')
        self.logger.info(f"Znaleziono ogłoszeń na stronie: {len(listings)}")

        for listing in listings:
            item = self._parse_listing(listing)     
            href = item.get('link')
            if href and href != 'N/A':
             
                href = response.urljoin(href)
                item['link'] = href
               
                yield response.follow(
                    href,
                    callback=self.parse_detail,
                    cb_kwargs={'item': item},
                )
            else:
                yield item
    


    def _parse_listing(self, listing):
        item = {
            'link': listing.css('a[data-cy="listing-item-link"]::attr(href)').get(default='N/A'),
            'title': listing.css('p[data-cy="listing-item-title"]::text').get(default='N/A'),
            'localisation': listing.css('p.css-oxb2ca.e1cuc5p50::text').get(default='N/A'),
            'price': listing.css('span.css-ussjv3.eanmlll1::text').get(default='N/A').replace('\u00a0', ' '),
            'floor': listing.css('dl.css-1k6eezo.e1am572w0 dt:contains("Piętro") + dd span::text').get(default='N/A'),
            'representative': 'N/A'
        }
        item['_id'] = extract_offer_id(item['link'])
        item['room_count'], item['space_sm'] = extract_room_and_space(listing)
        item['representative'] = extract_representative(listing)

        price_raw = item['price'].replace('zł', '').replace(',', '.').replace('\u00a0', '').replace(' ', '').strip()
        try:
            item['price'] = float(price_raw)
        except:
            item['price'] = None  

        item['room_count'] = extract_room_count(item['room_count'])
        item['floor'] = extract_floor(item['floor'])

        (item['street'], item['house_number'], item['neighbourhood'], item['district'], item['city'], item['province']) = data_localisation(item['localisation'])

        item['space_sm'] = extract_space(item['space_sm'])
        item['house_number'] = parse_house_number(item['house_number'])
        
        return item
    
    def parse_detail(self, response, item):
        description = extract_description_from_response(response)
        
        item['description'] = description
        if not description:
            self.logger.warning(f"Opis nie znaleziony dla: {response.url}")
        else:
            self.logger.info(f"Opis znaleziony, długość: {len(description)} znaków")
        
        data_json = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        extract_additional_info_from_json(item, data_json, spider_type='sale')
        
        yield item
    
# Uruchomienie Scrapy
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
})

process.crawl(RentSpider)
process.crawl(SaleSpider)
process.start()

