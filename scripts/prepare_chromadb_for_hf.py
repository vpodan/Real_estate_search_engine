"""
Подготовка ChromaDB для развертывания на Hugging Face Spaces
"""
import os
import tarfile
import shutil
from pathlib import Path

CHROMA_DIR = "chroma_real_estate"
ARCHIVE_NAME = "chroma_real_estate.tar.gz"
MAX_SIZE_MB = 500  # Максимальный размер для удобной загрузки

def get_dir_size(path):
    """Вычисляет размер директории в MB"""
    total = 0
    for entry in os.scandir(path):
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += get_dir_size(entry.path)
    return total / (1024 * 1024)  # Convert to MB

def create_archive():
    """Создает архив ChromaDB"""
    print("📦 Создание архива ChromaDB...")
    
    if not os.path.exists(CHROMA_DIR):
        print(f"❌ Директория {CHROMA_DIR} не найдена!")
        print("   Сначала создайте и заполните ChromaDB:")
        print("   python real_estate_vector_db.py --populate --stats")
        return False
    
    # Проверяем размер
    size_mb = get_dir_size(CHROMA_DIR)
    print(f"📊 Размер директории: {size_mb:.2f} MB")
    
    if size_mb > MAX_SIZE_MB:
        print(f"⚠️ ВНИМАНИЕ: Директория слишком большая (>{MAX_SIZE_MB}MB)")
        print("   Рекомендуется:")
        print("   1. Создать ChromaDB на HF Spaces при первом запуске")
        print("   2. Или использовать Git LFS для загрузки")
        
        response = input("\nПродолжить создание архива? (y/n): ")
        if response.lower() != 'y':
            return False
    
    # Создаем архив
    try:
        with tarfile.open(ARCHIVE_NAME, "w:gz") as tar:
            tar.add(CHROMA_DIR, arcname=os.path.basename(CHROMA_DIR))
        
        archive_size = os.path.getsize(ARCHIVE_NAME) / (1024 * 1024)
        print(f"✅ Архив создан: {ARCHIVE_NAME}")
        print(f"📊 Размер архива: {archive_size:.2f} MB")
        print(f"📉 Степень сжатия: {(1 - archive_size/size_mb)*100:.1f}%")
        
        if archive_size < MAX_SIZE_MB:
            print("✅ Размер подходит для загрузки на HF Spaces")
        else:
            print("⚠️ Архив большой, используйте Git LFS:")
            print("   git lfs install")
            print(f"   git lfs track '{ARCHIVE_NAME}'")
            print("   git add .gitattributes")
            print(f"   git add {ARCHIVE_NAME}")
            print("   git commit -m 'Add ChromaDB'")
            print("   git push")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка создания архива: {e}")
        return False

def extract_archive():
    """Извлекает архив (для проверки)"""
    print("\n🔍 Проверка архива...")
    
    if not os.path.exists(ARCHIVE_NAME):
        print(f"❌ Архив {ARCHIVE_NAME} не найден!")
        return False
    
    try:
        # Создаем временную директорию
        test_dir = "test_extract"
        os.makedirs(test_dir, exist_ok=True)
        
        with tarfile.open(ARCHIVE_NAME, "r:gz") as tar:
            tar.extractall(test_dir)
        
        extracted_path = os.path.join(test_dir, CHROMA_DIR)
        if os.path.exists(extracted_path):
            size_mb = get_dir_size(extracted_path)
            print(f"✅ Архив извлечен успешно")
            print(f"📊 Размер извлеченных данных: {size_mb:.2f} MB")
            
            # Проверяем структуру
            files = list(Path(extracted_path).rglob("*"))
            print(f"📁 Количество файлов: {len(files)}")
            
            # Удаляем тестовую директорию
            shutil.rmtree(test_dir)
            print("✅ Проверка пройдена")
            return True
        else:
            print("❌ Ошибка извлечения")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка проверки архива: {e}")
        return False

def create_hf_space_structure():
    """Создает структуру файлов для HF Space"""
    print("\n📁 Создание структуры для HF Space...")
    
    hf_dir = "hf_space_files"
    os.makedirs(hf_dir, exist_ok=True)
    
    files_to_copy = [
        "app_hf_simple.py",
        "requirements_hf.txt",
        "README_HF.md",
        "real_estate_vector_db.py",
        "real_estate_embedding_function.py",
        "hybrid_search.py",
        "main.py",
        "rent_listings.json",
        "sale_listings.json",
    ]
    
    # Копируем файлы
    copied = []
    missing = []
    
    for file in files_to_copy:
        if os.path.exists(file):
            shutil.copy2(file, hf_dir)
            copied.append(file)
        else:
            missing.append(file)
    
    # Копируем архив если есть
    if os.path.exists(ARCHIVE_NAME):
        shutil.copy2(ARCHIVE_NAME, hf_dir)
        copied.append(ARCHIVE_NAME)
    
    print(f"✅ Скопировано файлов: {len(copied)}")
    for file in copied:
        print(f"   ✓ {file}")
    
    if missing:
        print(f"\n⚠️ Отсутствующие файлы: {len(missing)}")
        for file in missing:
            print(f"   ✗ {file}")
    
    # Создаем README для директории
    with open(os.path.join(hf_dir, "UPLOAD_INSTRUCTIONS.txt"), "w", encoding="utf-8") as f:
        f.write("""
ИНСТРУКЦИЯ ПО ЗАГРУЗКЕ НА HUGGING FACE SPACES
=============================================

1. Переименуйте файлы:
   - app_hf_simple.py → app.py
   - requirements_hf.txt → requirements.txt
   - README_HF.md → README.md

2. Загрузите все файлы в ваш Space:
   - Через Web UI (Files → Add file → Upload files)
   - Или через Git:
     git clone https://huggingface.co/spaces/YOUR_USERNAME/SPACE_NAME
     cp * SPACE_NAME/
     cd SPACE_NAME
     git add .
     git commit -m "Initial upload"
     git push

3. Настройте Secrets в Settings:
   - OPENAI_API_KEY = ваш OpenAI API ключ
   - (опционально) MONGODB_URI = ваш MongoDB connection string

4. Дождитесь сборки и запуска Space

5. Протестируйте поиск!

Подробности: HF_SPACES_DEPLOYMENT.md
""")
    
    print(f"\n📁 Все файлы готовы в директории: {hf_dir}/")
    print(f"📄 Читайте: {hf_dir}/UPLOAD_INSTRUCTIONS.txt")
    
    return True

def main():
    """Главная функция"""
    print("="*60)
    print("🚀 ПОДГОТОВКА CHROMADB ДЛЯ HUGGING FACE SPACES")
    print("="*60 + "\n")
    
    # 1. Создаем архив
    if create_archive():
        # 2. Проверяем архив
        extract_archive()
    
    # 3. Создаем структуру для HF Space
    create_hf_space_structure()
    
    print("\n" + "="*60)
    print("✅ ПОДГОТОВКА ЗАВЕРШЕНА")
    print("="*60)
    
    print("\n📝 СЛЕДУЮЩИЕ ШАГИ:")
    print("1. Перейдите в директорию hf_space_files/")
    print("2. Следуйте инструкциям в UPLOAD_INSTRUCTIONS.txt")
    print("3. Загрузите файлы на Hugging Face Spaces")
    print("4. Настройте Secrets (OPENAI_API_KEY)")
    print("5. Дождитесь сборки и тестируйте!")
    
    print("\n📖 Полное руководство: HF_SPACES_DEPLOYMENT.md")
    print()

if __name__ == "__main__":
    main()

