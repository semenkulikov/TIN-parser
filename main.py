import asyncio
import logging
import sys
import signal
import time
import os
import atexit
from parser_base import DataManager, ParserManager, CompanyData
from site_parsers import (
    FocusKonturParser,
    CheckoParser,
    ZaChestnyiBiznesParser,
    AuditItParser,
    RbcCompaniesParser,
    DadataParser
)
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
here = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(here, '.env'), override=True)

# Настройка логирования
logger = logging.getLogger("TIN_Parser.Main")

# Глобальная переменная для хранения менеджера данных
data_manager = None
# Флаг для предотвращения повторного сохранения
is_saving = False
# Флаг для обозначения, что программа завершается
is_exiting = False
SAVE_INTERVAL = int(os.getenv('SAVE_INTERVAL', '50'))


# Функция для загрузки API ключей из переменных окружения
def load_api_keys(env_prefix):
    """
    Загружает все API ключи с указанным префиксом из переменных окружения
    
    :param env_prefix: Префикс для переменных окружения (например, 'DADATA_TOKEN')
    :return: Список найденных ключей
    """
    keys = []
    
    # Ищем основной ключ
    main_key = os.getenv(env_prefix)
    if main_key:
        keys.append(main_key)
    
    # Ищем дополнительные ключи с номерами (DADATA_TOKEN_1, DADATA_TOKEN_2 и т.д.)
    i = 1
    while True:
        key = os.getenv(f"{env_prefix}_{i}")
        if not key:
            break
        keys.append(key)
        i += 1
    
    logger.info(f"Загружено {len(keys)} ключей с префиксом {env_prefix}")
    return keys



# Функция для форсированного сохранения при любом завершении
@atexit.register
def save_on_exit():
    global data_manager, is_saving, is_exiting
    if data_manager and not is_saving and not is_exiting:
        try:
            is_saving = True
            logger.info("Функция завершения работы - сохраняем результаты")
            
            # Обновляем основной словарь результатов из runtime_results
            for inn, company in data_manager.runtime_results.items():
                data_manager.results[inn] = company
                data_manager.processed_inns.add(inn)
            
            # Сначала сохраняем в кэш для гарантии
            data_manager._save_cache()
            logger.info(f"Кэш успешно сохранен с {len(data_manager.runtime_results)} новыми записями")
            
            # Затем форсированное сохранение в CSV
            data_manager._save_to_csv()
            logger.info("Результаты успешно сохранены при завершении.")
        except Exception as e:
            logger.error(f"Ошибка при сохранении результатов при завершении: {e}")
        finally:
            is_saving = False

def signal_handler(sig, frame):
    """Обработчик сигнала прерывания (Ctrl+C)"""
    global data_manager, is_saving, is_exiting
    
    if is_exiting:
        logger.info("Повторный сигнал прерывания - выходим немедленно")
        os._exit(1)
    
    is_exiting = True
    logger.info("Получен сигнал прерывания. Сохраняем результаты перед выходом...")
    
    if data_manager and not is_saving:
        try:
            is_saving = True
            # Обновляем основной словарь результатов из runtime_results
            for inn, company in data_manager.runtime_results.items():
                data_manager.results[inn] = company
                data_manager.processed_inns.add(inn)
            
            # Сначала сохраняем в кэш для гарантии
            data_manager._save_cache()
            logger.info(f"Кэш успешно сохранен с {len(data_manager.runtime_results)} новыми записями")
            
            # Затем форсированное сохранение в CSV
            data_manager._save_to_csv()
            logger.info("Результаты успешно сохранены в CSV.")
        except Exception as e:
            logger.error(f"Ошибка при сохранении результатов: {e}")
        finally:
            is_saving = False
    
    logger.info("Процесс был прерван пользователем")
    os._exit(0)  # Немедленное завершение без вызова деструкторов (может помочь избежать ошибок при закрытии)

async def main():
    """Основная функция парсера"""
    global data_manager, is_saving, is_exiting
    
    # Настройка обработчика сигналов
    signal.signal(signal.SIGINT, signal_handler)
    
    # Проверка аргументов командной строки
    input_file = "test.xlsx"
    output_file = "results.csv"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    logger.info(f"Запуск парсера с входным файлом: {input_file}, выходным файлом: {output_file}")
    
    try:
        # Инициализация менеджера данных с интервалом сохранения из конфигурации
        data_manager = DataManager(input_file, output_file, save_interval=SAVE_INTERVAL)
        
        # Инициализация менеджера парсеров
        parser_manager = ParserManager(data_manager)
        
        # Загружаем ключи для Dadata
        dadata_keys = load_api_keys('DADATA_TOKEN')
        
        # Загружаем ключи для Checko
        checko_keys = load_api_keys('CHECKO_TOKEN')
        
        # Настраиваем максимальное количество параллельных процессов в зависимости от числа ключей
        # max_workers = min(len(dadata_keys) + len(checko_keys), 5) if (dadata_keys or checko_keys) else 1
        
        # Ограничиваем количество параллельных процессов до 1 для каждого типа ключа
        # Так как лимит Checko - 100 запросов в сутки на ключ, не имеет смысла
        # запускать несколько параллельных процессов с одним ключом
        max_workers = 1
        parser_manager.max_workers = max_workers
        logger.info(f"Установлено максимальное количество параллельных процессов: {max_workers} для сохранения лимита API-запросов")
        
        # Добавляем парсеры с параметрами из .env
        if dadata_keys:
            # Используем только первый ключ из списка для передачи в конструктор
            # Остальные ключи будут загружены автоматически из переменных окружения
            dadata_rate_limit = float(os.getenv('DADATA_RATE_LIMIT', '0.2'))
            # parser_manager.add_parser(DadataParser(token=dadata_keys[0], rate_limit=dadata_rate_limit))
        else:
            logger.warning("Переменные окружения для DADATA_TOKEN не заданы, парсер Dadata не будет использоваться")
        
        # Добавляем стандартные парсеры (закомментированы для текущего использования)
        # Параметры взяты из .env файла
        # focus_rate_limit = float(os.getenv('FOCUS_KONTUR_RATE_LIMIT', '5.0'))
        checko_rate_limit = float(os.getenv('CHECKO_RATE_LIMIT', '2.0'))
        # zachestny_rate_limit = float(os.getenv('ZACHESTNY_RATE_LIMIT', '3.0'))
        # audit_it_rate_limit = float(os.getenv('AUDIT_IT_RATE_LIMIT', '2.0'))
        # rbc_rate_limit = float(os.getenv('RBC_RATE_LIMIT', '2.0'))
        
        # parser_manager.add_parser(FocusKonturParser(rate_limit=focus_rate_limit))
        
        # Добавляем парсер CheckoParser с API ключом, если он есть
        if checko_keys:
            parser_manager.add_parser(CheckoParser(token=checko_keys[0], rate_limit=checko_rate_limit))
            logger.info(f"Добавлен парсер CheckoParser с API ключом")
        else:
            # Если ключи отсутствуют, сообщаем об ошибке - парсер требует API ключ
            logger.error("Переменные окружения для CHECKO_TOKEN не заданы. Парсер Checko требует API ключ для работы!")
            logger.error("Добавьте ключ API в файл .env: CHECKO_TOKEN=ваш_ключ_api")
            
        # parser_manager.add_parser(ZaChestnyiBiznesParser(rate_limit=zachestny_rate_limit))
        # parser_manager.add_parser(AuditItParser(rate_limit=audit_it_rate_limit))
        # parser_manager.add_parser(RbcCompaniesParser(rate_limit=rbc_rate_limit))
        
        # Запуск процесса парсинга
        start_time = time.time()
        await parser_manager.run()
        end_time = time.time()
        
        # Выводим статистику
        total_time = end_time - start_time
        processed_count = len(data_manager.processed_inns)
        logger.info(f"Работа парсера завершена. Обработано {processed_count} компаний за {total_time:.2f} секунд")
        if processed_count > 0:
            logger.info(f"Среднее время на компанию: {total_time/processed_count:.2f} секунд")
        
        # Закрываем все процессы хрома через os.system
        logger.info("Закрываем все процессы хрома...")
        try:
            if sys.platform == 'win32':
                os.system("taskkill /f /im chrome.exe")
            else:
                os.system("pkill -f chrome")
            logger.info("Все процессы хрома успешно закрыты")
        except Exception as e:
            logger.error(f"Ошибка при закрытии процессов хрома: {e}")
    except KeyboardInterrupt:
        # Перехватываем Ctrl+C для корректного завершения
        if data_manager and not is_saving:
            try:
                is_saving = True
                
                # Обновляем основной словарь результатов из runtime_results
                for inn, company in data_manager.runtime_results.items():
                    data_manager.results[inn] = company
                    data_manager.processed_inns.add(inn)
                
                # Сначала сохраняем в кэш для гарантии
                data_manager._save_cache()
                logger.info(f"Кэш успешно сохранен с {len(data_manager.runtime_results)} новыми записями")
                
                # Затем форсированное сохранение в CSV
                data_manager._save_to_csv()
                logger.info("Результаты сохранены при получении сигнала прерывания")
            except Exception as e:
                logger.error(f"Ошибка при сохранении результатов: {e}")
            finally:
                is_saving = False
        logger.info("Процесс был прерван пользователем")
        return 130
    except Exception as e:
        logger.error(f"Произошла ошибка при выполнении парсера: {e}")
        # Пытаемся сохранить данные даже при ошибке
        if data_manager and not is_saving:
            try:
                is_saving = True
                
                # Обновляем основной словарь результатов из runtime_results
                for inn, company in data_manager.runtime_results.items():
                    data_manager.results[inn] = company
                    data_manager.processed_inns.add(inn)
                
                # Сначала сохраняем в кэш для гарантии
                data_manager._save_cache()
                logger.info(f"Кэш успешно сохранен с {len(data_manager.runtime_results)} новыми записями")
                
                # Затем форсированное сохранение в CSV
                data_manager._save_to_csv()
                logger.info("Результаты сохранены несмотря на ошибку")
            except Exception as save_error:
                logger.error(f"Ошибка при сохранении результатов: {save_error}")
            finally:
                is_saving = False
        return 1
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        # Устанавливаем флаг, что программа завершается
        is_exiting = True
        sys.exit(exit_code)
    except KeyboardInterrupt:
        # Устанавливаем флаг, что программа завершается
        is_exiting = True
        logger.info("Процесс был прерван пользователем")
        sys.exit(0)  # Используем код 0 вместо 130
    except Exception as e:
        # Устанавливаем флаг, что программа завершается
        is_exiting = True
        logger.error(f"Необработанная ошибка: {e}")
        sys.exit(1) 