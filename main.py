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
    RbcCompaniesParser
)

# Настройка логирования
logger = logging.getLogger("TIN_Parser.Main")

# Глобальная переменная для хранения менеджера данных
data_manager = None
# Флаг для предотвращения повторного сохранения
is_saving = False
# Флаг для обозначения, что программа завершается
is_exiting = False

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
        # Инициализация менеджера данных (с сохранением каждых 50 записей)
        data_manager = DataManager(input_file, output_file, save_interval=50)
        
        # Инициализация менеджера парсеров
        parser_manager = ParserManager(data_manager)
        
        # Добавление парсеров для каждого сайта
        parser_manager.add_parser(FocusKonturParser(rate_limit=5.0))
        # parser_manager.add_parser(CheckoParser(rate_limit=2.0))
        # parser_manager.add_parser(ZaChestnyiBiznesParser(rate_limit=3.0))
        # parser_manager.add_parser(AuditItParser(rate_limit=2.0))
        # parser_manager.add_parser(RbcCompaniesParser(rate_limit=2.0))
        
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