import asyncio
import logging
import sys
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

async def main():
    """Основная функция парсера"""
    # Проверка аргументов командной строки
    input_file = "test.xlsx"
    output_file = "results.csv"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    logger.info(f"Запуск парсера с входным файлом: {input_file}, выходным файлом: {output_file}")
    
    try:
        # Инициализация менеджера данных
        data_manager = DataManager(input_file, output_file)
        
        # Инициализация менеджера парсеров
        parser_manager = ParserManager(data_manager)
        
        # Добавление парсеров для каждого сайта
        parser_manager.add_parser(FocusKonturParser(rate_limit=2.0))
        # parser_manager.add_parser(CheckoParser(rate_limit=2.0))
        # parser_manager.add_parser(ZaChestnyiBiznesParser(rate_limit=3.0))
        # parser_manager.add_parser(AuditItParser(rate_limit=2.0))
        # parser_manager.add_parser(RbcCompaniesParser(rate_limit=2.0))
        
        # Запуск процесса парсинга
        await parser_manager.run()
        
        logger.info("Работа парсера завершена")
    except Exception as e:
        logger.error(f"Произошла ошибка при выполнении парсера: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Процесс был прерван пользователем")
        sys.exit(130) 