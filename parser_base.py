import logging
import asyncio
import pandas as pd
import os
import time
from typing import Dict, List, Optional, Tuple, Any, Set
import json
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import concurrent.futures

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parser.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TIN_Parser")

class CompanyData:
    """Класс для хранения данных о компании"""
    
    def __init__(self, name: str, inn: str, ogrn: Optional[str] = None, address: Optional[str] = None, ceo_name: Optional[str] = None, ceo_inn: Optional[str] = None):
        self.name = name
        self.inn = inn
        self.chairman_name: Optional[str] = None
        self.chairman_inn: Optional[str] = None
        self.source: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертирует данные компании в словарь"""
        return {
            "Юридическое название": self.name,
            "ИНН": self.inn,
            "ФИО Председателя": self.chairman_name,
            "ИНН Председателя": self.chairman_inn,
            "Источник": self.source
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CompanyData':
        """Создает объект CompanyData из словаря"""
        company = cls(data["Юридическое название"], data["ИНН"])
        company.chairman_name = data.get("ФИО Председателя")
        company.chairman_inn = data.get("ИНН Председателя")
        company.source = data.get("Источник")
        return company

class DataManager:
    """Менеджер данных для работы с Excel и сохранением результатов"""
    
    def __init__(self, input_file: str, output_file: str = "results.csv", save_interval: int = 2):
        self.input_file = input_file
        self.output_file = output_file
        self.cache_file = "parsed_data_cache.json"
        self.processed_inns: Set[str] = set()
        self.results: Dict[str, CompanyData] = {}
        self.save_interval = save_interval  # Сохранять каждые N обработанных компаний
        self.last_save_time = time.time()
        self.save_results_counter = 0
        self.runtime_results: Dict[str, CompanyData] = {}  # Результаты текущего запуска
        self._load_cache()
    
    def read_input_data(self) -> pd.DataFrame:
        """Чтение исходных данных из Excel-файла"""
        logger.info(f"Чтение данных из {self.input_file}")
        try:
            df = pd.read_excel(self.input_file, engine='openpyxl')
            logger.info(f"Успешно загружено {len(df)} записей")
            return df
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {self.input_file}: {e}")
            raise
    
    def get_companies_to_process(self) -> List[CompanyData]:
        """Получение списка компаний для обработки (исключая уже обработанные)"""
        df = self.read_input_data()
        companies = []
        
        for _, row in df.iterrows():
            inn = str(row['ИНН'])
            # Проверяем, был ли уже обработан данный ИНН
            if inn in self.processed_inns:
                logger.debug(f"Пропуск {inn}, уже обработан")
                continue
            
            # Если у нас уже есть данные о председателе, тоже пропускаем
            if not pd.isna(row.get('ФИО Председателя', pd.NA)) and not pd.isna(row.get('ИНН Председателя', pd.NA)):
                logger.debug(f"Пропуск {inn}, данные уже заполнены")
                self.processed_inns.add(inn)
                continue
                
            company = CompanyData(row['Юридическое название'], inn)
            companies.append(company)
        
        logger.info(f"Подготовлено {len(companies)} компаний для обработки")
        return companies
    
    def save_results(self, force: bool = False) -> None:
        """
        Сохранение результатов в кэш и CSV.
        
        Args:
            force: Принудительное сохранение независимо от счетчика
        """
        # Если нет новых результатов и не форсированное сохранение, пропускаем
        if not force and self.save_results_counter == 0 and not self.runtime_results:
            logger.debug("Нет новых результатов для сохранения, пропускаем")
            return
        
        # Проверяем условия для сохранения
        current_time = time.time()
        should_save = (
            force or 
            self.save_results_counter >= self.save_interval or 
            (current_time - self.last_save_time) > 300  # 5 минут
        )
        
        if should_save:
            logger.info(f"Сохранение результатов (обработано: {self.save_results_counter} после предыдущего сохранения)")
            
            try:
                # 1. Сначала обновляем основной словарь результатов и множество обработанных ИНН
                for inn, company in self.runtime_results.items():
                    self.results[inn] = company
                    self.processed_inns.add(inn)
                
                # 2. Сохраняем в кэш
                self._save_cache()
                logger.info(f"Кэш обновлён с {len(self.runtime_results)} новыми записями")
                
                # 3. Если нужно сохранить в CSV (делаем это только при force=True или периодически)
                if force:
                    # Сохраняем в CSV
                    self._save_to_csv()
                
                # 4. Сбрасываем счетчик и обновляем время последнего сохранения
                self.save_results_counter = 0
                self.last_save_time = time.time()
                
                # НЕ очищаем runtime_results - это делается только при успешном завершении всего процесса
                
            except Exception as e:
                logger.error(f"Ошибка при сохранении результатов: {e}")
    
    def _save_to_csv(self) -> None:
        """Сохранение результатов в CSV файл"""
        try:
            # Проверяем, что есть результаты для сохранения
            if not self.results:
                logger.warning("Нет результатов для сохранения в CSV")
                return
            
            logger.info(f"Сохранение результатов в CSV файл {self.output_file}")
            
            # Преобразование результатов в DataFrame
            results_df = pd.DataFrame([company.to_dict() for company in self.results.values()])
            
            # Создаем пустой файл с заголовками, если его нет
            if not os.path.exists(self.output_file) or os.path.getsize(self.output_file) == 0:
                logger.info(f"Создаем новый файл {self.output_file}")
                results_df.to_csv(self.output_file, index=False, encoding='utf-8')
            else:
                try:
                    # Если файл существует, читаем его
                    existing_df = pd.read_csv(self.output_file, encoding='utf-8')
                    
                    # Объединяем с текущими результатами, избегая дублирования
                    merged_data = {}
                    
                    # Сначала добавляем все записи из текущих результатов
                    for _, row in results_df.iterrows():
                        inn = str(row['ИНН'])
                        merged_data[inn] = row.to_dict()
                    
                    # Затем добавляем записи из существующего файла, если их нет в текущих результатах
                    for _, row in existing_df.iterrows():
                        inn = str(row['ИНН'])
                        if inn not in merged_data:
                            merged_data[inn] = row.to_dict()
                    
                    # Создаем новый DataFrame из объединенных данных
                    final_df = pd.DataFrame(list(merged_data.values()))
                    
                    # Сохраняем результаты
                    final_df.to_csv(self.output_file, index=False, encoding='utf-8')
                    logger.info(f"Результаты успешно сохранены в CSV, всего записей: {len(final_df)}")
                except Exception as e:
                    logger.warning(f"Не удалось прочитать существующий файл, создаем новый: {e}")
                    results_df.to_csv(self.output_file, index=False, encoding='utf-8')
                    logger.info(f"Результаты успешно сохранены в CSV, всего записей: {len(results_df)}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении результатов в CSV: {e}")
    
    def update_results(self, company: CompanyData) -> None:
        """
        Обновление результатов парсинга компании.
        
        Args:
            company: Данные о компании
        """
        # Сохраняем результат текущего запуска
        self.runtime_results[company.inn] = company
        
        # Увеличиваем счетчик обработанных записей
        self.save_results_counter += 1
        
        # Если достигнут интервал сохранения - сохраняем в кэш
        if self.save_results_counter >= self.save_interval:
            logger.info(f"Достигнут интервал сохранения ({self.save_interval}), сохраняем промежуточные результаты")
            self.save_results(force=False)
    
    def _load_cache(self) -> None:
        """Загрузка кэша обработанных ИНН"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    self.processed_inns = set(cache_data.get('processed_inns', []))
                    
                    # Загрузка результатов, если они есть
                    if 'results' in cache_data:
                        for data in cache_data['results']:
                            try:
                                company = CompanyData.from_dict(data)
                                self.results[company.inn] = company
                            except Exception as e:
                                logger.warning(f"Не удалось загрузить данные компании из кэша: {e}")
                            
                logger.info(f"Загружен кэш с {len(self.processed_inns)} обработанными ИНН")
            except Exception as e:
                logger.error(f"Ошибка при загрузке кэша: {e}")
                self.processed_inns = set()
                self.results = {}
                self.runtime_results = {}
    
    def _save_cache(self) -> None:
        """Сохранение кэша обработанных ИНН"""
        try:
            # Создаем резервную копию кэша перед перезаписью
            if os.path.exists(self.cache_file):
                backup_file = f"{self.cache_file}.bak"
                try:
                    with open(self.cache_file, 'r', encoding='utf-8') as src, open(backup_file, 'w', encoding='utf-8') as dst:
                        dst.write(src.read())
                except Exception as e:
                    logger.warning(f"Не удалось создать резервную копию кэша: {e}")
            
            # Обновляем результаты из текущего запуска
            for inn, company in self.runtime_results.items():
                self.results[inn] = company
                self.processed_inns.add(inn)
            
            cache_data = {
                'processed_inns': list(self.processed_inns),
                'results': [company.to_dict() for company in self.results.values()]
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=4)
            logger.info(f"Кэш успешно сохранен, {len(self.processed_inns)} обработанных ИНН")
        except Exception as e:
            logger.error(f"Ошибка при сохранении кэша: {e}")

class BaseSiteParser:
    """Базовый класс для парсеров различных сайтов"""
    
    def __init__(self, site_name: str, rate_limit: float = 1.0):
        self.site_name = site_name
        self.rate_limit = rate_limit  # Задержка между запросами в секундах
        self.logger = logging.getLogger(f"TIN_Parser.{site_name}")
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """
        Парсит информацию о компании
        Должен быть переопределен в подклассах
        """
        raise NotImplementedError("Subclasses must implement parse_company()")
    
    async def parse_companies(self, companies: List[CompanyData]) -> List[CompanyData]:
        """Парсит список компаний с соблюдением ограничений на запросы"""
        results = []
        self.logger.info(f"Начинаем обработку {len(companies)} компаний")
        
        for i, company in enumerate(companies):
            try:
                self.logger.info(f"[{i+1}/{len(companies)}] Обработка компании: {company.name} (ИНН: {company.inn})")
                
                # Соблюдаем задержку между запросами
                await asyncio.sleep(self.rate_limit)
                
                # Парсим информацию о компании
                result = await self.parse_company(company)
                if result:
                    result.source = self.site_name
                    results.append(result)
                    self.logger.info(f"Успешно получены данные для {company.name}")
                else:
                    self.logger.warning(f"Не удалось получить данные для {company.name}")
            except Exception as e:
                self.logger.error(f"Ошибка при обработке компании {company.name}: {e}")
        
        self.logger.info(f"Завершена обработка компаний, успешно: {len(results)} из {len(companies)}")
        return results

class ParserManager:
    """Менеджер парсеров для координации работы различных парсеров сайтов"""
    
    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager
        self.parsers: List[BaseSiteParser] = []
        self.logger = logging.getLogger("TIN_Parser.Manager")
        self.batch_size = 50  # Размер пакета компаний для обработки
    
    def add_parser(self, parser: BaseSiteParser) -> None:
        """Добавление парсера"""
        self.parsers.append(parser)
        self.logger.info(f"Добавлен парсер для сайта {parser.site_name}")
    
    def distribute_companies(self, companies: List[CompanyData]) -> Dict[BaseSiteParser, List[CompanyData]]:
        """Распределение компаний между парсерами"""
        if not self.parsers:
            self.logger.error("Нет доступных парсеров")
            return {}
        
        # Равномерно распределяем компании между парсерами
        parser_companies: Dict[BaseSiteParser, List[CompanyData]] = {parser: [] for parser in self.parsers}
        
        for i, company in enumerate(companies):
            parser_index = i % len(self.parsers)
            parser_companies[self.parsers[parser_index]].append(company)
        
        for parser, assigned_companies in parser_companies.items():
            self.logger.info(f"Парсеру {parser.site_name} назначено {len(assigned_companies)} компаний")
        
        return parser_companies
    
    async def process_batch(self, parser: BaseSiteParser, companies: List[CompanyData]) -> None:
        """Обработка пакета компаний одним парсером"""
        try:
            results = await parser.parse_companies(companies)
            self.logger.info(f"Парсер {parser.site_name} завершил обработку пакета, обработано: {len(results)} компаний")
            
            # Обновляем результаты
            for company in results:
                self.data_manager.update_results(company)
                
            # Принудительно сохраняем результаты после каждого пакета
            self.data_manager.save_results(force=True)
        except Exception as e:
            self.logger.error(f"Ошибка при обработке пакета парсером {parser.site_name}: {e}")
    
    def process_batch_sync(self, parser: BaseSiteParser, companies: List[CompanyData]) -> None:
        """ Синхронная обертка, инициализирует запуск парсера. """
        try:
            results = asyncio.run(parser.parse_companies(companies))
            self.logger.info(f"Парсер {parser.site_name} завершил обработку пакета")
            
            # Обновляем результаты
            for company in results:
                self.data_manager.update_results(company)
                
            # Принудительно сохраняем результаты после каждого пакета
            self.data_manager.save_results(force=True)
        except Exception as e:
            self.logger.error(f"Ошибка при обработке пакета парсером {parser.site_name}: {e}")
    
    async def run(self) -> None:
        """Запуск процесса парсинга"""
        self.logger.info("Начало процесса парсинга")
        
        # Получаем компании для обработки
        companies = self.data_manager.get_companies_to_process()
        if not companies:
            self.logger.info("Нет компаний для обработки")
            return
        
        # Распределяем компании между парсерами
        parser_companies = self.distribute_companies(companies)
        
        # # Обрабатываем компании пакетами
        # tasks = []
        # for parser, assigned_companies in parser_companies.items():
        #     if not assigned_companies:
        #         continue
                
        #     # Разбиваем компании на пакеты для каждого парсера
        #     for i in range(0, len(assigned_companies), self.batch_size):
        #         batch = assigned_companies[i:i+self.batch_size]
        #         task = asyncio.create_task(self.process_batch(parser, batch))
        #         tasks.append(task)
        
        # # Ожидаем завершения всех задач
        # if tasks:
        #     await asyncio.gather(*tasks)

        tasks = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
            loop = asyncio.get_event_loop()
            for parser_class, assigned_companies in parser_companies.items():
                if not assigned_companies:
                    continue

                batches = [assigned_companies[i:i + self.batch_size] for i in range(0, len(assigned_companies), self.batch_size)]
                for batch in batches:
                    task = loop.run_in_executor(executor, self.process_batch_sync, parser_class, batch)
                    tasks.append(task)

            if tasks:
                await asyncio.gather(*tasks)
        
        # Финальное сохранение результатов
        self.data_manager.save_results(force=True)
        self.logger.info("Процесс парсинга завершен") 