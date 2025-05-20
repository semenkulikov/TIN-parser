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
        # Гарантируем, что ИНН хранится как строка, сохраняя ведущие нули
        self.inn = str(inn).strip()
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
        # Сохраняем ИНН как строку
        inn = str(data["ИНН"]).strip()
        company = cls(data["Юридическое название"], inn)
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
        self._load_csv_results()  # Загружаем результаты из CSV
    
    def read_input_data(self) -> pd.DataFrame:
        """Чтение исходных данных из Excel-файла"""
        logger.info(f"Чтение данных из {self.input_file}")
        try:
            # Явно указываем, что ИНН должен быть строкой
            df = pd.read_excel(self.input_file, engine='openpyxl', dtype={'ИНН': str})
            
            # Дополнительная обработка для гарантии, что ИНН является строкой и с сохранением ведущих нулей
            if 'ИНН' in df.columns:
                df['ИНН'] = df['ИНН'].astype(str).str.strip()
                
            logger.info(f"Успешно загружено {len(df)} записей")
            return df
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {self.input_file}: {e}")
            raise
    
    def _load_csv_results(self) -> None:
        """Загрузка результатов из CSV файла"""
        if os.path.exists(self.output_file) and os.path.getsize(self.output_file) > 0:
            try:
                df = pd.read_csv(self.output_file, encoding='utf-8')
                loaded_count = 0
                
                for _, row in df.iterrows():
                    inn = str(row['ИНН'])
                    
                    # Пропускаем, если ИНН уже в результатах или если нет данных о председателе
                    if inn in self.results:
                        continue
                    
                    chairman_name = row.get('ФИО Председателя')
                    chairman_inn = row.get('ИНН Председателя')
                    
                    # Только если есть данные о председателе, добавляем в список обработанных
                    if not pd.isna(chairman_name) and not pd.isna(chairman_inn):
                        company = CompanyData(row['Юридическое название'], inn)
                        company.chairman_name = chairman_name
                        company.chairman_inn = chairman_inn
                        company.source = row.get('Источник')
                        
                        self.results[inn] = company
                        self.processed_inns.add(inn)
                        loaded_count += 1
                
                logger.info(f"Загружено {loaded_count} записей из CSV файла {self.output_file}")
            except Exception as e:
                logger.error(f"Ошибка при загрузке данных из CSV: {e}")
    
    def get_companies_to_process(self) -> List[CompanyData]:
        """Получение списка компаний для обработки (исключая уже обработанные)"""
        df = self.read_input_data()
        companies = []
        
        # Подсчет статистики для логирования
        total_records = len(df)
        already_processed = 0
        has_data = 0
        
        for _, row in df.iterrows():
            # Гарантируем, что ИНН - это строка с сохранением ведущих нулей
            inn = str(row['ИНН']).strip()
            
            # Проверяем, был ли уже обработан данный ИНН
            if inn in self.processed_inns:
                logger.debug(f"Пропуск {inn}, уже обработан")
                already_processed += 1
                continue
            
            # Если у нас уже есть данные о председателе, тоже пропускаем
            if not pd.isna(row.get('ФИО Председателя', pd.NA)) and not pd.isna(row.get('ИНН Председателя', pd.NA)):
                logger.debug(f"Пропуск {inn}, данные уже заполнены")
                self.processed_inns.add(inn)
                has_data += 1
                continue
                
            company = CompanyData(row['Юридическое название'], inn)
            companies.append(company)
        
        logger.info(f"Всего записей: {total_records}, уже обработано: {already_processed}, имеют данные: {has_data}")
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
                    # Гарантируем, что ИНН хранится как строка с ведущими нулями
                    inn_str = str(inn).strip()
                    self.results[inn_str] = company
                    self.processed_inns.add(inn_str)
                
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
                # Явно указываем, что ИНН это строковый тип, чтобы сохранить ведущие нули
                if 'ИНН' in results_df.columns:
                    results_df['ИНН'] = results_df['ИНН'].astype(str)
                results_df.to_csv(self.output_file, index=False, encoding='utf-8')
            else:
                try:
                    # Если файл существует, читаем его с явным указанием типа для ИНН
                    existing_df = pd.read_csv(self.output_file, encoding='utf-8', dtype={'ИНН': str})
                    
                    # Объединяем с текущими результатами, избегая дублирования
                    merged_data = {}
                    
                    # Сначала добавляем все записи из текущих результатов
                    for _, row in results_df.iterrows():
                        inn = str(row['ИНН']).strip()
                        merged_data[inn] = row.to_dict()
                    
                    # Затем добавляем записи из существующего файла, если их нет в текущих результатах
                    for _, row in existing_df.iterrows():
                        inn = str(row['ИНН']).strip()
                        if inn not in merged_data:
                            merged_data[inn] = row.to_dict()
                    
                    # Создаем новый DataFrame из объединенных данных
                    final_df = pd.DataFrame(list(merged_data.values()))
                    
                    # Гарантируем, что ИНН остается строкой для сохранения ведущих нулей
                    if 'ИНН' in final_df.columns:
                        final_df['ИНН'] = final_df['ИНН'].astype(str)
                    
                    # Сохраняем результаты
                    final_df.to_csv(self.output_file, index=False, encoding='utf-8')
                    logger.info(f"Результаты успешно сохранены в CSV, всего записей: {len(final_df)}")
                except Exception as e:
                    logger.warning(f"Не удалось прочитать существующий файл, создаем новый: {e}")
                    # Гарантируем, что ИНН остается строкой для сохранения ведущих нулей
                    if 'ИНН' in results_df.columns:
                        results_df['ИНН'] = results_df['ИНН'].astype(str)
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
        # Гарантируем, что ИНН хранится как строка с ведущими нулями
        inn_str = str(company.inn).strip()
        
        # Обновляем компанию в объекте, если требуется
        company.inn = inn_str
        
        # Сохраняем результат текущего запуска
        self.runtime_results[inn_str] = company
        
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
                    # Преобразуем все ИНН в списке в строки, сохраняя ведущие нули
                    self.processed_inns = set(str(inn).strip() for inn in cache_data.get('processed_inns', []))
                    
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
                self.results[str(inn).strip()] = company
                self.processed_inns.add(str(inn).strip())
            
            # Конвертируем все ИНН в строки для сохранения ведущих нулей
            inns_list = [str(inn).strip() for inn in self.processed_inns]
            
            cache_data = {
                'processed_inns': inns_list,
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
        self.max_workers = 3  # Максимальное количество параллельных процессов
    
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
        self.logger.info(f"Запуск обработки с {self.max_workers} параллельными процессами")
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            loop = asyncio.get_event_loop()
            for parser, assigned_companies in parser_companies.items():
                if not assigned_companies:
                    continue

                # Получаем все доступные API ключи для Dadata, если парсер - DadataParser
                dadata_keys = []
                if parser.__class__.__name__ == "DadataParser" and hasattr(parser, "dadata_keys"):
                    for i in range(parser.dadata_keys.get_all_keys_count()):
                        if i == 0:
                            dadata_keys.append(parser.primary_token)
                        else:
                            key = os.getenv(f"DADATA_TOKEN_{i}")
                            if key:
                                dadata_keys.append(key)
                    
                    self.logger.info(f"Найдено {len(dadata_keys)} API ключей для распределения между пакетами")

                # Разбиваем компании на пакеты
                batches = [assigned_companies[i:i + self.batch_size] for i in range(0, len(assigned_companies), self.batch_size)]
                
                # Если у нас DadataParser, создаём отдельный экземпляр для каждого пакета с отдельным ключом
                if parser.__class__.__name__ == "DadataParser" and dadata_keys:
                    for batch_idx, batch in enumerate(batches):
                        # Выбираем ключ для этого пакета (циклически)
                        key_idx = batch_idx % len(dadata_keys)
                        api_key = dadata_keys[key_idx]
                        
                        # Создаём новый экземпляр парсера для этого пакета с выбранным ключом
                        from site_parsers import DadataParser
                        batch_parser = DadataParser(token=api_key, rate_limit=parser.rate_limit)
                        # Принудительно устанавливаем ключ для этого экземпляра
                        batch_parser.set_specific_token(api_key)
                        
                        self.logger.info(f"Пакет {batch_idx+1} будет использовать ключ {key_idx+1}/{len(dadata_keys)}")
                        
                        # Добавляем задачу с новым экземпляром парсера
                        task = loop.run_in_executor(executor, self.process_batch_sync, batch_parser, batch)
                        tasks.append(task)
                else:
                    # Для других типов парсеров обрабатываем как обычно
                    for batch in batches:
                        task = loop.run_in_executor(executor, self.process_batch_sync, parser, batch)
                        tasks.append(task)

            if tasks:
                await asyncio.gather(*tasks)
        
        # Финальное сохранение результатов
        self.data_manager.save_results(force=True)
        self.logger.info("Процесс парсинга завершен") 