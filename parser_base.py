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
        self.inn = str(inn).strip() if inn is not None else None
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
        inn = str(data["ИНН"]).strip() if data.get("ИНН") is not None else None
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
                df['ИНН'] = df['ИНН'].astype(str).str.strip().str.zfill(9)  # Заполняем ведущими нулями до 9 знаков как минимум
                
            logger.info(f"Успешно загружено {len(df)} записей")
            return df
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {self.input_file}: {e}")
            raise
    
    def _load_csv_results(self) -> None:
        """Загрузка результатов из CSV файла"""
        if os.path.exists(self.output_file) and os.path.getsize(self.output_file) > 0:
            try:
                df = pd.read_csv(self.output_file, encoding='utf-8', dtype={'ИНН': str})
                loaded_count = 0
                
                for _, row in df.iterrows():
                    inn = str(row['ИНН']).strip() if not pd.isna(row['ИНН']) else None
                    
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
            inn = str(row['ИНН']).strip() if not pd.isna(row['ИНН']) else None
            
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
                        inn = str(row['ИНН']).strip() if not pd.isna(row['ИНН']) else None
                        if inn:
                            merged_data[inn] = row.to_dict()
                    
                    # Затем добавляем записи из существующего файла, если их нет в текущих результатах
                    for _, row in existing_df.iterrows():
                        inn = str(row['ИНН']).strip() if not pd.isna(row['ИНН']) else None
                        if inn and inn not in merged_data:
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
        inn_str = str(company.inn).strip() if company.inn is not None else None
        
        # Обновляем компанию в объекте, если требуется
        company.inn = inn_str
        
        # Сохраняем результат текущего запуска
        if inn_str:
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
                    self.processed_inns = set(str(inn).strip() for inn in cache_data.get('processed_inns', []) if inn is not None)
                    
                    # Загрузка результатов, если они есть
                    if 'results' in cache_data:
                        for data in cache_data['results']:
                            try:
                                company = CompanyData.from_dict(data)
                                if company.inn:
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
                if inn:
                    self.results[str(inn).strip()] = company
                    self.processed_inns.add(str(inn).strip())
            
            # Конвертируем все ИНН в строки для сохранения ведущих нулей
            inns_list = [str(inn).strip() for inn in self.processed_inns if inn is not None]
            
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
        """
        Обрабатывает пакет компаний через указанный парсер асинхронно
        
        :param parser: Парсер сайта
        :param companies: Список компаний для обработки
        """
        try:
            # Обрабатываем список компаний через парсер
            self.logger.info(f"Запуск обработки {len(companies)} компаний через {parser.__class__.__name__}")
            from site_parsers import ApiLimitExceeded
            
            try:
                results = await parser.parse_companies(companies)
                self.logger.info(f"Обработано {len(results)} компаний через {parser.__class__.__name__}")
                
                # Сохраняем полученные результаты
                for result in results:
                    self.data_manager.update_results(result)
            except ApiLimitExceeded as e:
                self.logger.warning(f"Обработка остановлена: {e.message}")
                # Принудительно сохраняем результаты
                self.data_manager.save_results(force=True)
                # Устанавливаем флаг для остановки всего процесса
                self.should_stop = True
                
        except Exception as e:
            self.logger.error(f"Ошибка при обработке пакета через {parser.__class__.__name__}: {e}")

    def process_batch_sync(self, parser: BaseSiteParser, companies: List[CompanyData]) -> None:
        """
        Обрабатывает пакет компаний через указанный парсер синхронно
        
        :param parser: Парсер сайта
        :param companies: Список компаний для обработки
        """
        try:
            # Запускаем задачу асинхронной обработки
            import asyncio
            from site_parsers import ApiLimitExceeded
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                loop.run_until_complete(self.process_batch(parser, companies))
            except ApiLimitExceeded as e:
                self.logger.warning(f"Обработка остановлена: {e.message}")
                # Принудительно сохраняем результаты
                self.data_manager.save_results(force=True)
                # Устанавливаем флаг для остановки всего процесса
                self.should_stop = True
            finally:
                loop.close()
        except Exception as e:
            self.logger.error(f"Ошибка при синхронной обработке пакета через {parser.__class__.__name__}: {e}")

    async def run(self) -> None:
        """
        Запускает процесс парсинга данных
        """
        self.logger.info(f"Начало процесса парсинга")
        
        # Получаем список компаний для обработки
        companies = self.data_manager.get_companies_to_process()
        
        if not companies:
            self.logger.warning(f"Нет компаний для обработки")
            return
            
        # Распределяем компании по парсерам
        distribution = self.distribute_companies(companies)
        
        # Создаем список задач для обработки компаний
        tasks = []
        
        # Добавляем флаг для остановки процесса при превышении лимита API
        self.should_stop = False
        
        # Запускаем обработку через все парсеры
        for parser, companies_batch in distribution.items():
            if not companies_batch:
                continue
                
            self.logger.info(f"Создаем задачу для парсера {parser.__class__.__name__} с {len(companies_batch)} компаниями")
            
            # Если у нас есть несколько парсеров с API ключами Checko, группируем задачи по ключам
            if parser.__class__.__name__ == "CheckoParser" and hasattr(parser, "checko_keys"):
                # Получаем все доступные API ключи для Checko, если парсер - CheckoParser
                checko_keys = []
                if parser.__class__.__name__ == "CheckoParser" and hasattr(parser, "checko_keys"):
                    # Используем only_env=True для загрузки ключей только из переменных окружения
                    checko_keys = [parser.checko_keys.get_current_key()]
                    
                    # Загружаем дополнительные ключи из переменных окружения
                    i = 1
                    while True:
                        key = os.getenv(f"CHECKO_TOKEN_{i}")
                        if not key:
                            break
                        checko_keys.append(key)
                        i += 1
                    
                    if checko_keys:
                        self.logger.info(f"Найдено {len(checko_keys)} API ключей Checko")
                    else:
                        self.logger.warning(f"Не найдено API ключей Checko")
                        
                # Распределяем компании по API ключам
                if checko_keys:
                    # Если API ключ один, используем его для всех компаний
                    if len(checko_keys) == 1:
                        self.logger.info(f"Используем единственный ключ Checko для всех {len(companies_batch)} компаний (лимит 100 запросов в сутки)")
                        tasks.append(self.process_batch(parser, companies_batch))
                    else:
                        # Если API ключей несколько, распределяем компании по ключам
                        self.logger.info(f"Распределяем компании по {len(checko_keys)} API ключам Checko")
                        
                        # Распределяем компании равномерно по ключам (до 100 компаний на ключ)
                        chunk_size = min(100, len(companies_batch) // len(checko_keys) + 1)
                        chunks = [companies_batch[i:i + chunk_size] for i in range(0, len(companies_batch), chunk_size)]
                        
                        # Создаем по одной задаче на каждый ключ
                        for i, chunk in enumerate(chunks):
                            if i < len(checko_keys):
                                # Выбираем ключ для этого пакета (циклически)
                                key_index = i % len(checko_keys)
                                key = checko_keys[key_index]
                                
                                # Создаем новый экземпляр парсера с этим ключом
                                import importlib
                                module = importlib.import_module(parser.__class__.__module__)
                                parser_class = getattr(module, parser.__class__.__name__)
                                new_parser = parser_class(token=key)
                                
                                self.logger.info(f"Создаем задачу для ключа {key_index+1}/{len(checko_keys)} с {len(chunk)} компаниями")
                                tasks.append(self.process_batch(new_parser, chunk))
                else:
                    # Если ключей нет, просто обрабатываем все компании через имеющийся парсер
                    tasks.append(self.process_batch(parser, companies_batch))
            else:
                # Для всех остальных парсеров - просто добавляем задачу
                tasks.append(self.process_batch(parser, companies_batch))
        
        # Запускаем все задачи с учетом ограничений на количество параллельных процессов
        from concurrent.futures import ProcessPoolExecutor
        
        # Определяем количество параллельных процессов (учитываем ограничения API и CPU)
        max_workers = min(
            int(os.getenv('MAX_WORKERS', os.cpu_count() or 4)),  # Ограничение по CPU
            10  # Верхняя граница параллельных процессов
        )
        
        # Если у нас есть парсеры с API лимитами, ограничиваем количество процессов
        has_api_parsers = any(
            parser.__class__.__name__ in ["CheckoParser", "DadataParser"] 
            for parser in distribution.keys()
        )
        
        if has_api_parsers:
            # Ограничиваем до 2-4 процессов для API-парсеров
            max_workers = min(max_workers, int(os.getenv('API_MAX_WORKERS', 2)))
            self.logger.info(f"Установлено максимальное количество параллельных процессов: {max_workers} для сохранения лимита API-запросов")
        
        # Если задача только одна или параллелизм отключен, запускаем задачи последовательно
        if max_workers <= 1 or len(tasks) <= 1:
            for task in tasks:
                # Проверяем флаг остановки
                if self.should_stop:
                    self.logger.warning("Обработка остановлена из-за превышения лимита API")
                    break
                await task
        else:
            # Используем процессы для распараллеливания задач
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                # Оборачиваем каждую задачу в синхронную функцию для ProcessPoolExecutor
                futures = []
                
                for i, (parser, companies_batch) in enumerate(distribution.items()):
                    if not companies_batch:
                        continue
                    
                    # Проверяем флаг остановки
                    if self.should_stop:
                        self.logger.warning("Обработка остановлена из-за превышения лимита API")
                        break
                        
                    # Запускаем обработку в отдельном процессе
                    future = executor.submit(self.process_batch_sync, parser, companies_batch)
                    futures.append(future)
                
                # Ожидаем завершения всех задач
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"Ошибка при выполнении задачи: {e}")
        
        # Сохраняем результаты
        self.data_manager.save_results(force=True)
        
        self.logger.info(f"Завершение процесса парсинга") 