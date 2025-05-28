import threading
import aiohttp
import asyncio
import logging
from bs4 import BeautifulSoup
import re
from typing import Optional, Dict, Any, List, Tuple
import os
import signal
import sys
import json
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from parser_base import BaseSiteParser, CompanyData, DataManager
# import undetected_chromedriver as uc
from dadata import Dadata, DadataAsync

from dotenv import load_dotenv, find_dotenv

# Загрузка переменных окружения из .env файла
if find_dotenv():
    here = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(here, '.env'), override=True)

# Настройка логирования
logger = logging.getLogger("TIN_Parser.site_parsers")

# Загрузка конфигурационных параметров из .env
RAIFFEISEN_BLOCK_TIME_SECONDS = int(os.getenv('RAIFFEISEN_BLOCK_TIME_SECONDS', '3600'))
RAIFFEISEN_SECONDARY_WAIT_SECONDS = int(os.getenv('RAIFFEISEN_SECONDARY_WAIT_SECONDS', '600'))
RAIFFEISEN_MAX_RETRY_ATTEMPTS = int(os.getenv('RAIFFEISEN_MAX_RETRY_ATTEMPTS', '24'))

# Блокировка для контроля доступа к Райфайзен банку
raiffeisen_lock = threading.Lock()
# Флаг блокировки Райфайзен
raiffeisen_blocked = False
# Время последней блокировки
raiffeisen_block_time = 0


# Функция для проверки, не заблокирован ли Райфайзен банк
def is_raiffeisen_blocked():
    """
    Проверяет, находится ли сайт Райфайзен в состоянии блокировки.
    Если с момента блокировки прошло более часа, снимает блокировку.
    
    :return: True, если сайт заблокирован, False в противном случае
    """
    global raiffeisen_blocked, raiffeisen_block_time
    
    with raiffeisen_lock:
        if not raiffeisen_blocked:
            return False
        
        # Проверяем, не прошло ли время блокировки
        current_time = time.time()
        if current_time - raiffeisen_block_time >= RAIFFEISEN_BLOCK_TIME_SECONDS:
            logger.info("Время блокировки Райфайзен банка истекло, снимаем блокировку")
            raiffeisen_blocked = False
            return False
            
        # Вычисляем, сколько времени осталось до конца блокировки
        remaining_time = int((raiffeisen_block_time + RAIFFEISEN_BLOCK_TIME_SECONDS - current_time) / 60)  # в минутах
        logger.info(f"Райфайзен банк заблокирован еще {remaining_time} минут")
        return True

# Функция для установки блокировки Райфайзен банка
def set_raiffeisen_blocked():
    """
    Устанавливает флаг блокировки сайта Райфайзен и сохраняет время блокировки
    """
    global raiffeisen_blocked, raiffeisen_block_time
    
    with raiffeisen_lock:
        # Устанавливаем блокировку, только если она еще не установлена или прошло больше 10 минут
        current_time = time.time()
        if not raiffeisen_blocked or (current_time - raiffeisen_block_time > RAIFFEISEN_SECONDARY_WAIT_SECONDS):
            raiffeisen_blocked = True
            raiffeisen_block_time = current_time
            logger.warning(f"Установлена блокировка для Райфайзен банка на {RAIFFEISEN_BLOCK_TIME_SECONDS // 60} минут")
        else:
            # Если блокировка уже установлена, выводим информацию о времени ожидания
            remaining_time = int((raiffeisen_block_time + RAIFFEISEN_BLOCK_TIME_SECONDS - current_time) / 60)  # в минутах
            logger.info(f"Райфайзен банк уже заблокирован, осталось ждать {remaining_time} минут")

class KeyRotator:
    """Класс для ротации API ключей"""
    
    def __init__(self, keys: List[str], source_name: str):
        """
        Инициализация менеджера ротации ключей
        
        :param keys: Список API ключей
        :param source_name: Название источника для логирования
        """
        self.keys = keys
        self.current_index = 0
        self.logger = logging.getLogger(f"TIN_Parser.{source_name}.KeyRotator")
        if not keys:
            self.logger.error("Список ключей пуст!")
        else:
            self.logger.info(f"Инициализирован ротатор ключей с {len(keys)} ключами")
    
    def get_current_key(self) -> Optional[str]:
        """
        Получить текущий активный ключ
        
        :return: Текущий ключ или None, если список пуст
        """
        if not self.keys:
            return None
        return self.keys[self.current_index]
    
    def rotate_key(self) -> Optional[str]:
        """
        Переключиться на следующий ключ
        
        :return: Следующий ключ или None, если список пуст
        """
        if not self.keys:
            return None
            
        self.current_index = (self.current_index + 1) % len(self.keys)
        key = self.keys[self.current_index]
        self.logger.info(f"Переключение на ключ {self.current_index + 1}/{len(self.keys)}")
        return key
    
    def is_empty(self) -> bool:
        """
        Проверить, пуст ли список ключей
        
        :return: True, если список пуст, иначе False
        """
        return len(self.keys) == 0
    
    def get_all_keys_count(self) -> int:
        """
        Получить общее количество доступных ключей
        
        :return: Количество ключей
        """
        return len(self.keys)

class FocusKonturParser(BaseSiteParser):
    """Парсер для сайта focus.kontur.ru"""
    
    def __init__(self, rate_limit: float = 2.0, max_retries: int = 1):
        super().__init__(rate_limit)
        self.site_name = "focus.kontur.ru"
        self.search_url = "https://focus.kontur.ru/search?country=RU"
        self.max_retries = max_retries
        
        # Настройка Chrome
        self.options = Options()
        # self.options.add_argument('--headless')  # Запуск в фоновом режиме
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        # self.options.add_argument('--disable-gpu')
        self.options.add_argument('--ignore-certificate-errors')
        self.options.add_argument('--ignore-ssl-errors')
        self.options.add_argument('--log-level=3')  # Уменьшаем вывод логов браузера
        
        # XPath-селекторы
        self.search_input_xpath = "/html/body/div[2]/div[2]/div/div/div/noindex/div/div/div[1]/div[2]/div/div/div[1]/input"
        self.search_button_xpath = "/html/body/div[2]/div[2]/div/div/div/noindex/div/div/div[1]/div[2]/div/div/div[1]/div/button"
        
        # Настройки таймаутов и ожидания
        self.page_load_timeout = 60  # Таймаут загрузки страницы (секунды)
        self.wait_timeout = 10  # Таймаут для ожидания элементов (секунды)
        self.wait_after_search = 10  # Ожидание после поиска (секунды)
        
        # Регулярные выражения для извлечения данных
        self.chairman_pattern = re.compile(r'(?:Председатель|Директор)[^\n]*\n([А-ЯЁ][а-яё]+ [А-ЯЁ][а-яё]+ [А-ЯЁ][а-яё]+)')
        self.chairman_inn_pattern = re.compile(r'ИНН (\d{10,12})')
        
        # Драйвер браузера (инициализируется в parse_companies)
        self.driver = None
        self.wait = None
        self.current_retry = 0
    
    async def parse_companies(self, companies: List[CompanyData]) -> List[CompanyData]:
        """Парсит список компаний с использованием одного экземпляра браузера"""
        results = []
        self.logger.info(f"Начинаем обработку {len(companies)} компаний")
        
        try:
            # Инициализация браузера (один раз для всех компаний)
            chromedriver_path = os.path.join(os.getcwd(), 'chromedriver.exe')
            if not os.path.exists(chromedriver_path):
                chromedriver_path = 'C:/chromedriver/chromedriver.exe'
                if not os.path.exists(chromedriver_path):
                    self.logger.error(f"ChromeDriver не найден по пути {chromedriver_path}")
                    return results
            
            # Создаем сервис и драйвер
            service = Service(executable_path=chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=self.options)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            self.wait = WebDriverWait(self.driver, self.wait_timeout)
            
            # Получаем ссылку на data_manager для обновления результатов
            data_manager = self._get_data_manager()
            
            # Обработка всех компаний
            for i, company in enumerate(companies):
                try:
                    # Проверяем на прерывание программы перед каждой компанией
                    try:
                        # Используем asyncio.sleep с очень маленьким таймаутом для проверки прерываний
                        await asyncio.sleep(0.01)
                    except asyncio.CancelledError:
                        self.logger.info("Обнаружено прерывание, останавливаем парсинг")
                        break
                    
                    self.logger.info(f"[{i+1}/{len(companies)}] Обработка компании: {company.name} (ИНН: {company.inn})")
                    
                    # Соблюдаем задержку между запросами
                    await asyncio.sleep(self.rate_limit)
                    
                    # Сбрасываем счетчик попыток для новой компании
                    self.current_retry = 0
                    
                    # Парсим информацию о компании (переиспользуя тот же браузер)
                    result = await self.parse_company(company)
                    if result:
                        result.source = self.site_name
                        results.append(result)
                        self.logger.info(f"Успешно получены данные для {company.name}")
                        
                        # Обновляем результаты в data_manager если он доступен
                        if data_manager:
                            data_manager.update_results(result)
                    else:
                        self.logger.warning(f"Не удалось получить данные для {company.name}")
                        
                    # После каждой 10-й компании проверяем, жив ли браузер
                    if (i + 1) % 10 == 0:
                        self.logger.info(f"Проверка активности браузера... ([{i+1}/{len(companies)}])")
                        try:
                            self.driver.current_url
                            if "https" not in self.driver.current_url:
                                raise WebDriverException  # Проверка активности браузера
                        except WebDriverException:
                            self.logger.warning("Браузер перестал отвечать, перезапускаем")
                            # Корректно закрываем браузер перед пересозданием
                            self._ensure_browser_closed()
                                    
                            # Пересоздаем драйвер
                            self.driver = webdriver.Chrome(service=service, options=self.options)
                            self.driver.set_page_load_timeout(self.page_load_timeout)
                            self.wait = WebDriverWait(self.driver, self.wait_timeout)
                            
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке компании {company.name}: {e}")
                    # Если произошла ошибка во время обработки компании, гарантируем закрытие браузера
                    if i < len(companies) - 1:  # Если это не последняя компания
                        self.logger.info("Перезапуск браузера после ошибки")
                        self._ensure_browser_closed()
                        
                        # Пересоздаем драйвер
                        try:
                            self.driver = webdriver.Chrome(service=service, options=self.options)
                            self.driver.set_page_load_timeout(self.page_load_timeout)
                            self.wait = WebDriverWait(self.driver, self.wait_timeout)
                        except Exception as browser_error:
                            self.logger.error(f"Не удалось перезапустить браузер: {browser_error}")
                            break  # Прекращаем обработку, если браузер не удалось перезапустить
            
            self.logger.info(f"Завершена обработка компаний, успешно: {len(results)} из {len(companies)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при инициализации браузера: {e}")
        finally:
            # Гарантируем закрытие браузера в любом случае
            self._ensure_browser_closed()
        
        return results
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта focus.kontur.ru"""
        
        for attempt in range(self.max_retries):
            try:
                # Открываем страницу поиска
                self.logger.info(f"Открываем страницу поиска для компании {company.inn} (попытка {attempt+1}/{self.max_retries})")
                self.driver.get(self.search_url)
                
                # Проверка на некорректный URL (data: и др.)
                if self.driver.current_url.startswith('data.;') or not self.driver.current_url.startswith('http'):
                    self.logger.error(f"Браузер вернул некорректный URL: {self.driver.current_url}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2)
                        # Пробуем обновить страницу
                        try:
                            self.driver.refresh()
                            await asyncio.sleep(3)  # Ждем после обновления
                        except:
                            pass
                        continue
                    else:
                        # Возвращаем данные с отметкой "не найдено"
                        company.chairman_name = "не найдено"
                        company.chairman_inn = "не найдено"
                        return company
                
                # Проверка на блокировку
                if "вы превысили лимит запросов к серверу" in self.driver.page_source.lower():
                    self.logger.warning(f"Сайт focus.kontur.ru заблокировал парсер.")
                    return None
                
                # # Если мы на странице "ничего не найдено"
                # if 'проверьте запрос на ошибки' in self.driver.page_source.lower():
                #     self.logger.warning(f"Компания {company.inn} не найдена на focus.kontur.ru")
                #     # Возвращаем данные с отметкой "не найдено"
                #     company.chairman_name = "не найдено"
                #     company.chairman_inn = "не найдено"
                #     return company
                
                # Ждем загрузки поля поиска
                try:
                    search_input = self.wait.until(EC.presence_of_element_located((By.XPATH, self.search_input_xpath)))
                except TimeoutException:
                    self.logger.warning(f"Тайм-аут при ожидании элемента поиска (попытка {attempt+1}/{self.max_retries})")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2)
                        continue
                    else:
                        # Возвращаем данные с отметкой "не найдено"
                        company.chairman_name = "не найдено"
                        company.chairman_inn = "не найдено"
                        return company
                
                # Вводим ИНН в поле поиска
                search_input.clear()
                search_input.send_keys(company.inn)
                
                # Нажимаем кнопку поиска или Enter
                try:
                    search_button = self.driver.find_element(By.XPATH, self.search_button_xpath)
                    search_button.click()
                except NoSuchElementException:
                    search_input.send_keys(Keys.RETURN)
                
                # Ждем загрузки результатов поиска
                try:
                    # Ждем появления страницы компании или страницы "ничего не найдено"
                    self.wait.until(lambda d: 'entity' in d.current_url or 'проверьте запрос на ошибки' in d.page_source.lower() or 'data:' in d.current_url)

                    # Проверка на некорректный URL
                    if self.driver.current_url.startswith('data:'):
                        self.logger.error(f"Браузер вернул data: URL после поиска")
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        else:
                            # Возвращаем данные с отметкой "не найдено"
                            company.chairman_name = "не найдено"
                            company.chairman_inn = "не найдено"
                            return company
                    
                    # Даем странице дополнительное время загрузиться (особенно важно для директора)
                    await asyncio.sleep(self.wait_after_search)
                    
                    # Если мы на странице "ничего не найдено"
                    if 'проверьте запрос на ошибки' in self.driver.page_source.lower():
                        self.logger.warning(f"Компания {company.inn} не найдена на focus.kontur.ru")
                        # Возвращаем данные с отметкой "не найдено"
                        company.chairman_name = "не найдено"
                        company.chairman_inn = "не найдено"
                        return company
                    
                    # Если мы на странице поиска, но нашли только одну компанию, нажимаем на неё
                    if 'entity' not in self.driver.current_url and 'search' in self.driver.current_url:
                        try:
                            company_link = self.driver.find_element(By.CSS_SELECTOR, "a.company-name")
                            company_link.click()
                            # Ждем загрузки страницы компании
                            await asyncio.sleep(self.wait_after_search)
                        except NoSuchElementException:
                            pass
                    
                    # Получаем весь текст из блока информации о компании
                    try:
                        # Ищем блок с информацией по классу unevenIndent
                        self.logger.info(f"Получаем информацию о компании из блока unevenIndent")
                        div_elem = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "unevenIndent")))
                        
                        # Получаем весь текст
                        company_text = div_elem.text
                        
                        # Парсим данные о председателе
                        # Шаг 1: Ищем строку с упоминанием "Председатель" или "Директор"
                        chairman_line = None
                        lines = company_text.split('\n')
                        for i, line in enumerate(lines):
                            if "Председатель" in line or "Директор" in line or "руководитель" in line:
                                chairman_line = i
                                break
                        
                        if chairman_line is not None:
                            # Шаг 2: Имя директора обычно на следующей строке после должности
                            if chairman_line + 1 < len(lines):
                                chairman_name = lines[chairman_line + 1].strip()
                                # Проверяем, что это похоже на ФИО (начинается с заглавной буквы и содержит пробелы)
                                if chairman_name and chairman_name[0].isupper() and ' ' in chairman_name:
                                    company.chairman_name = chairman_name
                                    self.logger.info(f"Извлечено имя директора: {chairman_name}")
                                else:
                                    # Если имя не в следующей строке, то возможно оно в той же строке
                                    name_parts = [word for word in lines[chairman_line].split() if word[0].isupper() and len(word) > 1]
                                    if len(name_parts) >= 2:  # Минимум имя и фамилия
                                        # Пропускаем слово "Председатель"/"Директор"
                                        if name_parts[0] in ["Председатель", "Директор"]:
                                            name_parts = name_parts[1:]
                                        
                                        if name_parts:
                                            company.chairman_name = " ".join(name_parts)
                                            self.logger.info(f"Извлечено имя директора из строки должности: {company.chairman_name}")
                        
                            # Шаг 3: Ищем ИНН директора (обычно в следующих 3-5 строках)
                            for j in range(chairman_line, min(chairman_line + 5, len(lines))):
                                inn_match = self.chairman_inn_pattern.search(lines[j])
                                if inn_match:
                                    company.chairman_inn = inn_match.group(1)
                                    self.logger.info(f"Извлечен ИНН директора: {company.chairman_inn}")
                                    break
                        
                            if not company.chairman_inn:
                                self.logger.warning(f"ИНН директора не найден для компании {company.inn}")
                                company.chairman_inn = "не найдено"
                        else:
                            self.logger.warning(f"Информация о директоре не найдена для компании {company.inn}")
                            company.chairman_name = "не найдено"
                            company.chairman_inn = "не найдено"
                        
                        return company
                        
                    except StaleElementReferenceException:
                        if attempt < self.max_retries - 1:
                            self.logger.warning(f"Элемент устарел, повторяем попытку ({attempt+1}/{self.max_retries})")
                            await asyncio.sleep(2)
                            continue
                        raise
                    except Exception as e:
                        self.logger.error(f"Ошибка при извлечении данных о компании: {e}")
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        # Возвращаем данные с отметкой "не найдено"
                        company.chairman_name = "не найдено"
                        company.chairman_inn = "не найдено"
                        return company
                    
                except TimeoutException:
                    self.logger.warning(f"Тайм-аут при получении данных для компании {company.inn}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2)
                        continue
                    # Возвращаем данные с отметкой "не найдено"
                    company.chairman_name = "не найдено"
                    company.chairman_inn = "не найдено"
                    return company
                    
            except WebDriverException as e:
                self.logger.error(f"Ошибка веб-драйвера при парсинге компании {company.inn}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2)
                    continue
                # Возвращаем данные с отметкой "не найдено"
                company.chairman_name = "не найдено"
                company.chairman_inn = "не найдено"
                return company
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге компании {company.inn}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2)
                    continue
                # Возвращаем данные с отметкой "не найдено"
                company.chairman_name = "не найдено"
                company.chairman_inn = "не найдено"
                return company
        
        # Если все попытки не удались
        company.chairman_name = "не найдено"
        company.chairman_inn = "не найдено"
        return company

    def _get_data_manager(self) -> Optional[DataManager]:
        """Получает ссылку на глобальный data_manager для обновления результатов"""
        try:
            # Ищем data_manager в глобальных переменных
            from parser_base import DataManager
            data_manager = None
            
            # Ищем data_manager в globals()
            for var_name, var_value in globals().items():
                if isinstance(var_value, DataManager):
                    data_manager = var_value
                    break
                    
            # Также ищем в ссылках из модуля main
            if data_manager is None:
                try:
                    import main
                    if hasattr(main, 'data_manager') and main.data_manager is not None:
                        data_manager = main.data_manager
                except:
                    pass
            
            return data_manager
        except Exception as e:
            self.logger.error(f"Ошибка при получении data_manager: {e}")
            return None

    def _ensure_browser_closed(self) -> None:
        """
        Надежное закрытие браузера
        """
        if self.driver:
            try:
                self.logger.info("Закрываем браузер...")
                # Закрываем браузер стандартным способом
                self.driver.quit()
                self.logger.info("Браузер успешно закрыт")
            except Exception as e:
                self.logger.error(f"Ошибка при закрытии браузера: {e}")
            finally:
                # Гарантируем, что ссылки на драйвер сбрасываются в любом случае
                self.driver = None
                self.wait = None

class CheckoParser(BaseSiteParser):
    """Парсер для сайта checko.ru"""
    
    def __init__(self, rate_limit: float = 2.0):
        super().__init__("checko.ru", rate_limit)
        self.search_url = "https://checko.ru/search"
        self.company_url = "https://checko.ru/company"
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта checko.ru"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                # Формируем URL для поиска по ИНН
                params = {
                    'q': company.inn
                }
                
                async with aiohttp.ClientSession() as session:
                    # Выполняем поисковый запрос
                    async with session.get(self.search_url, params=params, headers=self.headers) as response:
                        if response.status != 200:
                            self.logger.warning(f"Ошибка при поиске компании {company.inn}: статус {response.status}")
                            if attempt < max_attempts - 1 and response.status >= 500:
                                await asyncio.sleep(2)
                                continue
                            return None
                        
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Ищем ссылку на страницу компании
                        company_link = soup.select_one('a.jss198')
                        if not company_link:
                            self.logger.warning(f"Компания {company.inn} не найдена на checko.ru")
                            return None
                        
                        company_href = company_link.get('href')
                        if not company_href:
                            return None
                        
                        company_url = f"https://checko.ru{company_href}"
                        
                        # Переходим на страницу компании
                        async with session.get(company_url, headers=self.headers) as company_response:
                            if company_response.status != 200:
                                self.logger.warning(f"Ошибка при получении данных компании {company.inn}: статус {company_response.status}")
                                if attempt < max_attempts - 1 and company_response.status >= 500:
                                    await asyncio.sleep(2)
                                    continue
                                return None
                            
                            company_html = await company_response.text()
                            company_soup = BeautifulSoup(company_html, 'html.parser')
                            
                            # Ищем информацию о руководителе
                            director_block = company_soup.select_one('div.jss294')
                            if not director_block:
                                self.logger.warning(f"Информация о руководителе компании {company.inn} не найдена")
                                return None
                            
                            # Извлекаем имя директора
                            director_name_elem = director_block.select_one('p.jss296')
                            if director_name_elem:
                                company.chairman_name = director_name_elem.text.strip()
                            
                            # ИНН директора обычно не представлен на странице checko.ru
                            
                            return company
            except aiohttp.ClientError as e:
                self.logger.error(f"Ошибка сети при парсинге компании {company.inn} на checko.ru: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2)
                    continue
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге компании {company.inn} на checko.ru: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2)
                    continue
        
        return None

class ZaChestnyiBiznesParser(BaseSiteParser):
    """Парсер для сайта zachestnyibiznes.ru"""
    
    def __init__(self, rate_limit: float = 3.0):
        super().__init__("zachestnyibiznes.ru", rate_limit)
        self.search_url = "https://zachestnyibiznes.ru/search"
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта zachestnyibiznes.ru"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                # Формируем URL для поиска по ИНН
                params = {
                    'query': company.inn
                }
                
                async with aiohttp.ClientSession() as session:
                    # Выполняем поисковый запрос
                    async with session.get(self.search_url, params=params, headers=self.headers) as response:
                        if response.status != 200:
                            self.logger.warning(f"Ошибка при поиске компании {company.inn}: статус {response.status}")
                            if attempt < max_attempts - 1 and response.status >= 500:
                                await asyncio.sleep(2)
                                continue
                            return None
                        
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Ищем ссылку на страницу компании
                        company_link = soup.select_one('a.card-title')
                        if not company_link:
                            self.logger.warning(f"Компания {company.inn} не найдена на zachestnyibiznes.ru")
                            return None
                        
                        company_href = company_link.get('href')
                        if not company_href:
                            return None
                        
                        company_url = f"https://zachestnyibiznes.ru{company_href}"
                        
                        # Переходим на страницу компании
                        async with session.get(company_url, headers=self.headers) as company_response:
                            if company_response.status != 200:
                                self.logger.warning(f"Ошибка при получении данных компании {company.inn}: статус {company_response.status}")
                                if attempt < max_attempts - 1 and company_response.status >= 500:
                                    await asyncio.sleep(2)
                                    continue
                                return None
                            
                            company_html = await company_response.text()
                            company_soup = BeautifulSoup(company_html, 'html.parser')
                            
                            # Ищем информацию о руководителе
                            director_block = company_soup.select_one('div.director-info')
                            if not director_block:
                                self.logger.warning(f"Информация о руководителе компании {company.inn} не найдена")
                                return None
                            
                            # Извлекаем имя директора
                            director_name_elem = director_block.select_one('h2')
                            if director_name_elem:
                                company.chairman_name = director_name_elem.text.strip()
                            
                            # Ищем ИНН директора
                            inn_elem = director_block.select_one('span.inn-value')
                            if inn_elem:
                                company.chairman_inn = inn_elem.text.strip()
                            
                            return company
            except aiohttp.ClientError as e:
                self.logger.error(f"Ошибка сети при парсинге компании {company.inn} на zachestnyibiznes.ru: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2)
                    continue
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге компании {company.inn} на zachestnyibiznes.ru: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2)
                    continue
        
        return None

class AuditItParser(BaseSiteParser):
    """Парсер для сайта audit-it.ru"""
    
    def __init__(self, rate_limit: float = 2.0):
        super().__init__(rate_limit)
        self.site_name = "www.audit-it.ru"
        self.search_url = "https://www.audit-it.ru/contragent"

        # Настройка Chrome
        self.options = Options()
        # # self.options.add_argument('--headless')  # Запуск в фоновом режиме
        # self.options.add_argument('--no-sandbox')
        # self.options.add_argument('--disable-dev-shm-usage')
        # # self.options.add_argument('--disable-gpu')
        # self.options.add_argument('--ignore-certificate-errors')
        # self.options.add_argument('--ignore-ssl-errors')
        # self.options.add_argument('--log-level=3')  # Уменьшаем вывод логов браузера

        # self.options = uc.ChromeOptions()
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_argument("--disable-extensions")
        self.options.add_argument('--ignore-certificate-errors')
        self.options.add_argument('--ignore-ssl-errors')
        # self.options.add_argument('--headless') 
        
        # XPath-селекторы
        self.search_input_xpath = "/html/body/div[1]/section/section[2]/div[3]/div/div[1]/div/div[1]/form/div/input[4]"
        self.search_button_xpath = "/html/body/div[1]/section/section[2]/div[3]/div/div[1]/div/div[1]/form/div/button"
        
        # Селекторы CSS классов
        self.table_class = "quick-profile"
        # Настройки таймаутов и ожидания
        self.page_load_timeout = 60  # Таймаут загрузки страницы (секунды)
        self.wait_timeout = 10  # Таймаут для ожидания элементов (секунды)
        self.wait_after_search = 10  # Ожидание после поиска (секунды)
        
        # Драйвер браузера (инициализируется в parse_companies)
        self.driver = None
        self.wait = None
        self.current_retry = 0

        self.max_retries = 1


    async def parse_companies(self, companies: List[CompanyData]) -> List[CompanyData]:
        """Парсит список компаний с использованием одного экземпляра браузера"""
        results = []
        self.logger.info(f"Начинаем обработку {len(companies)} компаний")
        
        try:
            # Инициализация браузера (один раз для всех компаний)
            chromedriver_path = os.path.join(os.getcwd(), 'chromedriver.exe')
            if not os.path.exists(chromedriver_path):
                chromedriver_path = 'C:/chromedriver/chromedriver.exe'
                if not os.path.exists(chromedriver_path):
                    self.logger.error(f"ChromeDriver не найден по пути {chromedriver_path}")
                    return results
            
            # Создаем сервис и драйвер
            service = Service(executable_path=chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=self.options)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            self.wait = WebDriverWait(self.driver, self.wait_timeout)
            
            # Получаем ссылку на data_manager для обновления результатов
            data_manager = self._get_data_manager()
            
            # Обработка всех компаний
            for i, company in enumerate(companies):
                try:
                    # Проверяем на прерывание программы перед каждой компанией
                    try:
                        # Используем asyncio.sleep с очень маленьким таймаутом для проверки прерываний
                        await asyncio.sleep(0.01)
                    except asyncio.CancelledError:
                        self.logger.info("Обнаружено прерывание, останавливаем парсинг")
                        break
                    
                    self.logger.info(f"[{i+1}/{len(companies)}] Обработка компании: {company.name} (ИНН: {company.inn})")
                    
                    # Соблюдаем задержку между запросами
                    await asyncio.sleep(self.rate_limit)
                    
                    # Сбрасываем счетчик попыток для новой компании
                    self.current_retry = 0
                    
                    # Парсим информацию о компании (переиспользуя тот же браузер)
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
            
        except Exception as e:
            self.logger.error(f"Ошибка при инициализации браузера: {e}")
        finally:
            # Гарантируем закрытие браузера в любом случае
            self._ensure_browser_closed()
        
        return results
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта www.audit-it.ru"""
        
        for attempt in range(self.max_retries):
            try:
                # Открываем страницу поиска
                self.logger.info(f"Открываем страницу поиска для компании {company.inn} (попытка {attempt+1}/{self.max_retries})")
                self.driver.get(self.search_url)
                
                # Проверка на некорректный URL (data: и др.)
                if self.driver.current_url.startswith('data.;') or not self.driver.current_url.startswith('http'):
                    self.logger.error(f"Браузер вернул некорректный URL: {self.driver.current_url}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2)
                        # Пробуем обновить страницу
                        try:
                            self.driver.refresh()
                            await asyncio.sleep(3)  # Ждем после обновления
                        except:
                            pass
                        continue
                    else:
                        # Возвращаем данные с отметкой "не найдено"
                        company.chairman_name = "не найдено"
                        company.chairman_inn = "не найдено"
                        return company
                
                # Проверка на блокировку
                # if "вы превысили лимит запросов к серверу" in self.driver.page_source.lower():
                #     self.logger.warning(f"Сайт focus.kontur.ru заблокировал парсер.")
                #     return None

                
                # Ждем загрузки поля поиска
                try:
                    search_input = self.wait.until(EC.presence_of_element_located((By.XPATH, self.search_input_xpath)))
                except TimeoutException:
                    self.logger.warning(f"Тайм-аут при ожидании элемента поиска (попытка {attempt+1}/{self.max_retries})")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2)
                        continue
                    else:
                        # Возвращаем данные с отметкой "не найдено"
                        company.chairman_name = "не найдено"
                        company.chairman_inn = "не найдено"
                        return company
                
                # Вводим ИНН в поле поиска
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_input)
                self.driver.execute_script("arguments[0].focus();", search_input)
                self.driver.execute_script("arguments[0].click();", search_input)
                try:
                    actions = webdriver.ActionChains(self.driver)
                    import random

                    actions.move_to_element_with_offset(search_input, random.randint(1, 5), random.randint(1, 5))
                    await asyncio.sleep(1)
                    search_input.send_keys(Keys.ENTER)
                    await asyncio.sleep(2)
                    actions.perform()

                    search_input.clear()
                    search_input.send_keys(company.inn)
                except Exception:
                    self.driver.execute_script("""
                        const input = arguments[0];
                        const value = arguments[1];
                        input.value = value;
                        input.dispatchEvent(new Event('input', { bubbles: true }));
                        input.dispatchEvent(new Event('change', { bubbles: true }));
                    """, search_input, company.inn)
                
                # Нажимаем кнопку поиска или Enter
                try:
                    search_button = self.driver.find_element(By.XPATH, self.search_button_xpath)
                    search_button.click()
                except NoSuchElementException:
                    search_input.send_keys(Keys.RETURN)
                
                # Ждем загрузки результатов поиска
                try:
                    # Ждем появления страницы компании или страницы "ничего не найдено"
                    self.wait.until(lambda d: 'контрагент' in d.page_source.lower())
                    
                    # Даем странице дополнительное время загрузиться (особенно важно для директора)
                    await asyncio.sleep(self.wait_after_search)
                    
                    # Если мы на странице "ничего не найдено"
                    if 'по вашему запросу ничего не найдено' in self.driver.page_source.lower():
                        self.logger.warning(f"Компания {company.inn} не найдена на www.audit-it.ru")
                        # Возвращаем данные с отметкой "не найдено"
                        company.chairman_name = "не найдено"
                        company.chairman_inn = "не найдено"
                        return company
                    
                    # Получаем весь текст из блока информации о компании
                    try:
                        # Ищем блок с информацией по классу quick-profile
                        self.logger.info(f"Получаем информацию о компании из блока {self.table_class}")
                        table_elem = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, self.table_class)))
                        
                        # Получаем весь текст
                        company_text = table_elem.text
                        
                        # TODO Парсим данные о председателе
                        # Шаг 1: Ищем строку с упоминанием "Председатель" или "Директор"
                        chairman_line = None
                        lines = company_text.split('\n')
                        for i, line in enumerate(lines):
                            if "Председатель" in line or "Директор" in line or "руководитель" in line:
                                chairman_line = i
                                break
                        
                        if chairman_line is not None:
                            # Шаг 2: Имя директора обычно на следующей строке после должности
                            if chairman_line + 1 < len(lines):
                                chairman_name = lines[chairman_line + 1].strip()
                                # Проверяем, что это похоже на ФИО (начинается с заглавной буквы и содержит пробелы)
                                if chairman_name and chairman_name[0].isupper() and ' ' in chairman_name:
                                    company.chairman_name = chairman_name
                                    self.logger.info(f"Извлечено имя директора: {chairman_name}")
                                else:
                                    # Если имя не в следующей строке, то возможно оно в той же строке
                                    name_parts = [word for word in lines[chairman_line].split() if word[0].isupper() and len(word) > 1]
                                    if len(name_parts) >= 2:  # Минимум имя и фамилия
                                        # Пропускаем слово "Председатель"/"Директор"
                                        if name_parts[0] in ["Председатель", "Директор"]:
                                            name_parts = name_parts[1:]
                                        
                                        if name_parts:
                                            company.chairman_name = " ".join(name_parts)
                                            self.logger.info(f"Извлечено имя директора из строки должности: {company.chairman_name}")
                        
                            # Шаг 3: Ищем ИНН директора (обычно в следующих 3-5 строках)
                            for j in range(chairman_line, min(chairman_line + 5, len(lines))):
                                inn_match = self.chairman_inn_pattern.search(lines[j])
                                if inn_match:
                                    company.chairman_inn = inn_match.group(1)
                                    self.logger.info(f"Извлечен ИНН директора: {company.chairman_inn}")
                                    break
                        
                            if not company.chairman_inn:
                                self.logger.warning(f"ИНН директора не найден для компании {company.inn}")
                                company.chairman_inn = "не найдено"
                        else:
                            self.logger.warning(f"Информация о директоре не найдена для компании {company.inn}")
                            company.chairman_name = "не найдено"
                            company.chairman_inn = "не найдено"
                        
                        return company
                        
                    except StaleElementReferenceException:
                        if attempt < self.max_retries - 1:
                            self.logger.warning(f"Элемент устарел, повторяем попытку ({attempt+1}/{self.max_retries})")
                            await asyncio.sleep(2)
                            continue
                        raise
                    except Exception as e:
                        self.logger.error(f"Ошибка при извлечении данных о компании: {e}")
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        # Возвращаем данные с отметкой "не найдено"
                        company.chairman_name = "не найдено"
                        company.chairman_inn = "не найдено"
                        return company
                    
                except TimeoutException:
                    self.logger.warning(f"Тайм-аут при получении данных для компании {company.inn}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2)
                        continue
                    # Возвращаем данные с отметкой "не найдено"
                    company.chairman_name = "не найдено"
                    company.chairman_inn = "не найдено"
                    return company
                    
            except WebDriverException as e:
                self.logger.error(f"Ошибка веб-драйвера при парсинге компании {company.inn}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2)
                    continue
                # Возвращаем данные с отметкой "не найдено"
                company.chairman_name = "не найдено"
                company.chairman_inn = "не найдено"
                return company
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге компании {company.inn}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2)
                    continue
                # Возвращаем данные с отметкой "не найдено"
                company.chairman_name = "не найдено"
                company.chairman_inn = "не найдено"
                return company
        
        # Если все попытки не удались
        company.chairman_name = "не найдено"
        company.chairman_inn = "не найдено"
        return company

    def _get_data_manager(self) -> Optional[DataManager]:
        """
        Получает объект DataManager из текущего экземпляра BaseSiteParser
        через поиск в родительских объектах
        
        :return: Экземпляр DataManager или None
        """
        try:
            # Получаем доступ к родительскому объекту ParserManager
            frame = sys._getframe(2)
            while frame:
                if 'self' in frame.f_locals:
                    parser_manager = frame.f_locals['self']
                    if hasattr(parser_manager, 'data_manager'):
                        return parser_manager.data_manager
                frame = frame.f_back
        except Exception as e:
            self.logger.debug(f"Не удалось получить доступ к data_manager: {e}")
        return None 

    def _ensure_browser_closed(self) -> None:
        """
        Надежное закрытие браузера
        """
        if self.driver:
            try:
                self.logger.info("Закрываем браузер...")
                # Закрываем браузер стандартным способом
                self.driver.quit()
                self.logger.info("Браузер успешно закрыт")
            except Exception as e:
                self.logger.error(f"Ошибка при закрытии браузера: {e}")
            finally:
                # Гарантируем, что ссылки на драйвер сбрасываются в любом случае
                self.driver = None
                self.wait = None

class RbcCompaniesParser(BaseSiteParser):
    """Парсер для сайта companies.rbc.ru"""
    
    def __init__(self, rate_limit: float = 2.0):
        super().__init__("companies.rbc.ru", rate_limit)
        self.search_url = "https://companies.rbc.ru/search/"
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта companies.rbc.ru"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                # Формируем URL для поиска по ИНН
                params = {
                    'query': company.inn
                }
                
                async with aiohttp.ClientSession() as session:
                    # Выполняем поисковый запрос
                    async with session.get(self.search_url, params=params, headers=self.headers) as response:
                        if response.status != 200:
                            self.logger.warning(f"Ошибка при поиске компании {company.inn}: статус {response.status}")
                            if attempt < max_attempts - 1 and response.status >= 500:
                                await asyncio.sleep(2)
                                continue
                            return None
                        
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Ищем ссылку на страницу компании
                        company_link = soup.select_one('a.company-name-link')
                        if not company_link:
                            self.logger.warning(f"Компания {company.inn} не найдена на companies.rbc.ru")
                            return None
                        
                        company_href = company_link.get('href')
                        if not company_href:
                            return None
                        
                        company_url = f"https://companies.rbc.ru{company_href}"
                        
                        # Переходим на страницу компании
                        async with session.get(company_url, headers=self.headers) as company_response:
                            if company_response.status != 200:
                                self.logger.warning(f"Ошибка при получении данных компании {company.inn}: статус {company_response.status}")
                                if attempt < max_attempts - 1 and company_response.status >= 500:
                                    await asyncio.sleep(2)
                                    continue
                                return None
                            
                            company_html = await company_response.text()
                            company_soup = BeautifulSoup(company_html, 'html.parser')
                            
                            # Ищем информацию о руководителе
                            director_section = company_soup.select_one('div.company-management')
                            if not director_section:
                                self.logger.warning(f"Информация о руководителе компании {company.inn} не найдена")
                                return None
                            
                            # Извлекаем имя директора
                            director_name_elem = director_section.select_one('span.management-name')
                            if director_name_elem:
                                company.chairman_name = director_name_elem.text.strip()
                            
                            # ИНН директора обычно не представлен на странице companies.rbc.ru
                            
                            return company
            except aiohttp.ClientError as e:
                self.logger.error(f"Ошибка сети при парсинге компании {company.inn} на companies.rbc.ru: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2)
                    continue
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге компании {company.inn} на companies.rbc.ru: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2)
                    continue
        
        return None

class DadataParser(BaseSiteParser):
    """Парсер для получения информации о компаниях через API dadata.ru"""
    
    def __init__(self, token: str, rate_limit: float = 0.2):
        """
        Инициализация клиента Dadata
        
        :param token: API ключ для доступа к сервису dadata.ru
        :param rate_limit: Задержка между запросами (по умолчанию 0.2 секунды, до 10000 запросов в день)
        """
        super().__init__("dadata.ru", rate_limit)
        
        # Инициализация ротаторов ключей
        dadata_keys = self._load_api_keys_from_env('DADATA_TOKEN')
        if token and token not in dadata_keys:
            dadata_keys.insert(0, token)
            
        self.dadata_keys = KeyRotator(dadata_keys, "dadata.ru")
        self.primary_token = token  # Сохраняем первичный токен

        # Настройка Chrome
        self.options = Options()
        # self.options.add_argument('--headless')  # Запуск в фоновом режиме
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--ignore-certificate-errors')
        self.options.add_argument('--ignore-ssl-errors')
        self.options.add_argument('--log-level=3')  # Уменьшаем вывод логов браузера
        
        # Загрузка параметров из .env
        self.page_load_timeout = int(os.getenv('PAGE_LOAD_TIMEOUT_SECONDS', '90'))
        self.element_wait_timeout = int(os.getenv('ELEMENT_WAIT_TIMEOUT_SECONDS', '10'))
        self.autocomplete_wait_seconds = int(os.getenv('AUTOCOMPLETE_WAIT_SECONDS', '5'))
        self.max_key_attempts = int(os.getenv('MAX_KEY_ATTEMPTS', '3'))
        self.raiffeisen_max_retry_attempts = int(os.getenv('RAIFFEISEN_MAX_RETRY_ATTEMPTS', '24'))
        
        self.dadata = None  # Инициализируется в parse_companies
        self.failed_key_attempts = {}  # Словарь для отслеживания неудачных попыток по ключам
        self.force_token = None  # Токен, который будет использоваться принудительно в этом экземпляре
        self.ignore_force_token_temporarily = False  # Флаг для временного игнорирования принудительного токена

        self.driver = None
        self.wait = None
        self.current_retry = 0

    def set_specific_token(self, token: str) -> None:
        """
        Устанавливает конкретный токен для использования в этом экземпляре
        
        :param token: API ключ для использования
        """
        self.force_token = token
        self.ignore_force_token_temporarily = False  # Сбрасываем флаг при установке нового токена
        self.logger.info(f"Установлен принудительный токен для этого экземпляра парсера")
    
    def _temporarily_ignore_force_token(self) -> None:
        """
        Временно игнорирует принудительно установленный токен из-за ошибки
        """
        self.ignore_force_token_temporarily = True
        self.logger.warning(f"Временно игнорируем принудительно установленный токен из-за ошибки")
    
    def _load_api_keys_from_env(self, env_prefix: str) -> List[str]:
        """
        Загружает API ключи из переменных окружения с указанным префиксом
        
        :param env_prefix: Префикс для переменных окружения (например, 'DADATA_TOKEN')
        :return: Список найденных ключей
        """
        keys = []
        # Ищем основной ключ
        main_key = os.getenv(env_prefix)
        if main_key:
            keys.append(main_key)
        
        # Ищем дополнительные ключи с номерами (DADATA_TOKEN_1, DADATA_TOKEN_2, и т.д.)
        i = 1
        while True:
            key = os.getenv(f"{env_prefix}_{i}")
            if not key:
                break
            keys.append(key)
            i += 1
        
        self.logger.info(f"Загружено {len(keys)} ключей с префиксом {env_prefix}")
        return keys
    
    async def _create_dadata_client(self) -> Optional[DadataAsync]:
        """
        Создает новый клиент Dadata с текущим активным ключом
        
        :return: DadataAsync клиент или None в случае ошибки
        """
        # Если установлен принудительный токен и не нужно его игнорировать, используем его
        if self.force_token and not self.ignore_force_token_temporarily:
            token = self.force_token
            self.logger.info(f"Используется принудительно установленный токен")
        else:
            token = self.dadata_keys.get_current_key()
            
        if not token:
            self.logger.error("Нет доступных API ключей Dadata")
            return None
        
        try:
            return DadataAsync(token)
        except Exception as e:
            self.logger.error(f"Ошибка при создании клиента Dadata: {e}")
            return None
            
    async def _rotate_dadata_client(self) -> Optional[DadataAsync]:
        """
        Переключает на следующий API ключ и создает новый клиент
        
        :return: Новый DadataAsync клиент или None в случае ошибки
        """
        # Закрываем текущий клиент, если он существует
        if self.dadata:
            try:
                await self.dadata.close()
            except:
                pass
            self.dadata = None
        
        # Если установлен принудительный токен и не нужно его игнорировать, всегда используем его
        if self.force_token and not self.ignore_force_token_temporarily:
            token = self.force_token
            self.logger.info(f"Продолжаем использовать принудительно установленный токен")
        else:
            # Переключаемся на следующий ключ
            token = self.dadata_keys.rotate_key()
            
        if not token:
            self.logger.error("Нет доступных API ключей Dadata для ротации")
            return None
        
        # Создаем новый клиент
        try:
            return DadataAsync(token)
        except Exception as e:
            self.logger.error(f"Ошибка при создании клиента Dadata с новым ключом: {e}")
            return None
    
    async def parse_companies(self, companies: List[CompanyData]) -> List[CompanyData]:
        """Парсит список компаний через API dadata.ru"""
        results = []
        self.logger.info(f"Начинаем обработку {len(companies)} компаний через API Dadata")
        
        # Сбрасываем счетчик неудачных попыток для ключей
        self.failed_key_attempts = {}
        
        try:
            # Инициализация браузера с учетом операционной системы
            if sys.platform == 'win32':
                # Windows путь
                chromedriver_path = os.path.join(os.getcwd(), 'chromedriver.exe')
                if not os.path.exists(chromedriver_path):
                    chromedriver_path = 'C:/chromedriver/chromedriver.exe'
                    if not os.path.exists(chromedriver_path):
                        self.logger.error(f"ChromeDriver не найден по указанным путям Windows")
                        return []
            else:
                # Linux/Mac путь
                chromedriver_path = "./chromedriver"
                if not os.path.exists(chromedriver_path):
                    self.logger.error(f"ChromeDriver не найден по пути {chromedriver_path}")
                    return []
            
            self.logger.info(f"Используется ChromeDriver по пути: {chromedriver_path}")
            
            # Создаем сервис и драйвер
            service = Service(executable_path=chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=self.options)
            self.driver.set_page_load_timeout(self.page_load_timeout)  # Таймаут загрузки страницы из конфигурации
            self.wait = WebDriverWait(self.driver, self.element_wait_timeout)  # Таймаут для ожидания элементов из конфигурации
            
            # Получаем ссылку на data_manager для обновления результатов
            data_manager = self._get_data_manager()
            
            # Обработка всех компаний
            for i, company in enumerate(companies):
                try:
                    # Проверяем на прерывание программы перед каждой компанией
                    try:
                        # Используем asyncio.sleep с очень маленьким таймаутом для проверки прерываний
                        await asyncio.sleep(0.01)
                    except asyncio.CancelledError:
                        self.logger.info("Обнаружено прерывание, останавливаем парсинг")
                        break
                    
                    self.logger.info(f"[{i+1}/{len(companies)}] Обработка компании: {company.name} (ИНН: {company.inn})")
                    
                    # Соблюдаем задержку между запросами
                    await asyncio.sleep(self.rate_limit)
                    
                    # Парсим информацию о компании
                    result = await self.parse_company(company)
                    if result:
                        result.source = self.site_name
                        results.append(result)
                        self.logger.info(f"Успешно получены данные для {company.name}")
                        
                        # Обновляем результаты в data_manager если он доступен
                        if data_manager:
                            data_manager.update_results(result)
                    else:
                        self.logger.warning(f"Не удалось получить данные для {company.name}")
                        
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке компании {company.name}: {e}")
            
            self.logger.info(f"Завершена обработка компаний через API Dadata, успешно: {len(results)} из {len(companies)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при инициализации API Dadata: {e}")
        finally:
            # Закрываем клиент Dadata
            if self.dadata:
                try:
                    await self.dadata.close()
                except:
                    pass
            self.dadata = None
            # Закрываем браузер только один раз после обработки всех компаний
            if self.driver:
                try:
                    self.driver.quit()
                except Exception as e:
                    self.logger.error(f"Ошибка при закрытии браузера: {e}")
                self.driver = None
                self.wait = None
        
        return results
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """
        Получает информацию о руководителе компании через API dadata.ru и его ИНН через сайт Райффайзен банка
        
        :param company: Объект с данными о компании
        :return: Обновленный объект с данными о компании или None в случае ошибки
        """
        # Максимальное количество попыток с разными ключами
        max_attempts = max(1, self.dadata_keys.get_all_keys_count())
        
        for attempt in range(max_attempts):
            # Создаем клиент Dadata, если он еще не создан
            if not self.dadata:
                self.dadata = await self._create_dadata_client()
                if not self.dadata:
                    self.logger.error("Не удалось создать клиент Dadata")
                    return None
            
            try:
                # Поиск компании по ИНН
                organizations = await self.dadata.find_by_id(name="party", query=company.inn)
                
                if not organizations:
                    self.logger.warning(f"Компания {company.name} с ИНН {company.inn} не найдена в dadata.ru")
                    # Возвращаем данные с отметкой "не найдено"
                    company.chairman_name = "не найдено"
                    company.chairman_inn = "не найдено"
                    return company
                
                # Берем первую найденную организацию (обычно самую релевантную)
                org_data = organizations[0]['data']
                
                # Проверяем наличие данных о руководителе
                if org_data.get('management') and org_data['management'].get('name'):
                    # Извлекаем имя руководителя
                    chairman_name = org_data['management']['name']
                    company.chairman_name = chairman_name
                    self.logger.info(f"Найден руководитель: {chairman_name}")
                    
                    # Попытка найти ИНН в данных от Dadata, если он там есть
                    chairman_inn = None
                    
                    if org_data.get('managers') and len(org_data['managers']) > 0:
                        for manager in org_data['managers']:
                            if manager.get('post') and ('председатель' in manager['post'].lower() or 'директор' in manager['post'].lower() or 'руководитель' in manager['post'].lower()):
                                if manager.get('inn'):
                                    chairman_inn = manager['inn']
                                    self.logger.info(f"Найден ИНН руководителя в Dadata: {chairman_inn}")
                                    break
                    
                    # Если ИНН не найден в Dadata, пытаемся получить его через сайт Райффайзен банка
                    if not chairman_inn:
                        try:
                            address = org_data.get('address').get("value")
                            cur_city = address.split(",")[0]
                        except Exception:
                            pass
                            
                        try:
                            chairman_inn = await self._get_chairman_inn_via_raiffeisen(chairman_name)
                            
                            if chairman_inn:
                                self.logger.info(f"Получен ИНН руководителя через сайт Райффайзен: {chairman_inn}")
                            else:
                                self.logger.warning(f"Не удалось получить ИНН руководителя {chairman_name} через сайт Райффайзен")
                                chairman_inn = "не найдено"
                        except Exception as raiffeisen_error:
                            self.logger.error(f"Ошибка при получении ИНН через Райффайзен: {raiffeisen_error}")
                            # Гарантируем закрытие браузера при ошибке
                            self._ensure_browser_closed()
                            chairman_inn = "не найдено"
                    
                    company.chairman_inn = chairman_inn
                else:
                    # Информация о руководителе не найдена
                    company.chairman_name = "не найдено"
                    company.chairman_inn = "не найдено"
                    self.logger.warning(f"Данные о руководителе компании {company.name} не найдены в dadata.ru")
                
                # Сбрасываем счетчик неудачных попыток для текущего ключа, так как запрос успешен
                current_key = self.dadata_keys.get_current_key()
                if current_key in self.failed_key_attempts:
                    del self.failed_key_attempts[current_key]
                
                # Сбрасываем флаг игнорирования принудительного токена, так как запрос успешен
                self.ignore_force_token_temporarily = False
                
                return company
                
            except Exception as e:
                import httpx
                
                # Проверяем тип ошибки и обрабатываем его
                if isinstance(e, httpx.HTTPStatusError):
                    current_key = self.dadata_keys.get_current_key()
                    
                    # Проверяем, является ли ошибка ошибкой авторизации (403 Forbidden)
                    if e.response.status_code == 403:
                        self.logger.error(
                            f"Ошибка при получении данных из API dadata.ru: ошибка авторизации (403 Forbidden). "
                            f"Проверьте правильность API-ключа. Получите действительный токен на сайте https://dadata.ru/profile/#info"
                        )
                        
                        # Отмечаем этот ключ как неудачный и пробуем следующий
                        self.failed_key_attempts[current_key] = self.failed_key_attempts.get(current_key, 0) + 1
                        
                        # Если этот ключ уже несколько раз подряд не работал, переходим к следующему
                        if self.failed_key_attempts.get(current_key, 0) >= self.max_key_attempts:
                            self.logger.warning(f"Ключ многократно вызывал ошибку авторизации, пробуем другой ключ")
                            
                            # Если был установлен принудительный токен и он не работает, временно игнорируем его
                            if self.force_token and current_key == self.force_token:
                                self._temporarily_ignore_force_token()
                            
                            # Закрываем текущий клиент и пробуем создать новый с другим ключом
                            if self.dadata:
                                try:
                                    await self.dadata.close()
                                except:
                                    pass
                            
                            self.dadata = await self._rotate_dadata_client()
                    
                    # Если ошибка связана с превышением лимита запросов (429 Too Many Requests)
                    elif e.response.status_code == 429:
                        self.logger.warning(f"Превышен лимит запросов для API-ключа Dadata, переключаемся на другой ключ")
                        
                        # Если был установлен принудительный токен и он превысил лимит, временно игнорируем его
                        if self.force_token and current_key == self.force_token:
                            self._temporarily_ignore_force_token()
                        
                        # Закрываем текущий клиент и пробуем создать новый с другим ключом
                        if self.dadata:
                            try:
                                await self.dadata.close()
                            except:
                                pass
                        
                        self.dadata = await self._rotate_dadata_client()
                    else:
                        self.logger.error(f"Ошибка HTTP при получении данных из API dadata.ru: {e}")
                else:
                    self.logger.error(f"Ошибка при получении данных из API dadata.ru: {e}")
                
                # Если мы перепробовали все ключи и попытки, возвращаем None
                if attempt >= max_attempts - 1:
                    self.logger.error(f"Исчерпаны все попытки получения данных о компании {company.name}")
                    # Убедимся, что браузер закрыт в случае ошибки
                    self._ensure_browser_closed()
                    return None
        
        # Убедимся, что браузер закрыт перед возвратом из функции
        self._ensure_browser_closed()
        return None
    
    async def _get_chairman_inn_via_raiffeisen(self, full_name: str) -> Optional[str]:
        """
        Получает ИНН физического лица по ФИО через сайт Райффайзен банка
        
        :param full_name: Полное имя руководителя (ФИО)
        :return: ИНН руководителя или None, если не удалось получить
        """
        self.logger.info(f"Поиск ИНН для {full_name} через сайт Райффайзен банка")
        
        # Максимальное число попыток восстановления после бана
        max_retry_attempts = self.raiffeisen_max_retry_attempts
        current_retry = 0
        
        while current_retry < max_retry_attempts:
            try:
                # Проверяем глобальный флаг блокировки перед каждой попыткой
                if is_raiffeisen_blocked():
                    self.logger.warning(f"Сайт Райфайзен банка заблокирован. Ожидаем перед повторной попыткой для {full_name}")
                    # Ожидаем снятия блокировки
                    await asyncio.sleep(RAIFFEISEN_SECONDARY_WAIT_SECONDS)
                    # Проверяем снова
                    if is_raiffeisen_blocked():
                        self.logger.warning(f"Блокировка все еще активна. Попытка {current_retry}/{max_retry_attempts}")
                        continue
                
                # Загрузка страницы
                self.logger.info(f"Открываем сайт reg-raiffeisen.ru (Поиск для {full_name})")
                self.driver.get("https://reg-raiffeisen.ru/")

                # Если мы здесь, значит страница успешно загрузилась
                if current_retry > 0:
                    self.logger.info(f"Сайт снова доступен после {current_retry} попыток!")
                
                # Прокручиваем страницу вниз
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # Находим поле ввода для поиска ИП или ООО
                try:
                    # Попытка найти поле ввода по XPath
                    xpath = "/html/body/div[1]/div[3]/div/div[2]/div[14]/div[2]/div/div/div/div/form/div[1]/div[1]/div/div/div[1]/div/div/div/div[1]/div[1]/input"
                    input_field = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                except Exception:
                    # Если XPath не сработал, пробуем найти по ID или другим атрибутам
                    self.logger.info("Поиск поля ввода по ID или атрибутам")
                    try:
                        input_field = self.wait.until(EC.presence_of_element_located((By.ID, "party")))
                    except Exception:
                        # Последняя попытка - найти по названию
                        input_field = self.wait.until(EC.presence_of_element_located((By.NAME, "party")))
                
                # Прокручиваем страницу к полю ввода
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", input_field)
                
                # Делаем паузу перед вводом
                await asyncio.sleep(2)
                
                # Очищаем поле ввода и вводим ФИО
                input_field.clear()
                input_field.send_keys(full_name)
                
                # Ждем появления выпадающего списка с подсказками
                self.logger.info(f"Ожидаем результаты автоподсказки для {full_name}")
                autocomplete_list = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "autocomplete-list")))
                
                # Ждем для полной загрузки списка
                await asyncio.sleep(self.autocomplete_wait_seconds)
                
                # Находим все элементы списка
                list_items = autocomplete_list.find_elements(By.TAG_NAME, "li")
                
                if not list_items:
                    self.logger.warning(f"Автоподсказки для {full_name} не найдены")
                    return None
                
                # Сначала пытаемся найти ИНН физического лица (12 цифр)
                found_physical_inn = None
                found_legal_inn = None
                
                # Первый проход - ищем только ИНН физических лиц (12 цифр)
                for item in list_items:
                    try:
                        # Находим элемент с деталями (содержит ИНН)
                        detail_element = item.find_element(By.CLASS_NAME, "ie_detail")
                        detail_text = detail_element.text
                        
                        # Ищем ИНН физического лица (12 цифр)
                        inn_match = re.search(r'(\d{12})', detail_text)
                        
                        if inn_match:
                            found_physical_inn = inn_match.group(1)
                            self.logger.info(f"Извлечен ИНН физического лица {found_physical_inn} для {full_name}")
                            return found_physical_inn
                    except Exception as item_e:
                        self.logger.debug(f"Ошибка при обработке элемента списка: {item_e}")
                        continue
                
                # Второй проход - ищем ИНН юридических лиц (10 цифр), если не нашли физлица
                for item in list_items:
                    try:
                        detail_element = item.find_element(By.CLASS_NAME, "ie_detail")
                        detail_text = detail_element.text
                        
                        # Ищем ИНН юридического лица (10 цифр)
                        inn_match = re.search(r'(\d{10})', detail_text)
                        
                        if inn_match:
                            found_legal_inn = inn_match.group(1)
                            self.logger.info(f"Извлечен ИНН юридического лица {found_legal_inn} для {full_name}")
                            return f"{found_legal_inn} (возможно юрлица)"
                    except Exception:
                        continue
                
                self.logger.warning(f"Не удалось извлечь ИНН из результатов поиска для {full_name}")
                return None
                
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге сайта Райффайзен банка")
                
                # Устанавливаем глобальный флаг блокировки Райфайзен банка
                set_raiffeisen_blocked()
                self.logger.warning(f"Установлена глобальная блокировка Райфайзен банка. Ожидаем...")
                
                # Закрываем и пересоздаем драйвер перед уходом в сон
                self._ensure_browser_closed()
                
                # Увеличиваем счетчик попыток
                current_retry += 1
                if current_retry >= max_retry_attempts:
                    self.logger.error(f"Превышено максимальное количество попыток ({max_retry_attempts}) для {full_name}")
                    return None
                
                # Пересоздаем драйвер после ошибки
                try:
                    # Создаем сервис и драйвер
                    if sys.platform == 'win32':
                        # Windows путь
                        chromedriver_path = os.path.join(os.getcwd(), 'chromedriver.exe')
                        if not os.path.exists(chromedriver_path):
                            chromedriver_path = 'C:/chromedriver/chromedriver.exe'
                    else:
                        # Linux/Mac путь
                        chromedriver_path = "./chromedriver"
                    
                    self.logger.info(f"Пересоздание драйвера, используется ChromeDriver по пути: {chromedriver_path}")
                    service = Service(executable_path=chromedriver_path)
                    self.driver = webdriver.Chrome(service=service, options=self.options)
                    self.driver.set_page_load_timeout(self.page_load_timeout)
                    self.wait = WebDriverWait(self.driver, self.element_wait_timeout)
                except Exception as browser_error:
                    self.logger.error(f"Не удалось создать браузер после ошибки: {browser_error}")
                    return None
                
                # Продолжаем попытку с той же компанией
                continue
            
        
        # Если исчерпали все попытки
        self.logger.error(f"Превышено максимальное количество попыток ({max_retry_attempts}) для {full_name}")
        return None
    
    def _get_data_manager(self) -> Optional[DataManager]:
        """
        Получает объект DataManager из текущего экземпляра BaseSiteParser
        через поиск в родительских объектах
        
        :return: Экземпляр DataManager или None
        """
        try:
            # Получаем доступ к родительскому объекту ParserManager
            frame = sys._getframe(2)
            while frame:
                if 'self' in frame.f_locals:
                    parser_manager = frame.f_locals['self']
                    if hasattr(parser_manager, 'data_manager'):
                        return parser_manager.data_manager
                frame = frame.f_back
        except Exception as e:
            self.logger.debug(f"Не удалось получить доступ к data_manager: {e}")
        return None 

    def _ensure_browser_closed(self) -> None:
        """
        Надежное закрытие браузера
        """
        if self.driver:
            try:
                self.logger.info("Закрываем браузер...")
                # Закрываем браузер стандартным способом
                self.driver.quit()
                self.logger.info("Браузер успешно закрыт")
            except Exception as e:
                self.logger.error(f"Ошибка при закрытии браузера: {e}")
            finally:
                # Гарантируем, что ссылки на драйвер сбрасываются в любом случае
                self.driver = None
                self.wait = None