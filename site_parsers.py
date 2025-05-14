import aiohttp
import asyncio
import logging
from bs4 import BeautifulSoup
import re
from typing import Optional, Dict, Any, List
import os
import signal
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from parser_base import BaseSiteParser, CompanyData, DataManager
import undetected_chromedriver as uc

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
                            if self.driver:
                                try:
                                    self.driver.quit()
                                except Exception:
                                    pass
                                    
                            # Пересоздаем драйвер
                            self.driver = webdriver.Chrome(service=service, options=self.options)
                            self.driver.set_page_load_timeout(self.page_load_timeout)
                            self.wait = WebDriverWait(self.driver, self.wait_timeout)
                            
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке компании {company.name}: {e}")
            
            self.logger.info(f"Завершена обработка компаний, успешно: {len(results)} из {len(companies)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при инициализации браузера: {e}")
        finally:
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
        # self.options = Options()
        # # self.options.add_argument('--headless')  # Запуск в фоновом режиме
        # self.options.add_argument('--no-sandbox')
        # self.options.add_argument('--disable-dev-shm-usage')
        # # self.options.add_argument('--disable-gpu')
        # self.options.add_argument('--ignore-certificate-errors')
        # self.options.add_argument('--ignore-ssl-errors')
        # self.options.add_argument('--log-level=3')  # Уменьшаем вывод логов браузера

        self.options = uc.ChromeOptions()
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
            self.driver = uc.Chrome(service=service, options=self.options)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            self.wait = WebDriverWait(self.driver, self.wait_timeout)
            
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