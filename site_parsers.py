import aiohttp
import asyncio
import logging
from bs4 import BeautifulSoup
import re
from typing import Optional, Dict, Any, List
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from parser_base import BaseSiteParser, CompanyData

class FocusKonturParser(BaseSiteParser):
    """Парсер для сайта focus.kontur.ru"""
    
    def __init__(self, rate_limit: float = 2.0):
        super().__init__("focus.kontur.ru", rate_limit)
        self.search_url = "https://focus.kontur.ru/search?country=RU"
        
        # Настройка Chrome
        self.options = Options()
        self.options.add_argument('--headless')  # Запуск в фоновом режиме
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        
        # CSS селекторы (предпочтительнее XPath)
        self.search_input_selector = "input[placeholder='ИНН, ОГРН, наименование или адрес организации']"
        self.search_button_selector = "button.search-icon"
        
        # JavaScript для получения данных о руководителе
        self.js_get_director_name = """
            return document.querySelector('.person-link span').textContent.trim();
        """
        self.js_get_director_inn = """
            const directorInfo = document.querySelector('.person-info-hidden');
            if (directorInfo) {
                const innMatch = directorInfo.textContent.match(/ИНН: (\\d+)/);
                return innMatch ? innMatch[1] : null;
            }
            return null;
        """
        
        # XPath в качестве запасного варианта
        self.director_block_xpath = "/html/body/div[2]/div[2]/div/main/div/div[2]/div/div[1]/div[1]/div[2]/div/div/div[1]/div/div/div/div[8]/div/div/div/div"
        self.director_name_xpath = "/html/body/div[2]/div[2]/div/main/div/div[2]/div/div[1]/div[1]/div[2]/div/div/div[1]/div/div/div/div[8]/div/div/div/div/div/span/span/a/span[1]/span/span"
        self.director_inn_xpath = "/html/body/div[2]/div[2]/div/main/div/div[2]/div/div[1]/div[1]/div[2]/div/div/div[1]/div/div/div/div[8]/div/div/div/div/div/div/span[2]/span/span/span"
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта focus.kontur.ru"""
        driver = None
        try:
            # Проверяем существование ChromeDriver
            chromedriver_path = './chromedriver'
            if not os.path.exists(chromedriver_path):
                self.logger.error("ChromeDriver не найден. Пожалуйста, поместите chromedriver в корень проекта.")
                return None
            
            # Инициализируем драйвер с использованием Service (актуальный способ для Selenium 4)
            service = Service(executable_path=chromedriver_path)
            driver = webdriver.Chrome(service=service, options=self.options)
            
            # Устанавливаем таймаут для ожидания загрузки элементов
            wait = WebDriverWait(driver, 10)
            
            # Открываем страницу поиска
            self.logger.info(f"Открываем страницу {self.search_url} для поиска компании {company.inn}")
            driver.get(self.search_url)
            
            # Ждем загрузки поля поиска (используем CSS селектор)
            search_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, self.search_input_selector)))
            
            # Вводим ИНН в поле поиска
            search_input.clear()
            search_input.send_keys(company.inn)
            
            # Нажимаем кнопку поиска или Enter
            try:
                search_button = driver.find_element(By.CSS_SELECTOR, self.search_button_selector)
                search_button.click()
            except NoSuchElementException:
                search_input.send_keys(Keys.RETURN)
            
            # Ждем загрузки результатов поиска
            # Сначала проверяем, есть ли результаты поиска
            try:
                # Ждем появления страницы компании (макс. 10 секунд)
                wait.until(lambda d: 'entity' in d.current_url or 'ничего не найдено' in d.page_source.lower())
                
                # Если мы на странице "ничего не найдено"
                if 'ничего не найдено' in driver.page_source.lower():
                    self.logger.warning(f"Компания {company.inn} не найдена на focus.kontur.ru")
                    return None
                
                # Ждем загрузки блока с информацией о директоре
                wait.until(EC.presence_of_element_located((By.XPATH, self.director_block_xpath)))
                
                # Пытаемся получить данные о директоре через JavaScript (более эффективный способ)
                try:
                    # Получаем имя директора
                    chairman_name = driver.execute_script(self.js_get_director_name)
                    if chairman_name:
                        company.chairman_name = chairman_name
                        self.logger.info(f"Получено имя директора: {company.chairman_name}")
                    
                    # Получаем ИНН директора
                    chairman_inn = driver.execute_script(self.js_get_director_inn)
                    if chairman_inn:
                        company.chairman_inn = chairman_inn
                        self.logger.info(f"Получен ИНН директора: {company.chairman_inn}")
                    
                except Exception as js_error:
                    self.logger.warning(f"Не удалось получить данные через JavaScript: {js_error}")
                    # Запасной вариант: использовать XPath
                    try:
                        director_name_elem = driver.find_element(By.XPATH, self.director_name_xpath)
                        company.chairman_name = director_name_elem.text.strip()
                        self.logger.info(f"Получено имя директора (XPath): {company.chairman_name}")
                    except NoSuchElementException:
                        self.logger.warning(f"Не удалось найти имя директора для компании {company.inn}")
                    
                    try:
                        director_inn_elem = driver.find_element(By.XPATH, self.director_inn_xpath)
                        company.chairman_inn = director_inn_elem.text.strip()
                        self.logger.info(f"Получен ИНН директора (XPath): {company.chairman_inn}")
                    except NoSuchElementException:
                        self.logger.warning(f"Не удалось найти ИНН директора для компании {company.inn}")
                
                return company
                
            except TimeoutException:
                self.logger.warning(f"Данные о директоре для компании {company.inn} не найдены или не загрузились вовремя")
                return None
                
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге компании {company.inn} на focus.kontur.ru: {e}")
            return None
        finally:
            # Закрываем браузер
            if driver:
                driver.quit()

class CheckoParser(BaseSiteParser):
    """Парсер для сайта checko.ru"""
    
    def __init__(self, rate_limit: float = 2.0):
        super().__init__("checko.ru", rate_limit)
        self.search_url = "https://checko.ru/search"
        self.company_url = "https://checko.ru/company"
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта checko.ru"""
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
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге компании {company.inn} на checko.ru: {e}")
            return None

class ZaChestnyiBiznesParser(BaseSiteParser):
    """Парсер для сайта zachestnyibiznes.ru"""
    
    def __init__(self, rate_limit: float = 3.0):
        super().__init__("zachestnyibiznes.ru", rate_limit)
        self.search_url = "https://zachestnyibiznes.ru/search"
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта zachestnyibiznes.ru"""
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
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге компании {company.inn} на zachestnyibiznes.ru: {e}")
            return None

class AuditItParser(BaseSiteParser):
    """Парсер для сайта audit-it.ru"""
    
    def __init__(self, rate_limit: float = 2.0):
        super().__init__("audit-it.ru", rate_limit)
        self.search_url = "https://www.audit-it.ru/contragent"
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта audit-it.ru"""
        try:
            # Формируем URL для прямого доступа к странице компании по ИНН
            company_url = f"{self.search_url}/search.php?inn={company.inn}"
            
            async with aiohttp.ClientSession() as session:
                # Выполняем запрос
                async with session.get(company_url, headers=self.headers) as response:
                    if response.status != 200:
                        self.logger.warning(f"Ошибка при поиске компании {company.inn}: статус {response.status}")
                        return None
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Ищем ссылку на детальную страницу компании
                    company_link = soup.select_one('a.company-name')
                    if not company_link:
                        self.logger.warning(f"Компания {company.inn} не найдена на audit-it.ru")
                        return None
                    
                    company_href = company_link.get('href')
                    if not company_href:
                        return None
                    
                    detail_url = f"https://www.audit-it.ru{company_href}"
                    
                    # Переходим на детальную страницу компании
                    async with session.get(detail_url, headers=self.headers) as detail_response:
                        if detail_response.status != 200:
                            self.logger.warning(f"Ошибка при получении данных компании {company.inn}: статус {detail_response.status}")
                            return None
                        
                        detail_html = await detail_response.text()
                        detail_soup = BeautifulSoup(detail_html, 'html.parser')
                        
                        # Ищем информацию о руководителе
                        director_block = detail_soup.select_one('div.director-info')
                        if not director_block:
                            self.logger.warning(f"Информация о руководителе компании {company.inn} не найдена")
                            return None
                        
                        # Извлекаем имя директора
                        director_name_elem = director_block.select_one('div.director-name')
                        if director_name_elem:
                            company.chairman_name = director_name_elem.text.strip()
                        
                        # Ищем ИНН директора
                        director_info = director_block.select_one('div.director-details')
                        if director_info:
                            inn_match = re.search(r'ИНН: (\d+)', director_info.text)
                            if inn_match:
                                company.chairman_inn = inn_match.group(1)
                        
                        return company
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге компании {company.inn} на audit-it.ru: {e}")
            return None

class RbcCompaniesParser(BaseSiteParser):
    """Парсер для сайта companies.rbc.ru"""
    
    def __init__(self, rate_limit: float = 2.0):
        super().__init__("companies.rbc.ru", rate_limit)
        self.search_url = "https://companies.rbc.ru/search/"
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта companies.rbc.ru"""
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
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге компании {company.inn} на companies.rbc.ru: {e}")
            return None 