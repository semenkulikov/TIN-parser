import aiohttp
import asyncio
import logging
from bs4 import BeautifulSoup
import re
from typing import Optional, Dict, Any, List
from parser_base import BaseSiteParser, CompanyData

class FocusKonturParser(BaseSiteParser):
    """Парсер для сайта focus.kontur.ru"""
    
    def __init__(self, rate_limit: float = 2.0):
        super().__init__("focus.kontur.ru", rate_limit)
        self.search_url = "https://focus.kontur.ru/search"
        self.entity_url = "https://focus.kontur.ru/entity"
    
    async def parse_company(self, company: CompanyData) -> Optional[CompanyData]:
        """Парсит информацию о председателе компании с сайта focus.kontur.ru"""
        try:
            # Формируем URL для поиска по ИНН
            params = {
                'query': company.inn,
                'country': 'RU'
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
                    company_link = soup.select_one('a.company-link')
                    if not company_link:
                        self.logger.warning(f"Компания {company.inn} не найдена на focus.kontur.ru")
                        return None
                    
                    company_href = company_link.get('href')
                    if not company_href:
                        return None
                    
                    # Извлекаем ID компании из URL
                    company_id = company_href.split('/')[-1]
                    company_url = f"{self.entity_url}/{company_id}"
                    
                    # Переходим на страницу компании
                    async with session.get(company_url, headers=self.headers) as company_response:
                        if company_response.status != 200:
                            self.logger.warning(f"Ошибка при получении данных компании {company.inn}: статус {company_response.status}")
                            return None
                        
                        company_html = await company_response.text()
                        company_soup = BeautifulSoup(company_html, 'html.parser')
                        
                        # Ищем информацию о руководителе
                        director_block = company_soup.select_one('div.top-persons')
                        if not director_block:
                            self.logger.warning(f"Информация о руководителе компании {company.inn} не найдена")
                            return None
                        
                        # Извлекаем имя директора
                        director_name_elem = director_block.select_one('span.person-link')
                        if director_name_elem:
                            company.chairman_name = director_name_elem.text.strip()
                        
                        # Ищем ИНН директора
                        director_info = director_block.select_one('div.person-info-hidden')
                        if director_info:
                            inn_match = re.search(r'ИНН: (\d+)', director_info.text)
                            if inn_match:
                                company.chairman_inn = inn_match.group(1)
                        
                        return company
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге компании {company.inn} на focus.kontur.ru: {e}")
            return None

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