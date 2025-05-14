import logging
import random
import asyncio
import time
import requests
from typing import List, Dict, Optional, Any
from fp.fp import FreeProxy
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

# Настройка логирования
logger = logging.getLogger("TIN_Parser.ProxyManager")

class ProxyManager:
    """Менеджер для работы с бесплатными прокси, использующий библиотеку free-proxy"""
    
    def __init__(self, check_url: str = "https://www.google.com", timeout: int = 10):
        """
        Инициализация менеджера прокси
        
        Args:
            check_url: URL для проверки работоспособности прокси
            timeout: Таймаут для проверки прокси (секунды)
        """
        self.check_url = check_url
        self.timeout = timeout
        self.working_proxies = []
        self.proxies_cache = {}  # Кеш для хранения прокси с временными метками
        self.proxy_cache_time = 300  # Время жизни прокси в кеше (секунды)
        self.countries = ['US', 'CA', 'BR', 'RU', 'IN', 'DE', 'FR', 'NL', 'GB']  # Страны для поиска прокси
        
        # Настройки для Chrome
        self.chrome_options = Options()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--ignore-certificate-errors')
        self.chrome_options.add_argument('--ignore-ssl-errors')
    
    async def get_proxy(self, country: str = None, anonym: bool = False) -> Optional[Dict[str, Any]]:
        """
        Получает прокси из библиотеки free-proxy
        
        Args:
            country: ISO код страны (опционально)
            anonym: Флаг для запроса только анонимных прокси
            
        Returns:
            Optional[Dict[str, Any]]: Словарь с информацией о прокси или None
        """
        try:
            # Если есть рабочие прокси в кеше, возвращаем один из них
            if self.proxies_cache:
                # Очищаем устаревшие прокси
                self._clean_cached_proxies()
                
                if self.proxies_cache:
                    # Выбираем случайный прокси из кеша
                    proxy_key = random.choice(list(self.proxies_cache.keys()))
                    proxy_info = self.proxies_cache[proxy_key]['proxy']
                    logger.info(f"Использую кешированный прокси: {proxy_info['ip']}:{proxy_info['port']}")
                    return proxy_info
            
            # Пытаемся получить новый прокси
            country_list = [country] if country else None
            
            # Этот вызов блокирующий, поэтому используем asyncio.to_thread
            proxy_url = await asyncio.to_thread(
                lambda: FreeProxy(
                    country_id=country_list, 
                    anonym=anonym,
                    https=True,
                    timeout=self.timeout,
                    rand=True
                ).get(repeat=True)
            )
            
            if not proxy_url:
                logger.warning("Не удалось получить прокси")
                return None
                
            # Парсим URL прокси
            try:
                parts = proxy_url.strip().split('://')
                if len(parts) != 2:
                    logger.warning(f"Некорректный формат прокси URL: {proxy_url}")
                    return None
                    
                protocol = parts[0]
                ip_port = parts[1].split(':')
                
                if len(ip_port) != 2:
                    logger.warning(f"Некорректный формат IP:PORT в прокси URL: {proxy_url}")
                    return None
                    
                ip = ip_port[0]
                port = ip_port[1]
                
                proxy_info = {
                    'ip': ip,
                    'port': port,
                    'protocol': protocol,
                    'country': country,
                    'url': proxy_url
                }
                
                # Проверяем работоспособность прокси
                if await self._check_proxy(proxy_info):
                    # Добавляем в кеш работающих прокси
                    cache_key = f"{ip}:{port}"
                    self.proxies_cache[cache_key] = {
                        'timestamp': time.time(),
                        'proxy': proxy_info
                    }
                    
                    logger.info(f"Получен и проверен рабочий прокси: {proxy_url}")
                    return proxy_info
                else:
                    logger.warning(f"Полученный прокси не работает: {proxy_url}")
                    return None
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке прокси URL {proxy_url}: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при получении прокси: {e}")
            return None
    
    async def _check_proxy(self, proxy: Dict[str, Any]) -> bool:
        """
        Проверяет работоспособность прокси
        
        Args:
            proxy: Информация о прокси
            
        Returns:
            bool: True если прокси работает, False иначе
        """
        proxy_url = proxy.get('url')
        
        if not proxy_url:
            protocol = proxy.get('protocol', 'http')
            ip = proxy.get('ip')
            port = proxy.get('port')
            
            if not ip or not port:
                return False
                
            proxy_url = f"{protocol}://{ip}:{port}"
        
        try:
            # Используем requests для проверки (блокирующий вызов)
            proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            
            response = await asyncio.to_thread(
                lambda: requests.get(
                    self.check_url,
                    proxies=proxies,
                    timeout=self.timeout,
                    verify=False  # Отключаем проверку SSL
                )
            )
            
            # Если получили успешный ответ
            return response.status_code == 200
            
        except Exception as e:
            logger.debug(f"Прокси {proxy_url} не работает: {e}")
            return False
    
    def _clean_cached_proxies(self) -> None:
        """Очищает устаревшие прокси из кеша"""
        current_time = time.time()
        
        # Создаем список ключей для удаления
        keys_to_delete = []
        
        for key, data in self.proxies_cache.items():
            if current_time - data['timestamp'] > self.proxy_cache_time:
                keys_to_delete.append(key)
        
        # Удаляем устаревшие прокси
        for key in keys_to_delete:
            del self.proxies_cache[key]
    
    def apply_proxy_to_selenium(self, options: Options, proxy: Dict[str, Any]) -> Options:
        """
        Применяет прокси к настройкам Selenium WebDriver
        
        Args:
            options: Настройки Selenium WebDriver
            proxy: Информация о прокси
            
        Returns:
            Options: Обновленные настройки Selenium WebDriver
        """
        ip = proxy.get('ip')
        port = proxy.get('port')
        
        if not ip or not port:
            logger.warning("Невозможно применить прокси - отсутствует IP или порт")
            return options
            
        proxy_string = f"{ip}:{port}"
        
        # Добавляем аргумент для прокси
        options.add_argument(f'--proxy-server={proxy_string}')
        
        # Добавляем аргументы для обхода обнаружения Selenium
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        
        logger.info(f"Применен прокси {proxy_string} к WebDriver")
        
        return options
    
    async def get_selenium_with_proxy(self) -> tuple[Optional[webdriver.Chrome], Optional[Dict[str, Any]]]:
        """
        Создает WebDriver с настроенным прокси
        
        Returns:
            tuple[Optional[webdriver.Chrome], Optional[Dict[str, Any]]]: WebDriver и информация о прокси
        """
        # Получаем рабочий прокси
        proxy = await self.get_proxy()
        
        # Создаем копию базовых опций
        options = Options()
        for argument in self.chrome_options.arguments:
            options.add_argument(argument)
        
        if not proxy:
            logger.warning("Не удалось найти рабочий прокси, используем прямое соединение")
            try:
                driver = webdriver.Chrome(options=options)
                return driver, None
            except WebDriverException as e:
                logger.error(f"Ошибка при создании WebDriver без прокси: {e}")
                return None, None
        
        # Применяем прокси к настройкам
        options = self.apply_proxy_to_selenium(options, proxy)
        
        try:
            # Создаем драйвер с прокси
            driver = webdriver.Chrome(options=options)
            return driver, proxy
        except WebDriverException as e:
            logger.error(f"Ошибка при создании WebDriver с прокси {proxy.get('ip')}:{proxy.get('port')}: {e}")
            # Если не удалось создать драйвер с прокси, пробуем без прокси
            try:
                options = Options()
                for argument in self.chrome_options.arguments:
                    options.add_argument(argument)
                driver = webdriver.Chrome(options=options)
                return driver, None
            except WebDriverException as e2:
                logger.error(f"Ошибка при создании WebDriver без прокси: {e2}")
                return None, None
