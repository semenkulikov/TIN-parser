# TIN Parser

Парсер для получения данных о председателях компаний по ИНН и названию.

## Настройка конфигурации через .env файл

Для корректной работы парсера создайте файл `.env` в корневой директории проекта. Пример конфигурации:

```env
# Таймауты браузера (в секундах)
PAGE_LOAD_TIMEOUT_SECONDS=90
ELEMENT_WAIT_TIMEOUT_SECONDS=10
AUTOCOMPLETE_WAIT_SECONDS=5

# Конфигурация попыток API ключей
MAX_KEY_ATTEMPTS=3

# Конфигурация для Райфайзен банка
RAIFFEISEN_BLOCK_TIME_SECONDS=3600     # Время блокировки после ошибки (1 час)
RAIFFEISEN_SECONDARY_WAIT_SECONDS=600  # Дополнительное время ожидания (10 минут)  
RAIFFEISEN_MAX_RETRY_ATTEMPTS=24       # Максимальное количество попыток

# API ключи Dadata (заполните своими значениями)
DADATA_TOKEN_1=your_dadata_token_1_here
DADATA_TOKEN_2=your_dadata_token_2_here
DADATA_TOKEN_3=your_dadata_token_3_here
DADATA_SECRET_KEY=your_dadata_secret_key_here

# API ключи Checko (заполните своими значениями)
CHECKO_TOKEN=your_checko_token_here
CHECKO_TOKEN_1=your_checko_token_1_here
CHECKO_TOKEN_2=your_checko_token_2_here
CHECKO_EMAIL=your_checko_email_here
```

### Описание параметров конфигурации

**Блокировки Райффайзен банка:**
- `RAIFFEISEN_BLOCK_TIME_SECONDS` - время блокировки в секундах после возникновения ошибки (по умолчанию 1 час = 3600 сек)
- `RAIFFEISEN_SECONDARY_WAIT_SECONDS` - дополнительное время ожидания, если блокировка не снята (по умолчанию 10 минут = 600 сек)
- `RAIFFEISEN_MAX_RETRY_ATTEMPTS` - максимальное количество попыток восстановления после блокировки (по умолчанию 24)

**Параметры браузера:**
- `PAGE_LOAD_TIMEOUT_SECONDS` - таймаут загрузки страницы в секундах (по умолчанию 90)
- `ELEMENT_WAIT_TIMEOUT_SECONDS` - таймаут ожидания элементов на странице (по умолчанию 10)
- `AUTOCOMPLETE_WAIT_SECONDS` - время ожидания автоподсказок на сайте Райфайзен (по умолчанию 5)
- `MAX_KEY_ATTEMPTS` - максимальное количество попыток с одним ключом API (по умолчанию 3)

## Настройка API ключей Dadata

Для корректной работы парсера Dadata необходимо получить API ключ:

1. Зарегистрируйтесь на сайте [dadata.ru](https://dadata.ru/)
2. После регистрации перейдите в [личный кабинет](https://dadata.ru/profile/#info)
3. Скопируйте API-ключ (токен)
4. Установите ключи в файл `.env`:

```env
DADATA_TOKEN_1=ваш_первый_токен_dadata
DADATA_TOKEN_2=ваш_второй_токен_dadata
DADATA_SECRET_KEY=ваш_секретный_ключ_dadata
```

### Использование нескольких ключей Dadata

Поскольку Dadata имеет ограничение 10 000 запросов в день на один ключ, парсер поддерживает использование нескольких ключей. Для этого:

1. Создайте несколько аккаунтов на сайте dadata.ru
2. Получите API-ключи для каждого аккаунта
3. Установите их в файл `.env` с разными номерами:

```env
DADATA_TOKEN_1=первый_токен_dadata
DADATA_TOKEN_2=второй_токен_dadata
DADATA_TOKEN_3=третий_токен_dadata
DADATA_SECRET_KEY=секретный_ключ
```

Парсер будет автоматически переключаться на следующий ключ при превышении лимита запросов.

## Настройка API ключей Checko

Для корректной работы парсера Checko необходимо получить API ключ:

1. Зарегистрируйтесь на сайте [checko.ru](https://checko.ru/)
2. После регистрации получите API-ключ (токен) в личном кабинете
3. Установите ключи в файл `.env`

### Особенности работы с API Checko:

- **Лимит запросов:** Базовый тариф включает 100 запросов API в сутки на один ключ
- **Эффективное использование:** Парсер автоматически отслеживает количество использованных запросов и сохраняет эту информацию между запусками
- **Несколько ключей:** Для увеличения дневного лимита можно использовать несколько ключей от разных аккаунтов:

```env
CHECKO_TOKEN=основной_ключ_checko
CHECKO_TOKEN_1=дополнительный_ключ_1
CHECKO_TOKEN_2=дополнительный_ключ_2
CHECKO_EMAIL=ваш_email_для_api_checko
```

- **Оптимизация:** Для более эффективного использования ограниченного числа запросов парсер:
  - Использует последовательную обработку компаний (без параллельных запросов с одним ключом)
  - Сообщает о количестве оставшихся запросов в логах
  - Помечает компании статусом "лимит API исчерпан" при достижении лимита

## Важные исправления в последней версии

### Исправление блокировки Райфайзен банка
- Исправлена логика работы с блокировкой сайта Райфайзен банка
- Парсер теперь корректно обрабатывает состояние блокировки и возобновляет работу после истечения времени блокировки
- Блокировка теперь глобальная для всех потоков парсера
- Добавлена корректная обработка ошибок подключения и таймаутов

### Исправление загрузки конфигурации
- Исправлена проблема с загрузкой переменных из .env файла
- Теперь все параметры корректно читаются из конфигурационного файла
- Если .env файл отсутствует, используются значения по умолчанию

### Улучшенная обработка ошибок
- Парсер теперь корректно обрабатывает различные типы ошибок подключения
- Улучшена логика пересоздания драйвера браузера после ошибок
- Добавлено более детальное логирование для отладки

## Конфигурация через .env файл (устаревшие параметры)

```
# Задержки между запросами для разных парсеров (в секундах)
DADATA_RATE_LIMIT=0.2
FOCUS_KONTUR_RATE_LIMIT=5.0
CHECKO_RATE_LIMIT=2.0
ZACHESTNY_RATE_LIMIT=3.0
AUDIT_IT_RATE_LIMIT=2.0
RBC_RATE_LIMIT=2.0

# Параметры сохранения данных
SAVE_INTERVAL=50
```

## Запуск парсера

```bash
python main.py [входной_файл.xlsx] [выходной_файл.csv]
```

По умолчанию используются файлы:
- Входной файл: `test.xlsx`
- Выходной файл: `results.csv`

## Доступные парсеры

- **DadataParser** - основной парсер, использующий комбинированный подход:
  - Автоматически получает ФИО председателя через Dadata API
  - Получает ИНН председателя через сайт Райффайзен банка
  - Поддерживает ротацию API ключей Dadata при достижении лимитов

- **FocusKonturParser** - парсинг с сайта focus.kontur.ru (лимит 100 запросов в день)
- **CheckoParser** - парсер для сайта checko.ru:
  - Работает только через API сервиса (не использует Selenium)
  - Имеет встроенную систему отслеживания лимита запросов API (100 запросов в сутки на ключ)
  - Автоматически переключается между ключами при достижении лимита
  - При исчерпании всех ключей помечает компании статусом "лимит API исчерпан"
  - Сохраняет счетчик использованных запросов между запусками программы
- **ZaChestnyiBiznesParser** - парсинг с сайта zachestnyibiznes.ru
- **AuditItParser** - парсинг с сайта audit-it.ru
- **RbcCompaniesParser** - парсинг с сайта companies.rbc.ru

## Процесс поиска ИНН председателя

1. Парсер сначала получает данные о компании по её ИНН через API Dadata
2. Из полученных данных извлекается ФИО руководителя (председателя)
3. Затем производится поиск ИНН руководителя через сайт Райффайзен банка
   - Открывается страница https://reg-raiffeisen.ru/
   - В поле "Вводи ИП или ООО" вводится ФИО председателя
   - Из автоподсказки извлекается ИНН физического лица (12 цифр) с приоритетом
   - Если ИНН физлица не найден, используется ИНН юрлица (10 цифр) с пометкой
   - Используется регулярное выражение для форматирования и валидации ИНН

## Формат входных данных

Входной файл Excel должен содержать следующие колонки:
- **Юридическое название** - название компании
- **ИНН** - ИНН компании

## Формат выходных данных

Выходной CSV-файл будет содержать:
- **Юридическое название** - название компании
- **ИНН** - ИНН компании
- **ФИО Председателя** - ФИО руководителя компании
- **ИНН Председателя** - ИНН руководителя (если удалось получить)
- **Источник** - сайт или API, откуда получены данные
