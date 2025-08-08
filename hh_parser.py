from bs4 import BeautifulSoup
import pandas as pd
import json
import requests
import random
import time

# Список бесплатных прокси
def find_proxis():
# URL страницы с прокси
    url = "https://free-proxy-list.net/"

# Запрос для получения HTML страницы
    response = requests.get(url)
    response.raise_for_status()  # Если запрос не удастся, выбросит ошибку

# Разбор HTML страницы с использованием BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

# Найдём таблицу с прокси
    table = soup.find('table', {'class': 'table table-striped table-bordered'})
# Список для хранения прокси
    proxies = []

# Извлекаем прокси из таблицы
    for row in table.find_all('tr')[1:]:  # Пропускаем заголовок таблицы
        columns = row.find_all('td')
        if len(columns) > 0:
            ip = columns[0].text.strip()
            port = columns[1].text.strip()
            proxy = f"http://{ip}:{port}"
            proxies.append(proxy)
    return proxies

# Функция для выполнения запросов с п/овторными попытками
def retry_request(url, params=None, retries=5, delay=5):
    for attempt in range(retries):
        proxy = {'http': random.choice(find_proxis())}  # Выбираем случайный прокси

        try:
            response = requests.get(url, params=params, proxies=proxy)
            response.raise_for_status()  # Вызовет исключение, если статус-код не успешный (4xx или 5xx)
            return response
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе с прокси {proxy}: {e}")
            if attempt < retries - 1:
                print(f"Попытка {attempt + 1} из {retries}. Повтор через {delay} секунд...")
                time.sleep(delay)
            else:
                print("Максимальное количество попыток достигнуто. Завершение работы.")

# per_page = 100#100  # Количество вакансий на странице
# search_queries = ['Инженер данных', 'Аналитик', 'data engineer','Юрист']  # Список текстов для поиска
# area = 1  # Регион (1 - Москва)
# period = 1  # Период (количество дней)
# pages_to_parse = 15 #15 # Количество страниц для парсинга
# field = Используйте параметр field со значением name (или company_name для поиска по названию компании).Другие варианты уточнения поиска:
    # name	В названии вакансии 
    # company_name В названии компании 
    # description	В описании вакансии (по умолчанию)
    # all	Во всех полях
#skills_search = True нужны ли skills в вакансии?


def query(per_page, search_queries, area, period, pages_to_parse, field, skills_search):
    # Создаём список для итогового DataFrame
    frames = []
    
    # Обход по разным ключевым словам
    for query in search_queries:
        print(f"\n🔍 Обрабатываю запрос: '{query}'")
        # Обход страниц для текущего запроса
        for page in range(pages_to_parse):
            print(f"  Страница {page + 1} из {pages_to_parse}")
            url = 'https://api.hh.ru/vacancies'
            params = {
                'page': page,
                'per_page': per_page,
                'text': f'!{query}',  # Точный поиск для текущего запроса
                'area': area,
                'period': period,
                'field': field
            }

            # Запрос к API для получения списка вакансий с повторными попытками
            if skills_search is True:
                try:
                    response = retry_request(url, params=params)
                    data_json = response.json()

                    # Проверка на наличие вакансий
                    if not data_json.get('items'):
                        print("    Вакансии не найдены на странице.")
                        continue
                    
                    # Проход по всем вакансиям на странице
                    for item in data_json['items']:
                        vacancy_id = item['id']
                        vacancy_url = f"https://api.hh.ru/vacancies/{vacancy_id}"

                        # Запрос для получения ключевых навыков конкретной вакансии с повторными попытками
                        try:
                            vacancy_response = retry_request(vacancy_url)
                            vacancy_data = vacancy_response.json()
                            key_skills = vacancy_data.get("key_skills", [])
                            skills = [skill["name"] for skill in key_skills]

                            # Добавляем ключевые навыки в item
                            item['key_skills'] = ", ".join(skills)
                        except Exception as e:
                            print(f"Ошибка при получении данных вакансии ID {vacancy_id}: {e}")
                            item['key_skills'] = None  # Если запрос по ID не удался

                        # Добавляем текущий запрос как отдельное поле
                        item['search_query'] = query
                        # Добавляем данные вакансии в список
                        frames.append(item)

                        time.sleep(0.2)  # Пауза между запросами
                except Exception as e:
                    print(f"Ошибка при запросе списка вакансий: {e}")
                    continue  # Продолжаем обработку следующей страницы или запроса
            else:
                response = retry_request(url, params=params)
                frames.extend(response.json()['items'])
    return frames


def df_main(frames):
    cols = ['name',
            'published_at',
            'url', 
            'alternate_url',
            'employer_name',
            'experience_name',
            'schedule_name',
            'professional_roles_name',
            'area_name']
    # Создание итогового DataFrame
    if frames:
        result = pd.DataFrame(frames)
        # Выбираем только важные колонки для записи
        # result = result[['id', 'name', 'employer', 'area', 'salary', 'key_skills', 'search_query']]
        print("\n✅ Итоговый DataFrame создан.")

        # Первый проход для нормализации столбцов
        result1 = result.copy()
        for i in result.columns:
            df_normalized = pd.json_normalize(result[i])
            if len(df_normalized.columns) > 1:
                # Переименовываем столбцы в нормализованном DataFrame
                df_normalized.columns = [f"{i}_{col}" for col in df_normalized.columns]
                # Объединяем результаты
                result1 = pd.concat([result1.drop(columns=[i]), df_normalized], axis=1)
        result1['professional_roles_id'] = result1['professional_roles'].apply(lambda x: x[0]['id'] if x else None)
        result1['professional_roles_name'] = result1['professional_roles'].apply(lambda x: x[0]['name'] if x else None)
        
        # Цикл для удаления столбцов с неправильными типами данных
        while True:
            columns_to_drop = []
            for i in result1.columns:
                # Проверяем, что столбец не пустой
                if not result1[i].empty:
                    # Проверяем тип данных всех элементов столбца
                    if result1[i].apply(lambda x: isinstance(x, (dict, list))).any():
                        columns_to_drop.append(i)
            # Удаляем столбцы, которые нужно удалить
            if columns_to_drop:
                result1.drop(columns=columns_to_drop, inplace=True)
            else:
                break
            
        # Сохранение в Excel (закомментировано)
        # result1.to_csv('vacancies_with_skills1.csv', index=False)
        # print("📁 Данные сохранены в 'vacancies_with_skills.xlsx'.")
    else:
        print("\n❌ Не удалось собрать данные.")

    return result1[cols]

def extract_description(vacancy_json):
    """
    Принимает JSON-строку вакансии hh.ru
    Возвращает чистый текст описания без HTML
    """
    # Преобразуем строку в Python-словарь
    data = json.loads(vacancy_json)
    
    # Достаем поле description (оно в формате HTML)
    html_description = data.get("description", "")
    if not html_description:
        return ""
    
    # Парсим HTML и превращаем в текст
    soup = BeautifulSoup(html_description, "html.parser")
    clean_text = soup.get_text(separator=" ", strip=True)
    return clean_text

def add_description(df):
    df_len = df.shape[0]
    description = []
    for i in range(df_len):
        response = requests.get(df['url'][i])  
        response.raise_for_status()
        description.append(extract_description(response.text))

    df['description'] = description
    return df


per_page = 50
search_queries = ['Data Scientist','LLM','NLP','ML Engineer']
area = 1
period = 1
pages_to_parse = 2
field = 'name'
skills_search = False
prof_name = ['Дата-сайентист']

res = query(per_page, search_queries, area, period, pages_to_parse, field, skills_search)
b = df_main(res)
b = b[b['professional_roles_name'].isin(prof_name)].reset_index()
new_df = add_description(b)
new_df.to_csv("vacancies_hh.csv",index=False)

