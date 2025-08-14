"""Парсер вакансий hh.ru."""
from bs4 import BeautifulSoup
import json
import random
import time
import os
from pathlib import Path

import pandas as pd
import requests

# --- Пути ---
CSV_PATH = Path(os.getenv("CSV_PATH", "/opt/airflow/data/vacancies_hh.csv"))


def find_proxis() -> list[str]:
    """Возвращает список бесплатных HTTP-прокси."""
    url = "https://free-proxy-list.net/"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"class": "table table-striped table-bordered"})
    proxies: list[str] = []
    for row in table.find_all("tr")[1:]:
        columns = row.find_all("td")
        if columns:
            ip = columns[0].text.strip()
            port = columns[1].text.strip()
            proxies.append(f"http://{ip}:{port}")
    return proxies


def retry_request(url, params=None, retries: int = 5, delay: int = 5):
    """Выполняет запрос с повторными попытками, выбирая случайный прокси."""
    for attempt in range(retries):
        proxy = {"http": random.choice(find_proxis())}
        try:
            response = requests.get(url, params=params, proxies=proxy)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе с прокси {proxy}: {e}")
            if attempt < retries - 1:
                print(f"Попытка {attempt + 1} из {retries}. Повтор через {delay} секунд...")
                time.sleep(delay)
            else:
                print(f"Максимальное количество попыток достигнуто: {url}")
                raise


def query(per_page, search_queries, area, period, pages_to_parse, field, skills_search):
    """Получает список вакансий из API hh.ru."""
    frames = []
    for query in search_queries:
        print(f"\n🔍 Обрабатываю запрос: '{query}'")
        for page in range(pages_to_parse):
            print(f"  Страница {page + 1} из {pages_to_parse}")
            url = "https://api.hh.ru/vacancies"
            params = {
                "page": page,
                "per_page": per_page,
                "text": f"!{query}",
                "area": area,
                "period": period,
                "field": field,
            }
            try:
                response = retry_request(url, params=params)
                data_json = response.json()
                if not data_json.get("items"):
                    print("    Вакансии не найдены.")
                    continue
                for item in data_json["items"]:
                    vacancy_id = item["id"]
                    vacancy_url = f"https://api.hh.ru/vacancies/{vacancy_id}"
                    try:
                        vacancy_response = retry_request(vacancy_url)
                        vacancy_data = vacancy_response.json()
                        key_skills = vacancy_data.get("key_skills", [])
                        skills = [skill["name"] for skill in key_skills]
                        item["key_skills"] = ", ".join(skills)
                    except Exception as e:
                        print(f"Ошибка при получении ID {vacancy_id}: {e}")
                        item["key_skills"] = None
                    item["search_query"] = query
                    frames.append(item)
                    time.sleep(0.2)
            except Exception as e:
                print(f"Ошибка при запросе списка: {e}")
                continue
    return frames


def df_main(frames: list[dict]) -> pd.DataFrame:
    """Нормализует список вакансий в DataFrame."""
    cols = [
        "name",
        "published_at",
        "url",
        "alternate_url",
        "employer_name",
        "experience_name",
        "schedule_name",
        "professional_roles_name",
        "area_name",
    ]
    if not frames:
        print("\n❌ Не удалось собрать данные.")
        return pd.DataFrame(columns=cols)
    result = pd.DataFrame(frames)
    result1 = result.copy()
    for col in result.columns:
        df_normalized = pd.json_normalize(result[col])
        if len(df_normalized.columns) > 1:
            df_normalized.columns = [f"{col}_{c}" for c in df_normalized.columns]
            result1 = pd.concat([result1.drop(columns=[col]), df_normalized], axis=1)
    result1["professional_roles_id"] = result1["professional_roles"].apply(
        lambda x: x[0]["id"] if x else None
    )
    result1["professional_roles_name"] = result1["professional_roles"].apply(
        lambda x: x[0]["name"] if x else None
    )
    while True:
        columns_to_drop = []
        for col in result1.columns:
            if not result1[col].empty and result1[col].apply(lambda x: isinstance(x, (dict, list))).any():
                columns_to_drop.append(col)
        if columns_to_drop:
            result1.drop(columns=columns_to_drop, inplace=True)
        else:
            break
    return result1[cols]


def extract_description(vacancy_json: str) -> str:
    """Возвращает чистый текст описания вакансии из JSON."""
    data = json.loads(vacancy_json)
    html_description = data.get("description", "")
    if not html_description:
        return ""
    soup = BeautifulSoup(html_description, "html.parser")
    return soup.get_text(separator=" ", strip=True)


def add_description(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет колонку description, запрашивая описание по API URL."""
    descriptions = []
    for api_url in df["url"]:
        response = requests.get(api_url)
        response.raise_for_status()
        descriptions.append(extract_description(response.text))
    df["description"] = descriptions
    return df


def parse_hh_vacancies(
    search_queries,
    per_page: int = 50,
    area: int = 1,
    period: int = 1,
    pages_to_parse: int = 2,
    field: str = "name",
    skills_search: bool = False,
    prof_names: list[str] | None = None,
) -> pd.DataFrame:
    frames = query(per_page, search_queries, area, period, pages_to_parse, field, skills_search)
    df = df_main(frames)
    if prof_names:
        df = df[df["professional_roles_name"].isin(prof_names)]
    df = df.reset_index(drop=True)
    df = add_description(df)
    df = df.rename(
        columns={
            "name": "title",
            "employer_name": "company",
            "experience_name": "experience",
            "alternate_url": "url",
        }
    )
    return df[["title", "company", "experience", "description", "url"]]


if __name__ == "__main__":
    queries = ["Data Scientist", "LLM", "NLP", "ML Engineer"]
    result_df = parse_hh_vacancies(queries, prof_names=["Дата-сайентист"])
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)  # создаём папку data, если нет
    result_df.to_csv(CSV_PATH, index=False)
    print("cwd:", os.getcwd())
    print(f"✅ Файл сохранён: {CSV_PATH}")
