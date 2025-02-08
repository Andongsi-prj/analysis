import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv
from tqdm import tqdm
import uuid
import re
import time

base_url = 'https://search.naver.com/search.naver?where=news&query=철강&sm=tab_pge&sort=0&ds={}&de={}&start={}'

start_date = datetime(2025, 2, 1)
end_date = datetime(2010, 1, 1)
max_pages = 5

session = requests.Session()
retry_strategy = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def generate_uuid():
    return str(uuid.uuid1())

def is_korean_text(text):
    return bool(re.search("[가-힣]", text))

def contains_keywords(text):
    keywords = ["철강"]
    return any(keyword in text for keyword in keywords)

def get_news_list(url, writer, counter):
    try:
        response = session.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    articles = soup.select(".news_area")

    for article in tqdm(articles, desc="크롤링 진행중 (철강)", ncols=80):
        try:
            title_element = article.select_one("a.news_tit")
            title = title_element.text.strip() if title_element else "제목 없음"
            link = title_element["href"] if title_element else ""

            media_com = article.select_one(".info.press").text.strip() if article.select_one(".info.press") else "언론사 정보 없음"
            article_data, publish_date = get_article_content(link)

            if (not is_korean_text(title) or not is_korean_text(article_data)) or (
                    not contains_keywords(title) and not contains_keywords(article_data)):
                continue

            doc_id = generate_uuid()
            writer.writerow({
                "doc_id": doc_id,
                "section": "철강",
                "crawl_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "media_com": media_com,
                "title": title,
                "contents": article_data,
                "url": link,
                "publish_date": publish_date,
                "part_dt": datetime.now().strftime("%Y-%m-%d")
            })

            counter[0] += 1
            print(f"현재 저장된 데이터 개수: {counter[0]}")
        except Exception as e:
            print(f"Error occurred: {e}")

def get_article_content(url, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            response = session.get(url, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            break
        except requests.RequestException as e:
            print(f"Error fetching article {url}: {e}")
            retries += 1
            time.sleep(2)

    if retries == max_retries:
        return "내용 없음", "알 수 없음"

    soup = BeautifulSoup(response.text, "html.parser")
    content_element = soup.select_one("#dic_area")
    contents = content_element.text.strip() if content_element else "내용 없음"

    publish_date_element = soup.select_one("span.info")
    publish_date = publish_date_element["data-date-time"] if publish_date_element else "알 수 없음" 

    return contents, publish_date

def date_range(start, end):
    delta = timedelta(days=1)
    current = start
    while current >= end:
        yield current
        current -= delta

if __name__ == "__main__":
    with open("naver_news_철강.csv", "w", newline="", encoding="utf-8") as output_file:
        fieldnames = ["doc_id", "section", "crawl_dt", "media_com", "title", "contents", "url", "publish_date", "part_dt"]
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        counter = [0]

        for single_date in date_range(start_date, end_date):
            formatted_date = single_date.strftime("%Y.%m.%d")
            for page in range(1, max_pages + 1):
                section_url = base_url.format(formatted_date, formatted_date, (page - 1) * 10 + 1)
                get_news_list(section_url, writer, counter)

    print("크롤링 및 CSV 파일 저장 완료.")
