import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import random
import pymysql
from tqdm import tqdm
import re

# ✅ 최근 7일간의 데이터를 크롤링하도록 설정
start_date = datetime.now()
end_date = start_date - timedelta(days=6)  # 최근 7일간

# 네이버 뉴스 검색 URL (✅ 7일간의 데이터 가져오기 위한 `nso=so:r,p:7d,a:all` 추가)
base_url = 'https://search.naver.com/search.naver?where=news&query=철강&sm=tab_pge&sort=sim&ds={}&de={}&start={}&nso=so:r,p:7d,a:all'

# 페이지 제한 설정
max_pages = 5  # 🔹 페이지 제한 (불필요한 크롤링 방지, 성능 최적화)

# ✅ User-Agent 목록 (랜덤 선택하여 차단 방지)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36",
]

# MySQL 접속 정보 (`pymysql` 사용)
DB_CONFIG = {
    "host": "192.168.0.163",  # MySQL 서버 주소
    "user": "analysis_user",  # MySQL 사용자 이름
    "password": "andong1234",  # MySQL 비밀번호
    "database": "analysis",  # 사용할 데이터베이스명
    "charset": "utf8mb4"  # UTF-8 지원
}

# HTTP 요청 세션 설정 (자동 재시도 적용)
session = requests.Session()
retry_strategy = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def connect_db():
    """MySQL 데이터베이스 연결 (`pymysql` 사용)"""
    return pymysql.connect(**DB_CONFIG)

def insert_news_to_db(news_list):
    """크롤링한 데이터를 MySQL `stage_news` 테이블에 저장 (날짜 형식 수정)"""
    if not news_list:
        print("⚠️ 저장할 뉴스 데이터가 없습니다.")
        return
    
    conn = connect_db()
    cursor = conn.cursor()

    sql = """
        INSERT INTO stage_news (title, content, news_date)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE content=VALUES(content), news_date=VALUES(news_date)
    """
    
    formatted_news_list = []
    
    for title, content, news_date in news_list:
        # 날짜 형식 변환 (YYYY-MM-DD 유지)
        if news_date and news_date != "알 수 없음":
            try:
                news_date = datetime.strptime(news_date, "%Y-%m-%d").date()
            except ValueError:
                news_date = None  # 날짜 형식이 맞지 않으면 NULL 저장
        else:
            news_date = None  # 알 수 없는 날짜는 NULL로 저장

        formatted_news_list.append((title, content, news_date))

    try:
        cursor.executemany(sql, formatted_news_list)  # 🔹 여러 개의 데이터 한 번에 삽입
        conn.commit()  # 🔹 변경 사항 적용
        
        print(f"✅ {cursor.rowcount}개의 뉴스가 MySQL `stage_news` 테이블에 저장되었습니다.")
    
    except pymysql.MySQLError as e:
        print(f"❌ MySQL 저장 오류: {e}")
    
    finally:
        cursor.close()
        conn.close()

def get_headers():
    """✅ 랜덤 User-Agent 적용하여 차단 방지"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.naver.com/",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Connection": "keep-alive"
    }

def get_news_list(url, news_list, seen_links):
    """네이버 뉴스 검색 결과에서 기사 리스트를 가져와 처리"""
    try:
        time.sleep(random.uniform(1.5, 4))  # ✅ 랜덤 딜레이 추가 (차단 방지)
        response = session.get(url, headers=get_headers(), timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Error fetching URL {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    articles = soup.select(".news_area")

    for article in tqdm(articles, desc="크롤링 진행중 (철강)", ncols=80):
        try:
            title_element = article.select_one("a.news_tit")
            title = title_element.text.strip() if title_element else "제목 없음"
            link = title_element["href"] if title_element else ""

            # ✅ 중복 방지를 위해 이미 저장된 링크는 건너뛴다
            if link in seen_links:
                continue
            seen_links.add(link)

            # 본문 내용과 날짜 가져오기
            content, news_date = get_article_content(link)

            # 유효한 뉴스인지 확인 후 리스트에 추가
            if title != "제목 없음" and content != "내용 없음" and len(content) > 50:
                news_list.append((title, content, news_date))

        except Exception as e:
            print(f"❌ Error occurred: {e}")

def get_article_content(url, max_retries=3):
    """네이버 뉴스 기사 본문과 날짜를 가져오는 함수"""
    retries = 0
    while retries < max_retries:
        try:
            time.sleep(random.uniform(1.5, 4))  # ✅ 랜덤 딜레이 추가 (차단 방지)
            response = session.get(url, headers=get_headers(), timeout=10)
            response.raise_for_status()
            break
        except requests.RequestException as e:
            print(f"❌ Error fetching article {url}: {e}")
            retries += 1

    if retries == max_retries:
        return "내용 없음", None  # ✅ news_date가 None이면 MySQL에서 NULL로 저장됨

    soup = BeautifulSoup(response.text, "html.parser")

    # 🔹 광고 및 불필요한 요소 제거
    for script in soup(["script", "style", "header", "footer", "nav"]):
        script.extract()

    # 🔹 본문 내용 추출
    content_element = soup.select_one("#dic_area, .newsct_article, .article_body, .art_txt, div[itemprop='articleBody']")
    content = content_element.get_text(strip=True) if content_element else "내용 없음"

    # 🔹 기사 날짜 추출
    news_date = None  
    date_element = soup.find("meta", {"property": "og:article:published_time"})
    if date_element:
        news_date = date_element["content"][:10]  

    if not news_date:
        possible_date_elements = soup.select(".media_end_head_info, .article_info, .date, time")
        for date_el in possible_date_elements:
            date_text = date_el.get_text(strip=True)
            match = re.search(r"\d{4}[./-]\d{2}[./-]\d{2}", date_text)
            if match:
                news_date = match.group().replace(".", "-")  
                break

    return content, news_date


if __name__ == "__main__":
    all_news = []
    seen_links = set()
    for single_date in range((start_date - end_date).days + 1):
        formatted_date = (start_date - timedelta(days=single_date)).strftime("%Y.%m.%d")
        for page in range(1, max_pages + 1):
            section_url = base_url.format(formatted_date, formatted_date, (page - 1) * 10 + 1)
            get_news_list(section_url, all_news, seen_links)

    if all_news:
        print("✅ 최근 7일간 뉴스 크롤링 완료.")
    else:
        print("⚠️ 저장할 뉴스가 없습니다.")
