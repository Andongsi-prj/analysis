# 하루 하루 하루


import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pymysql  # ✅ MySQL 라이브러리
import re
import time

# ✅ 네이버 뉴스 검색 URL
base_url = "https://search.naver.com/search.naver?where=news&query=철강&sm=tab_opt&sort=0&photo=0&field=0&pd=3&ds={}&de={}&start={}"

# ✅ MySQL 연결 설정
db = pymysql.connect(
    host="192.168.0.163",  # MySQL 호스트
    user="root",   # MySQL 사용자 이름
    password="andong1234",  # MySQL 비밀번호
    database="analysis",    # 사용할 데이터베이스 이름
    charset="utf8mb4"
)
cursor = db.cursor()

# ✅ HTTP 세션 설정 및 재시도 전략 적용
session = requests.Session()
retry_strategy = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ✅ 네이버 차단 방지를 위한 헤더 설정
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Referer": "https://www.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

# ✅ 중복 방지를 위한 제목 저장 (이미 저장된 뉴스 체크)
seen_titles = set()

def get_news_list(url, counter, current_date):
    """네이버 뉴스 검색 결과에서 제목에 '철강'이 포함된 기사만 크롤링 후 MySQL에 저장"""
    try:
        response = session.get(url, headers=headers)
        if response.status_code == 403:
            print("❌ 403 Forbidden - 네이버 차단됨. 일정 시간 후 다시 시도하세요.")
            time.sleep(600)
            return False

        response.raise_for_status()
        random_sleep()
    except requests.RequestException as e:
        print(f"❌ 검색 결과 페이지 요청 실패: {url}, 오류: {e}")
        return False

    soup = BeautifulSoup(response.text, "html.parser")
    articles = soup.select(".news_area")

    if not articles:
        return False

    for article in articles:
        try:
            # ✅ 기사 제목 크롤링
            title_element = article.select_one("a.news_tit")
            title = title_element.text.strip() if title_element else "제목 없음"

            # ✅ 제목에 '철강'이 포함되지 않으면 스킵
            if "철강" not in title:
                continue

            # ✅ 중복 기사 방지
            if title in seen_titles:
                continue
            seen_titles.add(title)

            # ✅ 보도 날짜 크롤링
            publish_date = current_date

            # ✅ 본문 요약 크롤링
            summary_element = article.select_one("a.dsc_txt_wrap")
            content = summary_element.text.strip() if summary_element else "요약 없음"

            # ✅ 데이터 MySQL에 저장
            save_to_db(title, content, publish_date)

            # ✅ 날짜와 저장된 데이터 개수만 출력
            counter[0] += 1
            print(f"✅ {publish_date} - 저장된 데이터 개수: {counter[0]}")
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
    
    return True

def save_to_db(title, content, publish_date):
    """MySQL에 뉴스 데이터 저장"""
    try:
        insert_query = """
        INSERT INTO stage_news (title, content, news_date)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (title, content, publish_date))
        db.commit()
    except Exception as e:
        print(f"❌ 데이터 저장 오류: {e}")
        db.rollback()

def random_sleep():
    """랜덤한 시간 동안 대기 (2~6초, 20% 확률로 10~15초)"""
    sleep_time = random.uniform(3.5, 4)
    if random.random() < 0.1:
        sleep_time = random.uniform(7, 8.3)

    print(f"⏳ {sleep_time:.2f}초 대기...")
    time.sleep(sleep_time)

if __name__ == "__main__":
    counter = [0]  

    # ✅ 1월 14일부터 2월 8일까지 크롤링
    start_date = datetime(2025, 1, 14)
    end_date = datetime(2025, 2, 8)

    while start_date <= end_date:
        ds = start_date.strftime("%Y.%m.%d")
        de = ds  # ✅ 하루 단위 크롤링

        print(f"🔍 {ds} 뉴스 검색 중...")

        page = 1
        max_pages = 1000  

        while page <= max_pages:
            search_url = base_url.format(ds, de, (page - 1) * 10 + 1)
            prev_count = counter[0]
            success = get_news_list(search_url, counter, ds)

            if not success or prev_count == counter[0]:  
                break
            page += 1  

        print(f"✅ {ds} 뉴스 저장 완료! (총 저장된 개수: {counter[0]})\n")

        start_date += timedelta(days=1)

    print("✅ 크롤링 및 MySQL 저장 완료.")

    cursor.close()
    db.close()


# # 어제 뉴스 수집 
# if __name__ == "__main__":
#     counter = [0]  

#     # ✅ 어제 날짜 설정
#     yesterday = datetime.now() - timedelta(days=1)
#     start_date = yesterday
#     end_date = yesterday

#     ds = start_date.strftime("%Y.%m.%d")
#     de = ds  # ✅ 하루 단위 크롤링

#     print(f"🔍 {ds} 뉴스 검색 중...")

#     page = 1
#     max_pages = 1000  

#     while page <= max_pages:
#         search_url = base_url.format(ds, de, (page - 1) * 10 + 1)
#         prev_count = counter[0]
#         success = get_news_list(search_url, counter, ds)

#         if not success or prev_count == counter[0]:  
#             break
#         page += 1  

#     print(f"✅ {ds} 뉴스 저장 완료! (총 저장된 개수: {counter[0]})\n")

#     print("✅ 크롤링 및 MySQL 저장 완료.")

#     cursor.close()
#     db.close()