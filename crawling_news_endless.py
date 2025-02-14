"""
과거로 무한 크롤링
철강 검색하면 뜨는 모든 기사
403 발생 시, 10분 후 재시도, 실패시 해당 날짜 기록
중복 기사 방지 : seen_titles 및 insert ignorea(쿼리)
"""


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
    host="192.168.0.163",
    user="root",
    password="andong1234",
    database="analysis",
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

# ✅ 403 에러 발생 후 실패한 날짜 저장 리스트
failed_dates = set()

def get_news_list(url, counter, current_date):
    """네이버 뉴스 검색 결과에서 모든 기사 크롤링 후 MySQL에 저장"""
    try:
        for attempt in range(2):  # ✅ 2번까지 재시도 가능
            response = session.get(url, headers=headers)
            if response.status_code == 403:
                if attempt == 0:
                    print(f"❌ 403 Forbidden - {current_date} 크롤링 차단됨. 10분 후 재시도합니다...")
                    time.sleep(600)  # ✅ 10분 대기 후 재시도
                    continue
                else:
                    print(f"❌ {current_date} 크롤링 재시도 실패. 해당 날짜를 기록합니다.")
                    failed_dates.add(current_date)  # ✅ 실패한 날짜 기록
                    return False

            response.raise_for_status()
            random_sleep()
            break  # ✅ 403이 발생하지 않으면 루프 탈출

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

            # ✅ 중복 기사 방지 (중복 기사라면 스킵)
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
    """MySQL에 뉴스 데이터 저장 (INSERT IGNORE 사용하여 중복 방지)"""
    try:
        insert_query = """
        INSERT IGNORE INTO stage_news (title, content, news_date)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (title, content, publish_date))
        db.commit()
    except Exception as e:
        print(f"❌ 데이터 저장 오류: {e}")
        db.rollback()

def random_sleep():
    """랜덤한 시간 동안 대기 (3~4초, 10% 확률로 7~8.3초)"""
    sleep_time = random.uniform(3.5, 4)
    if random.random() < 0.1:
        sleep_time = random.uniform(7, 8.3)

    print(f"⏳ {sleep_time:.2f}초 대기...")
    time.sleep(sleep_time)

if __name__ == "__main__":
    counter = [0]  
    start_date = datetime.now() - timedelta(days=1)  # ✅ 어제부터 시작

    try:
        while True:  # ✅ 종료할 때까지 무한 반복
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

            # ✅ 하루씩 과거로 이동
            start_date -= timedelta(days=1)

    except KeyboardInterrupt:
        print("\n⏹ 크롤링 중지 요청됨. 현재 진행 중인 날짜 마무리 후 종료합니다...")
    finally:
        # ✅ 크롤링 실패한 날짜 출력
        if failed_dates:
            print("\n❌ 크롤링 실패한 날짜 목록:")
            for date in sorted(failed_dates):
                print(f"- {date}")

        print("✅ 크롤링 및 MySQL 저장 완료.")
        cursor.close()
        db.close()
