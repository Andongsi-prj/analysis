import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pymysql  # ✅ MySQL 연결을 위한 라이브러리
import re
import time

# ✅ 네이버 뉴스 검색 URL
base_url = "https://search.naver.com/search.naver?where=news&query=철강&sm=tab_opt&sort=2&photo=0&field=0&pd=3&ds={}&de={}&start={}"

# ✅ MySQL 연결 설정
db = pymysql.connect(
    host="192.168.0.163",  # MySQL 호스트
    user="analysis_user",       # MySQL 사용자 이름
    password="andong1234",  # MySQL 비밀번호
    database="analysis",   # 사용할 데이터베이스 이름
    charset="utf8mb4"
)

cursor = db.cursor()

# ✅ HTTP 세션 설정 및 재시도 전략 적용
session = requests.Session()
retry_strategy = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ✅ 네이버 차단 방지를 위한 헤더 설정 (User-Agent 및 Cookie 추가)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Referer": "https://www.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Cookie": "NNB=your_cookie_here;"  # 네이버에서 직접 가져온 쿠키 값 추가
}

# ✅ 중복 방지를 위한 제목 저장 (이미 크롤링한 기사 체크)
seen_titles = set()

def get_news_list(url, counter, last_known_date):
    """네이버 뉴스 검색 결과에서 뉴스 제목, 본문 요약, 보도 날짜 크롤링 후 MySQL에 저장"""
    try:
        response = session.get(url, headers=headers)  # ✅ User-Agent 및 쿠키 추가된 헤더 사용
        if response.status_code == 403:
            print("❌ 403 Forbidden - 네이버 차단됨. 일정 시간 후 다시 시도하세요.")
            time.sleep(600)  # ✅ 10분 대기 후 재시도
            return False, last_known_date

        response.raise_for_status()
        time.sleep(3)  # ✅ 네이버 차단 방지를 위해 3초 대기
    except requests.RequestException as e:
        print(f"❌ 검색 결과 페이지 요청 실패: {url}, 오류: {e}")
        return False, last_known_date  # ✅ 튜플 반환

    soup = BeautifulSoup(response.text, "html.parser")
    articles = soup.select(".news_area")

    if not articles:
        return False, last_known_date  # ✅ 튜플 반환

    for article in articles:
        try:
            # ✅ 기사 제목 크롤링
            title_element = article.select_one("a.news_tit")
            title = title_element.text.strip() if title_element else "제목 없음"

            # ✅ 중복 기사 방지
            if title in seen_titles:
                continue
            seen_titles.add(title)

            # ✅ 보도 날짜 크롤링
            publish_date_element = article.select("span.info")
            publish_date = "알 수 없음"
            for elem in publish_date_element:
                date_text = elem.get_text(strip=True)
                publish_date = parse_publish_date(date_text)
                break

            # ✅ 날짜 형식 변경 (YYYY-MM-DD)
            if publish_date == "알 수 없음":
                publish_date = last_known_date  # ✅ 이전 뉴스의 날짜로 설정
            else:
                last_known_date = publish_date  # ✅ 현재 날짜를 저장하여 이후에 활용

            # ✅ 본문 요약 크롤링
            summary_element = article.select_one("div.news_contents > div > div > a")
            content = summary_element.text.strip() if summary_element else "요약 없음"

            # ✅ 데이터 MySQL에 저장
            save_to_db(title, content, publish_date)

            # ✅ 날짜와 저장된 데이터 개수만 출력
            counter[0] += 1
            print(f"✅ {publish_date} - 저장된 데이터 개수: {counter[0]}")
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
    
    return True, last_known_date  # ✅ 튜플 반환

def parse_publish_date(date_text):
    """네이버 뉴스에서 가져온 날짜 텍스트를 변환 (YYYY-MM-DD)"""
    current_time = datetime.now()

    match_day = re.search(r"(\d+)일 전", date_text)
    if match_day:
        days_ago = int(match_day.group(1))
        return (current_time - timedelta(days=days_ago)).strftime("%Y-%m-%d")

    match_date = re.search(r"(\d{4})[.](\d{2})[.](\d{2})[.]", date_text)
    if match_date:
        return f"{match_date.group(1)}-{match_date.group(2)}-{match_date.group(3)}"

    return "알 수 없음"

def save_to_db(title, content, news_date):
    """MySQL에 뉴스 데이터 저장"""
    try:
        insert_query = """
        INSERT INTO stage_news (title, content, news_date)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (title, content, news_date))
        db.commit()
    except Exception as e:
        print(f"❌ 데이터 저장 오류: {e}")
        db.rollback()

if __name__ == "__main__":
    counter = [0] 
    last_known_date = None  # ✅ 처음에는 날짜가 없으므로 None으로 설정

    # ✅ 12월 6일부터 크롤링 시작
    start_date = datetime(2024, 12, 6)
    end_date = datetime(2025, 2, 12)

    while start_date <= end_date:
        # ✅ 3일 단위 설정
        next_date = start_date + timedelta(days=2)
        if next_date > end_date:
            next_date = end_date

        ds = start_date.strftime("%Y.%m.%d")
        de = next_date.strftime("%Y.%m.%d")

        print(f"🔍 {ds} ~ {de} 뉴스 검색 중...")

        page = 1
        max_pages = 1000  

        while page <= max_pages:
            search_url = base_url.format(ds, de, (page - 1) * 10 + 1)
            prev_count = counter[0]
            success, last_known_date = get_news_list(search_url, counter, last_known_date)

            if not success or prev_count == counter[0]:  
                break
            page += 1  

        print(f"✅ {ds} ~ {de} 뉴스 저장 완료! (총 저장된 개수: {counter[0]})\n")

        # ✅ 다음 3일로 이동
        start_date = next_date + timedelta(days=1)

    print("✅ 크롤링 및 MySQL 저장 완료.")

    # ✅ MySQL 연결 종료
    cursor.close()
    db.close()
