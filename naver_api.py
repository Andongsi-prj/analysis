import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# CSV 파일명
csv_filename = "steel_news.csv"

# 검색할 키워드 및 기간 설정
query = "철강"
days = 2  # 최근 2일간의 뉴스만 수집

# 날짜 기준 설정 (오늘 날짜에서 2일 전까지 허용)
start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

# 최신 브라우저 User-Agent 사용
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

def get_news_links(query):
    """
    네이버 뉴스 검색 결과에서 최근 2일간의 뉴스 기사 링크, 제목, 날짜를 수집
    """
    news_data = []
    seen_links = set()
    page = 1  # 페이지 시작 값

    while True:  # 🔹 페이지 제한 없이 날짜 기준으로 종료
        search_url = f"https://search.naver.com/search.naver?where=news&query={query}&sort=sim&nso=so:r,p:2d,a:all&start={page}"
        print(f"🔍 [페이지 {page // 10 + 1}] 뉴스 검색 중: {search_url}")

        try:
            response = requests.get(search_url, headers=headers, timeout=10)

            # 403 Forbidden 에러 방지
            if response.status_code == 403:
                print("⛔ [차단 감지] 403 Forbidden - User-Agent 변경 & VPN 사용 추천")
                time.sleep(10)
                continue
            elif response.status_code != 200:
                print(f"❌ [오류] 페이지 요청 실패 - 상태 코드: {response.status_code}")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            articles = soup.select("ul.list_news li.bx")

            if not articles:
                print("✅ 더 이상 기사가 없습니다. 크롤링 종료.")
                break  # 🔹 뉴스가 없으면 크롤링 종료

            for article in articles:
                title_tag = article.select_one("a.news_tit")
                date_tag = article.select("div.info_group span.info")
                link = title_tag["href"] if title_tag else ""
                title = title_tag["title"] if title_tag else "제목 없음"

                # 유효한 URL인지 확인
                if not link.startswith("http"):
                    continue

                if link in seen_links:
                    continue
                seen_links.add(link)

                news_date = "날짜 없음"
                if date_tag:
                    for tag in date_tag:
                        text = tag.get_text()
                        if "전" in text:
                            news_date = convert_relative_date(text)
                            break

                # **✅ 날짜 검증 추가: 2일 초과하면 크롤링 중단**
                if news_date < start_date:
                    print(f"⏹ [중단] {news_date} 기사는 {start_date} 이전 뉴스이므로 크롤링 종료.")
                    return news_data  # 🔹 최신 뉴스가 아니면 크롤링 종료

                if link:
                    news_data.append({"title": title, "link": link, "news_date": news_date})

            page += 10  # 🔹 다음 페이지로 이동
            time.sleep(random.uniform(2, 5))  # 🔹 랜덤 딜레이 추가하여 탐지 방지

        except Exception as e:
            print(f"❌ [오류] 뉴스 검색 실패: {e}")
            break

    print(f"✅ 총 {len(news_data)}개의 뉴스 링크를 수집했습니다.")
    return news_data

def convert_relative_date(relative_date):
    """
    "1일 전", "2시간 전" 같은 상대적 날짜를 실제 날짜로 변환
    """
    now = datetime.now()
    if "일 전" in relative_date:
        days_ago = int(relative_date.replace("일 전", "").strip())
        return (now - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    elif "시간 전" in relative_date:
        hours_ago = int(relative_date.replace("시간 전", "").strip())
        return (now - timedelta(hours=hours_ago)).strftime('%Y-%m-%d')
    elif "분 전" in relative_date:
        minutes_ago = int(relative_date.replace("분 전", "").strip())
        return (now - timedelta(minutes=minutes_ago)).strftime('%Y-%m-%d')
    return now.strftime('%Y-%m-%d')

def save_to_csv(data, filename):
    """
    데이터를 CSV 파일에 실시간 저장하는 함수
    """
    df = pd.DataFrame(data, columns=["title", "content", "news_date"])
    file_exists = os.path.isfile(filename)
    df.to_csv(filename, mode='a', header=not file_exists, index=False, encoding="utf-8-sig")

def crawl_naver_news():
    """
    네이버 뉴스 검색 결과에서 뉴스 제목, 본문, 날짜 크롤링 후 CSV 저장 (멀티쓰레딩 적용)
    """
    news_data = get_news_links(query)

    collected_news = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(get_news_links, news): news for news in news_data}

        for future in as_completed(futures):
            try:
                news_content = future.result()
                collected_news.append(news_content)
                save_to_csv([news_content], csv_filename)
                print(f"✅ [저장] {news_content['title']}")

            except Exception as e:
                print(f"❌ [오류] 뉴스 처리 실패: {e}")

    print(f"✅ [완료] {len(collected_news)}개의 뉴스 크롤링이 끝났습니다. CSV 파일을 확인하세요! 🚀")

if __name__ == "__main__":
    crawl_naver_news()
