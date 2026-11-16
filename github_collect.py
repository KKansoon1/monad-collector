"""
GitHub Actions 클라우드 전용 고속 브랜드 수집기 (24시간 무인 실행 / PC 불필요)
- 매 실행 시 최대 20분 동안 고속 멀티스레드 수집 후, 결과를 구글 시트(Apps Script 웹훅)로 전송하고 종료
- 시트 전송이 실제로 성공한 건만 collected_history.txt에 기록하여 중복 방지 및 유실 방지
- 비관련 기업/SaaS/소프트웨어/관공서/뉴스 노이즈 필터링 강화
"""

import base64
import json
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse, parse_qs, quote_plus
import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SHEET_WEBHOOK_URL = os.environ.get("SHEET_WEBHOOK_URL", "").strip().strip('"').strip("'")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "monad_auto_2026").strip().strip('"').strip("'")

MAX_RUN_SECONDS = 1200      # 1회 실행 최대 20분 (상태 저장 후 깔끔히 종료)
BATCH_PUSH_SIZE = 10        # 10건 모일 때마다 즉시 구글 시트 전송
MAX_WORKERS = 30
HISTORY_FILE = "collected_history.txt"
STATE_FILE = "collector_state.json"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
]

PRODUCT_CATEGORIES = [
    "밀키트", "간편식", "김치", "반찬", "젓갈", "곱창", "막창", "족발", "보쌈", "떡볶이",
    "돈까스", "만두", "떡갈비", "삼겹살", "한우", "훈제오리", "닭가슴살", "육포", "쥐포", "오징어",
    "전복", "간장게장", "새우장", "연어장", "김부각", "조미김", "멸치", "다시팩", "육수", "참기름",
    "들기름", "고춧가루", "된장", "고추장", "청국장", "사과즙", "배도라지즙", "양배추즙", "흑마늘즙", "석류즙",
    "홍삼", "꿀", "마누카꿀", "프로폴리스", "유산균", "콜라겐", "비타민", "루테인", "오메가3", "단백질쉐이크",
    "효소", "콤부차", "애사비", "그릭요거트", "그래놀라", "견과류", "호두정과", "한과", "약과", "떡",
    "베이커리", "쿠키", "휘낭시에", "소금빵", "베이글", "원두커피", "드립백", "콜드브루", "전통주", "막걸리",
    "세탁세제", "섬유유연제", "주방세제", "식기세척기세제", "얼룩제거제", "탈취제", "방향제", "디퓨저", "캔들", "인센스",
    "수건", "타올", "베개", "경추베개", "이불", "차렵이불", "매트리스", "토퍼", "패드", "커튼",
    "러그", "발매트", "주방매트", "프라이팬", "냄비", "무쇠냄비", "도마", "엔드그레인도마", "식기", "도자기그릇",
    "밀폐용기", "보온병", "텀블러", "수세미", "고무장갑", "물티슈", "화장지", "키친타올", "칫솔", "치약",
    "가습기", "온열매트", "카본매트", "전기매트", "온수매트", "히터", "공기청정기", "제습기", "서큘레이터", "선풍기",
    "음식물처리기", "진공청소기", "로봇청소기", "스팀다리미", "헤어드라이기", "고데기", "마사지기", "안마기", "종아리마사지기", "목마사지기",
    "에어프라이어", "전기포트", "커피머신", "믹서기", "착즙기", "보조배터리", "충전기", "멀티탭", "블랙박스", "차량용방향제",
    "앰플", "세럼", "크림", "선크림", "마스크팩", "클렌징폼", "클렌징오일", "토너패드", "미스트", "아이크림",
    "샴푸", "탈모샴푸", "트리트먼트", "바디워시", "바디로션", "핸드크림", "입욕제", "아기세제", "아기로션", "젖병세정제",
    "강아지간식", "강아지사료", "고양이간식", "고양이모래", "배변패드", "강아지옷", "강아지유모차", "캠핑의자", "캠핑테이블", "침낭",
    "요가복", "레깅스", "아대", "보호대", "깔창", "슬리퍼", "실내화", "잠옷", "파자마", "에코백"
]

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

PRIORITY_PREFIXES = (
    "mkt", "marketing", "partner", "partnership", "md", "biz", "business",
    "pr", "press", "sales", "brand", "collab", "live", "commerce", "ad", "event"
)

EXCLUDE_PREFIXES = (
    "noreply", "no-reply", "privacy", "security", "webmaster", "terms",
    "postmaster", "recruit", "hr", "ir", "abuse", "tax", "legal", "copyright",
    "oceanir", "staff", "feedback"
)

PLATFORM_STORE_DOMAINS = (
    "smartstore.naver.com", "brand.naver.com", "wadiz.kr",
    "tumblbug.com", "idus.com", "ohou.se", "makers.kakao.com"
)

SKIP_EMAIL_DOMAINS = (
    "daangn.com", "coupang.com", "wadiz.kr", "tumblbug.com", "idus.com",
    "ohou.se", "kurly.com", "musinsa.com", "29cm.co.kr", "wconcept.co.kr",
    "zigzag.kr", "ably.com", "oliveyoung.co.kr", "ssg.com", "lotteon.com",
    "gmarket.co.kr", "auction.co.kr", "11st.co.kr", "baemin.com", "toss.im",
    "example.com", "domain.com", "sentry.io", "w3.org", "schema.org",
    "imweb.me", "cafe24.com", "sixshop.com", "makeshop.co.kr", "shopify.com",
    "newswire.co.kr", "mediabee.com", "navercorp.com", "kakaocorp.com",
    "chosun.com", "joongang.co.kr", "donga.com", "mk.co.kr", "hankyung.com", "yna.co.kr",
    "adobe.com", "hancom.com", "estsoft.com", "airbnb.com", "hanwha.com",
    "op.gg", "zigbang.com", "kyobobook.com", "novelpia.com", "casenote.kr",
    "visitseoul.net", "burberry.com", "voyagerx.com", "kidsnote.com", "ubion.co.kr",
    "icbanq.com", "panocean.com", "unidocs.co.kr", "markinfo.co.kr", "smbs.co.kr",
    "lbox.kr", "ezenac.co.kr", "filejo.com", "google.com", "microsoft.com"
)

SKIP_DOMAINS = (
    "blog.naver.com", "cafe.naver.com", "kin.naver.com", "news.naver.com",
    "shopping.naver.com", "search.naver.com", "map.naver.com", "post.naver.com",
    "daum.net", "tistory.com", "brunch.co.kr", "youtube.com", "youtu.be",
    "instagram.com", "facebook.com", "x.com", "twitter.com", "linkedin.com",
    "velog.io", "notion.site", "linktr.ee", "saramin.co.kr", "jobkorea.co.kr",
    "wanted.co.kr", "incruit.com", "scribd.com", "slideshare.net", "wikipedia.org",
    "namu.wiki", "newswire.co.kr", "chosun.com", "joongang.co.kr", "hani.co.kr",
    "mk.co.kr", "hankyung.com", "etnews.com", "zdnet.co.kr", "yna.co.kr",
    "duckduckgo.com", "google.com", "bing.com", "microsoft.com",
    "daangn.com", "bunjang.co.kr", "joongna.com", "toss.im", "baemin.com",
    "yogiyo.co.kr", "aliexpress.com", "temu.com", "amazon.com",
    "airbnb.co.kr", "airbnb.com", "adobe.com", "hancom.com", "estsoft.co.kr",
    "op.gg", "zigbang.com", "kyobobook.co.kr", "casenote.kr", "visitseoul.net",
    "novelpia.com", "voyagerx.com", "kidsnote.com", "icbanq.com", "panocean.com"
)

EXCLUDE_ORG_KEYWORDS = (
    "당근", "번개장터", "중고나라", "중계", "판매대행", "위탁판매",
    "벤더", "밴더", "총판", "소싱대행", "복지관", "재단", "협회", "학회",
    "연구소", "병원", "의원", "치과", "한의원", "성형외과", "피부과", "약국",
    "대학교", "학교", "학원", "어학원", "유치원", "공사", "공단", "시청", "구청",
    "법무법인", "변호사", "세무사", "회계사", "노무사", "공인중개사", "부동산",
    "분양", "인테리어시공", "결혼정보", "장례", "대출", "보험", "물류대행",
    "택배", "운송", "해운", "조선", "컨설팅", "SaaS", "ERP", "클라우드", "보안솔루션",
    "뉴스", "매거진", "커뮤니티", "찾기", "추천", "순위", "랭킹", "모음", "비교", "리스트", "포털",
    "소프트웨어", "유틸리티", "웹하드", "게임", "웹소설", "특허", "상표등록", "지식재산",
    "대법원", "판례", "법률", "외국환", "관광", "여행", "숙박", "에어비앤비",
    "카드", "은행", "증권", "전자부품", "반도체", "방열판", "냉각팬", "에듀", "캠퍼스",
    "어도비", "한글과컴퓨터", "한컴", "알캡처", "노벨피아", "버버리", "한화오션"
)

BAD_BRAND_EXACT = {
    "고객센터", "회사개요", "사업자등록번호", "개인정보", "이용약관", "공지사항",
    "자주묻는질문", "로그인", "회원가입", "장바구니", "주문조회", "마이페이지",
    "상표등록", "요금제", "작용을", "귀사의", "누구나", "단독", "실적", "조건",
    "협력사", "고양시", "경복궁", "러시아", "대법원", "하나카드", "축구", "컬렉션",
    "키프리스란", "개인정보위가", "신세계인터내셔날은", "일리있는 선택"
}

# 실제 쇼핑몰/소비재 브랜드 여부를 가르는 강한 커머스 시그널 (최소 2개 이상 필요)
COMMERCE_SIGNAL_KEYWORDS = (
    "쇼핑몰", "공식몰", "자사몰", "스마트스토어", "브랜드스토어", "와디즈",
    "장바구니", "구매하기", "주문하기", "결제", "무료배송", "배송비", "택배",
    "통신판매업", "통신판매업신고", "공동구매", "공구", "라이브커머스",
    "선물세트", "입점문의", "제휴문의", "할인가", "정가", "적립금", "회원혜택"
)


def get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }


def is_platform_store_url(url: str) -> bool:
    d = urlparse(url).netloc.lower()
    return any(p in d for p in PLATFORM_STORE_DOMAINS)


def get_entity_key(url: str) -> str:
    parsed = urlparse(url)
    d = parsed.netloc.lower().replace("www.", "")
    if any(p in d for p in PLATFORM_STORE_DOMAINS):
        segs = [s for s in parsed.path.split("/") if s][:3]
        return f"{d}/{'/'.join(segs)}" if segs else d
    return d


def is_skip_domain(domain: str) -> bool:
    d = domain.lower()
    if d.endswith(".or.kr") or d.endswith(".go.kr") or d.endswith(".ac.kr") or d.endswith(".edu") or d.endswith(".gov"):
        return True
    if any(p in d for p in PLATFORM_STORE_DOMAINS):
        return False
    return any(s in d for s in SKIP_DOMAINS)


def is_valid_email(email: str) -> bool:
    email = email.lower().strip().rstrip(".,;)>]\"'")
    if len(email) < 6 or len(email) > 60:
        return False
    if any(ext in email for ext in (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js", ".woff", ".ttf")):
        return False
    local, _, domain = email.partition("@")
    if not local or not domain or "." not in domain:
        return False
    if domain.endswith(".or.kr") or domain.endswith(".go.kr") or domain.endswith(".ac.kr") or domain.endswith(".edu") or domain.endswith(".gov"):
        return False
    if any(d in domain for d in SKIP_EMAIL_DOMAINS):
        return False
    if local in EXCLUDE_PREFIXES or any(local.startswith(p + ".") or local.startswith(p + "_") for p in EXCLUDE_PREFIXES):
        return False
    return True


def classify_email_priority(email: str) -> int:
    local = email.split("@")[0].lower()
    if any(k in local for k in PRIORITY_PREFIXES):
        return 1
    if local in ("contact", "info", "hello", "official", "master", "cs", "help", "admin", "shop"):
        return 2
    return 1


def clean_korean_brand_name(raw_title: str) -> str:
    if not raw_title:
        return ""
    if any(k in raw_title for k in EXCLUDE_ORG_KEYWORDS):
        return ""

    if "," in raw_title:
        first_comma = raw_title.split(",")[0].strip()
        if 2 <= len(first_comma) <= 15 and re.search(r"[가-힣]", first_comma):
            raw_title = first_comma

    part = re.split(r"[|\｜::\-–—/\[\]()（）]", raw_title)[0].strip()
    remove_words = (
        "주식회사", "유한회사", "주)", "(주", "유)", "(유",
        "공식 온라인 쇼핑몰", "공식 온라인 스토어", "공식 온라인몰",
        "공식 홈페이지", "공식쇼핑몰", "공식 스토어", "공식스토어",
        "공식몰", "온라인스토어", "온라인몰", "브랜드몰", "스토어", "쇼핑몰", "홈페이지",
        "네이버", "스마트스토어", "브랜드스토어", "와디즈", "텀블벅", "아이디어스"
    )
    for w in remove_words:
        part = part.replace(w, "").strip()

    part = re.sub(r"^.*?[은는이가]\s*역시\s+", "", part)
    part = re.sub(r"^(국가대표|대한민국\s*대표|프리미엄|건강한|맛있는|신선한|정직한|우리집|자연담은)\s+", "", part)
    part = re.sub(r"^(밀키트|화장품|스킨케어|건강기능식품|생활용품|주방용품|리빙|가전)\s+(?=\S)", "", part)
    part = re.sub(r"[^가-힣0-9a-zA-Z\s]", "", part).strip()

    tokens = part.split()
    if len(tokens) >= 3:
        last = tokens[-1]
        first = tokens[0]
        if re.search(r"[가-힣]", first) and 2 <= len(first) <= 8:
            part = first
        elif re.search(r"[가-힣]", last) and 2 <= len(last) <= 8:
            part = last
        else:
            return ""

    if not re.search(r"[가-힣]", part) or len(part) < 2 or len(part) > 12:
        return ""
    if part in BAD_BRAND_EXACT:
        return ""
    if any(k in part for k in EXCLUDE_ORG_KEYWORDS):
        return ""
    # 조사로 끝나는 비정상 문장 조각 제외
    if re.search(r"(은|는|이|가|을|를|의|에서|으로)$", part) and len(part) <= 4:
        return ""
    return part


def extract_company_from_footer(text: str) -> str:
    patterns = [
        r"(?:상호명|상호|회사명|법인명)\s*[:：]?\s*(?:\(주\)|주식회사)?\s*([가-힣a-zA-Z0-9]{2,12})",
        r"주식회사\s+([가-힣a-zA-Z0-9]{2,10})",
        r"\(주\)\s*([가-힣a-zA-Z0-9]{2,10})",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            cand = clean_korean_brand_name(m.group(1))
            if cand:
                return cand
    return ""


def is_aptner_target_page(raw_title: str, page_text: str, min_signals: int = 2) -> bool:
    head_text = f"{raw_title} {page_text[:1500]}"
    if any(bad in raw_title for bad in EXCLUDE_ORG_KEYWORDS):
        return False
    if sum(1 for bad in EXCLUDE_ORG_KEYWORDS if bad in head_text) >= 2:
        return False
    sample = f"{raw_title} {page_text[:5000]}"
    return sum(1 for kw in COMMERCE_SIGNAL_KEYWORDS if kw in sample) >= min_signals


def decode_bing_url(url: str) -> str:
    if "bing.com/ck/a" not in url:
        return url
    try:
        u_param = parse_qs(urlparse(url).query).get("u", [""])[0]
        if u_param.startswith("a1"):
            b64 = u_param[2:]
            b64 += "=" * (-len(b64) % 4)
            decoded = base64.urlsafe_b64decode(b64).decode("utf-8", errors="ignore")
            if decoded.startswith("http"):
                return decoded
    except Exception:
        pass
    return url


def load_history() -> tuple[set[str], set[str], set[str]]:
    seen_brands, seen_emails, seen_domains = set(), set(), set()
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 2:
                    seen_brands.add(parts[0])
                    seen_emails.add(parts[1].lower())
                if len(parts) >= 3 and parts[2]:
                    seen_domains.add(parts[2].lower())
    print(f"[중복 방지 로드] 기존 브랜드 {len(seen_brands)}개 / 이메일 {len(seen_emails)}개")
    return seen_brands, seen_emails, seen_domains


def append_history_batch(items: list[tuple[str, str, str]]):
    if not items:
        return
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        for brand, email, domain in items:
            f.write(f"{brand}\t{email}\t{domain}\n")


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"used_queries": [], "last_newswire_no": 1047250}


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def validate_webhook_url() -> bool:
    if not SHEET_WEBHOOK_URL:
        print("  [오류] SHEET_WEBHOOK_URL 시크릿이 비어 있습니다.")
        return False
    if not SHEET_WEBHOOK_URL.startswith("https://"):
        print("  [오류] SHEET_WEBHOOK_URL이 'https://'로 시작하지 않습니다. GitHub Secrets에 따옴표 없이 전체 웹 앱 URL(https://script.google.com/macros/s/.../exec)을 입력했는지 확인하세요.")
        return False
    if "script.google.com/macros/s/" not in SHEET_WEBHOOK_URL:
        print("  [오류] SHEET_WEBHOOK_URL 형식이 Apps Script 웹 앱 URL이 아닙니다.")
        return False
    return True


def push_to_google_sheet(batch: list[tuple[str, str, str]]) -> bool:
    if not batch:
        return True
    if not validate_webhook_url():
        return False
    rows = [[b, em] for b, em, _ in batch]
    try:
        resp = requests.post(
            SHEET_WEBHOOK_URL,
            json={"secret": WEBHOOK_SECRET, "rows": rows},
            timeout=25,
            allow_redirects=True,
        )
        text_preview = resp.text[:120].replace("\n", " ")
        print(f"  [구글 시트 적재] {len(rows)}건 전송 (HTTP {resp.status_code}) -> {text_preview}")
        if resp.status_code == 200 and ('"ok":true' in resp.text or '"ok": true' in resp.text):
            append_history_batch(batch)
            return True
        return False
    except Exception as e:
        print(f"  [시트 전송 오류] {e}")
        return False


def fetch_soup(url: str, timeout: int = 7) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, headers=get_headers(), timeout=timeout, verify=False)
        if resp.status_code == 200:
            if resp.apparent_encoding and resp.encoding and resp.encoding.lower() == "iso-8859-1":
                resp.encoding = resp.apparent_encoding
            return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException:
        pass
    return None


def extract_brand_and_email(url: str, fallback_brand: str = "") -> tuple[str, str]:
    soup = fetch_soup(url, timeout=6)
    if not soup:
        return "", ""
    raw_title = soup.title.get_text(strip=True) if soup.title else ""
    page_text = soup.get_text(" ", strip=True)
    if not is_aptner_target_page(raw_title, page_text, min_signals=2):
        return "", ""
    brand = clean_korean_brand_name(raw_title) or extract_company_from_footer(page_text) or fallback_brand
    if not brand:
        return "", ""

    found = set()
    urls_to_check = [url]
    if not is_platform_store_url(url):
        base_netloc = urlparse(url).netloc
        sub_keywords = ("contact", "partner", "about", "company", "cs", "support", "제휴", "입점", "문의", "협찬")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            text = a.get_text(strip=True).lower()
            if any(k in href.lower() or k in text for k in sub_keywords):
                full_u = urljoin(url, href)
                if urlparse(full_u).netloc == base_netloc and full_u not in urls_to_check:
                    urls_to_check.append(full_u)

    for idx, target_url in enumerate(urls_to_check[:3]):
        page_soup = soup if idx == 0 else fetch_soup(target_url, timeout=5)
        if not page_soup:
            continue
        txt = page_text if idx == 0 else page_soup.get_text(" ", strip=True)
        for m in EMAIL_REGEX.findall(txt):
            if is_valid_email(m):
                found.add(m.lower())
        for a in page_soup.find_all("a", href=True):
            if a["href"].lower().startswith("mailto:"):
                addr = a["href"][7:].split("?")[0].strip().lower()
                if is_valid_email(addr):
                    found.add(addr)

    if not found:
        return "", ""
    return brand, sorted(found, key=classify_email_priority)[0]


def process_press_url(press_url: str) -> tuple[str, str, str]:
    p_soup = fetch_soup(press_url, timeout=6)
    if not p_soup:
        return "", "", ""
    raw_title = p_soup.title.get_text(strip=True) if p_soup.title else ""
    body_area = p_soup.select_one(".release-story, .news_body, #content, article") or p_soup
    body_text = body_area.get_text(" ", strip=True)
    if any(bad in raw_title for bad in EXCLUDE_ORG_KEYWORDS):
        return "", "", ""
    if sum(1 for kw in COMMERCE_SIGNAL_KEYWORDS if kw in f"{raw_title} {body_text[:2500]}") < 2:
        return "", "", ""

    title_brand = clean_korean_brand_name(raw_title.replace("- 뉴스와이어", "").strip())
    site_url = ""
    for a in body_area.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("http"):
            d = urlparse(href).netloc.lower().replace("www.", "")
            if d and not is_skip_domain(d):
                site_url = href if is_platform_store_url(href) else f"{urlparse(href).scheme}://{urlparse(href).netloc}"
                break

    press_emails = [m.lower() for m in EMAIL_REGEX.findall(body_text) if is_valid_email(m)]
    if press_emails and title_brand:
        best_em = sorted(set(press_emails), key=classify_email_priority)[0]
        ekey = get_entity_key(site_url) if site_url else best_em.split("@")[1]
        if ekey in ("naver.com", "gmail.com", "daum.net", "hanmail.net", "kakao.com"):
            ekey = f"{title_brand}_{best_em}"
        return title_brand, best_em, ekey

    if site_url:
        entity_key = get_entity_key(site_url)
        brand, email = extract_brand_and_email(site_url, fallback_brand=title_brand)
        return brand, email, entity_key
    return "", "", ""


def search_web_multi(query: str) -> list[tuple[str, str, str]]:
    results = []
    seen_urls = set()

    def add_res(u: str, tit: str = "", snip: str = ""):
        if u and u.startswith("http") and u not in seen_urls:
            seen_urls.add(u)
            results.append((u, tit, snip))

    for start in (1, 11):
        try:
            resp = requests.get(
                f"https://search.naver.com/search.naver?where=web&query={quote_plus(query)}&start={start}",
                headers=get_headers(),
                timeout=6,
                verify=False,
            )
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for box in soup.select(".total_wrap, .bx, li.bx"):
                    a = box.select_one("a.link_tit, a.title_link, .total_tit a, a[href^='http']")
                    if a and a.get("href", "").startswith("http") and "naver.com/search" not in a["href"]:
                        add_res(a["href"], a.get_text(" ", strip=True), box.get_text(" ", strip=True))
        except requests.RequestException:
            pass

    for first in (1, 11, 21):
        try:
            resp = requests.get(
                "https://www.bing.com/search",
                params={"q": query, "mkt": "ko-KR", "first": first},
                headers=get_headers(),
                timeout=6,
                verify=False,
            )
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for li in soup.select("li.b_algo"):
                    a = li.select_one("h2 a")
                    p = li.select_one(".b_caption p")
                    if a and a.get("href", "").startswith("http"):
                        add_res(decode_bing_url(a["href"]), a.get_text(" ", strip=True), p.get_text(" ", strip=True) if p else "")
        except requests.RequestException:
            pass

    return results


def build_queries() -> list[str]:
    cats = list(PRODUCT_CATEGORIES)
    random.shuffle(cats)
    queries = []
    for c in cats:
        queries.append(f"{c} 공식몰 고객센터 이메일")
        queries.append(f"site:smartstore.naver.com {c} 이메일")
        queries.append(f"site:wadiz.kr/web/campaign/detail {c} 이메일")
        queries.append(f'"{c}" "대표자 이메일:" "통신판매업신고번호"')
    return queries


def main():
    start_ts = time.time()
    if not validate_webhook_url():
        raise SystemExit(1)

    seen_brands, seen_emails, seen_domains = load_history()
    state = load_state()
    lock = threading.Lock()
    pending_push = []
    added_count = 0
    pushed_count = 0

    def record_item(brand: str, email: str, entity_key: str):
        nonlocal pending_push, added_count, pushed_count
        batch_to_send = []
        with lock:
            if not brand or not email or not entity_key:
                return
            email = email.lower().strip()
            if entity_key in seen_domains or brand in seen_brands or email in seen_emails:
                return
            seen_domains.add(entity_key)
            seen_brands.add(brand)
            seen_emails.add(email)
            pending_push.append((brand, email, entity_key))
            added_count += 1
            print(f"  + [수집 {added_count}건] {brand} | {email}")
            if len(pending_push) >= BATCH_PUSH_SIZE:
                batch_to_send = pending_push
                pending_push = []
        if batch_to_send:
            if push_to_google_sheet(batch_to_send):
                pushed_count += len(batch_to_send)

    # 1. 뉴스와이어 최신 번호 역순 스캔 (회차당 180건 스캔)
    start_no = int(state.get("last_newswire_no", 1047250))
    if start_no < 1015000:
        start_no = 1047250
    for batch_start in range(start_no, start_no - 180, -60):
        if time.time() - start_ts > MAX_RUN_SECONDS:
            break
        urls = [f"https://www.newswire.co.kr/newsRead.php?no={no}" for no in range(batch_start, batch_start - 60, -1)]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_press_url, u) for u in urls]
            for fut in as_completed(futures):
                try:
                    b, em, ek = fut.result()
                    record_item(b, em, ek)
                except Exception:
                    pass
        state["last_newswire_no"] = batch_start - 60
        save_state(state)

    # 2. 멀티 검색엔진 스윕 (남은 시간 동안 계속 실행)
    all_queries = build_queries()
    used_set = set(state.get("used_queries", []))
    remaining = [q for q in all_queries if q not in used_set]
    if not remaining:
        state["used_queries"] = []
        remaining = all_queries

    for query in remaining:
        if time.time() - start_ts > MAX_RUN_SECONDS:
            break
        print(f"[검색] {query}")
        hits = search_web_multi(query)
        if hits:
            def handle_hit(item):
                page_url, tit, snippet = item
                snip_emails = [m.lower() for m in EMAIL_REGEX.findall(f"{tit} {snippet}") if is_valid_email(m)]
                snip_brand = clean_korean_brand_name(tit)
                entity_key = get_entity_key(page_url)
                domain_only = urlparse(page_url).netloc.lower().replace("www.", "")

                if snip_emails and snip_brand and is_aptner_target_page(tit, snippet, min_signals=2):
                    best_em = sorted(set(snip_emails), key=classify_email_priority)[0]
                    ekey = entity_key if (domain_only and not is_skip_domain(domain_only)) else f"{snip_brand}_{best_em}"
                    record_item(snip_brand, best_em, ekey)
                    return

                if not domain_only or is_skip_domain(domain_only):
                    return
                with lock:
                    if entity_key in seen_domains:
                        return
                b, em = extract_brand_and_email(page_url, fallback_brand=snip_brand)
                if not em and b and snip_emails:
                    em = sorted(set(snip_emails), key=classify_email_priority)[0]
                record_item(b, em, entity_key)

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                list(executor.map(handle_hit, hits))

        state.setdefault("used_queries", []).append(query)
        save_state(state)

    if pending_push:
        if push_to_google_sheet(pending_push):
            pushed_count += len(pending_push)
    print(f"=== 이번 클라우드 수집 완료: 발굴 {added_count}건 / 시트 적재 성공 {pushed_count}건 ===")


if __name__ == "__main__":
    main()
