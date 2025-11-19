# api_fetchers.py
"""
Final robust fetcher API.
- FastAPI endpoints for Codeforces, LeetCode (robust mirrors + optional Playwright), CodeChef, Duolingo, HackerRank
- TTL cache, rate limiter, CORS
- LeetCode fetcher: tries multiple public mirrors, tolerant JSON parsing, optional Playwright fallback
"""
from fastapi import FastAPI, HTTPException, Request, Depends, status
from fastapi.middleware.cors import CORSMiddleware
import requests
from bs4 import BeautifulSoup
from collections import defaultdict, deque
from typing import Dict, Any
import threading
import time
import re
import json

# Optional playwright (only used if installed and mirror fail)
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

CACHE_TTL = 60         # seconds
RATE_LIMIT = 30        # requests
RATE_PERIOD = 60       # seconds

# Browser-like headers for generic scrapes
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://leetcode.com/",
}

# LeetCode mirror providers (try in order)
LEETCODE_MIRRORS = [
    "https://alfa-leetcode-api.onrender.com",              # often reliable
    "https://leetcode-api-faisalshohag.vercel.app",       # alternative (may vary)
    "https://leetcode-api.faisal.sh",                     # alternative host (if available)
    "https://leetcode-stats-api.herokuapp.com",           # alternative mirror
]

# Alternative LeetCode stats API (different format)
LEETCODE_STATS_API = "https://leetcode-stats-api.herokuapp.com"

# ---------------------------------------------------
# TTL CACHE
# ---------------------------------------------------

class TTLCache:
    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            rec = self._data.get(key)
            if not rec: return None
            val, exp = rec
            if time.time() > exp:
                del self._data[key]
                return None
            return val

    def set(self, key: str, val: Any, ttl=CACHE_TTL):
        with self._lock:
            self._data[key] = (val, time.time() + ttl)

    def clear(self):
        with self._lock:
            self._data.clear()

cache = TTLCache()

# ---------------------------------------------------
# RATE LIMITER
# ---------------------------------------------------

_rate_buckets = defaultdict(lambda: deque())
_rate_lock = threading.Lock()

def check_rate_limit(request: Request):
    ip = request.client.host
    now = time.time()
    with _rate_lock:
        dq = _rate_buckets[ip]
        while dq and dq[0] <= now - RATE_PERIOD:
            dq.popleft()
        if len(dq) >= RATE_LIMIT:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded ({RATE_LIMIT} req / {RATE_PERIOD}s)"
            )
        dq.append(now)
    return True

# ---------------------------------------------------
# RESPONSE TEMPLATES
# ---------------------------------------------------

def leetcode_template(username):
    return {
        "message": "LeetCode report fetched successfully",
        "report": {
            "error": True,
            "errorMessage": "Detailed problem statistics not available from public APIs. Profile data retrieved successfully.",
            "username": username,
            "totalSolved": 0,
            "easySolved": 0,
            "mediumSolved": 0,
            "hardSolved": 0,
            "acceptanceRate": 0,
            "ranking": None,
            "reputation": 0,
            "contributionPoints": 0,
            "streak": 0,
            "totalActiveDays": 0,
            "submissionCalendar": {}
        }
    }

def codechef_template(username):
    return {
        "message": "CodeChef report generated successfully",
        "report": {
            "username": username,
            "name": username,
            "stars": "Unrated",
            "rating": "0",
            "highestRating": "N/A",
            "globalRank": "N/A",
            "countryRank": "N/A",
            "problemsSolved": "0",
            "country": None,
            "institution": None,
            "profileUrl": f"https://www.codechef.com/users/{username}"
        }
    }

def duolingo_template(username):
    return {
        "message": "Duolingo report generated successfully",
        "report": {
            "username": username,
            "name": None,
            "streak": 0,
            "totalXp": 0,
            "languages": [],
            "avatarUrl": None,
            "country": None,
            "bio": None,
            "creationDate": None,
            "learningLanguage": None,
            "fromLanguage": None
        }
    }

def codeforces_template(username):
    return {
        "message": "Codeforces report generated successfully",
        "report": {
            "username": username,
            "rating": 0,
            "maxRating": 0,
            "rank": "unrated",
            "maxRank": "unrated",
            "organization": "N/A",
            "contribution": 0,
            "friendOfCount": 0,
            "totalContests": 0,
            "avgChange": 0,
            "lastContest": None,
            "problemsSolved": 0,
            "firstName": None,
            "lastName": None,
            "country": None,
            "city": None,
            "registrationTimeSeconds": None,
            "lastOnlineTimeSeconds": None,
            "profileUrl": f"https://codeforces.com/profile/{username}",
            "avatar": "https://userpic.codeforces.org/no-avatar.jpg"
        }
    }

def hackerrank_template(username):
    return {
        "username": username,
        "fullName": None,
        "bio": None,
        "country": None,
        "profileImage": None,
        "followersCount": 0,
        "followingCount": 0,
        "socialLinks": {"github": None, "linkedin": None, "twitter": None, "website": None},
        "badges": [],
        "skills": [],
        "certifications": [],
        "articles": [],
        "contest": {"rating": None, "globalRank": None, "percentile": None}
    }

# ---------------------------------------------------
# LeetCode robust fetcher
# ---------------------------------------------------

def _normalize_leetcode_json(j: Dict) -> Dict:
    """Given a variety of possible mirror JSON shapes, extract a canonical dict with keys we need."""
    # Possible shapes:
    # 1) {"data": {"matchedUser": {...}, "userContestRanking": {...}}}
    # 2) {"matchedUser": {...}, ...}
    # 3) mirror-specific flat shape
    out = {}
    if not isinstance(j, dict):
        return out

    # prefer j['data']['matchedUser']
    mu = None
    if "data" in j and isinstance(j["data"], dict):
        mu = j["data"].get("matchedUser") or j["data"].get("user")
    if not mu:
        mu = j.get("matchedUser") or j.get("user") or j.get("profile")

    if mu and isinstance(mu, dict):
        out["matchedUser"] = mu
        # Ensure submitStatsGlobal is preserved
        if "submitStatsGlobal" in mu:
            out["matchedUser"]["submitStatsGlobal"] = mu["submitStatsGlobal"]
        if "submitStats" in mu:
            out["matchedUser"]["submitStats"] = mu["submitStats"]

    # sometimes submitStats are at top-level
    if "submitStatsGlobal" in j:
        out.setdefault("matchedUser", {})["submitStatsGlobal"] = j.get("submitStatsGlobal")
    if "submitStats" in j:
        out.setdefault("matchedUser", {})["submitStats"] = j.get("submitStats")

    # contest data
    ucr = None
    if "data" in j and isinstance(j["data"], dict):
        ucr = j["data"].get("userContestRanking")
    if not ucr:
        ucr = j.get("userContestRanking") or j.get("userContestRankingHistory")
    if ucr:
        out["userContestRanking"] = ucr

    # some mirrors return a 'submissionCalendar' top-level
    if "submissionCalendar" in j:
        out.setdefault("matchedUser", {}).setdefault("submissionCalendar", j.get("submissionCalendar"))

    # also copy through common top-level fields (acceptanceRate, streak, activeDays)
    for k in ("acceptanceRate", "streak", "activeDays", "submissionCalendar"):
        if k in j:
            out[k] = j[k]

    return out


def _extract_stats_from_normalized(nj: Dict, username: str) -> Dict:
    tpl = leetcode_template(username)
    tpl["report"]["error"] = True

    mu = nj.get("matchedUser") if nj else None
    if not mu:
        return tpl

    prof = mu.get("profile") or {}
    
    # Try multiple possible locations for submitStats
    ssg = mu.get("submitStatsGlobal") or mu.get("submitStats") or {}
    ac = ssg.get("acSubmissionNum") or []
    
    # If ac is empty, try alternative structure
    if not ac and "submitStats" in mu:
        ssg = mu.get("submitStats") or {}
        ac = ssg.get("acSubmissionNum") or []
    
    # Also try submitStats at top level of matchedUser
    if not ac:
        ssg = mu.get("submitStatsGlobal") or {}
        ac = ssg.get("acSubmissionNum") or []

    total = easy = medium = hard = 0
    for row in ac:
        if not isinstance(row, dict):
            continue
        diff = (row.get("difficulty") or "").lower()
        try:
            c = int(row.get("count") or row.get("submissions") or 0)
        except Exception:
            c = 0
        if "all" in diff or diff == "":
            total = c
        elif "easy" in diff:
            easy = c
        elif "medium" in diff:
            medium = c
        elif "hard" in diff:
            hard = c

    # Handle submissionCalendar - might be string or dict
    submission_cal = {}
    cal_data = mu.get("submissionCalendar") or nj.get("submissionCalendar")
    if cal_data:
        if isinstance(cal_data, str):
            try:
                submission_cal = json.loads(cal_data)
            except:
                submission_cal = {}
        elif isinstance(cal_data, dict):
            submission_cal = cal_data
    
    # Handle streak and activeDays from userCalendar
    streak = 0
    total_active_days = 0
    user_cal = mu.get("userCalendar") or {}
    if user_cal:
        streak = user_cal.get("streak") or 0
        total_active_days = user_cal.get("totalActiveDays") or 0
    
    # Fallback to top-level values
    if not streak:
        streak = nj.get("streak") or mu.get("streak") or 0
    if not total_active_days:
        total_active_days = nj.get("activeDays") or mu.get("activeDays") or 0

    # Check if we have actual stats or just partial data
    has_stats = total > 0 or easy > 0 or medium > 0 or hard > 0
    
    # Update error message based on what we have
    error_msg = None
    if not has_stats:
        if submission_cal or prof.get("ranking"):
            error_msg = "Profile data retrieved, but detailed problem statistics (totalSolved, easySolved, etc.) are not available from public APIs. LeetCode's GraphQL API requires authentication."
        else:
            error_msg = "Unable to retrieve LeetCode profile data. User may not exist or profile may be private."
    
    tpl["report"].update({
        "error": False if has_stats else True,  # Mark as error if no stats found
        "errorMessage": error_msg if error_msg else None,
        "username": username,
        "totalSolved": total,
        "easySolved": easy,
        "mediumSolved": medium,
        "hardSolved": hard,
        "acceptanceRate": nj.get("acceptanceRate") or mu.get("acceptanceRate") or 0,
        "ranking": prof.get("ranking") if prof else None,
        "reputation": prof.get("reputation", 0) if prof else 0,
        "contributionPoints": prof.get("postViewCount", 0) if prof else 0,
        "streak": streak,
        "totalActiveDays": total_active_days,
        "submissionCalendar": submission_cal
    })

    return tpl


def fetch_leetcode_live(username: str) -> Dict:
    """Robust LeetCode fetcher with direct web scraping.
    Strategy:
      1) Try Playwright to scrape the actual profile page and intercept GraphQL calls
      2) Fallback to direct GraphQL API call
      3) Fallback to HTML scraping with BeautifulSoup
      4) Try mirrors as last resort
    """
    tpl = leetcode_template(username)

    # Strategy 1: Use Playwright to scrape and intercept GraphQL
    if PLAYWRIGHT_AVAILABLE:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # Intercept network requests to catch GraphQL responses
                graphql_data = {}
                
                def handle_response(response):
                    if "graphql" in response.url.lower():
                        try:
                            data = response.json()
                            if isinstance(data, dict) and "data" in data:
                                graphql_data.update(data["data"])
                        except:
                            pass
                
                page.on("response", handle_response)
                
                # Navigate to profile page
                page.goto(f"https://leetcode.com/{username}/", wait_until="networkidle", timeout=30000)
                
                # Wait a bit for GraphQL calls to complete
                page.wait_for_timeout(3000)
                
                # Try to extract data from page context
                try:
                    # Check if we got GraphQL data
                    if graphql_data:
                        normalized = _normalize_leetcode_json({"data": graphql_data})
                        if normalized.get("matchedUser"):
                            browser.close()
                            return _extract_stats_from_normalized(normalized, username)
                    
                    # Try to get data from window objects
                    page_data = page.evaluate("""
                        () => {
                            if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                            if (window.__INITIAL_DATA__) return window.__INITIAL_DATA__;
                            if (window.__APOLLO_STATE__) return window.__APOLLO_STATE__;
                            return null;
                        }
                    """)
                    
                    if page_data:
                        normalized = _normalize_leetcode_json(page_data)
                        if normalized.get("matchedUser"):
                            browser.close()
                            return _extract_stats_from_normalized(normalized, username)
                    
                    # Try direct GraphQL query via page context
                    graphql_query = """
                    query userProfile($username: String!) {
                        matchedUser(username: $username) {
                            username
                            profile {
                                realName
                                ranking
                                reputation
                                postViewCount
                            }
                            submitStatsGlobal {
                                acSubmissionNum {
                                    difficulty
                                    count
                                    submissions
                                }
                            }
                            submissionCalendar
                            userCalendar {
                                activeYears
                                streak
                                totalActiveDays
                            }
                        }
                        userContestRanking(username: $username) {
                            rating
                            globalRanking
                            totalParticipants
                            topPercentage
                        }
                    }
                    """
                    
                    graphql_result = page.evaluate(f"""
                        async () => {{
                            try {{
                                const response = await fetch('https://leetcode.com/graphql/', {{
                                    method: 'POST',
                                    headers: {{
                                        'Content-Type': 'application/json',
                                        'Referer': 'https://leetcode.com/{username}/'
                                    }},
                                    body: JSON.stringify({{
                                        query: `{graphql_query.replace('`', '\\`')}`,
                                        variables: {{ username: '{username}' }}
                                    }})
                                }});
                                return await response.json();
                            }} catch (e) {{
                                return null;
                            }}
                        }}
                    """)
                    
                    if graphql_result and isinstance(graphql_result, dict):
                        normalized = _normalize_leetcode_json(graphql_result)
                        if normalized.get("matchedUser"):
                            browser.close()
                            return _extract_stats_from_normalized(normalized, username)
                    
                    # Fallback: Scrape HTML directly
                    html_content = page.content()
                    soup = BeautifulSoup(html_content, "html.parser")
                    
                    # Extract stats from HTML
                    stats = _scrape_leetcode_html(soup, username)
                    if stats and stats.get("totalSolved", 0) > 0:
                        browser.close()
                        tpl["report"].update(stats)
                        tpl["report"]["error"] = False
                        return tpl
                    
                except Exception as e:
                    pass
                
                browser.close()
        except Exception as e:
            pass

    # Strategy 2: Direct GraphQL API call with requests
    # Try multiple GraphQL query variations
    graphql_queries = [
        # Query 1: Standard query
        """
        query userProfile($username: String!) {
            matchedUser(username: $username) {
                username
                profile {
                    realName
                    ranking
                    reputation
                    postViewCount
                }
                submitStatsGlobal {
                    acSubmissionNum {
                        difficulty
                        count
                        submissions
                    }
                }
                submissionCalendar
                userCalendar {
                    activeYears
                    streak
                    totalActiveDays
                }
            }
            userContestRanking(username: $username) {
                rating
                globalRanking
                totalParticipants
                topPercentage
            }
        }
        """,
        # Query 2: Alternative query structure
        """
        query getUserProfile($username: String!) {
            allQuestionsCount {
                difficulty
                count
            }
            matchedUser(username: $username) {
                username
                profile {
                    realName
                    ranking
                    reputation
                    postViewCount
                }
                submitStats: submitStatsGlobal {
                    acSubmissionNum {
                        difficulty
                        count
                        submissions
                    }
                }
                submissionCalendar
                userCalendar {
                    activeYears
                    streak
                    totalActiveDays
                }
            }
        }
        """
    ]
    
    for graphql_query in graphql_queries:
        try:
            session = requests.Session()
            session.headers.update({
                **HEADERS,
                "Content-Type": "application/json",
                "Origin": "https://leetcode.com",
                "Referer": f"https://leetcode.com/{username}/",
                "x-csrftoken": "fetch",  # Some endpoints may need this
            })
            
            response = session.post(
                "https://leetcode.com/graphql/",
                json={
                    "query": graphql_query,
                    "variables": {"username": username}
                },
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                # Check for errors in response
                if data.get("errors"):
                    continue
                normalized = _normalize_leetcode_json(data)
                if normalized.get("matchedUser"):
                    result = _extract_stats_from_normalized(normalized, username)
                    # If we got stats, return immediately
                    if result["report"].get("totalSolved", 0) > 0:
                        return result
                    # Otherwise continue to try next query
        except Exception:
            continue

    # Strategy 3: Try alternative stats API (leetcode-stats-api) - THIS ONE WORKS! Try it early
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        stats_url = f"{LEETCODE_STATS_API}/{username}"
        r = session.get(stats_url, timeout=15)
        if r.status_code == 200:
            try:
                j = r.json()
                # This API returns: {"status": "success", "totalSolved": 431, ...}
                if j.get("status") == "success" and j.get("totalSolved", 0) > 0:
                    # Extract submission calendar if available
                    submission_cal = j.get("submissionCalendar", {})
                    if isinstance(submission_cal, str):
                        try:
                            submission_cal = json.loads(submission_cal)
                        except:
                            submission_cal = {}
                    
                    # This API returns all the stats we need!
                    result = {
                        "error": False,
                        "username": username,
                        "totalSolved": j.get("totalSolved", 0),
                        "easySolved": j.get("easySolved", 0),
                        "mediumSolved": j.get("mediumSolved", 0),
                        "hardSolved": j.get("hardSolved", 0),
                        "acceptanceRate": round(j.get("acceptanceRate", 0), 2),
                        "ranking": j.get("ranking"),
                        "reputation": j.get("reputation", 0),
                        "contributionPoints": j.get("contributionPoints", 0),
                        "streak": 0,  # Not available from this API
                        "totalActiveDays": 0,  # Not available from this API
                        "submissionCalendar": submission_cal
                    }
                    tpl["report"].update(result)
                    return tpl
            except Exception as e:
                pass
    except:
        pass

    # Strategy 4: HTML scraping with requests + BeautifulSoup (slower, less reliable)
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        response = session.get(f"https://leetcode.com/{username}/", timeout=15)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            stats = _scrape_leetcode_html(soup, username)
            if stats and stats.get("totalSolved", 0) > 0:
                tpl["report"].update(stats)
                tpl["report"]["error"] = False
                return tpl
    except Exception:
        pass

    # Strategy 5: Try mirrors as last resort
    # Note: Mirrors often don't include submitStats, so we'll try them but may still need GraphQL
    mirror_result = None
    session = requests.Session()
    session.headers.update(HEADERS)
    for base in LEETCODE_MIRRORS:
        try:
            url = base.rstrip('/') + '/' + username
            r = session.get(url, timeout=10)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            try:
                j = r.json()
            except Exception:
                text = r.text
                m = re.search(r'\{.*\}', text, flags=re.S)
                if m:
                    try:
                        j = json.loads(m.group(0))
                    except Exception:
                        j = None
                else:
                    j = None

            if not j:
                continue

            if "data" in j and isinstance(j["data"], dict) and ("matchedUser" in j["data"]):
                normalized = _normalize_leetcode_json(j["data"])
            else:
                normalized = _normalize_leetcode_json(j)

            if normalized.get("matchedUser"):
                result = _extract_stats_from_normalized(normalized, username)
                # Check if we got actual stats (not just profile info)
                if result["report"].get("totalSolved", 0) > 0:
                    return result
                # Save partial result in case GraphQL also fails
                if not mirror_result:
                    mirror_result = result
        except Exception:
            continue

    # If we got partial data from mirrors but no stats, try GraphQL one more time with better error handling
    if mirror_result and mirror_result["report"].get("totalSolved", 0) == 0:
        try:
            session = requests.Session()
            session.headers.update({
                **HEADERS,
                "Content-Type": "application/json",
                "Origin": "https://leetcode.com",
                "Referer": f"https://leetcode.com/{username}/"
            })
            
            graphql_query = """
            query userProfile($username: String!) {
                matchedUser(username: $username) {
                    username
                    profile {
                        realName
                        ranking
                        reputation
                        postViewCount
                    }
                    submitStatsGlobal {
                        acSubmissionNum {
                            difficulty
                            count
                            submissions
                        }
                    }
                    submissionCalendar
                    userCalendar {
                        activeYears
                        streak
                        totalActiveDays
                    }
                }
                userContestRanking(username: $username) {
                    rating
                    globalRanking
                    totalParticipants
                    topPercentage
                }
            }
            """
            
            response = session.post(
                "https://leetcode.com/graphql/",
                json={
                    "query": graphql_query,
                    "variables": {"username": username}
                },
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("data") and data["data"].get("matchedUser"):
                    normalized = _normalize_leetcode_json(data)
                    if normalized.get("matchedUser"):
                        result = _extract_stats_from_normalized(normalized, username)
                        if result["report"].get("totalSolved", 0) > 0:
                            return result
                        # Merge with mirror data if GraphQL has stats but mirror has other info
                        if mirror_result:
                            # Prefer GraphQL stats, but keep mirror data for fields GraphQL might not have
                            for key in ["ranking", "reputation", "contributionPoints"]:
                                if mirror_result["report"].get(key) and not result["report"].get(key):
                                    result["report"][key] = mirror_result["report"][key]
                            return result
        except Exception:
            pass

    # Return best available result (mirror data if available, otherwise template)
    if mirror_result:
        return mirror_result

    # Return template with error
    tpl["report"]["error"] = True
    return tpl


def _scrape_leetcode_html(soup: BeautifulSoup, username: str) -> Dict:
    """Extract LeetCode stats from HTML page."""
    stats = {
        "username": username,
        "totalSolved": 0,
        "easySolved": 0,
        "mediumSolved": 0,
        "hardSolved": 0,
        "acceptanceRate": 0,
        "ranking": None,
        "reputation": 0,
        "contributionPoints": 0,
        "streak": 0,
        "totalActiveDays": 0,
        "submissionCalendar": {}
    }
    
    try:
        # Try to find stats in various possible locations
        # Look for script tags with JSON data
        scripts = soup.find_all("script")
        for script in scripts:
            if script.string:
                # Look for __NEXT_DATA__ or similar - this contains the full page data
                if "__NEXT_DATA__" in script.string:
                    try:
                        # Extract the full __NEXT_DATA__ JSON
                        start = script.string.find('{"props"')
                        if start == -1:
                            start = script.string.find('{"pageProps"')
                        if start != -1:
                            # Find the matching closing brace
                            brace_count = 0
                            end = start
                            for i, char in enumerate(script.string[start:], start):
                                if char == '{':
                                    brace_count += 1
                                elif char == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        end = i + 1
                                        break
                            
                            if end > start:
                                data_str = script.string[start:end]
                                data = json.loads(data_str)
                                # Navigate through __NEXT_DATA__ structure
                                if "props" in data and "pageProps" in data["props"]:
                                    page_props = data["props"]["pageProps"]
                                    # Try to find user data in various locations
                                    if "dehydratedState" in page_props:
                                        dehydrated = page_props["dehydratedState"]
                                        if "queries" in dehydrated:
                                            for query in dehydrated["queries"]:
                                                if "state" in query and "data" in query["state"]:
                                                    query_data = query["state"]["data"]
                                                    normalized = _normalize_leetcode_json(query_data)
                                                    if normalized.get("matchedUser"):
                                                        mu = normalized["matchedUser"]
                                                        ssg = mu.get("submitStatsGlobal") or {}
                                                        ac = ssg.get("acSubmissionNum") or []
                                                        
                                                        for row in ac:
                                                            diff = (row.get("difficulty") or "").lower()
                                                            count = int(row.get("count") or 0)
                                                            if "all" in diff:
                                                                stats["totalSolved"] = count
                                                            elif "easy" in diff:
                                                                stats["easySolved"] = count
                                                            elif "medium" in diff:
                                                                stats["mediumSolved"] = count
                                                            elif "hard" in diff:
                                                                stats["hardSolved"] = count
                                                        
                                                        prof = mu.get("profile") or {}
                                                        stats["ranking"] = prof.get("ranking")
                                                        stats["reputation"] = prof.get("reputation", 0)
                                                        
                                                        if stats["totalSolved"] > 0:
                                                            return stats
                    except Exception as e:
                        continue
                
                # Also try simpler pattern matching for matchedUser
                if "matchedUser" in script.string:
                    try:
                        # Try to extract JSON object containing matchedUser
                        patterns = [
                            r'\{[^{}]*"matchedUser"[^{}]*\{[^}]*submitStatsGlobal[^}]*\{[^}]*acSubmissionNum[^}]*\[[^\]]*\][^\}]*\}[^\}]*\}[^\}]*\}',
                            r'"matchedUser"\s*:\s*\{[^}]*"submitStatsGlobal"[^}]*"acSubmissionNum"\s*:\s*\[[^\]]*\]',
                        ]
                        for pattern in patterns:
                            match = re.search(pattern, script.string, re.DOTALL)
                            if match:
                                # Try to extract a larger context
                                start = max(0, match.start() - 100)
                                end = min(len(script.string), match.end() + 500)
                                context = script.string[start:end]
                                # Try to find a valid JSON object
                                brace_start = context.rfind('{', 0, match.start() - start)
                                if brace_start != -1:
                                    try:
                                        # Try to parse from brace_start
                                        test_str = context[brace_start:]
                                        # Find matching closing brace
                                        brace_count = 0
                                        brace_end = 0
                                        for i, char in enumerate(test_str):
                                            if char == '{':
                                                brace_count += 1
                                            elif char == '}':
                                                brace_count -= 1
                                                if brace_count == 0:
                                                    brace_end = i + 1
                                                    break
                                        if brace_end > 0:
                                            data = json.loads(test_str[:brace_end])
                                            normalized = _normalize_leetcode_json(data)
                                            if normalized.get("matchedUser"):
                                                mu = normalized["matchedUser"]
                                                ssg = mu.get("submitStatsGlobal") or {}
                                                ac = ssg.get("acSubmissionNum") or []
                                                
                                                for row in ac:
                                                    diff = (row.get("difficulty") or "").lower()
                                                    count = int(row.get("count") or 0)
                                                    if "all" in diff:
                                                        stats["totalSolved"] = count
                                                    elif "easy" in diff:
                                                        stats["easySolved"] = count
                                                    elif "medium" in diff:
                                                        stats["mediumSolved"] = count
                                                    elif "hard" in diff:
                                                        stats["hardSolved"] = count
                                                
                                                if stats["totalSolved"] > 0:
                                                    return stats
                                    except:
                                        continue
                    except:
                        continue
        
        # Try to find stats in text/divs (fallback)
        # Look for stats in common LeetCode profile page structures
        # Pattern: "Solved" followed by numbers, or difficulty badges with counts
        
        # Look for solved problems count - common patterns
        solved_patterns = [
            soup.find("div", string=re.compile(r"Solved", re.I)),
            soup.find("span", string=re.compile(r"Solved", re.I)),
            soup.find(text=re.compile(r"Problems Solved", re.I)),
            soup.find(text=re.compile(r"Total.*Solved", re.I)),
        ]
        
        for pattern in solved_patterns:
            if pattern:
                parent = pattern.parent if hasattr(pattern, 'parent') else pattern
                if parent:
                    text = parent.get_text() if hasattr(parent, 'get_text') else str(parent)
                    numbers = re.findall(r'\d+', text)
                    if numbers:
                        try:
                            stats["totalSolved"] = int(numbers[0])
                            break
                        except:
                            pass
        
        # Look for difficulty-specific stats
        # Common class names: difficulty-easy, difficulty-medium, difficulty-hard
        for difficulty in ["easy", "medium", "hard"]:
            # Try multiple selectors
            selectors = [
                f'div[class*="{difficulty}"]',
                f'span[class*="{difficulty}"]',
                f'div[class*="difficulty-{difficulty}"]',
            ]
            for selector in selectors:
                try:
                    elements = soup.select(selector)
                    for elem in elements:
                        text = elem.get_text()
                        numbers = re.findall(r'\d+', text)
                        if numbers:
                            try:
                                count = int(numbers[0])
                                if difficulty == "easy":
                                    stats["easySolved"] = max(stats["easySolved"], count)
                                elif difficulty == "medium":
                                    stats["mediumSolved"] = max(stats["mediumSolved"], count)
                                elif difficulty == "hard":
                                    stats["hardSolved"] = max(stats["hardSolved"], count)
                            except:
                                pass
                except:
                    continue
        
        # Try to find stats in data attributes or JSON-LD
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld and json_ld.string:
            try:
                data = json.loads(json_ld.string)
                # Extract stats if available in structured data
                if isinstance(data, dict):
                    # Look for common patterns
                    pass
            except:
                pass
        
    except Exception:
        pass
    
    return stats

# ---------------------------------------------------
# Other simple fetchers (Codeforces, CodeChef, Duolingo, HackerRank)
# Keep them mostly as before, tolerant and simple
# ---------------------------------------------------

def fetch_codeforces_live(username: str) -> Dict:
    tpl = codeforces_template(username)
    try:
        # Get user info
        r = requests.get(
            "https://codeforces.com/api/user.info",
            params={"handles": username},
            headers=HEADERS,
            timeout=10
        )
        r.raise_for_status()
        j = r.json()
        if j.get("status") == "OK":
            u = j["result"][0]
            tpl["report"]["rating"] = u.get("rating", 0)
            tpl["report"]["maxRating"] = u.get("maxRating", 0)
            tpl["report"]["rank"] = u.get("rank", "unrated")
            tpl["report"]["maxRank"] = u.get("maxRank", "unrated")
            tpl["report"]["organization"] = u.get("organization", "N/A")
            tpl["report"]["contribution"] = u.get("contribution", 0)
            tpl["report"]["friendOfCount"] = u.get("friendOfCount", 0)
            tpl["report"]["avatar"] = u.get("titlePhoto", tpl["report"]["avatar"])
            # Additional fields
            tpl["report"]["firstName"] = u.get("firstName", None)
            tpl["report"]["lastName"] = u.get("lastName", None)
            tpl["report"]["country"] = u.get("country", None)
            tpl["report"]["city"] = u.get("city", None)
            tpl["report"]["registrationTimeSeconds"] = u.get("registrationTimeSeconds", None)
            tpl["report"]["lastOnlineTimeSeconds"] = u.get("lastOnlineTimeSeconds", None)
        
        # Get contest history for additional stats
        try:
            r2 = requests.get(
                "https://codeforces.com/api/user.rating",
                params={"handle": username},
                headers=HEADERS,
                timeout=10
            )
            if r2.status_code == 200:
                j2 = r2.json()
                if j2.get("status") == "OK" and j2.get("result"):
                    contests = j2["result"]
                    tpl["report"]["totalContests"] = len(contests)
                    if contests:
                        # Calculate average change
                        changes = [c.get("newRating", 0) - c.get("oldRating", 0) for c in contests if c.get("oldRating")]
                        if changes:
                            tpl["report"]["avgChange"] = round(sum(changes) / len(changes), 2)
                        # Get last contest
                        last_contest = contests[-1]
                        tpl["report"]["lastContest"] = {
                            "contestId": last_contest.get("contestId"),
                            "contestName": last_contest.get("contestName"),
                            "rank": last_contest.get("rank"),
                            "ratingChange": last_contest.get("newRating", 0) - last_contest.get("oldRating", 0),
                            "newRating": last_contest.get("newRating"),
                            "oldRating": last_contest.get("oldRating")
                        }
        except:
            pass
        
        # Get submission stats (problems solved) - optimized to get recent submissions first
        try:
            # Try to get problems solved count efficiently
            # First try with a reasonable count limit
            r3 = requests.get(
                "https://codeforces.com/api/user.status",
                params={"handle": username, "from": 1, "count": 1000},  # Get up to 1000 recent submissions
                headers=HEADERS,
                timeout=12
            )
            if r3.status_code == 200:
                j3 = r3.json()
                if j3.get("status") == "OK":
                    submissions = j3.get("result", [])
                    solved_problems = set()
                    for sub in submissions:
                        if sub.get("verdict") == "OK":
                            problem = sub.get("problem", {})
                            problem_id = f"{problem.get('contestId', '')}{problem.get('index', '')}"
                            if problem_id:
                                solved_problems.add(problem_id)
                    tpl["report"]["problemsSolved"] = len(solved_problems)
        except:
            pass
    except:
        pass
    return tpl


def fetch_codechef_live(username: str) -> Dict:
    tpl = codechef_template(username)
    try:
        r = requests.get(f"https://www.codechef.com/users/{username}", headers=HEADERS, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Rating
        rating = soup.select_one(".rating-number") or soup.find("div", {"class": "rating-number"})
        if rating and rating.text.strip():
            val = rating.text.strip()
            tpl["report"]["rating"] = val
            # Determine stars based on rating
            try:
                rating_num = int(val)
                if rating_num >= 3000:
                    tpl["report"]["stars"] = "7★"
                elif rating_num >= 2500:
                    tpl["report"]["stars"] = "6★"
                elif rating_num >= 2200:
                    tpl["report"]["stars"] = "5★"
                elif rating_num >= 2000:
                    tpl["report"]["stars"] = "4★"
                elif rating_num >= 1800:
                    tpl["report"]["stars"] = "3★"
                elif rating_num >= 1600:
                    tpl["report"]["stars"] = "2★"
                elif rating_num >= 1400:
                    tpl["report"]["stars"] = "1★"
                else:
                    tpl["report"]["stars"] = "Unrated"
            except:
                tpl["report"]["stars"] = val
        
        # Highest rating
        highest_rating_elem = soup.find(text=re.compile(r"Highest Rating", re.I))
        if highest_rating_elem:
            parent = highest_rating_elem.parent
            if parent:
                next_elem = parent.find_next(string=re.compile(r"\d+"))
                if next_elem:
                    tpl["report"]["highestRating"] = next_elem.strip()
        
        # Problems solved - try multiple patterns
        solved_patterns = [
            soup.find(text=re.compile(r"Problems solved", re.I)),
            soup.find(text=re.compile(r"Problems Solved", re.I)),
            soup.find(text=re.compile(r"Total.*Solved", re.I)),
        ]
        for solved in solved_patterns:
            if solved:
                parent = solved.parent
                if parent:
                    # Look for number in parent or next sibling
                    text = parent.get_text()
                    numbers = re.findall(r'\d+', text)
                    if numbers:
                        tpl["report"]["problemsSolved"] = numbers[0]
                        break
                    # Try next element
                    nxt = parent.find_next(string=re.compile(r"\d+"))
                    if nxt:
                        tpl["report"]["problemsSolved"] = nxt.strip()
                        break
        
        # Global rank
        global_rank_elem = soup.find(text=re.compile(r"Global Rank", re.I))
        if global_rank_elem:
            parent = global_rank_elem.parent
            if parent:
                rank_text = parent.get_text()
                numbers = re.findall(r'[\d,]+', rank_text)
                if numbers:
                    tpl["report"]["globalRank"] = numbers[0].replace(',', '')
        
        # Country rank
        country_rank_elem = soup.find(text=re.compile(r"Country Rank", re.I))
        if country_rank_elem:
            parent = country_rank_elem.parent
            if parent:
                rank_text = parent.get_text()
                numbers = re.findall(r'[\d,]+', rank_text)
                if numbers:
                    tpl["report"]["countryRank"] = numbers[0].replace(',', '')
        
        # Name
        name_tag = soup.find("h2") or soup.find("h1")
        if name_tag and name_tag.text.strip():
            tpl["report"]["name"] = name_tag.text.strip()
        
        # Country
        country_elem = soup.find(text=re.compile(r"Country", re.I))
        if country_elem:
            parent = country_elem.parent
            if parent:
                country_text = parent.get_text()
                # Extract country name (usually after "Country:")
                match = re.search(r'Country[:\s]+([A-Za-z\s]+)', country_text, re.I)
                if match:
                    tpl["report"]["country"] = match.group(1).strip()
        
        # Institution
        institution_elem = soup.find(text=re.compile(r"Institution|Organization|University|School", re.I))
        if institution_elem:
            parent = institution_elem.parent
            if parent:
                inst_text = parent.get_text()
                # Extract institution name
                match = re.search(r'(?:Institution|Organization|University|School)[:\s]+(.+)', inst_text, re.I)
                if match:
                    tpl["report"]["institution"] = match.group(1).strip()
    except:
        pass
    return tpl


def fetch_duolingo_live(username: str) -> Dict:
    tpl = duolingo_template(username)
    try:
        r = requests.get(
            "https://www.duolingo.com/2017-06-30/users",
            params={"username": username},
            headers=HEADERS,
            timeout=12
        )
        if r.status_code == 200:
            j = r.json()
            tpl["report"]["username"] = j.get("username", username)
            tpl["report"]["streak"] = j.get("site_streak") or j.get("streak") or 0
            tpl["report"]["totalXp"] = j.get("totalXp") or j.get("total_xp") or 0
            
            # Name
            tpl["report"]["name"] = j.get("name") or j.get("fullname") or None
            
            # Country
            tpl["report"]["country"] = j.get("country") or None
            
            # Bio
            tpl["report"]["bio"] = j.get("bio") or None
            
            # Languages
            langs = []
            language_data = j.get("language_data") or j.get("languages") or []
            if isinstance(language_data, dict):
                for code, info in language_data.items():
                    if isinstance(info, dict):
                        langs.append({
                            "language": info.get("language_name") or info.get("language") or code,
                            "level": info.get("level") or 0,
                            "xp": info.get("points") or info.get("xp") or 0,
                            "crowns": info.get("crowns") or 0,
                            "fluency_score": info.get("fluency_score") or 0
                        })
            tpl["report"]["languages"] = langs
            
            # Avatar
            tpl["report"]["avatarUrl"] = j.get("avatar") or j.get("picture") or j.get("avatar_url") or tpl["report"]["avatarUrl"]
            
            # Additional stats
            tpl["report"]["creationDate"] = j.get("creationDate") or None
            tpl["report"]["learningLanguage"] = j.get("learningLanguage") or None
            tpl["report"]["fromLanguage"] = j.get("fromLanguage") or None
            
            return tpl
        
        # Fallback: Try profile page
        r2 = requests.get(f"https://www.duolingo.com/profile/{username}", headers=HEADERS, timeout=12)
        if r2.status_code == 200:
            soup = BeautifulSoup(r2.text, "html.parser")
            og = soup.find("meta", property="og:image")
            if og:
                tpl["report"]["avatarUrl"] = og.get("content")
            
            # Try to extract name from page
            name_tag = soup.find("h1") or soup.find("h2")
            if name_tag and name_tag.text.strip():
                tpl["report"]["name"] = name_tag.text.strip()
    except:
        pass

    return tpl


def fetch_hackerrank_live(username: str) -> Dict:
    tpl = hackerrank_template(username)
    try:
        url = f"https://www.hackerrank.com/{username}"
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            
            # Full name
            name_tag = soup.find("h1") or soup.find("h2")
            if name_tag and name_tag.text.strip():
                tpl["fullName"] = name_tag.text.strip()
            
            # Bio
            bio_tag = soup.find("div", {"class": re.compile("bio", re.I)}) or soup.find("p", {"class": re.compile("bio", re.I)})
            if bio_tag:
                tpl["bio"] = bio_tag.text.strip()
            
            # Country
            country_elem = soup.find(text=re.compile(r"Country|Location", re.I))
            if country_elem:
                parent = country_elem.parent
                if parent:
                    country_text = parent.get_text()
                    match = re.search(r'(?:Country|Location)[:\s]+(.+)', country_text, re.I)
                    if match:
                        tpl["country"] = match.group(1).strip()
            
            # Profile image
            img_tag = soup.find("img", {"class": re.compile("avatar|profile", re.I)}) or soup.find("img", {"alt": re.compile(username, re.I)})
            if img_tag and img_tag.get("src"):
                tpl["profileImage"] = img_tag.get("src")
            
            # Followers/Following
            followers_elem = soup.find(text=re.compile(r"Followers|Follower", re.I))
            if followers_elem:
                parent = followers_elem.parent
                if parent:
                    text = parent.get_text()
                    numbers = re.findall(r'\d+', text)
                    if numbers:
                        tpl["followersCount"] = int(numbers[0])
            
            following_elem = soup.find(text=re.compile(r"Following", re.I))
            if following_elem:
                parent = following_elem.parent
                if parent:
                    text = parent.get_text()
                    numbers = re.findall(r'\d+', text)
                    if numbers:
                        tpl["followingCount"] = int(numbers[0])
            
            # Social links
            links = soup.find_all("a", href=re.compile(r"github|linkedin|twitter|website", re.I))
            for link in links:
                href = link.get("href", "")
                if "github" in href.lower():
                    tpl["socialLinks"]["github"] = href
                elif "linkedin" in href.lower():
                    tpl["socialLinks"]["linkedin"] = href
                elif "twitter" in href.lower():
                    tpl["socialLinks"]["twitter"] = href
                elif "website" in href.lower() or "http" in href.lower():
                    tpl["socialLinks"]["website"] = href
            
            # Skills
            skills_elem = soup.find(text=re.compile(r"Skills", re.I))
            if skills_elem:
                parent = skills_elem.parent
                if parent:
                    skills_container = parent.find_next("div") or parent.find_next("ul")
                    if skills_container:
                        skill_tags = skills_container.find_all("span") or skills_container.find_all("li")
                        skills = []
                        for tag in skill_tags:
                            skill_text = tag.get_text().strip()
                            if skill_text:
                                skills.append(skill_text)
                        if skills:
                            tpl["skills"] = skills
            
            # Badges
            badge_tags = soup.find_all("div", {"class": re.compile("badge", re.I)}) or soup.find_all("span", {"class": re.compile("badge", re.I)})
            badges = []
            for badge in badge_tags:
                badge_text = badge.get_text().strip()
                if badge_text:
                    badges.append(badge_text)
            if badges:
                tpl["badges"] = badges[:10]  # Limit to 10 badges
    except:
        pass
    return tpl

# ---------------------------------------------------
# FASTAPI APP
# ---------------------------------------------------

app = FastAPI(title="Platform Reports API", version="3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_cached_or_fetch(key, fn, arg):
    val = cache.get(key)
    if val is not None:
        return val
    data = fn(arg)
    cache.set(key, data)
    return data

# Individual endpoints
@app.get("/v1/leetcode/{username}")
def api_leetcode(username: str, _rl=Depends(check_rate_limit)):
    return get_cached_or_fetch(f"lc:{username}", fetch_leetcode_live, username)

@app.get("/v1/codechef/{username}")
def api_codechef(username: str, _rl=Depends(check_rate_limit)):
    return get_cached_or_fetch(f"cc:{username}", fetch_codechef_live, username)

@app.get("/v1/duolingo/{username}")
def api_duolingo(username: str, _rl=Depends(check_rate_limit)):
    return get_cached_or_fetch(f"duo:{username}", fetch_duolingo_live, username)

@app.get("/v1/codeforces/{username}")
def api_cf(username: str, _rl=Depends(check_rate_limit)):
    return get_cached_or_fetch(f"cf:{username}", fetch_codeforces_live, username)

@app.get("/v1/hackerrank/{username}")
def api_hr(username: str, _rl=Depends(check_rate_limit)):
    return get_cached_or_fetch(f"hr:{username}", fetch_hackerrank_live, username)

# Unified endpoint
@app.get("/v1/report/{platform}/{username}")
def api_unified(platform: str, username: str, _rl=Depends(check_rate_limit)):
    p = platform.lower()
    if p in ("leetcode", "lc"):
        return api_leetcode(username)
    if p in ("codechef", "cc"):
        return api_codechef(username)
    if p in ("duolingo", "duo"):
        return api_duolingo(username)
    if p in ("codeforces", "cf"):
        return api_cf(username)
    if p in ("hackerrank", "hr"):
        return api_hr(username)
    raise HTTPException(404, "Unknown platform.")

# Health + cache admin
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/admin/clear_cache")
def _clear():
    cache.clear()
    return {"cache": "cleared"}
