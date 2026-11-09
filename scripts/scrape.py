#!/usr/bin/env python3
"""Refresh data/live.json for the Bahrain Job Finder (runs in GitHub Actions).
Fetches real listings for Bahrain (full role list) and the other GCC countries (one broad search per job family),
tags every job with a normalized `country` (and `remote` when it is remote work), dedupes,
and drops anything older than 30 days."""
import json, re, time, html, os, sys, threading
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urlencode
from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "data", "live.json")
DAYS = 30
FAMILIES = {   # search keywords per job family (general Bahrain listings; the page filters by each visitor's preferences)
    "Banking & finance ops": ["banking operations", "operations officer", "operations analyst", "back office", "payments operations",
                              "settlement", "reconciliation", "credit administration", "credit operations", "trade finance",
                              "treasury operations", "bank"],
    "Compliance, risk & audit": ["KYC", "AML", "client onboarding", "compliance", "financial crime", "internal control", "audit", "risk"],
    "Relationship & client mgmt": ["relationship manager", "corporate banking", "account manager"],
    "Accounting & finance": ["accountant", "finance officer", "financial analyst", "accounts payable"],
    "HR & admin": ["payroll", "HR operations", "HR officer", "recruiter", "government relations", "administration officer",
                   "office manager", "secretary", "receptionist"],
    "IT & software": ["software engineer", "IT support", "network engineer", "data analyst", "cyber security"],
    "Engineering & technical": ["mechanical engineer", "electrical engineer", "civil engineer", "project engineer", "technician"],
    "Sales & business dev": ["sales executive", "business development", "sales manager"],
    "Marketing & media": ["marketing", "digital marketing", "graphic designer", "social media"],
    "Customer service": ["customer service", "call centre"],
    "Healthcare": ["nurse", "pharmacist", "doctor", "medical"],
    "Hospitality & F&B": ["hotel", "restaurant", "chef", "front office"],
    "Retail": ["retail", "store manager", "cashier"],
    "Logistics & supply chain": ["logistics", "procurement", "supply chain", "warehouse", "driver"],
    "Education": ["teacher", "lecturer"],
    "Legal": ["legal", "lawyer"],
    "Project & management": ["project manager", "operations manager"],
}
ROLES = list(dict.fromkeys(r for rs in FAMILIES.values() for r in rs))
ROLE_FAMILY = {r: f for f, rs in FAMILIES.items() for r in rs}
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
HDR = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9",
       "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8"}
BH = re.compile(r"bahrain|manama|muharraq|riffa|seef|sanabis|juffair|isa town|hamad town|sitra|budaiya|askar|hidd|amwaj", re.I)
COUNTRY_RE = [   # checked in order; explicit country names first, then cities
    ("Bahrain", re.compile(r"\bbahrain\b", re.I)),
    ("Saudi Arabia", re.compile(r"saudi|\bksa\b", re.I)),
    ("United Arab Emirates", re.compile(r"united arab emirates|\bu\.?a\.?e\b|emirates", re.I)),
    ("Qatar", re.compile(r"\bqatar\b", re.I)),
    ("Kuwait", re.compile(r"\bkuwait\b", re.I)),
    ("Oman", re.compile(r"\boman\b|sultanate", re.I)),
    ("Bahrain", BH),
    ("Saudi Arabia", re.compile(r"riyadh|jeddah|jiddah|dammam|khobar|dhahran|makkah|mecca|madinah|medina|jubail|tabuk|abha|yanbu|neom|qassim|buraydah|hofuf|al ahsa", re.I)),
    ("United Arab Emirates", re.compile(r"dubai|abu dhabi|sharjah|ajman|ras al[- ]khaimah|fujairah|umm al[- ]quwain|al ain|jebel ali", re.I)),
    ("Qatar", re.compile(r"doha|lusail|al rayyan|al wakrah|ras laffan|mesaieed", re.I)),
    ("Kuwait", re.compile(r"hawalli|farwaniya|ahmadi|salmiya|jahra|mubarak al", re.I)),
    ("Oman", re.compile(r"muscat|masqat|sohar|salalah|nizwa|duqm|ruwi|seeb|\bsur\b|\bibri\b", re.I)),
]
COUNTRIES = ["Bahrain", "Saudi Arabia", "United Arab Emirates", "Qatar", "Kuwait", "Oman"]
OTHER = COUNTRIES[1:]
REMOTE = re.compile(r"\bremote\b|work from home|\bwfh\b", re.I)
def country_of(loc, default=None):
    for c, rx in COUNTRY_RE:
        if rx.search(loc or ""): return c
    return default
# one broad keyword per family for the other GCC countries (keeps LinkedIn requests low), plus an all-jobs search
BROAD = [""] + ["operations", "compliance", "relationship manager", "accountant", "HR", "IT", "engineer", "sales",
                "marketing", "customer service", "nurse", "hotel", "retail", "logistics", "teacher", "legal", "project manager"]
BROAD_FAMILY = {"operations": "Banking & finance ops", "compliance": "Compliance, risk & audit", "relationship manager": "Relationship & client mgmt",
                "accountant": "Accounting & finance", "HR": "HR & admin", "IT": "IT & software", "engineer": "Engineering & technical",
                "sales": "Sales & business dev", "marketing": "Marketing & media", "customer service": "Customer service", "nurse": "Healthcare",
                "hotel": "Hospitality & F&B", "retail": "Retail", "logistics": "Logistics & supply chain", "teacher": "Education",
                "legal": "Legal", "project manager": "Project & management"}

_last, _tl = {}, threading.Lock()
GAP = {"www.linkedin.com": 1.8, "jobs.workable.com": 0.5}
def get(url, gap=None):
    host = urlparse(url).netloc; gap = GAP.get(host, 0.3) if gap is None else gap
    while True:
        with _tl:
            wait = _last.get(host, 0) + gap - time.time()
            if wait <= 0: _last[host] = time.time(); break
        time.sleep(wait)
    return requests.get(url, headers=HDR, timeout=25)

def words(role):
    return [w for w in re.findall(r"[a-z0-9]+", role.lower()) if w not in {"and", "or", "of", "the", "in", "officer", "specialist", "executive"}]

def src_linkedin(role, country="Bahrain", remote=False):
    out = []
    pages = (0, 10, 20, 30) if country == "Bahrain" and not remote else ((0, 10, 20, 30, 40) if role == "" else (0, 10))
    for start in pages:
        q = {"keywords": role, "location": country, "start": start, "f_TPR": f"r{DAYS*86400}"}
        r = None
        for attempt in range(4):
            r = get("https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?" + urlencode(q))
            if r.status_code != 429: break
            time.sleep(6 * (attempt + 1))
        if r.status_code == 429: raise RuntimeError("rate limited (429)")
        if r.status_code != 200: raise RuntimeError(f"HTTP {r.status_code}")
        cards = BeautifulSoup(r.text, "html.parser").select("li")
        if not cards: break
        for c in cards:
            t = c.select_one(".base-search-card__title"); a = c.select_one("a.base-card__full-link")
            if not t or not a: continue
            co = c.select_one(".base-search-card__subtitle"); loc = c.select_one(".job-search-card__location"); tm = c.select_one("time")
            out.append(dict(title=t.get_text(strip=True), company=co.get_text(strip=True) if co else "",
                            location=loc.get_text(strip=True) if loc else country,
                            posted=(tm.get("datetime") if tm else ""), url=a["href"].split("?")[0]))
        if len(cards) < 10: break
    return out

def src_workable(role, country="Bahrain", remote=False):
    out, token = [], None
    for _ in range(3 if country == "Bahrain" else 1):
        q = {"query": role, "location": country}
        if token: q["pageToken"] = token
        r = get("https://jobs.workable.com/api/v1/jobs?" + urlencode(q))
        if r.status_code != 200: raise RuntimeError(f"HTTP {r.status_code}")
        d = r.json()
        for j in d.get("jobs", []):
            locs = " ".join(j.get("locations") or []) + " " + json.dumps(j.get("location") or {})
            if country_of(locs) != country: continue
            text = (j.get("title", "") + " " + (j.get("description") or "")).lower()
            if not any(w in text for w in words(role)): continue
            out.append(dict(title=j.get("title", ""), company=(j.get("company") or {}).get("title", ""),
                            location=(j.get("locations") or ["Bahrain"])[0], posted=(j.get("created") or "")[:10], url=j.get("url")))
        token = d.get("nextPageToken")
        if not token: break
    return out

GREENHOUSE = ["binance", "careem", "tamara", "okx", "bybit", "coinbase", "wise", "gemini", "stripe"]
LEVER = ["crypto", "binance"]
ASHBY = ["rain", "ziina", "airwallex"]
_ats = None
def _ats_all():
    jobs = []
    def gh(s):
        r = get(f"https://boards-api.greenhouse.io/v1/boards/{s}/jobs")
        return [dict(title=j["title"], company=s.title(), location=j.get("location", {}).get("name", ""),
                     posted=(j.get("updated_at") or "")[:10], url=j.get("absolute_url")) for j in r.json().get("jobs", [])] if r.ok else []
    def lv(s):
        r = get(f"https://api.lever.co/v0/postings/{s}?mode=json")
        return [dict(title=j["text"], company=s.title(), location=" / ".join([str(j.get("categories", {}).get("location", ""))] + (j.get("categories", {}).get("allLocations") or [])),
                     posted=datetime.fromtimestamp(j.get("createdAt", 0)/1000, timezone.utc).strftime("%Y-%m-%d") if j.get("createdAt") else "",
                     url=j.get("hostedUrl")) for j in r.json()] if r.ok else []
    def ab(s):
        r = get(f"https://api.ashbyhq.com/posting-api/job-board/{s}")
        return [dict(title=j["title"], company=s.title(), location=" / ".join([j.get("location", "")] + [x.get("location", "") for x in j.get("secondaryLocations") or []]),
                     posted=(j.get("publishedAt") or "")[:10], url=j.get("jobUrl")) for j in r.json().get("jobs", [])] if r.ok else []
    with ThreadPoolExecutor(6) as ex:
        futs = [ex.submit(gh, s) for s in GREENHOUSE] + [ex.submit(lv, s) for s in LEVER] + [ex.submit(ab, s) for s in ASHBY]
        for f in futs:
            try: jobs += f.result()
            except Exception: pass
    return [j for j in jobs if country_of(j["location"] or "")]
_atslock = threading.Lock()
def src_ats(role, country="Bahrain", remote=False):
    global _ats
    with _atslock:
        if _ats is None: _ats = _ats_all()
    ws = words(role)
    return [j for j in _ats if ws and sum(w in j["title"].lower() for w in ws) >= max(1, len(ws) // 2)]


def src_gulfair():
    import html as _h
    r = get("https://gulfairgroup.sniperhire.net/")
    out = []
    if not r.ok: return out
    for m in re.finditer(r'<h3>\s*<a href="(/vacancy/\d+)">(.*?)</a>.*?Company</label>(.*?)</div>.*?Location</label>(.*?)</div>', r.text, re.S):
        loc = _h.unescape(m.group(4)).strip()
        if not country_of(loc): continue
        out.append(dict(title=re.sub(r"\s+", " ", _h.unescape(m.group(2))).strip(), company=_h.unescape(m.group(3)).strip() or "Gulf Air",
                        location=loc.replace(" - Head Quarter", ""), posted="", url="https://gulfairgroup.sniperhire.net" + m.group(1)))
    return out

# ---------- Direct employer careers boards ----------
# Every open job on these boards is included (any role), as long as it is in a GCC country.
# Each board is fetched independently (own try/except + timeout); a failure is logged in live.json and never stops the run.
CODE_CC = {"BH": "Bahrain", "BHR": "Bahrain", "SA": "Saudi Arabia", "SAU": "Saudi Arabia", "AE": "United Arab Emirates", "ARE": "United Arab Emirates",
           "QA": "Qatar", "QAT": "Qatar", "KW": "Kuwait", "KWT": "Kuwait", "OM": "Oman", "OMN": "Oman"}
POOL_RE = re.compile(r"talent pool|general application|general cv|cv submission|future vacanc|future opportunit|fresh graduate|graduate opportunities|"
                     r"expression of interest|open application|spontaneous application|register your interest|join our talent", re.I)
DIRECT_MAX_AGE = 120     # days; direct boards only list open jobs, but drop very old evergreen postings
BOT_UA = "Mozilla/5.0 (compatible; BahrainJobFinder/1.0; +https://jobsearch-bh.github.io)"
DT = 20                  # per-request timeout for employer boards
def dget(url, **kw):
    kw.setdefault("timeout", DT); h = dict(HDR); h.update(kw.pop("headers", {}))
    return requests.get(url, headers=h, **kw)
def cc_of(loc):
    c = country_of(loc)
    if c: return c
    for tok in re.findall(r"\b[A-Z]{2,3}\b", loc or ""):
        if tok in CODE_CC: return CODE_CC[tok]
    return None
def _clean(s): return re.sub(r"\s+", " ", html.unescape(s or "")).strip()
def _rss_date(s):
    try: return datetime.strptime(s.strip()[:16].strip(), "%a, %d %b %Y").strftime("%Y-%m-%d")
    except Exception:
        m = re.search(r"(\d{1,2}) (\w{3}) (\d{4})", s or "")
        try: return datetime.strptime(" ".join(m.groups()), "%d %b %Y").strftime("%Y-%m-%d") if m else ""
        except Exception: return ""

def b_jazzhr(slug, company):
    r = dget(f"https://{slug}.applytojob.com/apply"); r.raise_for_status()
    out = []
    for li in BeautifulSoup(r.text, "html.parser").select("li.list-group-item"):
        a = li.find("a", href=True)
        if not a or "/apply/" not in a["href"]: continue
        parts = list(li.stripped_strings)
        out.append(dict(title=_clean(a.get_text()), company=company, location=parts[-1] if len(parts) > 1 else "", posted="", url=a["href"]))
    return out

def b_sniperhire_gulfair():
    return [dict(j, company=j["company"] if "gulf air" in j["company"].lower() else "Gulf Air") for j in src_gulfair()]

def b_stc():
    r = dget("https://careers.stc.com.bh/stc/VacancySearch.aspx?PageID=10621"); r.raise_for_status()
    s = BeautifulSoup(r.text, "html.parser"); out = []
    for sec, co in (("op-stc-current", "stc Bahrain"), ("op-stcpay-current", "stc pay")):
        box = s.find(id=sec)
        if not box: continue
        for row in box.select(".row.jobdetail"):
            t = row.select_one(".jb-title"); a = row.find("a", href=True)
            if not t or not a: continue
            cells = [c.get_text(" ", strip=True) for c in row.select(".searchcaption")]
            closing = _rss_date(cells[3]) if len(cells) > 3 else ""
            title = re.sub(r"\s*\(stc pay\)\s*$", "", _clean(t.get_text()), flags=re.I)
            out.append(dict(title=title, company=co, location="Bahrain", posted="", closing=closing,
                            url="https://careers.stc.com.bh" + a["href"]))
    return out

def b_oracle(host, site, company):
    u = (f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.secondaryLocations"
         f"&finder=findReqs;siteNumber={site},limit=200,sortBy=POSTING_DATES_DESC")
    r = dget(u, headers={"Accept": "application/json"}); r.raise_for_status()
    out = []
    for it in r.json().get("items", []):
        for q in it.get("requisitionList", []):
            loc = q.get("PrimaryLocation") or ""
            if q.get("PrimaryLocationCountry") in CODE_CC and not country_of(loc): loc += ", " + CODE_CC[q["PrimaryLocationCountry"]]
            out.append(dict(title=_clean(q.get("Title")), company=company, location=loc, posted=(q.get("PostedDate") or "")[:10],
                            url=f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/job/{q.get('Id')}"))
    return out

def b_bayt(host, company):
    out, page = [], 1
    while page <= 10:
        for attempt in range(2):
            r = dget(f"https://{host}/app/control/byt_job_search_manager?action=1&page={page}",
                     headers={"X-Requested-With": "XMLHttpRequest", "Accept": "*/*", "User-Agent": BOT_UA})
            if r.status_code != 401: break
            time.sleep(4)
        r.raise_for_status()
        d = r.json(); jobs = d.get("jobs") or []
        for j in jobs:
            out.append(dict(title=_clean(j.get("title")).rstrip("."), company=company, location=j.get("loc") or "",
                            posted=(j.get("crtDate") or "")[:10], closing=(j.get("expDate") or "")[:10], url=f"https://{host}" + j.get("url", "")))
        if not jobs or len(out) >= int(d.get("totalJobs") or 0): break
        page += 1
    return out

def b_sf_rss(host, company, keywords=("",), locale="en_GB"):
    out, seen = [], set()
    for k in keywords:
        r = dget(f"https://{host}/services/rss/job/?locale={locale}&keywords=({requests.utils.quote(k)})"); r.raise_for_status()
        for it in re.findall(r"<item>(.*?)</item>", r.text, re.S):
            t = re.search(r"<title><!\[CDATA\[(.*?)\]\]></title>", it, re.S); l = re.search(r"<link>(.*?)</link>", it, re.S)
            p = re.search(r"<pubDate>(.*?)</pubDate>", it, re.S)
            if not t or not l: continue
            full = _clean(t.group(1)); m = re.match(r"(.*)\(([^()]*)\)\s*$", full)
            title, loc = (m.group(1).strip(), m.group(2)) if m else (full, "")
            url = html.unescape(l.group(1)).split("?")[0]
            if url in seen: continue
            seen.add(url)
            out.append(dict(title=title, company=company, location=loc, posted=_rss_date(p.group(1)) if p else "", url=url))
    return out

def b_workday(host, tenant, site, company, searches=("Bahrain", "Saudi Arabia", "United Arab Emirates", "Qatar", "Kuwait", "Oman")):
    out, seen = [], set()
    for q in searches:
        for off in (0, 20, 40):
            r = requests.post(f"https://{host}/wday/cxs/{tenant}/{site}/jobs", json={"appliedFacets": {}, "limit": 20, "offset": off, "searchText": q},
                              headers={"User-Agent": UA, "Accept": "application/json", "Content-Type": "application/json"}, timeout=DT)
            r.raise_for_status(); d = r.json(); ps = d.get("jobPostings") or []
            for p in ps:
                path = p.get("externalPath") or ""
                if not path or path in seen: continue
                seen.add(path)
                loc = p.get("locationsText") or ""
                if re.match(r"\d+ Locations", loc): loc = p.get("title", "")   # multi-location: rely on the title naming the country
                out.append(dict(title=_clean(p.get("title")), company=company, location=loc, posted="",
                                url=f"https://{host}/en-US/{site}{path}"))
            if len(ps) < 20 or not any(cc_of(p.get("locationsText") or "") for p in ps): break
    return out

def b_eightfold(host, domain, company, locs=("Bahrain", "United Arab Emirates", "Qatar", "Kuwait", "Oman", "Saudi Arabia")):
    out, seen = [], set()
    for loc in locs:
        for start in range(0, 100, 10):
            r = dget(f"https://{host}/api/apply/v2/jobs?" + urlencode({"domain": domain, "location": loc, "start": start, "num": 10}),
                     headers={"Accept": "application/json"}); r.raise_for_status()
            d = r.json(); ps = d.get("positions") or []
            for p in ps:
                if p["id"] in seen: continue
                seen.add(p["id"])
                out.append(dict(title=_clean(p.get("name")), company=company, location=p.get("location") or "",
                                posted=datetime.fromtimestamp(p["t_create"], timezone.utc).strftime("%Y-%m-%d") if p.get("t_create") else "",
                                url=p.get("canonicalPositionUrl") or f"https://{host}/careers/job/{p['id']}"))
            if len(ps) < 10 or start + 10 >= (d.get("count") or 0): break
    return out

def b_teamtailor(base, company):
    r = dget(f"{base}/jobs.rss"); r.raise_for_status(); out = []
    for it in re.findall(r"<item>(.*?)</item>", r.text, re.S):
        t = re.search(r"<title>(.*?)</title>", it, re.S); l = re.search(r"<link>(.*?)</link>", it, re.S); p = re.search(r"<pubDate>(.*?)</pubDate>", it, re.S)
        locs = [f"{_clean(c)}, {_clean(k)}" for c, k in re.findall(r"<tt:city>(.*?)</tt:city>\s*<tt:country>(.*?)</tt:country>", it, re.S)]
        rem = re.search(r"<remoteStatus>(\w+)</remoteStatus>", it)
        if not t or not l: continue
        gcc = [x for x in locs if country_of(x)] or locs
        loc = " / ".join(dict.fromkeys(gcc)) + (" (Remote)" if rem and rem.group(1) == "fully" else "")
        out.append(dict(title=_clean(t.group(1)), company=company, location=loc, posted=_rss_date(p.group(1)) if p else "", url=l.group(1).strip()))
    return out

def b_tamkeen():
    r = dget("https://tamkeen.bh/en/job-openings/"); r.raise_for_status(); out = []
    for d in BeautifulSoup(r.text, "html.parser").select(".job-details"):
        t = d.select_one(".jobtitle"); a = d.find("a", href=True)
        if t and a: out.append(dict(title=_clean(t.get_text()), company="Tamkeen", location="Bahrain", posted="", url=a["href"]))
    return out

def b_greenhouse(slug, company):
    r = dget(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"); r.raise_for_status()
    return [dict(title=_clean(j["title"]), company=company, location=(j.get("location") or {}).get("name", ""),
                 posted=(j.get("first_published") or j.get("updated_at") or "")[:10], url=j.get("absolute_url")) for j in r.json().get("jobs", [])]
def b_ashby(slug, company):
    r = dget(f"https://api.ashbyhq.com/posting-api/job-board/{slug}"); r.raise_for_status()
    out = []
    for j in r.json().get("jobs", []):
        locs = [j.get("location") or ""] + [x.get("location", "") for x in j.get("secondaryLocations") or []]
        addr = ((j.get("address") or {}).get("postalAddress") or {}).get("addressCountry") or ""
        out.append(dict(title=_clean(j["title"]), company=company, location=" / ".join(x for x in locs + [addr] if x),
                        posted=(j.get("publishedAt") or "")[:10], url=j.get("jobUrl")))
    return out
def b_workable(slug, company):
    r = dget(f"https://apply.workable.com/api/v1/widget/accounts/{slug}"); r.raise_for_status()
    return [dict(title=_clean(j["title"]), company=company, location=", ".join(x for x in (j.get("city"), j.get("country")) if x),
                 posted=(j.get("published_on") or j.get("created_at") or "")[:10], url=j.get("url") or j.get("shortlink")) for j in r.json().get("jobs", [])]
def b_recruitee(slug, company):
    r = dget(f"https://{slug}.recruitee.com/api/offers"); r.raise_for_status()
    return [dict(title=_clean(j["title"]), company=company, location=", ".join(x for x in (j.get("location"), j.get("country")) if x),
                 posted=(j.get("published_at") or "")[:10], url=j.get("careers_url")) for j in r.json().get("offers", [])]
def b_smartrecruiters(cid, company, countries=("bh", "ae", "sa", "qa", "kw", "om")):
    out = []
    for c in countries:
        r = dget(f"https://api.smartrecruiters.com/v1/companies/{cid}/postings?country={c}&limit=100"); r.raise_for_status()
        for j in r.json().get("content", []):
            l = j.get("location") or {}
            out.append(dict(title=_clean(j["name"]), company=company, location=", ".join(x for x in (l.get("city"), CODE_CC.get((l.get("country") or "").upper(), "")) if x),
                            posted=(j.get("releasedDate") or "")[:10], url=f"https://jobs.smartrecruiters.com/{cid}/{j['id']}"))
    return out
def b_bamboohr(slug, company):
    r = dget(f"https://{slug}.bamboohr.com/careers/list", headers={"Accept": "application/json"}); r.raise_for_status()
    out = []
    for j in r.json().get("result", []):
        l = j.get("location") or {}
        out.append(dict(title=_clean(j.get("jobOpeningName")), company=company, location=", ".join(x for x in (l.get("city"), l.get("state"), l.get("country")) if x),
                        posted="", url=f"https://{slug}.bamboohr.com/careers/{j.get('id')}"))
    return out

GCC_Q = ("Bahrain", "Dubai", "Abu Dhabi", "Saudi", "Qatar", "Kuwait", "Oman")
DIRECT = [   # (board name shown in the status list, fetch function)
    # Bahrain banks, investment & financial firms
    ("Bank ABC (JazzHR)", lambda: b_jazzhr("bankabc", "Bank ABC")),
    ("ila Bank (JazzHR)", lambda: b_jazzhr("ilabank", "ila Bank")),
    ("KFH Bahrain / Ahli United (SuccessFactors)", lambda: b_sf_rss("careers.ahliunited.com", "KFH Bahrain")),
    ("Standard Chartered (SuccessFactors)", lambda: b_sf_rss("jobs.standardchartered.com", "Standard Chartered", GCC_Q + ("Manama",))),
    ("HSBC (Eightfold)", lambda: b_eightfold("portal.careers.hsbc.com", "hsbc.com", "HSBC")),
    ("Citi (Workday)", lambda: b_workday("citi.wd5.myworkdayjobs.com", "citi", "2", "Citi")),
    ("Visa (Workday)", lambda: b_workday("visa.wd5.myworkdayjobs.com", "visa", "Visa", "Visa")),
    # Bahrain big employers
    ("Bapco Energies (Oracle HCM)", lambda: b_oracle("iadygs.fa.ocs.oraclecloud.com", "CX_1", "Bapco Energies")),
    ("Gulf Air (careers portal)", b_sniperhire_gulfair),
    ("stc Bahrain + stc pay (careers portal)", b_stc),
    ("Tamkeen (careers page)", b_tamkeen),
    ("GPIC (SuccessFactors)", lambda: b_sf_rss("gpiccareer.gpic.com", "GPIC")),
    ("APM Terminals / Maersk (Workday)", lambda: b_workday("maersk.wd3.myworkdayjobs.com", "maersk", "Maersk_Careers", "Maersk / APM Terminals")),
    ("GE Vernova (Workday)", lambda: b_workday("gevernova.wd5.myworkdayjobs.com", "gevernova", "Vernova_ExternalSite", "GE Vernova")),
    ("Hitachi (Workday)", lambda: b_workday("hitachi.wd1.myworkdayjobs.com", "hitachi", "hitachi", "Hitachi")),
    # Bahrain & GCC startups / fintechs
    ("Calo (Teamtailor)", lambda: b_teamtailor("https://careers.calo.app", "Calo")),
    ("Tarabut (Workable)", lambda: b_workable("tarabut-gateway", "Tarabut")),
    ("NymCard (BambooHR)", lambda: b_bamboohr("nymcard", "NymCard")),
    ("talabat / Delivery Hero (SmartRecruiters)", lambda: b_smartrecruiters("deliveryhero", "talabat / Delivery Hero")),
    ("Careem (Greenhouse)", lambda: b_greenhouse("careem", "Careem")),
    ("Tamara (Greenhouse)", lambda: b_greenhouse("tamara", "Tamara")),
    ("Lean Technologies (Ashby)", lambda: b_ashby("leantech", "Lean Technologies")),
    ("Ziina (Ashby)", lambda: b_ashby("ziina", "Ziina")),
    ("Thndr (Ashby)", lambda: b_ashby("thndr", "Thndr")),
    ("Foodics (Workable)", lambda: b_workable("foodics", "Foodics")),
    ("Salla (Workable)", lambda: b_workable("salla", "Salla")),
    ("Lucidya (Workable)", lambda: b_workable("lucidya", "Lucidya")),
    ("Unifonic (Recruitee)", lambda: b_recruitee("unifonic", "Unifonic")),
]
GENERIC_CO = {"bank", "the", "company", "group", "holding", "holdings", "bsc", "b", "s", "c", "w", "l", "llc", "ltd", "limited", "plc",
              "inc", "co", "careers", "bahrain", "international", "and", "of", "corporation", "corp", "na", "n", "a"}
def co_words(c): return {w for w in norm(c).split() if w not in GENERIC_CO and len(w) > 1}
CO_ALIAS = {"gulf international bank gib": {"gib"}, "beyon batelco": {"batelco", "beyon"}, "stc bahrain": {"stc"}, "stc pay": {"stc"},
            "maersk apm terminals": {"maersk", "apm"}, "talabat delivery hero": {"talabat", "delivery", "hero"}, "kfh bahrain": {"kfh", "kuwait", "finance", "house", "ahli", "united"},
            "lean technologies": {"lean"}}
def run_direct():
    """Returns (jobs, status rows). Each board is isolated."""
    def one(item):
        name, fn = item; t = time.time()
        try:
            jobs = fn() or []
            return name, jobs, None, round(time.time() - t, 1)
        except Exception as e: return name, [], (type(e).__name__ + ": " + str(e))[:140], round(time.time() - t, 1)
    all_jobs, rows = [], []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=DIRECT_MAX_AGE)).strftime("%Y-%m-%d")
    with ThreadPoolExecutor(8) as ex:
        for name, jobs, err, secs in ex.map(one, DIRECT):
            kept = []
            for j in jobs:
                if not j.get("title") or not j.get("url") or POOL_RE.search(j["title"]): continue
                c = cc_of(j.get("location", ""))
                if not c: continue
                if j.get("posted") and j["posted"] < cutoff: continue
                j["country"] = c; j["_board"] = name; kept.append(j)
            all_jobs += kept
            rows.append({"board": name, "ok": err is None, "count": len(kept), "fetched": len(jobs), "error": err or "", "secs": secs})
            print(f"  direct: {name}: {len(kept)} kept of {len(jobs)}" + (f"  ERROR {err}" if err else ""))
    return all_jobs, rows
def merge_direct(results, jobs, src="Employer careers (ATS)"):
    """Adds direct-board jobs; when LinkedIn/Workable already has the same title at the same company (and country),
    the entry is kept once with the careers-page link first."""
    idx = {}
    for k, x in results.items(): idx.setdefault((norm(x["title"]), x.get("country") or "Bahrain"), []).append(k)
    added = merged = 0
    for j in jobs:
        jc = j.pop("country"); board = j.pop("_board", "")
        cw = co_words(j["company"]) | CO_ALIAS.get(norm(j["company"]), set())
        hit = None
        for k in idx.get((norm(j["title"]), jc), []):
            if cw & (co_words(results[k]["company"]) | CO_ALIAS.get(norm(results[k]["company"]), set())): hit = k; break
        if hit:
            x = results[hit]
            if src not in x["sources"]: x["sources"].insert(0, src)
            x["links"][src] = j["url"]; x["url"] = j["url"]
            if j.get("closing"): x["closing"] = j["closing"]
            if j.get("posted") and not x.get("posted"): x["posted"] = j["posted"]
            merged += 1
        else:
            k = norm(j["title"]) + "|" + norm(j["company"]) + ("" if jc == "Bahrain" else "|" + norm(jc))
            if k in results: continue
            results[k] = dict(j, id=k, country=jc, sources=[src], roles=["Any job"], links={src: j["url"]}, direct=True)
            if REMOTE.search(j.get("location", "") + " " + j["title"]): results[k]["remote"] = True
            idx.setdefault((norm(j["title"]), jc), []).append(k); added += 1
    print(f"Direct employer boards: {added} new jobs, {merged} merged with existing listings")
    return added, merged

SOURCES = {"LinkedIn": src_linkedin, "Workable": src_workable, "Employer careers (ATS)": src_ats}

def norm(s): return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()

# ---------- LinkedIn closed-posting check ----------
# A closed LinkedIn posting ("No longer accepting applications") has an empty apply area on the guest job page
# (no Apply button) or a closed-job notice. Results are cached in data/linkedin_status.json across runs:
# closed ids are never rechecked (just dropped), open ids are rechecked after ~24 h, new ids are checked first.
# Any failed or blocked check keeps the job.
LI_STATUS = os.path.join(ROOT, "data", "linkedin_status.json")
T0 = time.time()
CHECK_DEADLINE_MIN = 31          # stop checking at this many minutes into the run (workflow limit is 40)
RECHECK_H = 24
LI_ID = re.compile(r"(\d{8,})(?:[/?#]|$)")
APPLY_RE = re.compile(r'top-card-layout__cta-container[^>]*>\s*(?:<!---->\s*)*<(?:button|a)\b')
CLOSED_RE = re.compile(r"no longer accepting applications|closed-job|applications are closed", re.I)
def li_id(url):
    m = LI_ID.search(url or ""); return m.group(1) if m else None
def li_check(jid):
    """'open' / 'closed', or None when the page could not be read (job is kept)."""
    for attempt in range(3):
        try: r = get(f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{jid}", gap=1.0)
        except Exception: time.sleep(3); continue
        if r.status_code == 429: time.sleep(8 * (attempt + 1)); continue
        if r.status_code == 404: return "closed"          # posting removed
        if r.status_code != 200 or "top-card-layout" not in r.text: return None
        if CLOSED_RE.search(r.text): return "closed"
        return "open" if APPLY_RE.search(r.text) else "closed"
    return "blocked"
def drop_closed_linkedin(results):
    try: cache = json.load(open(LI_STATUS))
    except Exception: cache = {}
    now = datetime.now(timezone.utc); nowi = now.isoformat(timespec="seconds")
    def age_h(e):
        try: return (now - datetime.fromisoformat(e["t"])).total_seconds() / 3600
        except Exception: return 1e9
    todo_new, todo_old = [], []
    for k, x in results.items():
        jid = li_id(x.get("links", {}).get("LinkedIn")) if "LinkedIn" in x.get("sources", []) else None
        if not jid: continue
        x["_li"] = jid; e = cache.get(jid)
        if e is None: todo_new.append((x.get("posted") or "", jid))
        elif e.get("s") == "open" and age_h(e) >= RECHECK_H: todo_old.append((e.get("t", ""), jid))
    todo = [j for _, j in sorted(todo_new, reverse=True)] + [j for _, j in sorted(todo_old)]   # newest new first, then stalest
    stats = {"checked": 0, "closed_new": 0, "failed": 0, "pending": 0, "stopped": ""}
    blocked_streak = 0
    for i, jid in enumerate(todo):
        if (time.time() - T0) / 60 > CHECK_DEADLINE_MIN: stats["stopped"] = "time budget"; stats["pending"] = len(todo) - i; break
        if blocked_streak >= 6: stats["stopped"] = "rate limited"; stats["pending"] = len(todo) - i; break
        st = li_check(jid)
        if st in ("open", "closed"):
            blocked_streak = 0; stats["checked"] += 1
            if st == "closed": stats["closed_new"] += 1
            cache[jid] = {"s": st, "t": nowi}
        else:
            stats["failed"] += 1; blocked_streak = blocked_streak + 1 if st == "blocked" else 0
    removed = 0
    for k in list(results):
        x = results[k]; jid = x.pop("_li", None)
        if jid and cache.get(jid, {}).get("s") == "closed":
            if len(x["sources"]) == 1: del results[k]
            else:
                x["sources"].remove("LinkedIn"); x["links"].pop("LinkedIn", None)
                x["url"] = x["links"][x["sources"][0]]
            removed += 1
    # keep the cache small: forget ids not seen for 45 days
    live = {li_id(x.get("links", {}).get("LinkedIn")) for x in results.values()}
    cutoff = (now - timedelta(days=45)).isoformat()
    cache = {j: e for j, e in cache.items() if j in live or e.get("t", "") >= cutoff}
    with open(LI_STATUS, "w") as f: json.dump(cache, f, separators=(",", ":"))
    stats["removed"] = removed; stats["cache"] = len(cache)
    print("LinkedIn closed check:", json.dumps(stats))
    return stats

def main():
    global OUT, SOURCES
    if "--only" in sys.argv:   # e.g. --only Workable --out /tmp/w.json (testing)
        i = sys.argv.index("--only"); SOURCES = {sys.argv[i+1]: SOURCES[sys.argv[i+1]]}
    if "--out" in sys.argv: OUT = os.path.abspath(sys.argv[sys.argv.index("--out") + 1])
    # Bahrain: every role on every source. Other GCC countries: broad family searches on LinkedIn + Workable.
    # Remote: a "remote" keyword search per country (LinkedIn's guest API ignores its remote-work filter); jobs are
    # flagged remote only when the title/location says so. The ATS source covers every country in one pass.
    tasks = [(s, r, "Bahrain", False) for r in ROLES for s in SOURCES]
    if "--only" not in sys.argv:
        tasks += [(s, r, c, False) for c in OTHER for r in BROAD for s in ("LinkedIn", "Workable") if s in SOURCES and (r or s == "LinkedIn")]
        tasks += [(s, "remote", c, False) for c in COUNTRIES for s in ("LinkedIn", "Workable") if s in SOURCES]
    status = {s: {"count": 0, "errors": []} for s in SOURCES}
    results = {}
    def run(t):
        s, r, c, rem = t
        try: return s, r, c, rem, SOURCES[s](r, c, rem), None
        except Exception as e: return s, r, c, rem, [], str(e)[:120]
    with ThreadPoolExecutor(6) as ex:
        for s, role, c, rem, jobs, err in ex.map(run, tasks):
            if err: status[s]["errors"].append(f"{role or 'all jobs'} ({c}{', remote' if rem else ''}): {err}")
            for j in jobs:
                if not j.get("url") or not j.get("title"): continue
                jc = country_of(j.get("location"), c if s != "Employer careers (ATS)" else None) or c
                if s == "Employer careers (ATS)" and c != "Bahrain": continue
                k = norm(j["title"]) + "|" + norm(j["company"]) + ("" if jc == "Bahrain" else "|" + norm(jc))
                if k not in results: results[k] = dict(j, id=k, country=jc, sources=[], roles=[], links={})
                x = results[k]
                if REMOTE.search(j.get("location", "") + " " + j["title"]): x["remote"] = True
                role = role or "Any job"
                if s not in x["sources"]: x["sources"].append(s); x["links"][s] = j["url"]
                if role not in x["roles"]: x["roles"].append(role)
                if j.get("posted") and (not x.get("posted") or j["posted"] > x["posted"]): x["posted"] = j["posted"]
    direct_rows = None
    if "--only" not in sys.argv or "--direct" in sys.argv:
        try:
            djobs, direct_rows = run_direct()
            merge_direct(results, djobs)
        except Exception as e: print("Direct employer boards failed:", e)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    for k in [k for k, j in results.items() if j.get("posted") and j["posted"][:10] < cutoff and not j.get("direct")]: del results[k]
    li_stats = drop_closed_linkedin(results) if "LinkedIn" in SOURCES and "--no-li-check" not in sys.argv else None
    jobs = [j for j in results.values() if not j.get("posted") or j["posted"][:10] >= cutoff or j.get("direct")]
    for j in jobs:
        j["posted"] = (j.get("posted") or "")[:10]
        j["families"] = list(dict.fromkeys(ROLE_FAMILY.get(r) or BROAD_FAMILY.get(r, "Other") for r in j.get("roles", [])))
        j["country"] = j.get("country") or "Bahrain"
        j.pop("direct", None)
        for s in j["sources"]: status.setdefault(s, {"count": 0, "errors": []})["count"] += 1
    jobs.sort(key=lambda j: j.get("posted") or "", reverse=True)
    st = [{"source": s, "ok": not (v["errors"] and v["count"] == 0) or (v["count"] > 0), "count": v["count"],
           "note": (v["errors"][0] if v["errors"] and v["errors"][0].startswith("blocked from GitHub") else f"{len(v['errors'])} of {sum(t[0] == s for t in tasks)} searches failed: " + "; ".join(v["errors"][:2]) if v["errors"] else "")} for s, v in status.items()]
    total = len(jobs)
    by_country = {c: sum(j["country"] == c for j in jobs) for c in COUNTRIES}
    by_country["Remote"] = sum(bool(j.get("remote")) for j in jobs)
    # Safety: if everything failed, keep the previous file instead of wiping the list
    if total == 0 and os.path.exists(OUT):
        print("No jobs fetched — keeping previous data/live.json"); print(json.dumps(st, indent=1)); return 1
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump({"fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "roles": ROLES, "families": list(FAMILIES),
                   "countries": by_country, "linkedin_check": li_stats, "employer_boards": direct_rows, "status": st, "jobs": jobs}, f, ensure_ascii=False, indent=0)
    print(f"{total} jobs written to {OUT}"); print(json.dumps(by_country)); print(json.dumps(st, indent=1))
    return 0

if __name__ == "__main__": sys.exit(main())
