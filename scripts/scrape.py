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
from bs4 import BeautifulSoup, NavigableString, Comment

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
                            location=(j.get("locations") or ["Bahrain"])[0], posted=(j.get("created") or "")[:10], url=j.get("url"),
                            _desc=desc_join((j.get("description"), ""), (j.get("requirementsSection"), "Requirements"), (j.get("benefitsSection"), "Benefits"))))
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
        r = get(f"https://boards-api.greenhouse.io/v1/boards/{s}/jobs?content=true")
        return [dict(title=j["title"], company=s.title(), location=j.get("location", {}).get("name", ""),
                     posted=(j.get("updated_at") or "")[:10], url=j.get("absolute_url"), _desc=html.unescape(j.get("content") or "")) for j in r.json().get("jobs", [])] if r.ok else []
    def lv(s):
        r = get(f"https://api.lever.co/v0/postings/{s}?mode=json")
        return [dict(title=j["text"], company=s.title(), location=" / ".join([str(j.get("categories", {}).get("location", ""))] + (j.get("categories", {}).get("allLocations") or [])),
                     posted=datetime.fromtimestamp(j.get("createdAt", 0)/1000, timezone.utc).strftime("%Y-%m-%d") if j.get("createdAt") else "",
                     url=j.get("hostedUrl"), _desc=lever_desc(j)) for j in r.json()] if r.ok else []
    def ab(s):
        r = get(f"https://api.ashbyhq.com/posting-api/job-board/{s}")
        return [dict(title=j["title"], company=s.title(), location=" / ".join([j.get("location", "")] + [x.get("location", "") for x in j.get("secondaryLocations") or []]),
                     posted=(j.get("publishedAt") or "")[:10], url=j.get("jobUrl"), _desc=j.get("descriptionHtml") or "") for j in r.json().get("jobs", [])] if r.ok else []
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
            p = re.search(r"<pubDate>(.*?)</pubDate>", it, re.S); dsc = re.search(r"<description><!\[CDATA\[(.*?)\]\]></description>", it, re.S)
            if not t or not l: continue
            full = _clean(t.group(1)); m = re.match(r"(.*)\(([^()]*)\)\s*$", full)
            title, loc = (m.group(1).strip(), m.group(2)) if m else (full, "")
            url = html.unescape(l.group(1)).split("?")[0]
            if url in seen: continue
            seen.add(url)
            out.append(dict(title=title, company=company, location=loc, posted=_rss_date(p.group(1)) if p else "", url=url,
                            _desc=dsc.group(1) if dsc else ""))
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
        rem = re.search(r"<remoteStatus>(\w+)</remoteStatus>", it); dsc = re.search(r"<description>(.*?)</description>", it, re.S)
        if not t or not l: continue
        gcc = [x for x in locs if country_of(x)] or locs
        loc = " / ".join(dict.fromkeys(gcc)) + (" (Remote)" if rem and rem.group(1) == "fully" else "")
        dh = dsc.group(1).strip() if dsc else ""
        if dh.startswith("<![CDATA["): dh = dh[9:-3]
        else: dh = html.unescape(dh)
        out.append(dict(title=_clean(t.group(1)), company=company, location=loc, posted=_rss_date(p.group(1)) if p else "", url=l.group(1).strip(), _desc=dh))
    return out

def b_tamkeen():
    r = dget("https://tamkeen.bh/en/job-openings/"); r.raise_for_status(); out = []
    for d in BeautifulSoup(r.text, "html.parser").select(".job-details"):
        t = d.select_one(".jobtitle"); a = d.find("a", href=True)
        if t and a: out.append(dict(title=_clean(t.get_text()), company="Tamkeen", location="Bahrain", posted="", url=a["href"]))
    return out

def b_greenhouse(slug, company):
    r = dget(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"); r.raise_for_status()
    return [dict(title=_clean(j["title"]), company=company, location=(j.get("location") or {}).get("name", ""),
                 posted=(j.get("first_published") or j.get("updated_at") or "")[:10], url=j.get("absolute_url"),
                 _desc=html.unescape(j.get("content") or "")) for j in r.json().get("jobs", [])]
def b_ashby(slug, company):
    r = dget(f"https://api.ashbyhq.com/posting-api/job-board/{slug}"); r.raise_for_status()
    out = []
    for j in r.json().get("jobs", []):
        locs = [j.get("location") or ""] + [x.get("location", "") for x in j.get("secondaryLocations") or []]
        addr = ((j.get("address") or {}).get("postalAddress") or {}).get("addressCountry") or ""
        out.append(dict(title=_clean(j["title"]), company=company, location=" / ".join(x for x in locs + [addr] if x),
                        posted=(j.get("publishedAt") or "")[:10], url=j.get("jobUrl"), _desc=j.get("descriptionHtml") or ""))
    return out
def b_workable(slug, company):
    r = dget(f"https://apply.workable.com/api/v1/widget/accounts/{slug}?details=true"); r.raise_for_status()
    return [dict(title=_clean(j["title"]), company=company, location=", ".join(x for x in (j.get("city"), j.get("country")) if x),
                 posted=(j.get("published_on") or j.get("created_at") or "")[:10], url=j.get("url") or j.get("shortlink"),
                 _desc=j.get("description") or "") for j in r.json().get("jobs", [])]
def b_recruitee(slug, company):
    r = dget(f"https://{slug}.recruitee.com/api/offers"); r.raise_for_status()
    return [dict(title=_clean(j["title"]), company=company, location=", ".join(x for x in (j.get("location"), j.get("country")) if x),
                 posted=(j.get("published_at") or "")[:10], url=j.get("careers_url"),
                 _desc=desc_join((j.get("description"), ""), (j.get("requirements"), "Requirements"))) for j in r.json().get("offers", [])]
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
            if j.get("_desc"): x["_desc"] = j["_desc"]; x["_dsrc"] = src
            if j.get("closing"): x["closing"] = j["closing"]
            if j.get("posted") and not x.get("posted"): x["posted"] = j["posted"]
            merged += 1
        else:
            k = norm(j["title"]) + "|" + norm(j["company"]) + ("" if jc == "Bahrain" else "|" + norm(jc))
            if k in results: continue
            results[k] = dict(j, id=k, country=jc, sources=[src], roles=["Any job"], links={src: j["url"]}, direct=True, _dsrc=src)
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
        if not APPLY_RE.search(r.text): return "closed"
        try:
            m = BeautifulSoup(r.text, "html.parser").select_one(".show-more-less-html__markup")
            if m: LI_DESC[jid] = m.decode_contents()
        except Exception: pass
        return "open"
    return "blocked"
def drop_closed_linkedin(results, has_desc=frozenset()):
    try: cache = json.load(open(LI_STATUS))
    except Exception: cache = {}
    now = datetime.now(timezone.utc); nowi = now.isoformat(timespec="seconds")
    def age_h(e):
        try: return (now - datetime.fromisoformat(e["t"])).total_seconds() / 3600
        except Exception: return 1e9
    todo_new, todo_old, todo_desc = [], [], []
    for k, x in results.items():
        jid = li_id(x.get("links", {}).get("LinkedIn")) if "LinkedIn" in x.get("sources", []) else None
        if not jid: continue
        x["_li"] = jid; e = cache.get(jid)
        if e is None: todo_new.append((x.get("posted") or "", jid))
        elif e.get("s") == "open" and age_h(e) >= RECHECK_H: todo_old.append((e.get("t", ""), jid))
        elif e.get("s") == "open" and k not in has_desc: todo_desc.append((x.get("posted") or "", jid))
    # newest new first, then stalest; then (capped) open jobs that still have no description, to fetch it
    todo = [j for _, j in sorted(todo_new, reverse=True)] + [j for _, j in sorted(todo_old)]
    n_main = len(todo); todo += [j for _, j in sorted(todo_desc, reverse=True)][:LI_DESC_EXTRA]
    stats = {"checked": 0, "closed_new": 0, "failed": 0, "pending": 0, "stopped": "", "desc_checks": max(0, len(todo) - n_main)}
    blocked_streak = 0
    for i, jid in enumerate(todo):
        if (time.time() - T0) / 60 > CHECK_DEADLINE_MIN: stats["stopped"] = "time budget"; stats["pending"] = len(todo) - i; break
        if blocked_streak >= 6: stats["stopped"] = "rate limited"; stats["pending"] = len(todo) - i; break
        if i >= n_main and (time.time() - T0) / 60 > LI_DESC_DEADLINE_MIN: break   # description-only checks are optional
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

# ---------- Job descriptions ----------
# Stored outside live.json in data/desc/XXX.json (1024 shards, shard = FNV-1a 32-bit hash of the job id % 1024, the page
# computes the same hash and lazy-loads one small shard when a job title is tapped). Each entry is sanitized plain text
# split into blocks ["h"|"p"|"li", text] and grouped: d = description, r = responsibilities, q = qualifications, x = more info.
# Cached across runs: only new jobs are fetched (capped, time-boxed, fault-isolated); entries for jobs no longer live are pruned.
DESC_DIR = os.environ.get("DESC_DIR") or os.path.join(ROOT, "data", "desc")
DESC_SHARDS = 1024
DESC_MAX = 9000                  # characters kept per job
DESC_FETCH_CAP = 400             # per-URL description fetches per run (careers pages)
DESC_BUDGET_S = 300              # seconds for those fetches
DESC_DEADLINE_MIN = 30           # never start description fetches after this many minutes into the run
LI_DESC_EXTRA = 150              # extra LinkedIn job-page reads per run for open jobs that still lack a description
LI_DESC_DEADLINE_MIN = 20
DESC_RETRY_H = 20                # retry a failed description after this many hours, at most 3 times
LI_DESC = {}                     # LinkedIn job id -> description HTML captured during the closed-posting check
def fnv32(s):
    h = 0x811c9dc5
    for b in s.encode("utf-8"): h = ((h ^ b) * 0x01000193) & 0xffffffff
    return h
def desc_shard(jid): return "%03x" % (fnv32(jid) % DESC_SHARDS)
def desc_join(*parts):
    """[(html, heading)] -> one HTML string with a heading before each non-empty part."""
    out = []
    for h, head in parts:
        if h and str(h).strip(): out.append((f"<h3>{html.escape(head)}</h3>" if head else "") + str(h))
    return "".join(out)
def lever_desc(j):
    try:
        return desc_join((j.get("description") or j.get("descriptionPlain"), ""),
                         *[("<ul>" + (l.get("content") or "") + "</ul>", l.get("text") or "") for l in j.get("lists") or []],
                         (j.get("additional"), ""))
    except Exception: return ""
_BLOCK = {"p", "div", "section", "article", "ul", "ol", "table", "tr", "td", "th", "tbody", "thead", "blockquote", "header",
          "footer", "dl", "dt", "dd", "hr", "pre", "main", "aside", "figure", "center", "font"}
_HEAD = {"h1", "h2", "h3", "h4", "h5", "h6"}
_DROP = ["script", "style", "noscript", "iframe", "img", "svg", "form", "button", "input", "select", "textarea", "video", "audio",
         "object", "embed", "canvas", "link", "meta", "head", "title"]
_BULLET = re.compile(r"^\s*(?:[•·▪●◦‣∙■□➢➤►✓✔\-–—*]|\d{1,2}[.)])\s+")
def _bt(s): return re.sub(r"\s+", " ", s.replace("\xa0", " ").replace("\u200b", "")).strip()
def _line_block(t, bold=False):
    t = _bt(t)
    if not t: return None
    if _BULLET.match(t) and len(t) > 3: return ["li", _BULLET.sub("", t)]
    if (bold and len(t) <= 100) or (len(t) <= 70 and t.endswith(":")): return ["h", t.rstrip(":").strip()]
    return ["p", t]
def html_blocks(h):
    """Untrusted HTML -> list of [kind, plain text] blocks (no markup survives, so nothing can be injected)."""
    h = (h or "").strip()
    if not h: return []
    if "<" not in h:
        return [b for b in (_line_block(x) for x in re.split(r"\r?\n", html.unescape(h))) if b]
    soup = BeautifulSoup(h, "html.parser")
    for x in soup(_DROP): x.decompose()
    out, buf = [], []          # buf: (text, is_bold) segments; "\n" segments split lines
    def flush():
        line, bold = [], True
        for t, bd in buf + [("\n", True)]:
            if t == "\n":
                b = _line_block("".join(line), bold and bool("".join(line).strip()))
                if b: out.append(b)
                line, bold = [], True; continue
            line.append(t)
            if t.strip() and not bd: bold = False
        buf.clear()
    def li_text(node):
        parts = []
        for c in node.children:
            if isinstance(c, Comment): continue
            if getattr(c, "name", None) in ("ul", "ol"): continue
            parts.append(str(c) if isinstance(c, NavigableString) else c.get_text(" "))
        return _bt(" ".join(parts))
    def walk(node, inbold=False):
        for c in node.children:
            if isinstance(c, Comment): continue
            if isinstance(c, NavigableString):
                buf.append((str(c).replace("\n", " "), inbold)); continue
            n = c.name
            if n == "br": buf.append(("\n", True)); continue
            if n in _HEAD:
                flush(); t = _bt(c.get_text(" "))
                if t: out.append(["h", t.rstrip(":").strip()])
                continue
            if n == "li":
                flush(); t = li_text(c)
                if t: out.append(["li", _BULLET.sub("", t)])
                for sub in c.find_all(["ul", "ol"], recursive=False): walk(sub)
                continue
            if n in _BLOCK: flush(); walk(c, inbold); flush(); continue
            walk(c, inbold or n in ("strong", "b") or bool(re.search(r"font-weight:\s*(bold|[6-9]00)", c.get("style") or "")))
    walk(soup); flush()
    # drop empty-ish and repeated blocks
    res, seen = [], set()
    for k, t in out:
        if not re.search(r"\w", t): continue
        if k == "h" and len(t) > 120: k = "p"
        key = (k, t)
        if key in seen and k != "li": continue
        seen.add(key); res.append([k, t])
    return res
RESP_RE = re.compile(r"responsibilit|\bduties\b|\bduty\b|accountabilit|what you.{0,12}(do|be doing|work on|achieve)|your (role|day|impact|mission)\b|"
                     r"key (tasks|activities|areas|result)|job (tasks|functions)|role description|in this role|day[- ]to[- ]day|what will you|"
                     r"the job\b|you will\b|you.ll\b|job specifications?|scope of (work|role)|main tasks|make an impact", re.I)
QUAL_RE = re.compile(r"qualification|requirement|\bskills?\b|\bexperience\b|what you.{0,15}(bring|need|have|offer)|who you are|about you\b|"
                     r"competenc|\beducation\b|eligib|ideal candidate|must[- ]have|nice[- ]to[- ]have|looking for|you (have|bring|are)\b|"
                     r"your (background|profile)|^background$|candidate (profile|requirements)|person specification|what we need|minimum|preferred|desired|knowledge,? (and|&)|abilities", re.I)
OTHER_RE = re.compile(r"benefit|perks|what we offer|we offer|why (join|work|you.ll love|us)|about (us|the company|the team|our)|our (culture|values|company|mission|story)|"
                      r"company description|who we are|additional information|how to apply|equal (employment )?opportunit|diversity|compensation|"
                      r"salary|life at|working at|join us|rewards", re.I)
RESP_STRONG = re.compile(r"responsibilit|\bduties\b|\bduty\b|accountabilit", re.I)
META_H = re.compile(r"^(location|job id|job number|req(uisition)? id|date posted|posted( on| date)?|job type|employment type|time type|business unit|sector|"
                    r"contract type|closing date|department|division|company|job title|workplace type|remote type|job family|job category|grade|job level)$", re.I)
def classify(blocks):
    b = {"d": [], "r": [], "q": [], "x": []}; cur = "d"
    # strip a leading "Label: value" metadata header (location, job id, posted date ...) that some careers sites put on top
    while len(blocks) >= 2 and blocks[0][0] == "h" and (META_H.match(blocks[0][1]) or len(blocks[0][1]) <= 40) and blocks[1][0] == "p" and len(blocks[1][1]) <= 60:
        blocks = blocks[2:]
    blocks = [x for i, x in enumerate(blocks) if not (x[0] == "h" and re.fullmatch(r"(job |position |role )?(description|summary|overview|details)", x[1], re.I)
                                                       and (i + 1 == len(blocks) or blocks[i + 1][0] == "h" or i == 0))]
    for k, t in blocks:
        if k == "h" and len(t) <= 80:
            nb = ("r" if RESP_STRONG.search(t) else "q" if QUAL_RE.search(t) else "r" if RESP_RE.search(t)
                  else "x" if OTHER_RE.search(t) else None)
            if nb == "x" and not (b["r"] or b["q"]): nb = None      # "About us" before the job details stays in the intro
            if nb:
                cur = nb
                if nb in ("r", "q") and not b[nb]: continue      # replaced by the page's own section label
            b[cur].append([k, t]); continue
        b[cur].append([k, t])
    # a heading with nothing after it is noise
    for key in b:
        while b[key] and b[key][-1][0] == "h": b[key].pop()
    return b
def make_entry(raw_html, src):
    blocks = html_blocks(raw_html)
    if sum(len(t) for _, t in blocks) < 40: return None
    b = classify(blocks); e = {"s": src}; n = 0; more = False
    for key in ("d", "r", "q", "x"):
        keep = []
        for blk in b[key]:
            if n + len(blk[1]) > DESC_MAX: more = True; break
            n += len(blk[1]); keep.append(blk)
        if keep: e[key] = keep
    if more: e["more"] = 1
    return e
# --- per-URL fetchers for careers pages whose list API has no description ---
EIGHTFOLD_DOMAIN = {"portal.careers.hsbc.com": "hsbc.com"}
def df_workday(u):
    m = re.match(r"https://([^/]+)/(?:[a-z]{2}-[A-Z]{2}/)?([^/]+)(/job/.+)$", u)
    if not m: return None
    host, site, path = m.groups()
    r = dget(f"https://{host}/wday/cxs/{host.split('.')[0]}/{site}{path}", headers={"Accept": "application/json"}); r.raise_for_status()
    return (r.json().get("jobPostingInfo") or {}).get("jobDescription")
def df_smartrecruiters(u):
    m = re.match(r"https://jobs\.smartrecruiters\.com/([^/]+)/(\d+)", u)
    if not m: return None
    r = dget(f"https://api.smartrecruiters.com/v1/companies/{m.group(1)}/postings/{m.group(2)}"); r.raise_for_status()
    sec = ((r.json().get("jobAd") or {}).get("sections")) or {}
    return desc_join(*[((sec.get(k) or {}).get("text"), (sec.get(k) or {}).get("title") or "") for k in
                       ("jobDescription", "qualifications", "additionalInformation", "companyDescription")])
def df_eightfold(u):
    m = re.match(r"https://([^/]+)/careers/job/(\d+)", u)
    if not m or m.group(1) not in EIGHTFOLD_DOMAIN: return None
    r = dget(f"https://{m.group(1)}/api/apply/v2/jobs/{m.group(2)}?domain={EIGHTFOLD_DOMAIN[m.group(1)]}", headers={"Accept": "application/json"})
    r.raise_for_status(); return r.json().get("job_description")
def df_oracle(u):
    m = re.match(r"https://([^/]+)/hcmUI/CandidateExperience/[a-z]+/sites/([^/]+)/job/(\d+)", u)
    if not m: return None
    host, site, jid = m.groups()
    r = dget(f'https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails?expand=all&onlyData=true&finder=ById;Id=%22{jid}%22,siteNumber={site}',
             headers={"Accept": "application/json"}); r.raise_for_status()
    it = (r.json().get("items") or [{}])[0]
    return desc_join((it.get("ExternalDescriptionStr"), ""), (it.get("ExternalResponsibilitiesStr"), "Responsibilities"),
                     (it.get("ExternalQualificationsStr"), "Qualifications"), (it.get("CorporateDescriptionStr"), "About the company"))
def df_jazzhr(u):
    r = dget(u); r.raise_for_status()
    e = BeautifulSoup(r.text, "html.parser").select_one("#job-description")
    return e.decode_contents() if e else None
def df_sniperhire(u):   # Gulf Air (sniperhire) and stc Bahrain use the same vacancy page layout
    r = dget(u); r.raise_for_status()
    s = BeautifulSoup(r.text, "html.parser"); rows = []
    for h5 in s.find_all("h5"):
        row = h5.find_parent("div", class_="row")
        if row is not None and row not in rows: rows.append(row)
    return "".join(str(x) for x in rows) or None
def df_bamboohr(u):
    m = re.match(r"https://([^.]+)\.bamboohr\.com/careers/(\d+)", u)
    if not m: return None
    r = dget(f"https://{m.group(1)}.bamboohr.com/careers/{m.group(2)}/detail", headers={"Accept": "application/json"}); r.raise_for_status()
    return ((r.json().get("result") or {}).get("jobOpening") or {}).get("description")
def df_greenhouse(u):
    m = re.search(r"greenhouse\.io/([^/]+)/jobs/(\d+)", u)
    if not m: return None
    r = dget(f"https://boards-api.greenhouse.io/v1/boards/{m.group(1)}/jobs/{m.group(2)}"); r.raise_for_status()
    return html.unescape(r.json().get("content") or "")
DESC_FETCHERS = [(re.compile(r"myworkdayjobs\.com/"), df_workday), (re.compile(r"jobs\.smartrecruiters\.com/"), df_smartrecruiters),
                 (re.compile(r"/careers/job/\d+"), df_eightfold), (re.compile(r"/hcmUI/CandidateExperience/"), df_oracle),
                 (re.compile(r"\.applytojob\.com/apply/"), df_jazzhr), (re.compile(r"sniperhire\.net/vacancy/|careers\.stc\.com\.bh/vacancy/"), df_sniperhire),
                 (re.compile(r"\.bamboohr\.com/careers/\d+"), df_bamboohr), (re.compile(r"greenhouse\.io/[^/]+/jobs/\d+"), df_greenhouse)]
def desc_fetcher(u):
    for rx, fn in DESC_FETCHERS:
        if rx.search(u or ""): return fn
    return None
def desc_src_label(s): return "Careers page" if s == "Employer careers (ATS)" else s
def load_desc_store():
    store = {}
    try:
        for fn in os.listdir(DESC_DIR):
            if re.fullmatch(r"[0-9a-f]{3}\.json", fn):
                try: store.update(json.load(open(os.path.join(DESC_DIR, fn))))
                except Exception as e: print("desc shard unreadable:", fn, e)
    except FileNotFoundError: pass
    return store
def update_descriptions(jobs, store):
    """Fills `store` (job id -> entry) for live jobs, prunes the rest, writes changed shards. Never raises."""
    t0 = time.time(); now = datetime.now(timezone.utc); stats = {"inline": 0, "linkedin": 0, "fetched": 0, "failed": 0, "skipped": 0}
    live = {j["id"] for j in jobs}
    def usable(e): return bool(e) and any(k in e for k in ("d", "r", "q", "x"))
    def miss(jid):
        e = store.get(jid) or {}; store[jid] = {"n": int(e.get("n", 0)) + 1, "a": now.isoformat(timespec="seconds")}
    queue = []
    for j in jobs:
        jid = j["id"]; e = store.get(jid)
        if usable(e): continue
        try:
            ent = None
            if j.get("_desc"): ent = make_entry(j["_desc"], desc_src_label(j.get("_dsrc") or j["sources"][0]))
            if not ent:
                lid = li_id(j.get("links", {}).get("LinkedIn"))
                if lid and lid in LI_DESC: ent = make_entry(LI_DESC[lid], "LinkedIn")
                if ent: stats["linkedin"] += 1
            elif ent: stats["inline"] += 1
            if ent: store[jid] = ent; continue
        except Exception as ex: print("desc parse error:", jid[:60], ex)
        if e:   # earlier failure: retry later, at most 3 times
            try: age = (now - datetime.fromisoformat(e.get("a"))).total_seconds() / 3600
            except Exception: age = 1e9
            if int(e.get("n", 0)) >= 3 or age < DESC_RETRY_H: continue
        for s in j.get("sources", []):
            fn = desc_fetcher(j["links"].get(s))
            if fn: queue.append((j.get("posted") or "", jid, fn, j["links"][s], desc_src_label(s))); break
    queue.sort(key=lambda q: q[0], reverse=True)
    if len(queue) > DESC_FETCH_CAP: stats["skipped"] = len(queue) - DESC_FETCH_CAP; queue = queue[:DESC_FETCH_CAP]
    deadline = min(time.time() + DESC_BUDGET_S, T0 + DESC_DEADLINE_MIN * 60 + DESC_BUDGET_S)
    if (time.time() - T0) / 60 > DESC_DEADLINE_MIN: stats["skipped"] += len(queue); queue = []
    host_lock = {}; hl = threading.Lock()
    def one(q):
        _, jid, fn, url, label = q
        if time.time() > deadline: return jid, None, "time"
        host = urlparse(url).netloc
        with hl: lk = host_lock.setdefault(host, threading.Lock())
        with lk:   # one request at a time per host, with a polite pause
            if time.time() > deadline: return jid, None, "time"
            try: raw = fn(url); time.sleep(0.4); return jid, (make_entry(raw, label) if raw else None), None
            except Exception as ex: time.sleep(0.4); return jid, None, (type(ex).__name__ + ": " + str(ex))[:100]
    if queue:
        with ThreadPoolExecutor(6) as ex:
            for jid, ent, err in ex.map(one, queue):
                if err == "time": stats["skipped"] += 1; continue
                if ent: store[jid] = ent; stats["fetched"] += 1
                else: stats["failed"] += 1; miss(jid)
    for k in [k for k in store if k not in live]: del store[k]
    # write shards (only files whose content changed, so unchanged shards make no git noise)
    shards = {}
    for k, e in store.items(): shards.setdefault(desc_shard(k), {})[k] = e
    os.makedirs(DESC_DIR, exist_ok=True); wrote = 0
    for fn in os.listdir(DESC_DIR):
        if re.fullmatch(r"[0-9a-f]{3}\.json", fn) and fn[:3] not in shards: os.remove(os.path.join(DESC_DIR, fn)); wrote += 1
    for sh, d in shards.items():
        txt = json.dumps(d, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        path = os.path.join(DESC_DIR, sh + ".json")
        try:
            if open(path, encoding="utf-8").read() == txt: continue
        except FileNotFoundError: pass
        with open(path, "w", encoding="utf-8") as f: f.write(txt)
        wrote += 1
    have = sum(1 for j in jobs if usable(store.get(j["id"])))
    by_src = {}
    for j in jobs:
        for s in j["sources"]: by_src.setdefault(s, [0, 0]); by_src[s][1] += 1; by_src[s][0] += usable(store.get(j["id"]))
    stats.update(live=len(jobs), with_desc=have, shards_written=wrote, secs=round(time.time() - t0, 1),
                 by_source={s: f"{a}/{b}" for s, (a, b) in by_src.items()})
    print("Descriptions:", json.dumps(stats))
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
                if k not in results: results[k] = dict(j, id=k, country=jc, sources=[], roles=[], links={}, _dsrc=s)
                x = results[k]
                if j.get("_desc") and not x.get("_desc"): x["_desc"] = j["_desc"]; x["_dsrc"] = s
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
    try: dstore = load_desc_store()
    except Exception as e: print("desc store load failed:", e); dstore = {}
    has_desc = {k for k, x in results.items() if x.get("_desc") or any(f in dstore.get(k, {}) for f in "drqx")}
    li_stats = drop_closed_linkedin(results, has_desc) if "LinkedIn" in SOURCES and "--no-li-check" not in sys.argv else None
    jobs = [j for j in results.values() if not j.get("posted") or j["posted"][:10] >= cutoff or j.get("direct")]
    for j in jobs:
        j["posted"] = (j.get("posted") or "")[:10]
        j["families"] = list(dict.fromkeys(ROLE_FAMILY.get(r) or BROAD_FAMILY.get(r, "Other") for r in j.get("roles", [])))
        j["country"] = j.get("country") or "Bahrain"
        j.pop("direct", None)
        for s in j["sources"]: status.setdefault(s, {"count": 0, "errors": []})["count"] += 1
    jobs.sort(key=lambda j: j.get("posted") or "", reverse=True)
    desc_stats = None
    if "--no-desc" not in sys.argv:
        try: desc_stats = update_descriptions(jobs, dstore)
        except Exception as e: print("Descriptions step failed (jobs still published):", e)
    for j in jobs: j.pop("_desc", None); j.pop("_dsrc", None)
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
                   "countries": by_country, "linkedin_check": li_stats, "employer_boards": direct_rows, "descriptions": desc_stats and {k: desc_stats[k] for k in ("live", "with_desc", "by_source", "fetched", "failed", "skipped", "secs")}, "status": st, "jobs": jobs}, f, ensure_ascii=False, indent=0)
    print(f"{total} jobs written to {OUT}"); print(json.dumps(by_country)); print(json.dumps(st, indent=1))
    return 0

if __name__ == "__main__": sys.exit(main())
