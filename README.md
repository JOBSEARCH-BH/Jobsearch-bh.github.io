# 🔎 Bahrain Job Finder 🇧🇭

Live at **https://jobsearch-bh.github.io/** — a free, fully static job finder for Bahrain.

* `index.html` — the whole app. Each visitor sets their own preferences (roles, words to exclude, seniority to hide,
  sectors/employers, location, max job age, salary range); preferences and ⭐ saved jobs stay in their own browser.
* `data/live.json` — real listings (LinkedIn, Workable, employer careers boards) across common job families,
  refreshed every 6 hours by `.github/workflows/refresh.yml` running `scripts/scrape.py`. Jobs older than 30 days are dropped.
