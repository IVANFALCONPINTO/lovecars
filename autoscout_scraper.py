# -*- coding: utf-8 -*-
import os, re, json
from datetime import date, datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import pandas as pd, requests
from bs4 import BeautifulSoup

HERE = os.path.abspath(os.path.dirname(__file__))

def read_cfg(path):
    try:
        import yaml
        with open(path,"r",encoding="utf-8") as f:
            return yaml.safe_load(f)
    except:
        return {"start_url":"https://www.autoscout24.es/profesionales/love-cars","delay_seconds":1.2,"max_pages":200,"output_dir":"./output"}

def ensure_dir(p): os.makedirs(p, exist_ok=True); return p
def clean(s): return re.sub(r"\s+"," ", (s or "").strip())
def to_int(x):
    if x is None: return None
    v = re.sub(r"[^\d]","", str(x))
    return int(v) if v.isdigit() else None
def to_price(txt):
    if txt is None: return None
    v = re.sub(r"[^\d\.,]","", str(txt)).replace(".","").replace(",",".")
    try: return float(v)
    except: return None
def listing_id(url):
    u = url.split("?")[0].rstrip("/")
    # Si el path trae un ID numérico (lo normal en AutoScout), úsalo como ID estable.
    m = re.search(r"(\d{6,})", u)
    if m:
        return m.group(1)
    # Si no hay número, usa el último segmento como fallback.
    return u.split("/")[-1]


def guess_category(title):
    t = title.upper()
    ind = ["FURGON","FURGÓN","VITO","TRAFIC","VIVARO","JUMPY","EXPERT","PARTNER","BERLINGO","SPRINTER","CRAFTER","DUCATO","BOXER","MASTER","MOVANO","KANGOO","CADDY","PROACE","DOBLO","COMBO","NV200"]
    return "Industrial" if any(k in t for k in ind) else "Turismo"

def parse_card(card, base):
    # enlace + id
    a = (card.select_one("a[data-item-name='detail-page-link']") or
         card.select_one("a[data-testid='result-list-entry-link']") or
         card.select_one("a[href*='/anuncios/']") or
         card.select_one("a[href*='/ofertas/']"))
    if not a:
        return None
    link = a.get("href", "")
    if link and not link.startswith("http"):
        link = base + link
    lid = listing_id(link)

    # título y texto bruto
    title = clean(a.get_text(" ", strip=True))
    if not title:
        h = card.select_one("h2, h3")
        if h:
            title = clean(h.get_text(" ", strip=True))
    raw = clean(card.get_text(" ", strip=True))

    # ---- PRECIO ----
    price = None
    for sel in ('[data-testid="price-label"]',
                '[data-testid="srp-price"]',
                '[itemprop="price"]',
                '[class*="Price"]',
                '[class*="price"]'):
        el = card.select_one(sel)
        if el:
            txt = el.get("content") or el.get_text(" ", strip=True)
            price = to_price(txt)
            if price is not None:
                break
    if price is None:
        m = re.search(r'€\s*([\d\.\s,]+)', raw)
        if m:
            price = to_price(m.group(1))

    # ---- KILÓMETROS ----
    km = None
    for sel in ('[data-testid="mileage"]', '[class*="mileage"]', '[class*="Mileage"]'):
        el = card.select_one(sel)
        if el:
            km = to_int(el.get_text(" ", strip=True))
            if km is not None:
                break
    if km is None:
        m = re.search(r'(\d{1,3}(?:[.\s]\d{3})+|\d+)\s*km', raw, re.I)
        if m:
            km = to_int(m.group(1))

    # ---- AÑO ----
    year = None
    for sel in ('[data-testid="first-registration"]',
                '[class*="first-registration"]',
                '[class*="FirstRegistration"]'):
        el = card.select_one(sel)
        if el:
            m = re.search(r'(\d{4})', el.get_text(" ", strip=True))
            if m:
                year = int(m.group(1))
                break
    if year is None:
        m = re.search(r'(20\d{2}|201\d)', raw)
        if m:
            year = int(m.group(1))

    # ---- FUEL / CAMBIO ----
    low = raw.lower()
    if "diesel" in low:
        fuel = "Diesel"
    elif any(k in low for k in ["gasolina", "híbrido", "hibrido", "eléctrico", "electrico"]):
        fuel = "Gasolina/Híbrido/Eléctrico"
    else:
        fuel = ""
    gearbox = "Automático" if "auto" in low else ("Manual" if "manual" in low else "")

    # ---- IMAGEN ----
    img = ""
    img_el = card.select_one("img")
    if img_el:
        img = (img_el.get("src")
               or img_el.get("data-src")
               or (img_el.get("data-srcset", "").split(" ")[0] if img_el.get("data-srcset") else ""))

    # ---- MARCA / MODELO ----
    parts = title.split()
    brand = parts[0] if parts else ""
    model = " ".join(parts[1:3]) if len(parts) > 2 else (parts[1] if len(parts) > 1 else "")

    return {
        "listing_id": lid,
        "brand": brand,
        "model": model,
        "version": title,
        "year": year,
        "km": km,
        "fuel": fuel,
        "gearbox": gearbox,
        "price": price,
        "vat_note": "",
        "link": link,
        "image": img,
        "category": guess_category(title),
    }

def parse_detail_html(html):
    """Extrae precio/km/año/combustible/cambio/potencia/imagen/IVA/descr de la ficha."""
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    out = {}

    # -------- JSON-LD (si existe) --------
    try:
        import json
        for s in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(s.string or "{}")
                if isinstance(data, dict):
                    offers = data.get("offers") or {}
                    price = offers.get("price")
                    if price is not None:
                        out["price"] = to_price(str(price))
                    # km en algunos ld+json no viene; lo seguiremos buscando
            except Exception:
                pass
    except Exception:
        pass

    # -------- PRECIO (fallback en DOM/regex) --------
    if out.get("price") is None:
        for sel in ('[data-testid="price-label"]',
                    '[data-testid="ad-price"]',
                    '[itemprop="price"]',
                    '[class*="Price"]','[class*="price"]'):
            el = soup.select_one(sel)
            if el:
                out["price"] = to_price(el.get("content") or el.get_text(" ", strip=True))
                if out["price"] is not None: break
        if out.get("price") is None:
            m = re.search(r'€\s*([\d\.\s,]+)', text)
            if m: out["price"] = to_price(m.group(1))

    # -------- KILÓMETROS --------
    for sel in ('[data-testid="mileage"]','[class*="mileage"]','[class*="Mileage"]'):
        el = soup.select_one(sel)
        if el:
            out["km"] = to_int(el.get_text(" ", strip=True))
            if out["km"] is not None: break
    if out.get("km") is None:
        m = re.search(r'(\d{1,3}(?:[.\s]\d{3})+|\d+)\s*km', text, re.I)
        if m: out["km"] = to_int(m.group(1))

    # -------- AÑO (primera matriculación) --------
    for sel in ('[data-testid="first-registration"]',
                '[class*="first-registration"]','[class*="FirstRegistration"]'):
        el = soup.select_one(sel)
        if el:
            m = re.search(r'(\d{4})', el.get_text(" ", strip=True))
            if m: out["year"] = int(m.group(1)); break
    if out.get("year") is None:
        m = re.search(r'(\d{2}/)?(20\d{2}|201\d)', text)
        if m:
            yy = re.search(r'(20\d{2}|201\d)', m.group(0)).group(1)
            out["year"] = int(yy)

    # -------- COMBUSTIBLE / CAMBIO / POTENCIA --------
    low = text.lower()
    if "diésel" in low or "diesel" in low: out["fuel"] = "Diésel"
    elif "gasolina" in low: out["fuel"] = "Gasolina"
    elif "híbrido" in low or "hibrido" in low: out["fuel"] = "Híbrido"
    elif "eléctrico" in low or "electrico" in low: out["fuel"] = "Eléctrico"

    out["gearbox"] = "Automático" if "automát" in low or "automatic" in low else ("Manual" if "manual" in low else out.get("gearbox",""))

    m = re.search(r'(\d+)\s*kW.*?\((\d+)\s*CV\)', text)
    if m:
        out["power_kw"] = int(m.group(1))
        out["power_cv"] = int(m.group(2))

    # -------- IVA / notas --------
    if "iva no incluido" in low: out["vat_note"] = "IVA no incluido"
    elif "iva incluido" in low: out["vat_note"] = "IVA incluido"
    elif "iva deducible" in low: out["vat_note"] = "IVA deducible"

    # -------- Imagen y descripción --------
    og = soup.select_one('meta[property="og:image"]')
    if og and og.get("content"): out["image"] = og["content"]
    desc = soup.select_one('[data-testid="description"], section[id*="descripcion"], [class*="Description"]')
    if desc:
        out["desc_excerpt"] = clean(desc.get_text(" ", strip=True))[:800]
    else:
        out["desc_excerpt"] = clean(text)[:800]

    return out


def enrich_items_with_details(start_url, items, delay, limit=None):
    """Abre cada ficha con Playwright y superpone datos fiables (precio/km/año…)."""
    from playwright.sync_api import sync_playwright

    ua = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
          "AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/124.0.0.0 Safari/537.36")

    with sync_playwright() as p:
        br = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = br.new_context(locale="es-ES", user_agent=ua, viewport={"width":1280,"height":2000})
        ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        page = ctx.new_page()

        for i, it in enumerate(items):
            if limit and i >= limit: break
            try:
                page.goto(it["link"], wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(int(delay*1000))
                det = parse_detail_html(page.content())
                # Superpone SOLO si trae valor
                for k in ["price","km","year","fuel","gearbox","vat_note","power_kw","power_cv","image","desc_excerpt"]:
                    v = det.get(k)
                    if v not in (None,""):
                        it[k] = v
            except Exception:
                pass

        ctx.close(); br.close()
    return items

def add_page(url, n):
    pr = urlparse(url)
    qs = dict(parse_qsl(pr.query))
    qs["page"] = str(n)
    return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, urlencode(qs, doseq=True), pr.fragment))

def collect_autoscout(start_url, delay, max_pages):
    """
    Recorre el perfil con 3 estrategias SIEMPRE:
    1) Scroll en página actual
    2) Botón 'Siguiente' si existe
    3) Barrido ?page=N hasta que no haya crecimiento (2 páginas seguidas sin nuevos)
    """
    from playwright.sync_api import sync_playwright
    rows, seen = [], set()

    def page_url(url, n):
        pr = urlparse(url)
        qs = dict(parse_qsl(pr.query))
        qs["page"] = str(n)
        return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, urlencode(qs, doseq=True), pr.fragment))

    def extract_on(page, base):
        soup = BeautifulSoup(page.content(), "lxml")
        cards = soup.select(
            "article, [data-testid='result-list'] article, [class*='ListItem_wrapper__'], [data-item-name='listing']"
        )
        new = []
        for c in cards:
            r = parse_card(c, base)
            if r and r.get("listing_id") and r["listing_id"] not in seen:
                new.append(r)
        return new

    def scroll_and_collect(page, base, max_scrolls=15, stagnation_limit=3):
        last_len = len(rows)
        stagnant = 0
        for _ in range(max_scrolls):
            page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            page.wait_for_timeout(int(delay * 1000))
            got = extract_on(page, base)
            added = 0
            for r in got:
                rows.append(r); seen.add(r["listing_id"]); added += 1
            if added == 0:
                stagnant += 1
            else:
                stagnant = 0
            if stagnant >= stagnation_limit:
                break

    with sync_playwright() as p:
        ua = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(locale="es-ES", user_agent=ua, viewport={"width": 1280, "height": 2000})
        ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        page = ctx.new_page()
        base = f"{urlparse(start_url).scheme}://{urlparse(start_url).netloc}"

        def accept_cookies():
            for sel in [
                'button:has-text("Aceptar")',
                'button:has-text("Aceptar todo")',
                'text=Aceptar',
                'button:has-text("Allow all")',
                '[id*=onetrust-accept]'
            ]:
                try:
                    el = page.locator(sel).first
                    if el.is_visible():
                        el.click(timeout=1200); return
                except: pass

        # 1) Página inicial
        page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
        accept_cookies()
        scroll_and_collect(page, base, max_scrolls=18, stagnation_limit=3)
        print(f"[LISTING] Página 1: total {len(rows)}")

        # 2) Botón “Siguiente”
        page_no = 1
        while page_no < max_pages:
            page_no += 1
            clicked = False
            for sel in ['a[rel="next"]', 'a:has-text("Siguiente")', '[data-testid*=next]', 'button:has-text("Siguiente")']:
                try:
                    nxt = page.locator(sel).first
                    if nxt.is_visible():
                        nxt.click(timeout=2000)
                        clicked = True
                        page.wait_for_timeout(int(delay * 1000))
                        accept_cookies()
                        break
                except: pass
            if not clicked:
                break
            before = len(rows)
            scroll_and_collect(page, base, max_scrolls=12, stagnation_limit=2)
            print(f"[LISTING] Página {page_no} (botón): +{len(rows)-before} (total {len(rows)})")
            if len(rows) == before:
                break

        # 3) Barrido ?page=N SIEMPRE, hasta no crecer
        no_growth = 0
        last_total = len(rows)
        for n in range(2, max_pages + 1):
            url_n = page_url(start_url, n)
            try:
                page.goto(url_n, wait_until="domcontentloaded", timeout=60000)
            except:
                break
            page.wait_for_timeout(int(delay * 1000))
            accept_cookies()
            before = len(rows)
            scroll_and_collect(page, base, max_scrolls=10, stagnation_limit=2)
            added = len(rows) - before
            print(f"[LISTING] Página {n} (?page): +{added} (total {len(rows)})")
            if len(rows) == last_total:
                no_growth += 1
            else:
                no_growth = 0
            last_total = len(rows)
            if no_growth >= 2:
                break

        ctx.close(); browser.close()

    return rows


def update_tracker(outdir, items, today):
    ensure_dir(outdir); ensure_dir(os.path.join(outdir,"media"))
    tracker_path = os.path.join(outdir,"tracker_master.json")
    if os.path.exists(tracker_path):
        with open(tracker_path,"r",encoding="utf-8") as f: tracker=json.load(f)
    else:
        tracker={}

    seen_today = set(i["listing_id"] for i in items if i.get("listing_id"))
    events=[]

    for it in items:
        lid = it["listing_id"]; title = f"{it.get('brand','')} {it.get('model','')} {it.get('version','')}".strip()
        node = tracker.get(lid)
        imgfile = ""
        if it.get("image"):
            imgfile = f"media/{lid}.jpg"
            dst = os.path.join(outdir, imgfile)
            if not os.path.exists(dst):
                try:
                    r=requests.get(it["image"], timeout=20)
                    if r.status_code==200 and len(r.content)>128:
                        with open(dst,"wb") as f: f.write(r.content)
                except: pass

        if node is None:
            tracker[lid] = {
                "listing_id": lid, "first_seen": today, "last_seen": today,
                "removed_on":"", "days_active":0, "status":"active",
                "brand": it["brand"], "model": it["model"], "version": it["version"],
                "year": it["year"], "km": it["km"], "fuel": it["fuel"], "gearbox": it["gearbox"],
                "vat_note": it["vat_note"], "link": it["link"], "category": it["category"],
                "image_file": imgfile, "desc_excerpt":"", "last_price": it["price"],
                "price_first_seen": today, "price_last_change": today, "price_changes_count": 0,
                "price_history":[{"date":today,"price":it["price"]}]
            }
        else:
            old = node.get("last_price")
            new = it.get("price", old)
            node.update({
                "last_seen": today, "status":"active", "removed_on":"",
                "brand": it["brand"] or node.get("brand",""),
                "model": it["model"] or node.get("model",""),
                "version": it["version"] or node.get("version",""),
                "year": it["year"] or node.get("year"),
                "km": it["km"] or node.get("km"),
                "fuel": it["fuel"] or node.get("fuel",""),
                "gearbox": it["gearbox"] or node.get("gearbox",""),
                "vat_note": it["vat_note"] or node.get("vat_note",""),
                "link": it["link"] or node.get("link",""),
                "category": it["category"] or node.get("category",""),
            })
            if imgfile and not node.get("image_file"): node["image_file"]=imgfile
            if new is not None and old is not None and abs(float(new)-float(old))>0.5:
                node["price_history"].append({"date":today,"price":new})
                node["price_last_change"]=today
                node["price_changes_count"]=int(node.get("price_changes_count",0))+1
                events.append({
                    "date": today, "listing_id": lid, "title": title,
                    "old_price": float(old), "new_price": float(new),
                    "delta": float(new-old),
                    "pct": float((new-old)/old) if old else None
                })
            node["last_price"]=new

    for lid, node in tracker.items():
        if node.get("status")=="active" and lid not in seen_today:
            node["status"]="removed"; node["removed_on"]=today

    for node in tracker.values():
        fs=node.get("first_seen"); ro=node.get("removed_on")
        try:
            d0=datetime.fromisoformat(fs).date()
            d1=datetime.fromisoformat(ro).date() if ro else date.today()
            node["days_active"]=(d1-d0).days
        except: node["days_active"]=0

    with open(tracker_path,"w",encoding="utf-8") as f: json.dump(tracker,f,ensure_ascii=False,indent=2)

    flat=[]
    for v in tracker.values():
        flat.append({
            "listing_id": v["listing_id"], "first_seen": v["first_seen"], "last_seen": v["last_seen"],
            "removed_on": v["removed_on"], "days_active": v["days_active"], "status": v["status"],
            "brand": v.get("brand",""), "model": v.get("model",""), "version": v.get("version",""),
            "year": v.get("year"), "km": v.get("km"), "fuel": v.get("fuel",""), "gearbox": v.get("gearbox",""),
            "vat_note": v.get("vat_note",""), "link": v.get("link",""), "category": v.get("category",""),
            "image_file": v.get("image_file",""), "desc_excerpt": v.get("desc_excerpt",""),
            "last_price": v.get("last_price"),
            "price_first_seen": v.get("price_first_seen"), "price_last_change": v.get("price_last_change"),
            "price_changes_count": v.get("price_changes_count",0),
            "price_history_json": json.dumps(v.get("price_history",[]), ensure_ascii=False)
        })

    df = pd.DataFrame(flat)
    master_csv = os.path.join(outdir, "lovecars_tracker_master.csv")
    today_csv  = os.path.join(outdir, f"lovecars_autoscout_consolidado_{today}.csv")
    df.to_csv(master_csv, index=False, encoding="utf-8-sig")
    df.to_csv(today_csv,  index=False, encoding="utf-8-sig")

    ev_path = os.path.join(outdir, f"lovecars_price_events_{today}.csv")
    pd.DataFrame(events, columns=["date","listing_id","title","old_price","new_price","delta","pct"]).to_csv(ev_path, index=False, encoding="utf-8-sig")

    altas=[v for v in flat if v["first_seen"]==today]
    bajas=[v for v in flat if v["removed_on"]==today]
    activos=[v for v in flat if v["status"]=="active"]

    return {"items_collected":len(set(i["listing_id"] for i in items)),
            "master_csv":master_csv, "consolidated_csv":today_csv, "price_events_csv":ev_path,
            "counts":{"activos":len(activos),"altas":len(altas),"bajas":len(bajas),"price_events":len(events)}}

def run_once(config_path):
    cfg = read_cfg(config_path)
    outdir = os.path.abspath(cfg.get("output_dir","./output"))
    delay = float(cfg.get("delay_seconds",1.2))
    maxp  = int(cfg.get("max_pages",300))
    items = collect_autoscout(cfg.get("start_url"), delay, maxp)
    items = enrich_items_with_details(cfg.get("start_url"), items, delay)
    today = date.today().isoformat()
    return update_tracker(outdir, items, today)

if __name__=="__main__":
    print(run_once(os.path.join(HERE,"config.yaml")))
