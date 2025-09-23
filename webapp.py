# -*- coding: utf-8 -*-
import os, sys, json, subprocess
from datetime import datetime, date
import pandas as pd
from functools import wraps
from flask import Flask, render_template, jsonify, send_from_directory, request, Response
from apscheduler.schedulers.background import BackgroundScheduler
from zoneinfo import ZoneInfo

HERE = os.path.abspath(os.path.dirname(__file__))
OUT  = os.path.join(HERE, "output")

app = Flask(__name__, template_folder=os.path.join(HERE, "templates"))
status = {"running": False, "message": "listo", "last_run": None}

# --------------------------- Config ---------------------------

def read_cfg():
    try:
        import yaml
        with open(os.path.join(HERE, "config.yaml"), "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except:
        return {"daily_run": "08:15", "timezone": "Europe/Madrid"}

# --------- AUTH BÁSICA (usuario/contraseña) opcional ----------

def _auth_enabled():
    cfg = read_cfg() or {}
    a = cfg.get("auth", {})
    return bool(a.get("enabled", False))

def _auth_creds():
    cfg = read_cfg() or {}
    a = cfg.get("auth", {})
    # Variables de entorno tienen prioridad si existen
    user = os.getenv("AUTH_USER", a.get("username", ""))
    pwd  = os.getenv("AUTH_PASS", a.get("password", ""))
    realm = a.get("realm", "Monitor LoveCars")
    return user, pwd, realm

def _needs_auth():
    u, p, _ = _auth_creds()
    return _auth_enabled() and (u != "" or p != "")

def _check_auth(auth):
    if not auth:
        return False
    u, p, _ = _auth_creds()
    return (auth.username == u) and (auth.password == p)

def _auth_response():
    _, _, realm = _auth_creds()
    return Response("Autenticación requerida", 401,
        {"WWW-Authenticate": f'Basic realm="{realm}", charset="UTF-8"'}
    )

def requires_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _needs_auth():
            return fn(*args, **kwargs)
        auth = request.authorization
        if not _check_auth(auth):
            return _auth_response()
        return fn(*args, **kwargs)
    return wrapper

# ----------------------- Carga de datos -----------------------

def latest_consolidated():
    if not os.path.isdir(OUT):
        return None
    xs = sorted([f for f in os.listdir(OUT)
                 if f.startswith("lovecars_autoscout_consolidado_") and f.endswith(".csv")])
    return os.path.join(OUT, xs[-1]) if xs else None

def load_frames():
    m = latest_consolidated()
    if not m:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    df = pd.read_csv(m, dtype=str).fillna("")
    today = date.today().isoformat()
    inv   = df[df.get("status","")== "active"].copy() if "status" in df.columns else df.copy()
    altas = df[df.get("first_seen","")==today].copy()
    bajas = df[df.get("removed_on","")==today].copy()
    pev   = os.path.join(OUT, f"lovecars_price_events_{today}.csv")
    try:
        pe = pd.read_csv(pev, dtype=str).fillna("") if os.path.exists(pev) else \
             pd.DataFrame(columns=["date","listing_id","title","old_price","new_price","delta","pct"])
    except:
        pe = pd.DataFrame(columns=["date","listing_id","title","old_price","new_price","delta","pct"])
    return inv, altas, bajas, pe

# --------------------- Subproceso scraper ---------------------

def run_scraper_subproc():
    cmd = [sys.executable, "-c",
           "import json,autoscout_scraper; print(json.dumps(autoscout_scraper.run_once('config.yaml')))" ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        return False, f"rc={p.returncode} stderr={p.stderr[:300]} | stdout={p.stdout[:300]}"
    try:
        return True, json.loads(p.stdout.strip())
    except Exception as e:
        return False, f"Salida no JSON: {e}"

# ----------------------- Rutas principales --------------------

@app.get("/")
@requires_auth
def index():
    inv, altas, bajas, pe = load_frames()
    return render_template(
        "index.html",
        inv=inv.to_dict(orient="records"),
        altas=altas.to_dict(orient="records"),
        bajas=bajas.to_dict(orient="records"),
        pe=pe.to_dict(orient="records"),
        last_run=status.get("last_run"),
    )

@app.post("/update")
@requires_auth
def update():
    if status["running"]:
        return jsonify({"ok": False, "message": "Proceso en curso"}), 409
    import threading
    def work():
        status["running"] = True
        status["message"] = "Actualizando…"
        ok, payload = run_scraper_subproc()
        status["running"] = False
        status["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status["message"] = ("OK: "+str(payload.get("items_collected",0))+" fichas") if ok else ("ERROR: "+str(payload))
    threading.Thread(target=work, daemon=True).start()
    return jsonify({"ok": True})

@app.get("/status")
@requires_auth
def st():
    return jsonify(status)

@app.get("/media/<path:p>")
@requires_auth
def media(p):
    return send_from_directory(OUT, p)

# --------------------- Helpers Diario (Altas/Bajas) ---------------------

def _load_master():
    mpath = os.path.join(OUT, "lovecars_tracker_master.csv")
    if not os.path.exists(mpath):
        return pd.DataFrame()
    try:
        return pd.read_csv(mpath, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame()

def _list_available_days():
    try:
        files = [f for f in os.listdir(OUT)
                 if f.startswith("lovecars_autoscout_consolidado_") and f.endswith(".csv")]
    except FileNotFoundError:
        files = []
    days = []
    for f in sorted(files):
        try:
            days.append(f.split("_")[-1].replace(".csv",""))
        except:
            pass
    if not days:
        m = _load_master()
        if not m.empty:
            for col in ("first_seen","removed_on"):
                if col in m.columns:
                    days += [d for d in m[col].dropna().unique().tolist() if d]
    days = sorted(set(days), reverse=True)
    return days or [date.today().isoformat()]

def _normalize_day(s: str) -> str:
    if not s:
        return date.today().isoformat()
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return s[:10]

def _record_to_card(r):
    d = r if isinstance(r, dict) else dict(r)
    title = " ".join([d.get("brand",""), d.get("model",""), d.get("version","")]).strip()
    imgf = d.get("image_file") or d.get("image") or ""
    thumb = f"/media/{imgf}" if (imgf and not imgf.startswith("http")) else imgf
    return {
        "title": title,
        "year": d.get("year",""),
        "km": d.get("km",""),
        "price": d.get("last_price","") or d.get("price",""),
        "link": d.get("link",""),
        "first_seen": d.get("first_seen",""),
        "last_seen": d.get("last_seen",""),
        "removed_on": d.get("removed_on",""),
        "listing_id": d.get("listing_id",""),
        "status": d.get("status",""),
        "category": d.get("category",""),
        "thumb": thumb
    }

@app.get("/days")
@requires_auth
def days():
    return jsonify({"days": _list_available_days()})

@app.get("/bydate")
@requires_auth
def bydate():
    day = _normalize_day(request.args.get("date"))
    df = _load_master()
    if df.empty:
        return jsonify({"date": day, "counts": {"altas":0,"bajas":0}, "altas": [], "bajas": []})
    for col in ("first_seen","removed_on"):
        if col not in df.columns:
            df[col] = ""
    altas = df[df["first_seen"] == day]
    bajas = df[df["removed_on"] == day]
    altas_cards = [_record_to_card(r) for _, r in altas.iterrows()]
    bajas_cards = [_record_to_card(r) for _, r in bajas.iterrows()]
    return jsonify({
        "date": day,
        "counts": {"altas": len(altas_cards), "bajas": len(bajas_cards)},
        "altas": altas_cards,
        "bajas": bajas_cards
    })

# --------------------- Planificación diaria ---------------------

def schedule_daily():
    cfg = read_cfg()
    hh, mm = (cfg.get("daily_run") or "08:15").split(":")
    tz = ZoneInfo(cfg.get("timezone", "Europe/Madrid"))
    sch = BackgroundScheduler(timezone=tz)
    sch.add_job(lambda: run_scraper_subproc(), "cron", hour=int(hh), minute=int(mm), id="daily")
    sch.start()

# --------------------------- Main -----------------------------

if __name__ == "__main__":
    schedule_daily()
    app.run(host="0.0.0.0", port=8000)

