from fastapi import FastAPI, APIRouter, HTTPException, Request, Response, Cookie, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import JSONResponse, RedirectResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import json
import logging
import httpx
from pathlib import Path
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Set
import uuid
from datetime import datetime, timezone, timedelta
import secrets
import string
from urllib.parse import urlencode
from google.oauth2 import id_token
from google.auth.transport import requests as g_requests

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ['MONGO_URI']
GOOGLE_CLIENT_ID = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
FRONTEND_URL = os.environ["FRONTEND_URL"].rstrip("/")
BACKEND_PUBLIC_URL = os.environ["BACKEND_PUBLIC_URL"].rstrip("/")
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

app = FastAPI()
api_router = APIRouter(prefix="/api")

# -------------------- Models --------------------
class User(BaseModel):
    user_id: str
    email: str
    name: str
    handle: Optional[str] = None  # 5-char unique tag (A-Z, 0-9)
    picture: Optional[str] = None
    created_at: datetime

class FriendAddRequest(BaseModel):
    name: str
    handle: str

class FriendOrderUpdate(BaseModel):
    order: List[str]  # ordered list of friend user_ids

class Exercise(BaseModel):
    key: str
    name: str
    unit: str = ""
    icon: str = "pushup"
    color: str = "#CCFF00"
    base_value: float = 0
    progression_pct: int = 10  # 1..10 — individuelle wöchentliche Steigerung in %
    added_week: Optional[int] = None  # In welcher Programm-Woche die Übung hinzugefügt wurde

    @field_validator("progression_pct")
    @classmethod
    def _clamp_progression_pct(cls, v):
        try:
            v = int(round(float(v)))
        except (TypeError, ValueError):
            v = 10
        return max(1, min(10, v))

class GoalsUpdate(BaseModel):
    exercises: List[Exercise]

class BoostRequest(BaseModel):
    exercise_key: str

class ProgressUpdate(BaseModel):
    week_number: int
    values: Optional[dict] = None    # legacy: total per exercise
    days: Optional[dict] = None      # new: {"0".."6": {exercise_key: number}}

class ProfileUpdate(BaseModel):
    name: Optional[str] = None
    picture: Optional[str] = None

class AuthPreferencesUpdate(BaseModel):
    remember_me: bool

DEFAULT_EXERCISES = [
    {"key": "ex1", "name": "Lauf",         "unit": "km", "icon": "run",    "color": "#CCFF00", "base_value": 10.0,  "progression_pct": 10},
    {"key": "ex2", "name": "Liegestütze",  "unit": "",   "icon": "pushup", "color": "#FF3B30", "base_value": 500,   "progression_pct": 10},
    {"key": "ex3", "name": "Klimmzüge",    "unit": "",   "icon": "pullup", "color": "#00F0FF", "base_value": 50,    "progression_pct": 10},
]
EXERCISE_PALETTE = ["#CCFF00", "#FF3B30", "#00F0FF", "#FF8800", "#A855F7"]

def _normalize_exercise_colors(exercises: list) -> list:
    if not exercises:
        return exercises
    for idx, ex in enumerate(exercises):
        if isinstance(ex, dict):
            ex["color"] = EXERCISE_PALETTE[idx % len(EXERCISE_PALETTE)]
    return exercises

BASE_INCREASE = 0.10
BOOST_INCREASE = 0.25
FUTURE_WEEKS = 10

# -------------------- WebSocket Manager --------------------
class ConnectionManager:
    def __init__(self):
        self.active: dict = {}

    async def connect(self, ws: WebSocket, user_id: Optional[str] = None):
        await ws.accept()
        self.active[ws] = user_id

    def disconnect(self, ws: WebSocket):
        self.active.pop(ws, None)

    def online_user_ids(self) -> Set[str]:
        return {uid for uid in self.active.values() if uid}

    async def broadcast(self, message: dict):
        dead = []
        for ws in list(self.active.keys()):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.active.pop(ws, None)

    async def broadcast_presence(self):
        await self.broadcast({
            "type": "presence_changed",
            "online_user_ids": list(self.online_user_ids()),
        })

manager = ConnectionManager()

# -------------------- Auth helpers --------------------
async def get_current_user(request: Request) -> User:
    token = request.cookies.get("session_token")
    if not token:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    session = await db.user_sessions.find_one({"session_token": token}, {"_id": 0})
    if not session:
        raise HTTPException(status_code=401, detail="Invalid session")

    expires_at = session["expires_at"]
    if isinstance(expires_at, str):
        expires_at = datetime.fromisoformat(expires_at)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=401, detail="Session expired")

    user_doc = await db.users.find_one({"user_id": session["user_id"]}, {"_id": 0})
    if not user_doc:
        raise HTTPException(status_code=401, detail="User not found")
    if isinstance(user_doc.get("created_at"), str):
        user_doc["created_at"] = datetime.fromisoformat(user_doc["created_at"])
    if not user_doc.get("handle"):
        h = await _generate_unique_handle()
        await db.users.update_one({"user_id": user_doc["user_id"]}, {"$set": {"handle": h}})
        user_doc["handle"] = h
    return User(**user_doc)

# -------------------- Handle (5-char unique tag) --------------------
HANDLE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

async def _generate_unique_handle() -> str:
    for _ in range(50):
        candidate = "".join(secrets.choice(HANDLE_ALPHABET) for _ in range(5))
        existing = await db.users.find_one({"handle": candidate}, {"_id": 0, "user_id": 1})
        if not existing:
            return candidate
    return uuid.uuid4().hex[:5].upper()

async def _backfill_handles():
    cursor = db.users.find({"$or": [{"handle": {"$exists": False}}, {"handle": None}, {"handle": ""}]}, {"_id": 0, "user_id": 1})
    async for u in cursor:
        h = await _generate_unique_handle()
        await db.users.update_one({"user_id": u["user_id"]}, {"$set": {"handle": h}})


# -------------------- Session/Cookie Helpers --------------------
SESSION_HINT_COOKIE = "session_hint"


def _set_session_cookies(response: Response, session_token: str, cookie_max_age: Optional[int]):
    """Setzt sowohl das echte HttpOnly session_token Cookie als auch das
    nicht-HttpOnly session_hint Cookie. cookie_max_age=None -> Session-Cookie."""
    response.set_cookie(
        key="session_token",
        value=session_token,
        httponly=True,
        secure=True,
        samesite="none",
        path="/",
        max_age=cookie_max_age,
    )
    response.set_cookie(
        key=SESSION_HINT_COOKIE,
        value="1",
        httponly=False,
        secure=True,
        samesite="none",
        path="/",
        max_age=cookie_max_age,
    )


async def _create_session_and_set_cookie(response: Response, user_id: str, remember_me: bool) -> str:
    """Erstellt eine DB-Session + setzt session_token + session_hint Cookies.
    Gibt den Token zurück (wird zusätzlich im OAuth-Callback ins URL-Fragment gehängt)."""
    session_token = secrets.token_urlsafe(48)
    if remember_me:
        session_days = 30
        cookie_max_age = 30 * 24 * 3600
    else:
        session_days = 1
        cookie_max_age = None
    expires_at = datetime.now(timezone.utc) + timedelta(days=session_days)
    await db.user_sessions.insert_one({
        "user_id": user_id,
        "session_token": session_token,
        "expires_at": expires_at.isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    _set_session_cookies(response, session_token, cookie_max_age)
    return session_token

async def _upsert_google_user(email: str, name: str, picture: Optional[str]) -> tuple[str, bool]:
    existing = await db.users.find_one({"email": email}, {"_id": 0})
    if existing:
        user_id = existing["user_id"]
        remember_me = existing.get("remember_me", True)
        update_set = {"name": name, "picture": picture}
        if not existing.get("handle"):
            update_set["handle"] = await _generate_unique_handle()
        await db.users.update_one(
            {"user_id": user_id},
            {"$set": update_set}
        )
        return user_id, remember_me

    user_id = f"user_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc)
    handle = await _generate_unique_handle()
    await db.users.insert_one({
        "user_id": user_id,
        "email": email,
        "name": name,
        "handle": handle,
        "picture": picture,
        "created_at": now.isoformat(),
        "remember_me": True,
        "friends": [],
        "friend_order": [],
    })
    await db.user_goals.insert_one({
        "user_id": user_id,
        "exercises": [dict(e) for e in DEFAULT_EXERCISES],
        "boosts": [],
        "weekly_increase": BASE_INCREASE,
        "start_date": now.isoformat(),
    })
    return user_id, True

# -------------------- Auth routes --------------------

@api_router.get("/auth/google/login")
async def google_login_start(redirect: str = "/dashboard"):
    state = secrets.token_urlsafe(24)
    if not redirect.startswith("/"):
        redirect = "/dashboard"
    await db.oauth_states.insert_one({
        "state": state,
        "redirect_to": redirect,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    callback_url = f"{BACKEND_PUBLIC_URL}/api/auth/google/callback"
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": callback_url,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "online",
        "prompt": "select_account",
        "include_granted_scopes": "true",
    }
    google_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
    return RedirectResponse(google_url, status_code=302)

@api_router.get("/auth/google/callback")
async def google_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
):
    """OAuth Callback: tauscht Code gegen ID-Token, erstellt Session, redirected zum Frontend."""
    def _fail(reason: str) -> RedirectResponse:
        return RedirectResponse(f"{FRONTEND_URL}/?auth_error={reason}", status_code=302)

    if error:
        return _fail(error)
    if not code or not state:
        return _fail("missing_params")

    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    try:
        await db.oauth_states.delete_many({"created_at": {"$lt": cutoff}})
    except Exception:
        pass

    state_doc = await db.oauth_states.find_one_and_delete({"state": state})
    if not state_doc:
        return _fail("invalid_state")
    redirect_to = state_doc.get("redirect_to", "/dashboard")
    if not redirect_to.startswith("/"):
        redirect_to = "/dashboard"

    callback_url = f"{BACKEND_PUBLIC_URL}/api/auth/google/callback"
    try:
        async with httpx.AsyncClient(timeout=10.0) as hc:
            token_resp = await hc.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "code": code,
                    "client_id": GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "redirect_uri": callback_url,
                    "grant_type": "authorization_code",
                },
            )
    except Exception:
        return _fail("token_request_failed")
    if token_resp.status_code != 200:
        return _fail("token_exchange_failed")
    tokens = token_resp.json()
    id_token_str = tokens.get("id_token")
    if not id_token_str:
        return _fail("no_id_token")

    try:
        idinfo = id_token.verify_oauth2_token(
            id_token_str, g_requests.Request(), GOOGLE_CLIENT_ID
        )
    except ValueError:
        return _fail("invalid_id_token")

    if not idinfo.get("email_verified", False):
        return _fail("email_not_verified")

    email = idinfo["email"]
    name = idinfo.get("name") or email
    picture = idinfo.get("picture")

    user_id, remember_me = await _upsert_google_user(email, name, picture)

    base_url = f"{FRONTEND_URL}{redirect_to}"
    response = RedirectResponse(base_url, status_code=302)
    session_token = await _create_session_and_set_cookie(response, user_id, remember_me)
    response.headers["location"] = f"{base_url}#token={session_token}"
    return response

@api_router.post("/auth/session")
async def process_session(request: Request, response: Response):
    """Legacy-Endpoint: nimmt ein Google ID-Token vom Frontend (Popup-Flow)."""
    body = await request.json()
    credential = body.get("credential") or body.get("id_token")
    if not credential:
        raise HTTPException(status_code=400, detail="credential required")

    try:
        idinfo = id_token.verify_oauth2_token(
            credential, g_requests.Request(), GOOGLE_CLIENT_ID,
        )
    except ValueError as e:
        raise HTTPException(status_code=401, detail=f"Invalid Google token: {e}")

    if not idinfo.get("email_verified", False):
        raise HTTPException(status_code=401, detail="Email not verified by Google")

    email = idinfo["email"]
    name = idinfo.get("name") or email
    picture = idinfo.get("picture")

    user_id, remember_me = await _upsert_google_user(email, name, picture)
    session_token = await _create_session_and_set_cookie(response, user_id, remember_me)
    return {
        "user_id": user_id,
        "email": email,
        "name": name,
        "picture": picture,
        "session_token": session_token,
    }

@api_router.get("/auth/me")
async def me(request: Request, response: Response, user: User = Depends(get_current_user)):
    user_doc = await db.users.find_one({"user_id": user.user_id}, {"_id": 0})
    if user_doc and user_doc.get("remember_me", True):
        token = request.cookies.get("session_token")
        if not token:
            auth = request.headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                token = auth[7:]
        if token:
            new_expires = datetime.now(timezone.utc) + timedelta(days=30)
            await db.user_sessions.update_one(
                {"session_token": token},
                {"$set": {"expires_at": new_expires.isoformat()}}
            )
            _set_session_cookies(response, token, 30 * 24 * 3600)
    return user.model_dump(mode="json")

@api_router.post("/auth/logout")
async def logout(request: Request, response: Response):
    token = request.cookies.get("session_token")
    if not token:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
    if token:
        await db.user_sessions.delete_one({"session_token": token})
    response.delete_cookie("session_token", path="/", samesite="none", secure=True)
    response.delete_cookie(SESSION_HINT_COOKIE, path="/", samesite="none", secure=True)
    return {"ok": True}

@api_router.get("/auth/preferences")
async def get_auth_prefs(user: User = Depends(get_current_user)):
    user_doc = await db.users.find_one({"user_id": user.user_id}, {"_id": 0})
    return {"remember_me": bool(user_doc.get("remember_me", True)) if user_doc else True}

@api_router.put("/auth/preferences")
async def set_auth_prefs(
    payload: AuthPreferencesUpdate,
    request: Request,
    response: Response,
    user: User = Depends(get_current_user),
):
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {"remember_me": payload.remember_me}}
    )

    token = request.cookies.get("session_token")
    if not token:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]

    if token:
        if payload.remember_me:
            new_expires = datetime.now(timezone.utc) + timedelta(days=30)
            cookie_max_age = 30 * 24 * 3600
        else:
            new_expires = datetime.now(timezone.utc) + timedelta(days=1)
            cookie_max_age = None
        await db.user_sessions.update_one(
            {"session_token": token},
            {"$set": {"expires_at": new_expires.isoformat()}}
        )
        _set_session_cookies(response, token, cookie_max_age)
    return {"remember_me": payload.remember_me}

# -------------------- Goals --------------------
def _calc_week_number(start_date: datetime) -> int:
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    start_monday = (start_date - timedelta(days=start_date.weekday())).date()
    today_monday = (now - timedelta(days=now.weekday())).date()
    return max(1, (today_monday - start_monday).days // 7 + 1)

def _round_goal(value: float, unit: str) -> float:
    u = (unit or "").lower()
    if "km" in u or "m" == u or "mi" in u:
        return float(int(value * 10 + 0.5)) / 10.0
    return float(int(value + 0.5))

async def _load_goals(user_id: str) -> dict:
    g = await db.user_goals.find_one({"user_id": user_id}, {"_id": 0})
    now = datetime.now(timezone.utc)
    if not g:
        g = {
            "user_id": user_id,
            "exercises": [dict(e) for e in DEFAULT_EXERCISES],
            "boosts": [],
            "weekly_increase": BASE_INCREASE,
            "start_date": now.isoformat(),
        }
        await db.user_goals.insert_one(dict(g))
        return g
    if "exercises" not in g:
        legacy = [
            {"key": "ex1", "name": "Lauf",         "unit": "km", "icon": "run",    "color": "#CCFF00", "base_value": float(g.get("base_run_km", 10.0))},
            {"key": "ex2", "name": "Liegestütze",  "unit": "",   "icon": "pushup", "color": "#FF3B30", "base_value": float(g.get("base_pushups", 500))},
            {"key": "ex3", "name": "Klimmzüge",    "unit": "",   "icon": "pullup", "color": "#00F0FF", "base_value": float(g.get("base_pullups", 50))},
        ]
        await db.user_goals.update_one(
            {"user_id": user_id},
            {"$set": {"exercises": legacy, "boosts": g.get("boosts", [])},
             "$unset": {"base_run_km": "", "base_pushups": "", "base_pullups": ""}},
        )
        g["exercises"] = legacy
        g["boosts"] = g.get("boosts", [])
    if "boosts" not in g:
        g["boosts"] = []
        await db.user_goals.update_one({"user_id": user_id}, {"$set": {"boosts": []}})
    _normalize_exercise_colors(g.get("exercises", []))
    _ensure_progression_pct(g.get("exercises", []))
    return g

def _ensure_progression_pct(exercises: list) -> list:
    if not exercises:
        return exercises
    for ex in exercises:
        if not isinstance(ex, dict):
            continue
        try:
            v = int(round(float(ex.get("progression_pct", 10))))
        except (TypeError, ValueError):
            v = 10
        ex["progression_pct"] = max(1, min(10, v))
    return exercises

def _boosted_weeks_for(g: dict, exercise_key: str) -> set:
    return {b["week_number"] for b in g.get("boosts", []) if b.get("exercise_key") == exercise_key}

async def _progress_by_week(user_id: str, exercises: list) -> dict:
    entries = await db.progress_entries.find({"user_id": user_id}, {"_id": 0}).to_list(1000)
    out = {}
    ex_keys = {e["key"] for e in exercises}
    for pe in entries:
        wn = pe.get("week_number")
        if wn is None:
            continue
        if "values" in pe:
            out[wn] = {k: float(v or 0) for k, v in pe["values"].items() if k in ex_keys}
        else:
            legacy = {
                "ex1": float(pe.get("run_km", 0) or 0),
                "ex2": float(pe.get("pushups", 0) or 0),
                "ex3": float(pe.get("pullups", 0) or 0),
            }
            out[wn] = {k: v for k, v in legacy.items() if k in ex_keys}
    return out

def _compute_progression(exercise: dict, all_boost_weeks: set, progress_by_week: dict,
                        current_week: int, future_weeks: int = FUTURE_WEEKS) -> dict:
    base = float(exercise.get("base_value", 0) or 0)
    unit = exercise.get("unit", "")
    ex_key = exercise["key"]

    try:
        pct = int(round(float(exercise.get("progression_pct", 10))))
    except (TypeError, ValueError):
        pct = 10
    pct = max(1, min(10, pct))
    ex_increase = pct / 100.0

    eff_idx = 0
    active_boosts = []
    missed = []
    progression = []

    last_week = max(current_week, 1) + future_weeks

    for w in range(1, last_week + 1):
        boost_this_week = w in all_boost_weeks
        if boost_this_week:
            active_boosts.append(w)

        goal_raw = base * ((1 + ex_increase) ** eff_idx) * ((1 + BOOST_INCREASE) ** len(active_boosts))
        goal_rounded = _round_goal(goal_raw, unit)

        if w < current_week:
            logged = float(progress_by_week.get(w, {}).get(ex_key, 0) or 0)
            # Float-Toleranz: 0.5% vom Ziel, mindestens 0.01.
            # Verhindert, dass akkumulierte Float-Rundungsfehler aus dem
            # Tages-Modus (Mo-So) eine erfuellte Woche faelschlich als
            # "missed" markieren und damit den Streak zerstoeren.
            tol = max(0.01, goal_rounded * 0.005)
            if logged + tol < goal_rounded:
                missed.append(w)
                active_boosts = [b for b in active_boosts if b > w]
                status = "missed"
                voided_boost = boost_this_week
            else:
                status = "completed"
                voided_boost = False
                eff_idx += 1
        elif w == current_week:
            status = "current"
            voided_boost = False
        else:
            status = "future"
            voided_boost = False

        progression.append({
            "week": w,
            "goal": goal_rounded,
            "status": status,
            "boost": boost_this_week,
            "voided_boost": voided_boost,
        })

        if status == "future":
            eff_idx += 1
        elif status == "current":
            eff_idx += 1

    current_goal = next((p["goal"] for p in progression if p["week"] == current_week), 0)

    return {
        "missed_weeks": missed,
        "effective_boost_weeks": sorted(active_boosts),
        "current_goal": current_goal,
        "progression": progression,
    }

async def _compute_user_state(user_id: str, g: dict, current_week: int) -> dict:
    exercises = g["exercises"]
    progress_by_week = await _progress_by_week(user_id, exercises)
    state = {}
    for ex in exercises:
        bws = _boosted_weeks_for(g, ex["key"])
        state[ex["key"]] = _compute_progression(ex, bws, progress_by_week, current_week)
    return state

@api_router.get("/goals/me")
async def get_my_goals(user: User = Depends(get_current_user)):
    g = await _load_goals(user.user_id)
    sd = g["start_date"]
    if isinstance(sd, str):
        sd = datetime.fromisoformat(sd)
    cur_week = _calc_week_number(sd)
    state = await _compute_user_state(user.user_id, g, cur_week)
    g["current_week"] = cur_week
    g["state"] = state
    return g

@api_router.put("/goals/me")
async def update_my_goals(payload: GoalsUpdate, user: User = Depends(get_current_user)):
    g = await _load_goals(user.user_id)
    if not (3 <= len(payload.exercises) <= 7):
        raise HTTPException(status_code=400, detail="Es müssen 3 bis 7 Übungen sein")
    keys = [e.key for e in payload.exercises]
    if len(set(keys)) != len(keys):
        raise HTTPException(status_code=400, detail="Übungs-Keys müssen eindeutig sein")
    exercises = [e.model_dump() for e in payload.exercises]
    _normalize_exercise_colors(exercises)
    _ensure_progression_pct(exercises)

    sd = g["start_date"]
    if isinstance(sd, str):
        sd = datetime.fromisoformat(sd)
    cur_week = _calc_week_number(sd)
    PROGRESSION_COOLDOWN = 4
    old_by_key = {e["key"]: e for e in g.get("exercises", [])}
    for ex in exercises:
        old = old_by_key.get(ex["key"])
        if not old:
            # Neue Übung -> in dieser Woche hinzugefügt, voll editierbar bis nächste Woche
            ex["added_week"] = cur_week
            ex["progression_last_changed_week"] = cur_week
            continue
        # Bestehende Übung: added_week unverändert übernehmen
        if old.get("added_week") is not None:
            ex["added_week"] = int(old.get("added_week"))
        elif ex.get("added_week") is None:
            ex["added_week"] = 1  # Legacy: alte Übungen ohne added_week -> Woche 1
        # In der Hinzufüge-Woche dürfen Startwert/Einheit/Steigerung frei geändert werden.
        added_week = int(ex.get("added_week") or 1)
        weeks_since_added = cur_week - added_week + 1
        is_new_in_first_week = weeks_since_added <= 1
        if cur_week > 1 and not is_new_in_first_week:
            try:
                if float(ex.get("base_value", 0)) != float(old.get("base_value", 0)):
                    ex["base_value"] = float(old.get("base_value", 0))
            except (TypeError, ValueError):
                ex["base_value"] = float(old.get("base_value", 0))
            # Einheit ebenfalls ab Woche 2 (nach Hinzufügen) gesperrt
            if (ex.get("unit") or "") != (old.get("unit") or ""):
                ex["unit"] = old.get("unit") or ""
        old_pct = int(old.get("progression_pct", 10))
        new_pct = int(ex.get("progression_pct", 10))
        last_changed = int(old.get("progression_last_changed_week", added_week))
        if new_pct != old_pct:
            if not is_new_in_first_week and cur_week > 1 and (cur_week - last_changed) < PROGRESSION_COOLDOWN:
                weeks_left = PROGRESSION_COOLDOWN - (cur_week - last_changed)
                raise HTTPException(
                    status_code=400,
                    detail=f"Steigerung für '{ex['name']}' kann erst in {weeks_left} Woche(n) wieder angepasst werden",
                )
            ex["progression_last_changed_week"] = cur_week
        else:
            ex["progression_last_changed_week"] = last_changed

    await db.user_goals.update_one(
        {"user_id": user.user_id},
        {"$set": {"exercises": exercises}}
    )
    await manager.broadcast({"type": "goals_updated", "user_id": user.user_id})
    g = await db.user_goals.find_one({"user_id": user.user_id}, {"_id": 0})
    return g

@api_router.post("/goals/me/reset-start")
async def reset_start_date(user: User = Depends(get_current_user)):
    now = datetime.now(timezone.utc).isoformat()
    g = await db.user_goals.find_one({"user_id": user.user_id}, {"_id": 0})
    exercises = g.get("exercises", []) if g else []
    for ex in exercises:
        ex["progression_last_changed_week"] = 1
        ex["added_week"] = 1
    await db.user_goals.update_one(
        {"user_id": user.user_id},
        {"$set": {"start_date": now, "exercises": exercises, "last_streak": 0}},
    )
    await db.progress_entries.delete_many({"user_id": user.user_id})
    await db.user_goals.update_one(
        {"user_id": user.user_id},
        {"$set": {"boosts": []}},
    )
    await manager.broadcast({"type": "goals_updated", "user_id": user.user_id})
    g = await db.user_goals.find_one({"user_id": user.user_id}, {"_id": 0})
    return g

# -------------------- Progress --------------------
@api_router.get("/progress/me")
async def my_progress(week: Optional[int] = None, user: User = Depends(get_current_user)):
    g = await _load_goals(user.user_id)
    sd = g["start_date"]
    if isinstance(sd, str):
        sd = datetime.fromisoformat(sd)
    target_week = week if week else _calc_week_number(sd)
    entry = await db.progress_entries.find_one(
        {"user_id": user.user_id, "week_number": target_week},
        {"_id": 0},
    )
    if not entry:
        entry = {
            "user_id": user.user_id,
            "week_number": target_week,
            "values": {e["key"]: 0 for e in g["exercises"]},
            "updated_at": None,
        }
    elif "values" not in entry:
        entry["values"] = {
            "ex1": entry.get("run_km", 0),
            "ex2": entry.get("pushups", 0),
            "ex3": entry.get("pullups", 0),
        }
    return entry

@api_router.put("/progress/me")
async def update_progress(payload: ProgressUpdate, user: User = Depends(get_current_user)):
    now = datetime.now(timezone.utc).isoformat()
    days = payload.days or {}
    if days:
        totals = {}
        for d, vals in days.items():
            for k, v in (vals or {}).items():
                totals[k] = totals.get(k, 0) + (float(v) or 0)
        values = totals
    else:
        values = payload.values or {}
    doc = {
        "user_id": user.user_id,
        "week_number": payload.week_number,
        "values": values,
        "days": days,
        "updated_at": now,
    }
    await db.progress_entries.update_one(
        {"user_id": user.user_id, "week_number": payload.week_number},
        {"$set": doc, "$unset": {"run_km": "", "pushups": "", "pullups": ""}},
        upsert=True,
    )
    await manager.broadcast({
        "type": "progress_updated",
        "user_id": user.user_id,
        "week_number": payload.week_number,
        "values": values,
    })
    return doc

# -------------------- Friends --------------------
async def _get_friend_ids_ordered(user_id: str) -> List[str]:
    udoc = await db.users.find_one({"user_id": user_id}, {"_id": 0, "friends": 1, "friend_order": 1})
    if not udoc:
        return []
    friends = list(udoc.get("friends") or [])
    order = list(udoc.get("friend_order") or [])
    seen = set()
    result = []
    for uid in order:
        if uid in friends and uid not in seen:
            result.append(uid)
            seen.add(uid)
    for uid in friends:
        if uid not in seen:
            result.append(uid)
            seen.add(uid)
    return result

@api_router.get("/friends")
async def list_friends(user: User = Depends(get_current_user)):
    ordered_ids = await _get_friend_ids_ordered(user.user_id)
    if not ordered_ids:
        return {"friends": []}
    docs = await db.users.find({"user_id": {"$in": ordered_ids}}, {"_id": 0, "user_id": 1, "name": 1, "handle": 1, "picture": 1}).to_list(500)
    by_id = {d["user_id"]: d for d in docs}
    out = []
    for uid in ordered_ids:
        d = by_id.get(uid)
        if not d:
            continue
        out.append({
            "user_id": d["user_id"],
            "name": d.get("name", ""),
            "handle": d.get("handle"),
            "picture": d.get("picture"),
        })
    return {"friends": out}

@api_router.post("/friends/add")
async def add_friend(payload: FriendAddRequest, user: User = Depends(get_current_user)):
    handle = (payload.handle or "").strip().upper().lstrip("#")
    name = (payload.name or "").strip()
    if not handle or not name:
        raise HTTPException(status_code=400, detail="Name und Hashtag erforderlich")
    if len(handle) != 5:
        raise HTTPException(status_code=400, detail="Hashtag muss 5 Zeichen lang sein")

    target = await db.users.find_one({"handle": handle}, {"_id": 0})
    if not target:
        raise HTTPException(status_code=404, detail="Kein User mit diesem Hashtag")
    if (target.get("name") or "").strip().lower() != name.lower():
        raise HTTPException(status_code=404, detail="Name passt nicht zum Hashtag")
    if target["user_id"] == user.user_id:
        raise HTTPException(status_code=400, detail="Dich selbst kannst du nicht hinzufügen")

    me_doc = await db.users.find_one({"user_id": user.user_id}, {"_id": 0, "friends": 1, "friend_order": 1})
    friends = list((me_doc or {}).get("friends") or [])
    if target["user_id"] in friends:
        raise HTTPException(status_code=400, detail="Bereits in deiner Crew")
    friends.append(target["user_id"])
    order = list((me_doc or {}).get("friend_order") or [])
    if target["user_id"] not in order:
        order.append(target["user_id"])
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {"friends": friends, "friend_order": order}},
    )
    return {
        "ok": True,
        "friend": {
            "user_id": target["user_id"],
            "name": target.get("name", ""),
            "handle": target.get("handle"),
            "picture": target.get("picture"),
        },
    }

@api_router.delete("/friends/{friend_user_id}")
async def remove_friend(friend_user_id: str, user: User = Depends(get_current_user)):
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$pull": {"friends": friend_user_id, "friend_order": friend_user_id}},
    )
    return {"ok": True}

@api_router.put("/friends/order")
async def reorder_friends(payload: FriendOrderUpdate, user: User = Depends(get_current_user)):
    me_doc = await db.users.find_one({"user_id": user.user_id}, {"_id": 0, "friends": 1})
    friends = set((me_doc or {}).get("friends") or [])
    cleaned = []
    seen = set()
    for uid in payload.order:
        if uid in friends and uid not in seen:
            cleaned.append(uid)
            seen.add(uid)
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {"friend_order": cleaned}},
    )
    return {"ok": True, "order": cleaned}

# -------------------- Live Board --------------------
def _streak_info(state: dict, exercises: list, progress_by_week: dict, current_week: int):
    missed_union = set()
    for ex in exercises:
        for w in state[ex["key"]]["missed_weeks"]:
            missed_union.add(w)

    completed = []
    for w in range(1, current_week + 1):
        if w in missed_union:
            continue
        all_done = True
        for ex in exercises:
            prog = state[ex["key"]]["progression"]
            target = next((p["goal"] for p in prog if p["week"] == w), None)
            logged = float(progress_by_week.get(w, {}).get(ex["key"], 0) or 0)
            # Gleiche Toleranz wie in _compute_progression: 0.5% vom Ziel,
            # mindestens 0.01. Sonst wuerden Float-Summen aus dem Tages-Modus
            # eine erfuellte Woche faelschlich als nicht completed werten.
            tol = max(0.01, (target or 0) * 0.005)
            if target is None or logged + tol < target:
                all_done = False
                break
        if all_done:
            completed.append(w)

    completed_set = set(completed)
    cur = 0
    # Starte beim aktuellen Wochen-Index. Wenn die aktuelle Woche noch nicht
    # abgeschlossen ist (z.B. gerade erst gestartet), wird ab der Vorwoche
    # gezählt – sonst würde der Streak beim Wochenwechsel fälschlicherweise
    # auf 0 fallen, obwohl die Vorwoche(n) erledigt waren.
    w = current_week
    if w not in completed_set:
        w -= 1
    while w >= 1 and w in completed_set:
        cur += 1
        w -= 1
    best = 0
    run = 0
    prev = None
    for w in completed:
        run = run + 1 if prev is not None and w == prev + 1 else 1
        best = max(best, run)
        prev = w
    return {"current": cur, "best": best, "completed_weeks": completed}

@api_router.get("/board")
async def board(week: Optional[int] = None, user: User = Depends(get_current_user)):
    friend_ids = await _get_friend_ids_ordered(user.user_id)
    target_ids = [user.user_id] + [fid for fid in friend_ids if fid != user.user_id]
    users_docs = await db.users.find({"user_id": {"$in": target_ids}}, {"_id": 0}).to_list(500)
    by_id = {u["user_id"]: u for u in users_docs}
    users = [by_id[uid] for uid in target_ids if uid in by_id]
    result = []
    for u in users:
        g = await _load_goals(u["user_id"])
        sd = g["start_date"]
        if isinstance(sd, str):
            sd = datetime.fromisoformat(sd)
        cur_week = week if week else _calc_week_number(sd)
        state = await _compute_user_state(u["user_id"], g, cur_week)
        progress_by_week = await _progress_by_week(u["user_id"], g["exercises"])
        exercises_out = []
        for ex in g["exercises"]:
            st = state[ex["key"]]
            try:
                _pp = int(round(float(ex.get("progression_pct", 10))))
            except (TypeError, ValueError):
                _pp = 10
            _pp = max(1, min(10, _pp))
            exercises_out.append({
                "key": ex["key"],
                "name": ex["name"],
                "unit": ex.get("unit", ""),
                "icon": ex.get("icon", "pushup"),
                "color": ex.get("color", "#CCFF00"),
                "goal": st["current_goal"],
                "progression_pct": _pp,
                "boosted_this_week": cur_week in st["effective_boost_weeks"],
                "boosted_weeks": st["effective_boost_weeks"],
                "missed_weeks": st["missed_weeks"],
            })
        entry = await db.progress_entries.find_one(
            {"user_id": u["user_id"], "week_number": cur_week},
            {"_id": 0},
        )
        if entry and "values" not in entry:
            entry["values"] = {
                "ex1": entry.get("run_km", 0),
                "ex2": entry.get("pushups", 0),
                "ex3": entry.get("pullups", 0),
            }
        values = entry["values"] if entry else {e["key"]: 0 for e in g["exercises"]}
        days = entry.get("days", {}) if entry else {}
        all_time_totals = {ex["key"]: 0.0 for ex in g["exercises"]}
        all_entries = await db.progress_entries.find(
            {"user_id": u["user_id"]}, {"_id": 0}
        ).to_list(1000)
        for pe in all_entries:
            week_vals = {}
            pe_days = pe.get("days") or {}
            if pe_days:
                for d_key, d_vals in pe_days.items():
                    if not isinstance(d_vals, dict):
                        continue
                    for k, v in d_vals.items():
                        try:
                            week_vals[k] = week_vals.get(k, 0.0) + float(v or 0)
                        except (TypeError, ValueError):
                            continue
            elif pe.get("values"):
                for k, v in (pe.get("values") or {}).items():
                    try:
                        week_vals[k] = float(v or 0)
                    except (TypeError, ValueError):
                        continue
            else:
                week_vals = {
                    "ex1": float(pe.get("run_km", 0) or 0),
                    "ex2": float(pe.get("pushups", 0) or 0),
                    "ex3": float(pe.get("pullups", 0) or 0),
                }
            for k, v in week_vals.items():
                if k in all_time_totals:
                    all_time_totals[k] += v
        streak = _streak_info(state, g["exercises"], progress_by_week, cur_week)
        last_streak = int(g.get("last_streak", 0))
        cur_streak = int(streak["current"])
        # pending_failed_week: wird gesetzt, wenn die Woche tatsächlich gefailt wurde
        # (Streak ist von >0 auf 0 gefallen). Wird beim ersten Anzeigen der
        # Animation vom Frontend per Endpoint wieder gecleared.
        pending_failed_week = int(g.get("pending_failed_week") or 0)
        if cur_streak != last_streak:
            if cur_streak > last_streak:
                await manager.broadcast({
                    "type": "week_completed",
                    "user_id": u["user_id"],
                    "user_name": u["name"],
                    "week_number": cur_week,
                    "streak": cur_streak,
                })
            elif last_streak > 0 and cur_streak == 0:
                # Die zuletzt vollendete Woche wäre cur_week - 1 (die gerade
                # vergangene). Diese Nummer wird gespeichert UND mitgesendet,
                # damit das Frontend "Week N failed" anzeigen kann.
                failed_week_num = max(1, cur_week - 1)
                pending_failed_week = failed_week_num
                await manager.broadcast({
                    "type": "streak_ended",
                    "user_id": u["user_id"],
                    "user_name": u["name"],
                    "previous_streak": last_streak,
                    "failed_week": failed_week_num,
                })
                await db.user_goals.update_one(
                    {"user_id": u["user_id"]},
                    {"$set": {"pending_failed_week": failed_week_num}},
                )
            await db.user_goals.update_one(
                {"user_id": u["user_id"]},
                {"$set": {"last_streak": cur_streak}},
            )
        result.append({
            "user_id": u["user_id"],
            "name": u["name"],
            "handle": u.get("handle"),
            "email": u["email"],
            "picture": u.get("picture"),
            "week_number": cur_week,
            "exercises": exercises_out,
            "values": values,
            "days": days,
            "updated_at": entry.get("updated_at") if entry else None,
            "streak": streak,
            "all_time": {k: round(v, 2) for k, v in all_time_totals.items()},
            "is_online": u["user_id"] in manager.online_user_ids(),
            # Nur für eigenen Eintrag relevant. Wird vom Frontend genutzt, um die
            # "Week N failed"-Animation einmalig beim Öffnen der neuen Woche zu zeigen.
            "pending_failed_week": pending_failed_week if u["user_id"] == user.user_id else 0,
        })
    return {"week_number": week, "users": result, "online_user_ids": list(manager.online_user_ids())}

# -------------------- Seen Effects (Animationen pro Viewer 1× pro Event) --------------------
# Ziel: Wenn jemand die Usercard eines anderen Users öffnet, soll die
# Celebration-/Failed-Animation für diese (target_user_id, week_number, type)
# nur EINMAL pro Viewer abgespielt werden – auch über Page-Reloads hinweg.
# Daher persistieren wir die "gesehen"-Markierung pro Viewer in der DB.

class SeenEffectMark(BaseModel):
    target_user_id: str
    week_number: int
    type: str  # "celebration" | "failure"

    @field_validator("type")
    @classmethod
    def _validate_type(cls, v):
        if v not in ("celebration", "failure"):
            raise ValueError("type must be celebration|failure")
        return v

@api_router.get("/me/seen-effects")
async def get_seen_effects(user: User = Depends(get_current_user)):
    docs = await db.seen_effects.find(
        {"viewer_user_id": user.user_id},
        {"_id": 0, "target_user_id": 1, "week_number": 1, "type": 1},
    ).to_list(5000)
    return {"items": docs}

@api_router.post("/me/seen-effects")
async def mark_seen_effect(payload: SeenEffectMark, user: User = Depends(get_current_user)):
    key = {
        "viewer_user_id": user.user_id,
        "target_user_id": payload.target_user_id,
        "week_number": int(payload.week_number),
        "type": payload.type,
    }
    await db.seen_effects.update_one(
        key,
        {"$set": {**key, "seen_at": datetime.now(timezone.utc).isoformat()}},
        upsert=True,
    )
    return {"ok": True}

@api_router.post("/me/clear-pending-failed-week")
async def clear_pending_failed_week(user: User = Depends(get_current_user)):
    await db.user_goals.update_one(
        {"user_id": user.user_id},
        {"$set": {"pending_failed_week": 0}},
    )
    return {"ok": True}

# -------------------- Boost --------------------
@api_router.post("/boost")
async def boost_exercise(payload: BoostRequest, user: User = Depends(get_current_user)):
    g = await _load_goals(user.user_id)
    sd = g["start_date"]
    if isinstance(sd, str):
        sd = datetime.fromisoformat(sd)
    cur_week = _calc_week_number(sd)
    if not any(e["key"] == payload.exercise_key for e in g["exercises"]):
        raise HTTPException(status_code=400, detail="Unknown exercise")
    if any(b["week_number"] == cur_week for b in g.get("boosts", [])):
        raise HTTPException(status_code=400, detail="Du hast diese Woche bereits geboostet")
    new_boost = {
        "exercise_key": payload.exercise_key,
        "week_number": cur_week,
        "applied_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.user_goals.update_one(
        {"user_id": user.user_id},
        {"$push": {"boosts": new_boost}},
    )
    await manager.broadcast({
        "type": "boost_applied",
        "user_id": user.user_id,
        "user_name": user.name,
        "exercise_key": payload.exercise_key,
        "week_number": cur_week,
    })
    return {"ok": True, "boost": new_boost}

@api_router.delete("/boost")
async def cancel_boost(user: User = Depends(get_current_user)):
    g = await _load_goals(user.user_id)
    sd = g["start_date"]
    if isinstance(sd, str):
        sd = datetime.fromisoformat(sd)
    cur_week = _calc_week_number(sd)
    boosts = g.get("boosts", [])
    target = next((b for b in boosts if b["week_number"] == cur_week), None)
    if not target:
        raise HTTPException(status_code=404, detail="Kein aktiver Boost diese Woche")
    await db.user_goals.update_one(
        {"user_id": user.user_id},
        {"$pull": {"boosts": {"week_number": cur_week}}},
    )
    await manager.broadcast({
        "type": "boost_canceled",
        "user_id": user.user_id,
        "user_name": user.name,
        "exercise_key": target["exercise_key"],
        "week_number": cur_week,
    })
    return {"ok": True}

@api_router.get("/boost/ranking")
async def boost_ranking(user: User = Depends(get_current_user)):
    users = await db.users.find({}, {"_id": 0}).to_list(100)
    ranking = []
    for u in users:
        g = await _load_goals(u["user_id"])
        sd = g["start_date"]
        if isinstance(sd, str):
            sd = datetime.fromisoformat(sd)
        cur_week = _calc_week_number(sd)
        state = await _compute_user_state(u["user_id"], g, cur_week)
        effective_records = []
        for b in g.get("boosts", []):
            ek = b.get("exercise_key")
            if ek in state and b.get("week_number") in state[ek]["effective_boost_weeks"]:
                effective_records.append(b)
        by_ex = {}
        for b in effective_records:
            by_ex[b["exercise_key"]] = by_ex.get(b["exercise_key"], 0) + 1
        ex_map = {e["key"]: e["name"] for e in g["exercises"]}
        ranking.append({
            "user_id": u["user_id"],
            "name": u["name"],
            "picture": u.get("picture"),
            "total_boosts": len(effective_records),
            "by_exercise": [
                {"key": k, "name": ex_map.get(k, k), "count": v}
                for k, v in by_ex.items()
            ],
            "latest_boost": effective_records[-1] if effective_records else None,
        })
    ranking.sort(key=lambda x: x["total_boosts"], reverse=True)
    return {"ranking": ranking}

# -------------------- Profile --------------------
@api_router.put("/profile")
async def update_profile(payload: ProfileUpdate, user: User = Depends(get_current_user)):
    update = {}
    if payload.name is not None and payload.name.strip():
        update["name"] = payload.name.strip()[:80]
    if payload.picture is not None:
        p = payload.picture
        if len(p) > 700_000:
            raise HTTPException(status_code=400, detail="Bild zu groß (max ~500KB)")
        update["picture"] = p
    if not update:
        raise HTTPException(status_code=400, detail="Nothing to update")
    await db.users.update_one({"user_id": user.user_id}, {"$set": update})
    await manager.broadcast({"type": "profile_updated", "user_id": user.user_id})
    updated = await db.users.find_one({"user_id": user.user_id}, {"_id": 0})
    return updated

# -------------------- Insights --------------------
@api_router.get("/insights/me")
async def insights_me(user: User = Depends(get_current_user)):
    g = await _load_goals(user.user_id)
    exercises = g["exercises"]
    ex_keys = {e["key"] for e in exercises}
    entries = await db.progress_entries.find({"user_id": user.user_id}, {"_id": 0}).to_list(1000)

    # Aktuelle Woche + heutiger Wochentag (0=Mo .. 6=So) für "erreichbare Tage"-Logik
    sd = g.get("start_date")
    if isinstance(sd, str):
        try:
            sd = datetime.fromisoformat(sd)
        except ValueError:
            sd = datetime.now(timezone.utc)
    if not isinstance(sd, datetime):
        sd = datetime.now(timezone.utc)
    current_week_num = _calc_week_number(sd)
    current_dow = datetime.now(timezone.utc).weekday()  # 0=Mo..6=So

    by_weekday = {k: [0.0] * 7 for k in ex_keys}
    weeks_active = {k: [0] * 7 for k in ex_keys}
    # Pro Wochentag: in wie vielen protokollierten Wochen war dieser Tag bereits erreichbar?
    reachable_per_day = [0] * 7
    weeks_with_data = 0

    for entry in entries:
        raw_days = entry.get("days") or {}
        if not isinstance(raw_days, dict) or not raw_days:
            continue
        active_today = {k: [False] * 7 for k in ex_keys}
        week_contributed = False
        for d_key, d_vals in raw_days.items():
            try:
                d = int(d_key)
            except (ValueError, TypeError):
                continue
            if d < 0 or d > 6 or not isinstance(d_vals, dict):
                continue
            for k, v in d_vals.items():
                if k not in ex_keys:
                    continue
                try:
                    fv = float(v) if v not in (None, "") else 0.0
                except (ValueError, TypeError):
                    fv = 0.0
                if fv > 0:
                    by_weekday[k][d] += fv
                    active_today[k][d] = True
                    week_contributed = True
        if week_contributed:
            weeks_with_data += 1
            for k, flags in active_today.items():
                for i, was_active in enumerate(flags):
                    if was_active:
                        weeks_active[k][i] += 1
            # Erreichbarkeit der Wochentage in dieser Woche bestimmen.
            # - Vergangene Wochen: alle 7 Tage erreichbar.
            # - Aktuelle (oder spätere) Woche: nur Tage bis einschließlich heute.
            try:
                week_num = int(entry.get("week_number", 0))
            except (ValueError, TypeError):
                week_num = 0
            if week_num < current_week_num:
                for d in range(7):
                    reachable_per_day[d] += 1
            else:
                for d in range(7):
                    if d <= current_dow:
                        reachable_per_day[d] += 1

    out = []
    for ex in exercises:
        k = ex["key"]
        u = (ex.get("unit", "") or "").lower()
        is_distance = "km" in u or u == "m" or "mi" in u
        if is_distance:
            totals = [round(v, 1) for v in by_weekday[k]]
        else:
            totals = [float(int(v + 0.5)) for v in by_weekday[k]]
        total_sum = sum(totals)

        # Power-Day: Tag mit dem höchsten Wert (>0)
        nonzero = [(i, v) for i, v in enumerate(totals) if v > 0]
        power_day = max(nonzero, key=lambda x: x[1])[0] if nonzero else None

        # Loser-Day: Tag mit dem geringsten Wert (0 explizit erlaubt) — über alle
        # bereits erreichbaren Wochentage. Mehrere Tage mit gleichem Minimum
        # werden alle als Loser-Days zurückgegeben (z.B. mehrere 0-Tage).
        reachable_indices = [i for i in range(7) if reachable_per_day[i] > 0]
        if reachable_indices:
            min_val = min(totals[i] for i in reachable_indices)
            weakest_days = [i for i in reachable_indices if totals[i] == min_val]
            # Falls Power-Day == einziger Loser-Day (alle Werte gleich), keinen Loser zeigen
            if len(weakest_days) == 1 and weakest_days[0] == power_day:
                weakest_days = []
            elif power_day is not None and power_day in weakest_days:
                weakest_days = [i for i in weakest_days if i != power_day]
            weakest_day = weakest_days[0] if weakest_days else None
        else:
            weakest_days = []
            weakest_day = None

        share_per_day = [round(100 * t / total_sum, 1) if total_sum > 0 else 0 for t in totals]
        # Konsistenz: % der erreichbaren Wochen mit Aktivität an diesem Tag.
        # Nenner ist NICHT mehr weeks_with_data, sondern reachable_per_day[d]
        # — damit zukünftige Tage der laufenden Woche die Quote nicht drücken.
        consistency = [
            round(100 * weeks_active[k][d] / reachable_per_day[d]) if reachable_per_day[d] > 0 else 0
            for d in range(7)
        ]
        # Tages-basierter Wochendurchschnitt: total / (verstrichene Programm-Tage) * 7.
        # Beispiel: 10km in Woche 1, Montag Woche 2 -> 10 / 8 * 7 = 8.75 km/Woche.
        total_days_elapsed = (current_week_num - 1) * 7 + current_dow + 1
        if total_days_elapsed > 0 and total_sum > 0:
            avg_raw = total_sum / total_days_elapsed * 7
            avg = round(avg_raw, 1) if is_distance else round(avg_raw, 2)
        else:
            avg = 0
        out.append({
            "key": k,
            "name": ex["name"],
            "unit": ex.get("unit", ""),
            "icon": ex.get("icon", "pushup"),
            "color": ex.get("color", "#CCFF00"),
            "by_weekday": totals,
            "share_per_day": share_per_day,
            "consistency": consistency,
            "power_day": power_day,
            "weakest_day": weakest_day,      # Backward-Compat: erster Loser-Day
            "weakest_days": weakest_days,    # Neu: Liste aller Loser-Days
            "total": round(total_sum, 2),
            "avg_per_week": avg,
        })

    return {"exercises": out, "weeks_tracked": weeks_with_data}

# -------------------- WebSocket --------------------
async def _resolve_ws_user_id(websocket: WebSocket) -> Optional[str]:
    token = websocket.cookies.get("session_token")
    if not token:
        token = websocket.query_params.get("token")
    if not token:
        return None
    session = await db.user_sessions.find_one({"session_token": token}, {"_id": 0})
    if not session:
        return None
    expires_at = session.get("expires_at")
    if isinstance(expires_at, str):
        try:
            expires_at = datetime.fromisoformat(expires_at)
        except ValueError:
            return None
    if expires_at and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at and expires_at < datetime.now(timezone.utc):
        return None
    return session.get("user_id")

@api_router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    user_id = await _resolve_ws_user_id(websocket)
    await manager.connect(websocket, user_id=user_id)
    try:
        await websocket.send_json({
            "type": "presence_snapshot",
            "online_user_ids": list(manager.online_user_ids()),
        })
        if user_id:
            await manager.broadcast_presence()
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        if user_id:
            await manager.broadcast_presence()
    except Exception:
        manager.disconnect(websocket)
        if user_id:
            try:
                await manager.broadcast_presence()
            except Exception:
                pass

@api_router.get("/")
async def root():
    return {"message": "NeonTracker API"}

app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@app.on_event("startup")
async def startup_db_indexes():
    try:
        await db.users.create_index("handle", unique=True, sparse=True)
    except Exception as e:
        logger.warning(f"handle index create failed: {e}")
    try:
        await _backfill_handles()
    except Exception as e:
        logger.warning(f"handle backfill failed: {e}")

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=5000)