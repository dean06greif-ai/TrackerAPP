from fastapi import FastAPI, APIRouter, HTTPException, Request, Response, Cookie, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import JSONResponse
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
from fastapi.responses import JSONResponse
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ['MONGO_URI']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

app = FastAPI()
api_router = APIRouter(prefix="/api")

# -------------------- Models --------------------
class User(BaseModel):
    user_id: str
    email: str
    name: str
    picture: Optional[str] = None
    created_at: datetime

class Exercise(BaseModel):
    key: str
    name: str
    unit: str = ""
    icon: str = "pushup"
    color: str = "#CCFF00"
    base_value: float = 0
    progression_pct: int = 10  # 1..10 — individuelle wöchentliche Steigerung in %

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
    values: Optional[dict] = None  # legacy: total per exercise
    days: Optional[dict] = None  # new: {"0".."6": {exercise_key: number}}

class ProfileUpdate(BaseModel):
    name: Optional[str] = None
    picture: Optional[str] = None

class AuthPreferencesUpdate(BaseModel):
    remember_me: bool
DEFAULT_EXERCISES = [
    {"key": "ex1", "name": "Lauf", "unit": "km", "icon": "run", "color": "#CCFF00", "base_value": 10.0, "progression_pct": 10},
    {"key": "ex2", "name": "Liegestütze", "unit": "", "icon": "pushup", "color": "#FF3B30", "base_value": 500, "progression_pct": 10},
    {"key": "ex3", "name": "Klimmzüge", "unit": "", "icon": "pullup", "color": "#00F0FF", "base_value": 50, "progression_pct": 10},
]
# Verbindliche Farb-Palette: Position bestimmt Farbe (Ziel 1..5).
# Ziel 4 = Orange (#FF8800), Ziel 5 = Violett (#A855F7) für deutlichen Kontrast zu Standard-Zielen.
EXERCISE_PALETTE = ["#CCFF00", "#FF3B30", "#00F0FF", "#FF8800", "#A855F7"]

def _normalize_exercise_colors(exercises: list) -> list:
    """Erzwingt die Palette-Farbe basierend auf der Position. Mutiert die Liste in-place und gibt sie zurück."""
    if not exercises:
        return exercises
    for idx, ex in enumerate(exercises):
        if isinstance(ex, dict):
            ex["color"] = EXERCISE_PALETTE[idx % len(EXERCISE_PALETTE)]
    return exercises

BASE_INCREASE = 0.10
BOOST_INCREASE = 0.25
FUTURE_WEEKS = 10  # how many future weeks to project in /goals/me progression

# -------------------- WebSocket Manager --------------------
class ConnectionManager:
    def __init__(self):
        # ws -> user_id (may be None for unauthenticated connections)
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
    return User(**user_doc)

# -------------------- Auth routes --------------------

@api_router.post("/auth/session")
async def process_session(request: Request, response: Response):
    body = await request.json()
    session_id = body.get("session_id")
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id required")

    async with httpx.AsyncClient(timeout=15.0) as http:
        r = await http.get(
            "https://demobackend.emergentagent.com/auth/v1/env/oauth/session-data",
            headers={"X-Session-ID": session_id},
        )
        if r.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid session_id")
        data = r.json()

    email = data["email"]
    name = data.get("name", email)
    picture = data.get("picture")
    session_token = data["session_token"]

    existing = await db.users.find_one({"email": email}, {"_id": 0})
    remember_me = True  # Default für neue User
    if existing:
        user_id = existing["user_id"]
        remember_me = existing.get("remember_me", True)
        await db.users.update_one(
            {"user_id": user_id},
            {"$set": {"name": name, "picture": picture}}
        )
    else:
        user_id = f"user_{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)
        await db.users.insert_one({
            "user_id": user_id,
            "email": email,
            "name": name,
            "picture": picture,
            "created_at": now.isoformat(),
            "remember_me": True,
        })
        # default goals
        await db.user_goals.insert_one({
            "user_id": user_id,
            "exercises": [dict(e) for e in DEFAULT_EXERCISES],
            "boosts": [],
            "weekly_increase": BASE_INCREASE,
            "start_date": now.isoformat(),
        })

    # Session-Dauer abhängig von remember_me
    if remember_me:
        session_days = 30
        cookie_max_age = 30 * 24 * 3600
    else:
        session_days = 1
        cookie_max_age = None  # Session-Cookie (Browser-Close => weg)

    expires_at = datetime.now(timezone.utc) + timedelta(days=session_days)
    await db.user_sessions.insert_one({
        "user_id": user_id,
        "session_token": session_token,
        "expires_at": expires_at.isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    response.set_cookie(
        key="session_token",
        value=session_token,
        httponly=True,
        secure=True,
        samesite="none",
        path="/",
        max_age=cookie_max_age,
    )
    return {"user_id": user_id, "email": email, "name": name, "picture": picture}


@api_router.get("/auth/me")
async def me(request: Request, response: Response, user: User = Depends(get_current_user)):
    # Sliding session: bei aktiven Nutzern Session automatisch verlängern
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
            response.set_cookie(
                key="session_token",
                value=token,
                httponly=True,
                secure=True,
                samesite="none",
                path="/",
                max_age=30 * 24 * 3600,
            )
    return user.model_dump(mode="json")

@api_router.post("/auth/logout")
async def logout(request: Request, response: Response):
    token = request.cookies.get("session_token")
    if token:
        await db.user_sessions.delete_one({"session_token": token})
    response.delete_cookie("session_token", path="/", samesite="none", secure=True)
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
            cookie_max_age = None  # Session-Cookie
        await db.user_sessions.update_one(
            {"session_token": token},
            {"$set": {"expires_at": new_expires.isoformat()}}
        )
        response.set_cookie(
            key="session_token",
            value=token,
            httponly=True,
            secure=True,
            samesite="none",
            path="/",
            max_age=cookie_max_age,
        )
    return {"remember_me": payload.remember_me}
# -------------------- Goals --------------------
def _calc_week_number(start_date: datetime) -> int:
    """ISO calendar week count. Week 1 = the calendar week (Mo-So) containing start_date.
    A new week begins every Monday at 00:00."""
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    # Anchor each date to the Monday of its ISO week
    start_monday = (start_date - timedelta(days=start_date.weekday())).date()
    today_monday = (now - timedelta(days=now.weekday())).date()
    return max(1, (today_monday - start_monday).days // 7 + 1)

def _round_goal(value: float, unit: str) -> float:
    """Round goal for clean display. km/distance -> 0.1 (100m). Reps -> nearest integer (half up)."""
    u = (unit or "").lower()
    if "km" in u or "m" == u or "mi" in u:
        # Half-up Rundung auf 1 Nachkommastelle (vermeidet Python banker's rounding)
        return float(int(value * 10 + 0.5)) / 10.0
    # Reps & sonstige Einheiten: auf nächste ganze Zahl runden (half up, nicht banker's)
    # Wichtig: nicht auf gerade Zahlen runden -> sonst wird 100 + 25% Boost = 125 fälschlich zu 124
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
    # Migrate legacy schema -> exercises[]
    if "exercises" not in g:
        legacy = [
            {"key": "ex1", "name": "Lauf", "unit": "km", "icon": "run", "color": "#CCFF00", "base_value": float(g.get("base_run_km", 10.0))},
            {"key": "ex2", "name": "Liegestütze", "unit": "", "icon": "pushup", "color": "#FF3B30", "base_value": float(g.get("base_pushups", 500))},
            {"key": "ex3", "name": "Klimmzüge", "unit": "", "icon": "pullup", "color": "#00F0FF", "base_value": float(g.get("base_pullups", 50))},
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
    # Erzwinge Palette-Farbe je nach Position (Ziel 4 = Orange, Ziel 5 = Violett)
    _normalize_exercise_colors(g.get("exercises", []))
    # Stelle sicher, dass jede Übung einen progression_pct (1..10) hat — Default 10
    _ensure_progression_pct(g.get("exercises", []))
    return g

def _ensure_progression_pct(exercises: list) -> list:
    """Setzt/clamped das progression_pct-Feld jeder Übung auf einen int in [1, 10]."""
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
    """Returns dict {week_number: {exercise_key: total_logged_value}}."""
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
            # legacy
            legacy = {
                "ex1": float(pe.get("run_km", 0) or 0),
                "ex2": float(pe.get("pushups", 0) or 0),
                "ex3": float(pe.get("pullups", 0) or 0),
            }
            out[wn] = {k: v for k, v in legacy.items() if k in ex_keys}
    return out


def _compute_progression(exercise: dict, all_boost_weeks: set, progress_by_week: dict,
                          current_week: int, future_weeks: int = FUTURE_WEEKS) -> dict:
    """Compute per-week progression for a single exercise.

    Rules:
      • Each completed past week → +10% progression carries forward.
      • Missed past week (logged < goal) → +10% paused for that week (no carry-forward).
      • Boosts in week W add +25% multiplicatively from week W onwards.
      • If any week is missed, ALL boosts on this exercise from prior or that week
        are voided (no longer count anywhere).
      • For weeks > current_week, we project assuming user completes them (no missed),
        carrying forward whatever boosts survived up to current_week.

    Returns:
        {
          "missed_weeks": [..],
          "effective_boost_weeks": [..],   # boosts still active at/after current week
          "current_goal": float,           # rounded current week goal
          "progression": [
            {"week", "goal", "status", "boost", "voided_boost"}, ...
          ]
        }
    """
    base = float(exercise.get("base_value", 0) or 0)
    unit = exercise.get("unit", "")
    ex_key = exercise["key"]

    # Individuelle wöchentliche Steigerung dieser Übung (Default 10 %, geclamped 1..10)
    try:
        pct = int(round(float(exercise.get("progression_pct", 10))))
    except (TypeError, ValueError):
        pct = 10
    pct = max(1, min(10, pct))
    ex_increase = pct / 100.0

    eff_idx = 0           # +x% multipliers applied so far
    active_boosts = []    # list of boost weeks still effective
    missed = []
    progression = []

    last_week = max(current_week, 1) + future_weeks

    for w in range(1, last_week + 1):
        # Apply boost made this week (before computing this week's goal)
        boost_this_week = w in all_boost_weeks
        if boost_this_week:
            active_boosts.append(w)

        goal_raw = base * ((1 + ex_increase) ** eff_idx) * ((1 + BOOST_INCREASE) ** len(active_boosts))
        goal_rounded = _round_goal(goal_raw, unit)

        if w < current_week:
            logged = float(progress_by_week.get(w, {}).get(ex_key, 0) or 0)
            if logged + 1e-9 < goal_rounded:
                # MISSED
                missed.append(w)
                # Void all boosts up to and including this week (boost "fliegt raus")
                active_boosts = [b for b in active_boosts if b > w]
                status = "missed"
                voided_boost = boost_this_week  # boost this week is also voided
            else:
                status = "completed"
                voided_boost = False
                eff_idx += 1
        elif w == current_week:
            status = "current"
            voided_boost = False
            # Note: do NOT increment eff_idx here; current week's success is unknown.
            # The projection for w+1 below uses (eff_idx + 1) assuming user completes it.
        else:
            # future weeks: assume completion. Increment eff_idx AFTER computing this week.
            status = "future"
            voided_boost = False

        progression.append({
            "week": w,
            "goal": goal_rounded,
            "status": status,
            "boost": boost_this_week,
            "voided_boost": voided_boost,
        })

        # For future weeks (status="future") we assume completion → carry +10%
        if status == "future":
            eff_idx += 1
        # For the current week, the projection of next week assumes completion → carry +10%
        elif status == "current":
            eff_idx += 1

    # Find current week's entry for current_goal
    current_goal = next((p["goal"] for p in progression if p["week"] == current_week), 0)

    return {
        "missed_weeks": missed,
        "effective_boost_weeks": sorted(active_boosts),
        "current_goal": current_goal,
        "progression": progression,
    }


async def _compute_user_state(user_id: str, g: dict, current_week: int) -> dict:
    """Returns per-exercise progression bundle (see _compute_progression)."""
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
    g["state"] = state  # per-exercise progression bundle
    return g

@api_router.put("/goals/me")
async def update_my_goals(payload: GoalsUpdate, user: User = Depends(get_current_user)):
    g = await _load_goals(user.user_id)
    if not (3 <= len(payload.exercises) <= 5):
        raise HTTPException(status_code=400, detail="Es müssen 3 bis 5 Übungen sein")
    # ensure unique keys
    keys = [e.key for e in payload.exercises]
    if len(set(keys)) != len(keys):
        raise HTTPException(status_code=400, detail="Übungs-Keys müssen eindeutig sein")
    exercises = [e.model_dump() for e in payload.exercises]
    # Farben gemäß Palette normalisieren (verhindert, dass alte Farben aus Frontend übernommen werden)
    _normalize_exercise_colors(exercises)
    # progression_pct nochmals normalisieren (Sicherheitsnetz auch wenn Pydantic schon validiert)
    _ensure_progression_pct(exercises)

    # --- Lock-Regeln: Startwert nach Woche 1 nicht änderbar, Progression nur 1x je 4 Wochen ---
    sd = g["start_date"]
    if isinstance(sd, str):
        sd = datetime.fromisoformat(sd)
    cur_week = _calc_week_number(sd)
    PROGRESSION_COOLDOWN = 4  # Wochen
    old_by_key = {e["key"]: e for e in g.get("exercises", [])}
    for ex in exercises:
        old = old_by_key.get(ex["key"])
        if not old:
            # Neue Übung -> ab Woche 2 ist Hinzufügen erlaubt, aber merken wann progression "gesetzt" wurde
            ex["progression_last_changed_week"] = cur_week
            continue
        # Startwert nach Woche 1 NICHT änderbar -> erzwinge alten Wert
        if cur_week > 1:
            try:
                if float(ex.get("base_value", 0)) != float(old.get("base_value", 0)):
                    ex["base_value"] = float(old.get("base_value", 0))
            except (TypeError, ValueError):
                ex["base_value"] = float(old.get("base_value", 0))
        # Progression-Cooldown: in Woche 1 immer frei, sonst nur alle 4 Wochen
        old_pct = int(old.get("progression_pct", 10))
        new_pct = int(ex.get("progression_pct", 10))
        last_changed = int(old.get("progression_last_changed_week", 1))
        if new_pct != old_pct:
            if cur_week > 1 and (cur_week - last_changed) < PROGRESSION_COOLDOWN:
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
    # Hole aktuelle Goals, setze für alle Übungen progression_last_changed_week auf 1
    g = await db.user_goals.find_one({"user_id": user.user_id}, {"_id": 0})
    exercises = g.get("exercises", []) if g else []
    for ex in exercises:
        ex["progression_last_changed_week"] = 1
    await db.user_goals.update_one(
        {"user_id": user.user_id},
        {"$set": {"start_date": now, "exercises": exercises, "last_streak": 0}},
    )
    # Alte Fortschritts-Einträge & Boosts löschen, damit man wirklich bei Null startet
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
        # legacy migration
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
    # Compute totals from days, or fall back to direct values
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

# -------------------- Live Board (everyone) --------------------
def _streak_info(state: dict, exercises: list, progress_by_week: dict, current_week: int):
    """A week is 'completed' for streak purposes if every exercise's logged
    value reached that exercise's at-time goal (i.e. week not in missed list).

    The CURRENT week is now also included: as soon as all targets of the
    current week are met, the streak ticks up immediately (no need to wait
    for the week to roll over)."""
    missed_union = set()
    for ex in exercises:
        for w in state[ex["key"]]["missed_weeks"]:
            missed_union.add(w)

    completed = []
    # Include the current week as well, so the streak increments as soon as
    # all target values of the active week are reached.
    for w in range(1, current_week + 1):
        if w in missed_union:
            continue
        all_done = True
        for ex in exercises:
            prog = state[ex["key"]]["progression"]
            target = next((p["goal"] for p in prog if p["week"] == w), None)
            logged = float(progress_by_week.get(w, {}).get(ex["key"], 0) or 0)
            if target is None or logged + 1e-9 < target:
                all_done = False
                break
        if all_done:
            completed.append(w)

    completed_set = set(completed)
    cur = 0
    # Start counting back from the current week (inclusive) so that a freshly
    # completed current week immediately contributes to the streak.
    w = current_week
    while w in completed_set:
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
    users = await db.users.find({}, {"_id": 0}).to_list(100)
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
            # Individuelle wöchentliche Steigerung dieser Übung (1..10 %)
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
        # All-Time-Total: Summe aller geloggten Werte je Übung über ALLE Wochen.
        # Direkt aus progress_entries lesen (robust gegen fehlende/leere values-Felder).
        # Quelle-der-Wahrheit-Reihenfolge: days -> values -> legacy.
        # Nach reset-start ist progress_entries leer => all_time_totals = 0 (automatisch).
        all_time_totals = {ex["key"]: 0.0 for ex in g["exercises"]}
        all_entries = await db.progress_entries.find(
            {"user_id": u["user_id"]}, {"_id": 0}
        ).to_list(1000)
        for pe in all_entries:
            week_vals = {}
            pe_days = pe.get("days") or {}
            if pe_days:
                # Bevorzugt: aus days aggregieren (source of truth)
                for d_key, d_vals in pe_days.items():
                    if not isinstance(d_vals, dict):
                        continue
                    for k, v in d_vals.items():
                        try:
                            week_vals[k] = week_vals.get(k, 0.0) + float(v or 0)
                        except (TypeError, ValueError):
                            continue
            elif pe.get("values"):
                # Fallback: aggregiertes values-Feld
                for k, v in (pe.get("values") or {}).items():
                    try:
                        week_vals[k] = float(v or 0)
                    except (TypeError, ValueError):
                        continue
            else:
                # Legacy-Schema vor exercises[]
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
                await manager.broadcast({
                    "type": "streak_ended",
                    "user_id": u["user_id"],
                    "user_name": u["name"],
                    "previous_streak": last_streak,
                })
            await db.user_goals.update_one(
                {"user_id": u["user_id"]},
                {"$set": {"last_streak": cur_streak}},
            )
        result.append({
            "user_id": u["user_id"],
            "name": u["name"],
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
        })
    return {"week_number": week, "users": result, "online_user_ids": list(manager.online_user_ids())}

# -------------------- Boost --------------------
@api_router.post("/boost")
async def boost_exercise(payload: BoostRequest, user: User = Depends(get_current_user)):
    g = await _load_goals(user.user_id)
    sd = g["start_date"]
    if isinstance(sd, str):
        sd = datetime.fromisoformat(sd)
    cur_week = _calc_week_number(sd)
    # validate exercise key
    if not any(e["key"] == payload.exercise_key for e in g["exercises"]):
        raise HTTPException(status_code=400, detail="Unknown exercise")
    # max 1 boost per user per week (across exercises)
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
        # Build effective boost list = boost records whose week is in effective_boost_weeks
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
        # accept data: URL or http(s) URL; cap size at ~500KB
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

# -------------------- Insights (Power-Day) --------------------
@api_router.get("/insights/me")
async def insights_me(user: User = Depends(get_current_user)):
    g = await _load_goals(user.user_id)
    exercises = g["exercises"]
    ex_keys = {e["key"] for e in exercises}
    entries = await db.progress_entries.find({"user_id": user.user_id}, {"_id": 0}).to_list(1000)

    by_weekday = {k: [0.0] * 7 for k in ex_keys}
    weeks_active = {k: [0] * 7 for k in ex_keys}  # weeks where user trained on this weekday
    weeks_with_data = 0

    for entry in entries:
        raw_days = entry.get("days") or {}
        if not isinstance(raw_days, dict) or not raw_days:
            # Legacy total-only entry oder leeres days-Feld: skip
            continue
        active_today = {k: [False] * 7 for k in ex_keys}
        week_contributed = False  # nur Wochen mit >0 Werten als "tracked" zählen
        for d_key, d_vals in raw_days.items():
            # Day-Key kann string ("0".."6") oder int sein -> robust parsen
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

    out = []
    for ex in exercises:
        k = ex["key"]
        u = (ex.get("unit", "") or "").lower()
        is_distance = "km" in u or u == "m" or "mi" in u
        # Reps -> ganze Zahlen, Distanz -> 0.1 Auflösung (passend zu _round_goal)
        if is_distance:
            totals = [round(v, 1) for v in by_weekday[k]]
        else:
            totals = [float(int(v + 0.5)) for v in by_weekday[k]]
        total_sum = sum(totals)
        nonzero = [(i, v) for i, v in enumerate(totals) if v > 0]
        power_day = max(nonzero, key=lambda x: x[1])[0] if nonzero else None
        weakest_day = min(nonzero, key=lambda x: x[1])[0] if nonzero else None
        share_per_day = [round(100 * t / total_sum, 1) if total_sum > 0 else 0 for t in totals]
        consistency = [round(100 * wa / weeks_with_data) if weeks_with_data > 0 else 0 for wa in weeks_active[k]]
        avg = round(total_sum / weeks_with_data, 2) if weeks_with_data > 0 else 0
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
            "weakest_day": weakest_day,
            "total": round(total_sum, 2),
            "avg_per_week": avg,
        })

    return {"exercises": out, "weeks_tracked": weeks_with_data}

# -------------------- WebSocket --------------------
async def _resolve_ws_user_id(websocket: WebSocket) -> Optional[str]:
    """Authenticate WebSocket via session_token (cookie or ?token=... query param)."""
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
        # Send initial presence snapshot to the connecting client
        await websocket.send_json({
            "type": "presence_snapshot",
            "online_user_ids": list(manager.online_user_ids()),
        })
        # Broadcast to others that presence may have changed (only if authenticated)
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

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=5000)
