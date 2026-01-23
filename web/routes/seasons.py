from fastapi import APIRouter, Request, Depends, Query
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload
from pathlib import Path
from sqlalchemy import func, asc, and_, or_, desc
from typing import Optional

from db.connection import get_session
from db import get_games
from db.models import Game, Team

router = APIRouter(prefix="/seasons")

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()

@router.get("")
async def list_seasons(request: Request, db: Session = Depends(get_db)):
    # Obtener la temporada más reciente (donde haya partidos)
    latest_season = db.query(Game.season).order_by(desc(Game.date)).first()
    
    if latest_season:
        return RedirectResponse(url=f"/seasons/{latest_season[0]}")
    
    # Si no hay temporadas, mostrar una página vacía o error
    return templates.TemplateResponse("seasons/list.html", {
        "request": request,
        "active_page": "seasons",
        "seasons": []
    })

@router.get("/{season}")
async def season_detail(request: Request, season: str, db: Session = Depends(get_db)):
    # Obtener todas las temporadas para el selector
    all_seasons_query = db.query(Game.season).distinct().order_by(desc(Game.season)).all()
    all_seasons = [s[0] for s in all_seasons_query]
    
    # --- REGULAR SEASON STANDINGS LOGIC ---
    # Home wins
    home_wins = db.query(
        Game.home_team_id.label('team_id'),
        func.count(Game.id).label('wins')
    ).filter(
        Game.season == season,
        Game.home_score > Game.away_score,
        Game.rs == True
    ).group_by(Game.home_team_id).all()
    
    # Away wins
    away_wins = db.query(
        Game.away_team_id.label('team_id'),
        func.count(Game.id).label('wins')
    ).filter(
        Game.season == season,
        Game.away_score > Game.home_score,
        Game.rs == True
    ).group_by(Game.away_team_id).all()
    
    # Losses
    home_losses = db.query(
        Game.home_team_id.label('team_id'),
        func.count(Game.id).label('losses')
    ).filter(
        Game.season == season,
        Game.home_score < Game.away_score,
        Game.rs == True
    ).group_by(Game.home_team_id).all()
    
    away_losses = db.query(
        Game.away_team_id.label('team_id'),
        func.count(Game.id).label('losses')
    ).filter(
        Game.season == season,
        Game.away_score < Game.home_score,
        Game.rs == True
    ).group_by(Game.away_team_id).all()
    
    # Aggregate results
    standings_dict = {}
    for hw in home_wins:
        standings_dict[hw.team_id] = {'wins': hw.wins, 'losses': 0}
    for aw in away_wins:
        if aw.team_id not in standings_dict:
            standings_dict[aw.team_id] = {'wins': 0, 'losses': 0}
        standings_dict[aw.team_id]['wins'] += aw.wins
    for hl in home_losses:
        if hl.team_id not in standings_dict:
            standings_dict[hl.team_id] = {'wins': 0, 'losses': 0}
        standings_dict[hl.team_id]['losses'] += hl.losses
    for al in away_losses:
        if al.team_id not in standings_dict:
            standings_dict[al.team_id] = {'wins': 0, 'losses': 0}
        standings_dict[al.team_id]['losses'] += al.losses
    
    # Enrich with team info
    standings = []
    teams_map = {t.id: t for t in db.query(Team).all()}
    for team_id, record in standings_dict.items():
        team = teams_map.get(team_id)
        if not team: continue
        wins, losses = record['wins'], record['losses']
        total = wins + losses
        pct = wins / total if total > 0 else 0
        standings.append({
            'team': team, 'wins': wins, 'losses': losses, 'pct': pct,
            'conf': team.conference, 'div': team.division
        })
    
    east_standings = sorted([s for s in standings if s['conf'] == 'East'], key=lambda x: x['pct'], reverse=True)
    west_standings = sorted([s for s in standings if s['conf'] == 'West'], key=lambda x: x['pct'], reverse=True)
    
    # --- PLAYOFF BRACKET LOGIC ---
    def get_bracket_data(games_list, is_ist=False):
        rounds_data = {1: [], 2: [], 3: [], 4: []}
        if not games_list:
            return rounds_data
            
        series_map = {}
        for g in games_list:
            if not g.home_team_id or not g.away_team_id: continue
            t1, t2 = sorted([g.home_team_id, g.away_team_id])
            s_key = (t1, t2)
            if s_key not in series_map:
                series_map[s_key] = {
                    'team1': g.home_team if g.home_team_id == t1 else g.away_team,
                    'team2': g.away_team if g.home_team_id == t1 else g.home_team,
                    't1_wins': 0,
                    't2_wins': 0,
                    't1_score': 0,
                    't2_score': 0,
                    'first_date': g.date,
                    'last_date': g.date,
                    'r_hint': None,
                    'r_pos': 99 # Default position for sorting
                }
            
            s = series_map[s_key]
            if g.winner_team_id == t1: 
                s['t1_wins'] += 1
                if is_ist:
                    s['t1_score'] = g.home_score if g.home_team_id == t1 else g.away_score
                    s['t2_score'] = g.away_score if g.home_team_id == t1 else g.home_score
            elif g.winner_team_id == t2: 
                s['t2_wins'] += 1
                if is_ist:
                    s['t2_score'] = g.home_score if g.home_team_id == t2 else g.away_score
                    s['t1_score'] = g.away_score if g.home_team_id == t2 else g.home_score
            
            if g.date < s['first_date']: s['first_date'] = g.date
            if g.date > s['last_date']: s['last_date'] = g.date
            
            # Detección de ronda y POSICIÓN basada en Game ID
            try:
                if len(g.id) == 10:
                    if is_ist:
                        # Final NBA Cup siempre es prefijo 006
                        if g.id.startswith('006'): 
                            s['r_hint'] = 4
                            s['r_pos'] = 0
                        else:
                            # Eliminatorias NBA Cup (RS=True)
                            # QF: 1201, 1202, 1203, 1204
                            # SF: 1229, 1230
                            if g.id.endswith('1201'): s['r_pos'] = 0; s['r_hint'] = 2
                            elif g.id.endswith('1202'): s['r_pos'] = 1; s['r_hint'] = 2
                            elif g.id.endswith('1203'): s['r_pos'] = 2; s['r_hint'] = 2
                            elif g.id.endswith('1204'): s['r_pos'] = 3; s['r_hint'] = 2
                            elif g.id.endswith('1229'): s['r_pos'] = 0; s['r_hint'] = 3
                            elif g.id.endswith('1230'): s['r_pos'] = 1; s['r_hint'] = 3
                    else:
                        # Playoffs estándar (004)
                        # Estructura: 004 YY 00 R Z G
                        # R (Ronda): dígito 8 (index 7)
                        # Z (Posición): dígito 9 (index 8)
                        if g.id.startswith('004'):
                            s['r_hint'] = int(g.id[7])
                            s['r_pos'] = int(g.id[8])
            except: pass

        # Ordenar las series por posición (r_pos) para que el cuadro sea consistente
        sorted_series = sorted(series_map.values(), key=lambda x: (x['r_hint'] or 0, x['r_pos']))
        
        for s in sorted_series:
            r = s['r_hint']
            # Fallback de ronda por si fallan los IDs (no debería ocurrir con IDs oficiales)
            if not r:
                # Si llegamos aquí, usamos una estimación cronológica básica
                # pero esto rompería la alineación vertical perfecta
                continue 
            
            if r in rounds_data:
                rounds_data[r].append({
                    'home_team': s['team1'],
                    'away_team': s['team2'],
                    'home_wins': s['t1_wins'],
                    'away_wins': s['t2_wins'],
                    'home_score': s['t1_score'],
                    'away_score': s['t2_score']
                })
        
        return rounds_data


    # Obtener Playoffs
    po_games = db.query(Game)\
        .options(joinedload(Game.home_team), joinedload(Game.away_team))\
        .filter(Game.season == season, Game.po == True)\
        .order_by(asc(Game.date)).all()
    
    po_rounds = get_bracket_data(po_games, is_ist=False)
    formatted_po_bracket = []
    round_names_po = {1: 'Primera Ronda', 2: 'Semis de Conferencia', 3: 'Finales de Conferencia', 4: 'Finales NBA'}
    for r_num in sorted(po_rounds.keys()):
        if po_rounds[r_num]:
            formatted_po_bracket.append({
                'num': r_num,
                'name': round_names_po.get(r_num, f'Ronda {r_num}'),
                'series': po_rounds[r_num]
            })

    # Obtener IST Knockout (NBA Cup)
    formatted_ist_bracket = []
    
    # Solo partidos de la fase eliminatoria de la Copa
    # Un partido de fase final de copa (Final) NO es Regular Season (según la NBA)
    # Los partidos de eliminatorias previas (QF/SF) SÍ son Regular Season.
    # Por tanto, filtramos por IST=True y (RS=False O IDs de eliminatorias)
    ist_ko_games = db.query(Game)\
        .options(joinedload(Game.home_team), joinedload(Game.away_team))\
        .filter(Game.season == season, Game.ist == True)\
        .filter(or_(
            Game.rs == False,
            Game.id.endswith('01201'), Game.id.endswith('01202'),
            Game.id.endswith('01203'), Game.id.endswith('01204'),
            Game.id.endswith('01229'), Game.id.endswith('01230')
        ))\
        .order_by(asc(Game.date)).all()

    if ist_ko_games:
        ist_rounds = get_bracket_data(ist_ko_games, is_ist=True)
        round_names_ist = {2: 'Cuartos de Final', 3: 'Semifinales', 4: 'Final (NBA Cup)'}
        for r_num in sorted(ist_rounds.keys()):
            if ist_rounds[r_num]:
                formatted_ist_bracket.append({
                    'num': r_num,
                    'name': round_names_ist.get(r_num, f'Ronda {r_num}'),
                    'series': ist_rounds[r_num]
                })

    return templates.TemplateResponse("seasons/detail.html", {
        "request": request,
        "active_page": "seasons",
        "season": season,
        "all_seasons": all_seasons,
        "east_standings": east_standings,
        "west_standings": west_standings,
        "bracket": formatted_po_bracket,
        "ist_bracket": formatted_ist_bracket
    })
