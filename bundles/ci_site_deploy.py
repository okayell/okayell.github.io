#!/usr/bin/env python3
"""Unified GitHub Actions orchestrator for okayell.github.io (all-in-one campus + 11 academies/arenas).

Supports two seamless workflows in a single repo (`okayell/okayell.github.io`):
  1. Push (`okayell.github.io.zip` uploaded as-is, no unzipping needed):
     - Deploys Campus Map (`index.html`, `404.html`) + all 7 static academies immediately.
     - For the 4 daily-updating arenas (`tw-stock-insight`, `nba-betting-iq`, `epl-odds-iq`, `f1-odds-iq`),
       keeps pre-built HTML when bundle data is >= cached data, or runs a fast local `--mode rebuild`
       against `.arena_cache` when GitHub Actions has newer scraped data (zero data rollback).
  2. Schedule / Workflow Dispatch:
     - Restores `.arena_cache/<arena>/latest_index.html` for non-scheduled arenas so atomic Pages
       deployments never regress another arena's page.
     - Runs `ci_update.py` only for arenas due in the current Taiwan-time window (or manual target),
       persists updated SQLite DBs + rendered `index.html` back into `.arena_cache`, strips internal
       `bundles/` artifacts from `_site/`, and writes `deploy=true|false` to `$GITHUB_OUTPUT`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import lzma
import os
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
from typing import Any

TZ_TW = dt.timezone(dt.timedelta(hours=8))

ARENAS: dict[str, dict[str, str]] = {
    'tw-stock-insight': {'db': 'stock.db', 'kind': 'stock', 'seed': 'seed-stock.json.xz'},
    'us-stock-insight': {'db': 'stock.db', 'kind': 'stock', 'seed': 'seed-us-stock.json.xz'},
    'nba-betting-iq': {'db': 'nba_betting.db', 'kind': 'sports'},
    'epl-odds-iq': {'db': 'epl_betting.sqlite3', 'kind': 'sports'},
    'f1-odds-iq': {'db': 'f1_betting.sqlite3', 'kind': 'sports'},
    'tw-basketball': {'db': 'tw_basketball.db', 'kind': 'sports'},
    'cinema': {'db': 'now_playing.json', 'kind': 'cinema'},
}


def _gh_output(key: str, value: str) -> None:
  path = os.environ.get('GITHUB_OUTPUT')
  if path:
    with open(path, 'a', encoding='utf-8') as f:
      f.write(f'{key}={value}\n')


def _sha256_file(fp: str) -> str:
  h = hashlib.sha256()
  with open(fp, 'rb') as f:
    for chunk in iter(lambda: f.read(65536), b''):
      h.update(chunk)
  return h.hexdigest()[:16]


def _load_json(fp: str) -> dict[str, Any]:
  try:
    with open(fp, encoding='utf-8') as f:
      d = json.load(f)
    return d if isinstance(d, dict) else {}
  except (OSError, ValueError):
    return {}


def _save_json(fp: str, data: dict[str, Any]) -> None:
  os.makedirs(os.path.dirname(fp), exist_ok=True)
  tmp = fp + '.tmp'
  with open(tmp, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
  os.replace(tmp, fp)


def _valid_db(fp: str) -> bool:
  if not os.path.isfile(fp):
    return False
  min_sz = 64 if fp.endswith('.json') else 4096
  return os.path.getsize(fp) >= min_sz


def _stock_max_date(db_path: str) -> str:
  if not _valid_db(db_path):
    return ''
  try:
    con = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    row = con.execute('SELECT MAX(date) FROM daily').fetchone()
    con.close()
    return str(row[0] or '') if row else ''
  except sqlite3.Error:
    return ''


def _stock_freshness(db_path: str) -> str:
  d = _stock_max_date(db_path)
  if not d:
    return ''
  try:
    con = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    row = con.execute("SELECT value FROM kv WHERE key='analytics'").fetchone()
    con.close()
    if row and row[0]:
      an = json.loads(row[0])
      upd = str(an.get('updated_at') or '')
      if upd:
        return f'{d}|{upd}'
  except (sqlite3.Error, ValueError):
    pass
  return d
def _sports_freshness(data_dir: str, db_name: str) -> str:
  snap = os.path.join(data_dir, 'snapshot.json')
  if os.path.isfile(snap):
    d = _load_json(snap)
    for k in ('generated_at', 'updated_at', 'synced_at', 'as_of'):
      if d.get(k):
        return str(d[k])
  db_fp = os.path.join(data_dir, db_name)
  if _valid_db(db_fp):
    return dt.datetime.fromtimestamp(os.path.getmtime(db_fp), TZ_TW).isoformat(timespec='seconds')
  return ''


def _data_freshness(arena: str, data_dir: str) -> str:
  info = ARENAS[arena]
  if info['kind'] == 'stock':
    return _stock_freshness(os.path.join(data_dir, info['db']))
  if info['kind'] == 'cinema':
    d = _load_json(os.path.join(data_dir, info['db']))
    return str(d.get('updatedAt') or '')
  return _sports_freshness(data_dir, info['db'])


def restore_stock_seed(bundles_dir: str, dst_db: str, seed_name: str = 'seed-stock.json.xz') -> bool:
  """Rebuilds `stock.db` from compact columnar `seed-*.json.xz` (or `seed-stock.db.xz`)."""
  json_xz = os.path.join(bundles_dir, seed_name)
  db_xz = os.path.join(bundles_dir, seed_name.replace('.json.xz', '.db.xz'))
  os.makedirs(os.path.dirname(dst_db), exist_ok=True)
  if os.path.isfile(json_xz):
    t0 = dt.datetime.now(TZ_TW)
    raw = lzma.decompress(open(json_xz, 'rb').read())
    payload = json.loads(raw.decode('utf-8'))
    tmp_db = dst_db + '.seed.tmp'
    if os.path.exists(tmp_db):
      os.remove(tmp_db)
    con = sqlite3.connect(tmp_db)
    con.execute('PRAGMA synchronous=OFF')
    con.execute('PRAGMA journal_mode=OFF')
    schemas = payload.get('schemas') or []
    for typ, _name, _tbl, sql in schemas:
      if typ == 'table' and sql:
        con.execute(sql)
    dates = payload.get('dates') or []
    daily_rows = []
    if payload.get('v') == 2 and payload.get('daily7'):
      for code, cols in (payload.get('daily7') or {}).items():
        ddis, dcs, dos, dhs, dls, vols = cols
        di = -1
        cc = 0
        for k in range(len(ddis)):
          di += ddis[k]
          dc = dcs[k]
          cc += dc
          oc = cc + dos[k]
          hc = cc + dhs[k]
          lc = cc + dls[k]
          daily_rows.append((
              code, dates[di], round(oc / 100.0, 2), round(hc / 100.0, 2),
              round(lc / 100.0, 2), round(cc / 100.0, 2), float(vols[k]),
          ))
      if daily_rows:
        con.executemany('INSERT OR REPLACE INTO daily VALUES (?,?,?,?,?,?,?)', daily_rows)
    elif payload.get('v') == 2 and payload.get('daily2'):
      for code, cols in (payload.get('daily2') or {}).items():
        ddis, dcs, dos, dhs, dls, vols, vals, dchgs = cols
        di = -1
        cc = 0
        for k in range(len(ddis)):
          di += ddis[k]
          dc = dcs[k]
          cc += dc
          oc = cc + dos[k]
          hc = cc + dhs[k]
          lc = cc + dls[k]
          chgc = dc + dchgs[k]
          daily_rows.append((
              code, dates[di], round(oc / 100.0, 2), round(hc / 100.0, 2),
              round(lc / 100.0, 2), round(cc / 100.0, 2),
              float(vols[k]), float(vals[k]) * 1000.0, 0, round(chgc / 100.0, 2),
          ))
      if daily_rows:
        con.executemany('INSERT OR REPLACE INTO daily VALUES (?,?,?,?,?,?,?,?,?,?)', daily_rows)
    else:
      for code, items in (payload.get('daily') or {}).items():
        for di, o, h, l, c, vol, val_k, chg in items:
          daily_rows.append((code, dates[di], o, h, l, c, float(vol), float(val_k) * 1000.0, 0, chg))
      if daily_rows:
        con.executemany('INSERT OR REPLACE INTO daily VALUES (?,?,?,?,?,?,?,?,?,?)', daily_rows)
    i_dates = payload.get('i_dates') or []
    inst_rows = []
    if payload.get('v') == 2 and payload.get('inst2'):
      for code, cols in (payload.get('inst2') or {}).items():
        diis, fis, tis, dis2 = cols
        ii = -1
        for k in range(len(diis)):
          ii += diis[k]
          fi, ti, di2 = fis[k], tis[k], dis2[k]
          inst_rows.append((code, i_dates[ii], float(fi), float(ti), float(di2), float(fi + ti + di2)))
    else:
      for code, items in (payload.get('inst') or {}).items():
        for ii, fi, ti, di2 in items:
          inst_rows.append((code, i_dates[ii], float(fi), float(ti), float(di2), float(fi + ti + di2)))
    if inst_rows:
      con.executemany('INSERT OR REPLACE INTO inst VALUES (?,?,?,?,?,?)', inst_rows)
    for tbl, rows in (payload.get('tables') or {}).items():
      if not rows:
        continue
      placeholders = ','.join(['?'] * len(rows[0]))
      con.executemany(f'INSERT OR REPLACE INTO [{tbl}] VALUES ({placeholders})', rows)
    for typ, _name, _tbl, sql in schemas:
      if typ == 'index' and sql:
        con.execute(sql)
    con.commit()
    con.execute('PRAGMA journal_mode=DELETE')
    con.close()
    os.replace(tmp_db, dst_db)
    sec = (dt.datetime.now(TZ_TW) - t0).total_seconds()
    print(f'[deploy] seeded stock.db from {seed_name} ({len(daily_rows):,} daily rows in {sec:.1f}s)')
    return True
  if os.path.isfile(db_xz):
    with lzma.open(db_xz, 'rb') as src, open(dst_db, 'wb') as out:
      shutil.copyfileobj(src, out)
    print(f'[deploy] seeded stock.db from seed-stock.db.xz ({os.path.getsize(dst_db):,} bytes)')
    return True
  return False


def _copy_data_dir(src_dir: str, dst_dir: str) -> None:
  if not os.path.isdir(src_dir):
    return
  os.makedirs(dst_dir, exist_ok=True)
  for root, dirs, files in os.walk(src_dir):
    dirs[:] = [d for d in dirs if not d.startswith('.')]
    rel_root = os.path.relpath(root, src_dir)
    target_root = dst_dir if rel_root == '.' else os.path.join(dst_dir, rel_root)
    os.makedirs(target_root, exist_ok=True)
    for fn in files:
      if fn.endswith(('-wal', '-shm', '.lock', '.tmp')):
        continue
      sp = os.path.join(root, fn)
      if fn.endswith(('.db', '.sqlite3')) and os.path.getsize(sp) < 4096:
        continue
      shutil.copy2(sp, os.path.join(target_root, fn))


def _unpack_arena(tar_path: str, work_arena: str) -> None:
  if os.path.exists(work_arena):
    shutil.rmtree(work_arena)
  os.makedirs(work_arena, exist_ok=True)
  with tarfile.open(tar_path, 'r:gz') as tar:
    tar.extractall(work_arena, filter='data')


def _effective_mode(arena: str, mode: str) -> str:
  kind = ARENAS[arena]['kind']
  if kind == 'stock' and mode == 'odds':
    return 'auto'
  if kind == 'sports' and mode in ('morning', 'intraday'):
    return 'auto'
  return mode


def _run_arena_ci(work_arena: str, mode: str, out_dir: str, schedule: str = '') -> bool:
  with tempfile.NamedTemporaryFile('w+', encoding='utf-8', delete=False) as tf:
    out_path = tf.name
  env = dict(os.environ)
  env['GITHUB_OUTPUT'] = out_path
  env['TZ'] = 'Asia/Taipei'
  if schedule:
    env['GITHUB_CRON_SCHEDULE'] = schedule.strip()
  try:
    res = subprocess.run(
        [sys.executable, os.path.join(work_arena, 'ci_update.py'), '--mode', mode, '--out', out_dir],
        cwd=work_arena,
        env=env,
        check=False,
    )
    kv = {}
    with open(out_path, encoding='utf-8') as f:
      for line in f:
        if '=' in line:
          k, v = line.strip().split('=', 1)
          kv[k] = v
    return res.returncode == 0 and kv.get('deploy') == 'true'
  finally:
    if os.path.exists(out_path):
      os.remove(out_path)


def _due_arenas(now_tw: dt.datetime, cache_dir: str = '') -> list[str]:
  """Selects arenas to refresh on scheduled runs (immune to 1-4h GitHub Actions cron queue delays).

  GitHub Actions free-tier `schedule` jobs routinely start 45 min to 3.5 hours after the nominal
  cron minute. Because all 4 sports arenas together complete in ~30-45s, we ALWAYS refresh every
  sports arena on every scheduled run so no sport can ever fall into a dead-zone window and get
  skipped. For TW Stock (~2-4 min), any weekday run from 07:15 TW onward (or any weekday where
  cache is behind today) triggers a refresh.
  """
  mins = now_tw.hour * 60 + now_tw.minute
  wd = now_tw.weekday()  # 0=Mon .. 6=Sun
  due: list[str] = ['nba-betting-iq', 'epl-odds-iq', 'f1-odds-iq', 'tw-basketball']
  if wd < 5:
    stock_db = os.path.join(cache_dir, 'tw-stock-insight', 'data', 'stock.db') if cache_dir else ''
    stock_last = _stock_max_date(stock_db) if stock_db else ''
    if mins >= 435 or stock_last < now_tw.strftime('%Y-%m-%d'):
      due.append('tw-stock-insight')
  if wd <= 5:
    us_db = os.path.join(cache_dir, 'us-stock-insight', 'data', 'stock.db') if cache_dir else ''
    us_last = _stock_max_date(us_db) if us_db else ''
    expected_us = (now_tw - dt.timedelta(days=1)).strftime('%Y-%m-%d') if mins < 720 else now_tw.strftime('%Y-%m-%d')
    if wd < 5 or mins < 720 or us_last < expected_us:
      due.append('us-stock-insight')
  cin_json = os.path.join(cache_dir, 'cinema', 'data', 'now_playing.json') if cache_dir else ''
  cin_wk = str(_load_json(cin_json).get('weekKey') or '') if cin_json else ''
  cur_wk = now_tw.strftime('%G-W%V')
  if wd == 0 or not cin_wk or cin_wk < cur_wk:
    due.append('cinema')
  return due


def _finalize_site(site_dir: str) -> None:
  """Removes CI-only bundle payloads so `_site/` contains pure static Pages assets."""
  for name in ('bundles', 'okayell.github.io.zip', 'auto-update.yml', '.github'):
    p = os.path.join(site_dir, name)
    if os.path.isdir(p):
      shutil.rmtree(p)
    elif os.path.isfile(p):
      os.remove(p)
  for fn in os.listdir(site_dir):
    if fn.endswith(('.tar.gz', '.xz', '.zip')):
      os.remove(os.path.join(site_dir, fn))
  with open(os.path.join(site_dir, '.nojekyll'), 'w', encoding='utf-8'):
    pass


def main() -> int:
  ap = argparse.ArgumentParser(description='Unified GitHub Pages builder & scheduler for okayell.github.io')
  ap.add_argument('--site', default='_site')
  ap.add_argument('--cache', default='.arena_cache')
  ap.add_argument('--work', default='_work')
  ap.add_argument('--event', default=os.environ.get('GITHUB_EVENT_NAME', 'push'))
  ap.add_argument('--target', default='auto',
                  choices=['auto', 'all', 'none', *ARENAS.keys()])
  ap.add_argument('--mode', default='auto',
                  choices=['auto', 'update', 'morning', 'intraday', 'odds', 'rebuild', 'deploy_only'])
  ap.add_argument('--schedule', default=os.environ.get('GITHUB_CRON_SCHEDULE', ''),
                  help='Original cron expression from github.event.schedule (survives runner queue delays)')
  args = ap.parse_args()

  site_dir = os.path.abspath(args.site)
  cache_dir = os.path.abspath(args.cache)
  work_dir = os.path.abspath(args.work)
  bundles_dir = os.path.join(site_dir, 'bundles')
  os.makedirs(cache_dir, exist_ok=True)
  os.makedirs(work_dir, exist_ok=True)

  seed_meta = _load_json(os.path.join(bundles_dir, 'seed-meta.json'))
  now_tw = dt.datetime.now(TZ_TW)
  print(f'[deploy] event={args.event} target={args.target} mode={args.mode} now={now_tw:%Y-%m-%d %H:%M} TW')

  # 1) Push / deploy_only path: fast publish of newly uploaded okayell.github.io.zip
  if args.event == 'push' or args.mode == 'deploy_only':
    for arena, info in ARENAS.items():
      tar_path = os.path.join(bundles_dir, f'{arena}.tar.gz')
      if not os.path.isfile(tar_path):
        continue
      b_sha = _sha256_file(tar_path)
      b_fresh = str((seed_meta.get(arena) or {}).get('freshness') or '')
      c_arena = os.path.join(cache_dir, arena)
      c_data = os.path.join(c_arena, 'data')
      c_db = os.path.join(c_data, info['db'])
      c_meta = _load_json(os.path.join(c_arena, 'meta.json'))
      c_fresh = _data_freshness(arena, c_data) if _valid_db(c_db) else ''
      out_arena = os.path.join(site_dir, arena)

      if _valid_db(c_db) and c_fresh and b_fresh and c_fresh > b_fresh and args.mode != 'deploy_only':
        print(f'[deploy] {arena}: cache ({c_fresh}) > bundle ({b_fresh}) -> rebuilding new UI with cached DB')
        w_arena = os.path.join(work_dir, arena)
        _unpack_arena(tar_path, w_arena)
        _copy_data_dir(c_data, os.path.join(w_arena, 'data'))
        built = _run_arena_ci(w_arena, 'rebuild', out_arena)
        if built:
          _copy_data_dir(os.path.join(w_arena, 'data'), c_data)
      else:
        print(f'[deploy] {arena}: keeping pre-built HTML (bundle={b_fresh or "n/a"}, cache={c_fresh or "empty"})')
        w_arena = os.path.join(work_dir, arena)
        _unpack_arena(tar_path, w_arena)
        if not _valid_db(c_db) or (b_fresh and b_fresh >= c_fresh):
          _copy_data_dir(os.path.join(w_arena, 'data'), c_data)
          if info.get('seed') and (not _valid_db(c_db) or (b_fresh and b_fresh > c_fresh)):
            restore_stock_seed(bundles_dir, c_db, info['seed'])

      site_html = os.path.join(out_arena, 'index.html')
      if os.path.isfile(site_html):
        os.makedirs(c_arena, exist_ok=True)
        shutil.copy2(site_html, os.path.join(c_arena, 'latest_index.html'))
        _save_json(os.path.join(c_arena, 'meta.json'), {
            'bundle_sha': b_sha,
            'freshness': _data_freshness(arena, c_data) or b_fresh,
            'updated_at': now_tw.isoformat(timespec='seconds'),
        })

    _finalize_site(site_dir)
    _gh_output('deploy', 'true')
    print('[deploy] push site ready -> deploy=true')
    return 0

  # 2) Schedule / workflow_dispatch path: preserve latest cached HTML for all arenas first
  bundle_shas: dict[str, str] = {}
  for arena in ARENAS:
    tar_path = os.path.join(bundles_dir, f'{arena}.tar.gz')
    if not os.path.isfile(tar_path):
      continue
    b_sha = _sha256_file(tar_path)
    bundle_shas[arena] = b_sha
    c_arena = os.path.join(cache_dir, arena)
    c_html = os.path.join(c_arena, 'latest_index.html')
    c_meta = _load_json(os.path.join(c_arena, 'meta.json'))
    if os.path.isfile(c_html) and c_meta.get('bundle_sha') == b_sha:
      dst_html = os.path.join(site_dir, arena, 'index.html')
      os.makedirs(os.path.dirname(dst_html), exist_ok=True)
      shutil.copy2(c_html, dst_html)

  if args.target == 'none':
    targets: list[str] = []
  elif args.target == 'all':
    targets = list(ARENAS)
  elif args.target in ARENAS:
    targets = [args.target]
  elif args.event == 'workflow_dispatch' and args.mode != 'auto':
    targets = list(ARENAS)
  else:
    targets = _due_arenas(now_tw, cache_dir)
    if args.event == 'workflow_dispatch' and not targets:
      targets = list(ARENAS)

  sched_mode = args.mode
  print(f'[deploy] scheduled targets={targets} mode={sched_mode} schedule={args.schedule!r}')
  any_built = False
  for arena in targets:
    tar_path = os.path.join(bundles_dir, f'{arena}.tar.gz')
    if not os.path.isfile(tar_path):
      print(f'[deploy] skip {arena}: {tar_path} missing')
      continue
    info = ARENAS[arena]
    c_arena = os.path.join(cache_dir, arena)
    c_data = os.path.join(c_arena, 'data')
    c_db = os.path.join(c_data, info['db'])
    w_arena = os.path.join(work_dir, arena)
    _unpack_arena(tar_path, w_arena)
    w_data = os.path.join(w_arena, 'data')
    os.makedirs(w_data, exist_ok=True)
    if _valid_db(c_db):
      _copy_data_dir(c_data, w_data)
    elif info.get('seed'):
      restore_stock_seed(bundles_dir, os.path.join(w_data, info['db']), info['seed'])

    eff_mode = _effective_mode(arena, sched_mode)
    out_arena = os.path.join(site_dir, arena)
    built = _run_arena_ci(w_arena, eff_mode, out_arena, schedule=args.schedule)
    _copy_data_dir(w_data, c_data)
    if built and os.path.isfile(os.path.join(out_arena, 'index.html')):
      any_built = True
      os.makedirs(c_arena, exist_ok=True)
      shutil.copy2(os.path.join(out_arena, 'index.html'), os.path.join(c_arena, 'latest_index.html'))
      _save_json(os.path.join(c_arena, 'meta.json'), {
          'bundle_sha': bundle_shas.get(arena, ''),
          'freshness': _data_freshness(arena, c_data),
          'updated_at': now_tw.isoformat(timespec='seconds'),
      })

  _finalize_site(site_dir)
  should_deploy = 'true' if any_built else 'false'
  _gh_output('deploy', should_deploy)
  print(f'[deploy] finished -> deploy={should_deploy}')
  return 0


if __name__ == '__main__':
  sys.exit(main())
