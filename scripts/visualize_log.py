#!/usr/bin/env python3
"""Create an HTML timeline from a HotStuff simulator .txt log.

Only the Python standard library is used.
"""

from __future__ import annotations

import argparse
import html
import re
from pathlib import Path

PATTERNS = [
    ('START_VIEW', re.compile(r'\[(B\d+)\] enter view=(\d+) leader=(B\d+) reason=(.+)')),
    ('TIMEOUT', re.compile(r'\[(B\d+)\] TIMEOUT view=(\d+) -> NEW_VIEW')),
    ('NEW_VIEW', re.compile(r'\[view=(\d+) leader=(B\d+)\] NEW_VIEW from (B\d+) \((\d+)/(\d+)\)')),
    ('PROPOSE', re.compile(r'\[view=(\d+) leader=(B\d+)\] PREPARE proposal ([0-9a-f]+): (.+) extends (.+)')),
    ('SAFE', re.compile(r'\[(B\d+)\] safeNode (accepted|rejected) ([^\s]+)(?:.*?\((.+)\))?(?:.*reason=(.+))?')),
    ('VOTE', re.compile(r'\[leader=(B\d+)\] vote (PREPARE|PRECOMMIT|COMMIT) for node=([0-9a-f]+) from=(B\d+) \((\d+)/(\d+)\)')),
    ('QC', re.compile(r'\[leader=(B\d+)\] formed ([A-Z]+QC)\(node=([0-9a-f]+) view=(\d+) voters=\[([^\]]*)\]\)')),
    ('STORE', re.compile(r'\[(B\d+)\] stores prepareQC = (.+)')),
    ('LOCK', re.compile(r'\[(B\d+)\] lockedQC = (.+)')),
    ('DECIDE', re.compile(r'\[(B\d+)\] DECIDE ([0-9a-f]+)')),
    ('EXECUTE', re.compile(r'\[(B\d+)\] EXECUTE (.+) -> (VALID|INVALID) (.+)')),
    ('SCENARIO', re.compile(r'\[scenario\] (.+)')),
    ('FINAL_STATE', re.compile(r'\[(B\d+)\] view=(\d+) locked=(.+)')),
    ('LEDGER_EQUALITY', re.compile(r'Ledger equality among correct replicas: (true|false)', re.I)),
]

IMPORTANT = {
    'START_VIEW', 'TIMEOUT', 'NEW_VIEW', 'PROPOSE', 'SAFE_REJECT',
    'PREPAREQC', 'PRECOMMITQC', 'COMMITQC', 'LOCK', 'DECIDE',
    'EXECUTE', 'BYZANTINE', 'SCENARIO', 'LEDGER_EQUALITY',
}


def node_from(text: str) -> str:
    match = re.search(r'node=([0-9a-f]+)', text)
    return match.group(1) if match else ''


def view_leader_from(text: str) -> tuple[str, str]:
    match = re.search(r'\[view=(\d+) leader=(B\d+)\]', text)
    return match.groups() if match else ('', 'scenario')


def event_class(kind: str, detail: str) -> str:
    if kind == 'EXECUTE' and 'INVALID' in detail:
        return 'execute-invalid'
    if kind == 'EXECUTE' and 'VALID' in detail:
        return 'execute-valid'
    return kind.lower().replace('_', '-')


def parse_line(number: int, line: str) -> dict | None:
    if 'BYZANTINE EQUIVOCATION' in line or 'Byzantine leader stops' in line or 'equivocation vote counts' in line:
        view, leader = view_leader_from(line)
        return {'n': number, 'lane': leader, 'view': view, 'kind': 'BYZANTINE', 'node': '', 'cmd': '', 'detail': '', 'line': line}

    for name, regex in PATTERNS:
        match = regex.search(line)
        if not match:
            continue
        groups = match.groups()
        if name == 'START_VIEW':
            replica, view, leader, reason = groups
            return {'n': number, 'lane': replica, 'view': view, 'kind': 'START_VIEW', 'node': '', 'cmd': '', 'detail': f'leader={leader}, reason={reason}', 'line': line}
        if name == 'TIMEOUT':
            replica, view = groups
            return {'n': number, 'lane': replica, 'view': view, 'kind': 'TIMEOUT', 'node': '', 'cmd': '', 'detail': 'move to next view', 'line': line}
        if name == 'NEW_VIEW':
            view, leader, sender, count, quorum = groups
            return {'n': number, 'lane': leader, 'view': view, 'kind': 'NEW_VIEW', 'node': '', 'cmd': '', 'detail': f'from {sender}, {count}/{quorum}', 'line': line}
        if name == 'PROPOSE':
            view, leader, node, command, parent = groups
            return {'n': number, 'lane': leader, 'view': view, 'kind': 'PROPOSE', 'node': node, 'cmd': command, 'detail': f'extends {parent}', 'line': line}
        if name == 'SAFE':
            replica, verdict, node, command, reason = groups
            kind = 'SAFE_ACCEPT' if verdict == 'accepted' else 'SAFE_REJECT'
            return {'n': number, 'lane': replica, 'view': '', 'kind': kind, 'node': node, 'cmd': command or '', 'detail': reason or verdict, 'line': line}
        if name == 'VOTE':
            leader, phase, node, voter, count, quorum = groups
            return {'n': number, 'lane': voter, 'view': '', 'kind': f'VOTE_{phase}', 'node': node, 'cmd': '', 'detail': f'leader={leader}, {count}/{quorum}', 'line': line}
        if name == 'QC':
            leader, qc_type, node, view, voters = groups
            return {'n': number, 'lane': leader, 'view': view, 'kind': qc_type, 'node': node, 'cmd': '', 'detail': f'voters=[{voters}]', 'line': line}
        if name == 'STORE':
            replica, qc = groups
            return {'n': number, 'lane': replica, 'view': '', 'kind': 'STORE_PREPARE_QC', 'node': node_from(qc), 'cmd': '', 'detail': qc, 'line': line}
        if name == 'LOCK':
            replica, qc = groups
            return {'n': number, 'lane': replica, 'view': '', 'kind': 'LOCK', 'node': node_from(qc), 'cmd': '', 'detail': qc, 'line': line}
        if name == 'DECIDE':
            replica, node = groups
            return {'n': number, 'lane': replica, 'view': '', 'kind': 'DECIDE', 'node': node, 'cmd': '', 'detail': '', 'line': line}
        if name == 'EXECUTE':
            replica, command, validity, result = groups
            return {'n': number, 'lane': replica, 'view': '', 'kind': 'EXECUTE', 'node': '', 'cmd': command, 'detail': f'{validity}: {result}', 'line': line}
        if name == 'SCENARIO':
            return {'n': number, 'lane': 'scenario', 'view': '', 'kind': 'SCENARIO', 'node': '', 'cmd': '', 'detail': groups[0], 'line': line}
        if name == 'FINAL_STATE':
            replica, view, locked = groups
            return {'n': number, 'lane': replica, 'view': view, 'kind': 'FINAL_STATE', 'node': node_from(locked), 'cmd': '', 'detail': locked, 'line': line}
        if name == 'LEDGER_EQUALITY':
            return {'n': number, 'lane': 'scenario', 'view': '', 'kind': 'LEDGER_EQUALITY', 'node': '', 'cmd': '', 'detail': groups[0].lower(), 'line': line}
    return None


def parse_events(lines: list[str]) -> list[dict]:
    return [event for i, line in enumerate(lines, 1) if (event := parse_line(i, line.rstrip()))]


def final_block(lines: list[str]) -> str:
    for i, line in enumerate(lines):
        if line.startswith('Final correct replica states:'):
            return ''.join(lines[i:]).strip()
    return ''


def stats(events: list[dict]) -> dict[str, str | int]:
    kinds = [event['kind'] for event in events]
    equality = next((event['detail'] for event in reversed(events) if event['kind'] == 'LEDGER_EQUALITY'), 'unknown')
    return {
        'events': len(events),
        'decisions': kinds.count('DECIDE'),
        'executions': kinds.count('EXECUTE'),
        'qcs': sum(kind.endswith('QC') for kind in kinds),
        'timeouts': kinds.count('TIMEOUT'),
        'safe rejects': kinds.count('SAFE_REJECT'),
        'ledger equality': equality,
    }


def lanes(events: list[dict]) -> list[str]:
    replica_lanes = sorted({e['lane'] for e in events if re.fullmatch(r'B\d+', e['lane'])}, key=lambda x: int(x[1:]))
    return replica_lanes + (['scenario'] if any(e['lane'] == 'scenario' for e in events) else [])


def timeline(events: list[dict]) -> str:
    lane_names = lanes(events) or ['scenario']
    y = {lane: 70 + i * 70 for i, lane in enumerate(lane_names)}
    width = max(980, 150 + len(events) * 15)
    height = 120 + len(lane_names) * 70
    out = [f'<svg class="timeline" viewBox="0 0 {width} {height}">']
    for lane, yy in y.items():
        out.append(f'<text class="lane-label" x="20" y="{yy + 4}">{html.escape(lane)}</text>')
        out.append(f'<line class="lane-line" x1="100" y1="{yy}" x2="{width - 30}" y2="{yy}"/>')
    last = {}
    for idx, event in enumerate(events):
        x = 115 + idx * 15
        yy = y.get(event['lane'], y[lane_names[-1]])
        if event['lane'] in last:
            px, py = last[event['lane']]
            out.append(f'<line class="link" x1="{px}" y1="{py}" x2="{x}" y2="{yy}"/>')
        last[event['lane']] = (x, yy)
        title = html.escape(f"#{event['n']} {event['kind']} {event['line']}")
        css = event_class(event['kind'], event['detail'])
        out.append(f'<g class="event {css}"><circle cx="{x}" cy="{yy}" r="7"><title>{title}</title></circle></g>')
    out.append('</svg>')
    return '\n'.join(out)


def table(events: list[dict]) -> str:
    rows = []
    for event in events:
        if event['kind'] not in IMPORTANT and not event['kind'].startswith('VOTE_'):
            continue
        css = event_class(event['kind'], event['detail'])
        rows.append(
            '<tr>'
            f'<td>{event["n"]}</td><td>{html.escape(event["view"])}</td><td>{html.escape(event["lane"])}</td>'
            f'<td><span class="pill {css}">{html.escape(event["kind"])}</span></td>'
            f'<td><code>{html.escape(event["node"])}</code></td><td>{html.escape(event["cmd"])}</td>'
            f'<td>{html.escape(event["detail"])}</td>'
            '</tr>'
        )
    return '\n'.join(rows)


def render(title: str, log_path: Path, lines: list[str], events: list[dict]) -> str:
    cards = ''.join(f'<div class="card"><b>{html.escape(str(v))}</b><span>{html.escape(k)}</span></div>' for k, v in stats(events).items())
    final = html.escape(final_block(lines) or 'No final state block found.')
    return f'''<!doctype html>
<html><head><meta charset="utf-8"><title>HotStuff visualization - {html.escape(title)}</title>
<style>
body{{margin:0;background:#0f172a;color:#e5e7eb;font-family:Arial,sans-serif}}main{{max-width:1400px;margin:auto;padding:28px}}.muted{{color:#94a3b8}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin:20px 0}}.card{{background:#111827;border:1px solid #334155;border-radius:14px;padding:14px}}.card b{{display:block;font-size:26px}}.card span{{color:#94a3b8;text-transform:uppercase;font-size:12px}}
.section{{background:#111827;border:1px solid #334155;border-radius:18px;padding:18px;margin:18px 0;overflow:auto}}.timeline{{min-width:980px;width:100%;background:#020617;border-radius:14px}}.lane-label{{fill:#94a3b8;font-weight:bold}}.lane-line{{stroke:#334155;stroke-width:2}}.link{{stroke:#1e293b;stroke-width:1}}
.event circle{{fill:#64748b;stroke:white;stroke-width:1}}.propose circle{{fill:#38bdf8}}.prepareqc circle,.precommitqc circle,.commitqc circle,.store-prepare-qc circle,.lock circle{{fill:#a78bfa}}.decide circle,.execute-valid circle{{fill:#22c55e}}.execute-invalid circle,.safe-reject circle{{fill:#ef4444}}.timeout circle,.byzantine circle{{fill:#f59e0b}}.new-view circle,.safe-accept circle{{fill:#14b8a6}}
table{{width:100%;border-collapse:collapse;font-size:14px}}td,th{{border-bottom:1px solid #334155;padding:8px;text-align:left;vertical-align:top}}th{{color:#94a3b8}}pre,code{{font-family:ui-monospace,Menlo,Consolas,monospace}}pre{{white-space:pre-wrap;line-height:1.45}}.pill{{border-radius:999px;padding:3px 8px;background:#334155;color:white;white-space:nowrap;font-size:12px}}
</style></head><body><main>
<h1>HotStuff log visualization: {html.escape(title)}</h1><p class="muted">Source log: <code>{html.escape(str(log_path))}</code>. Hover timeline dots to see raw log lines.</p>
<div class="cards">{cards}</div>
<section class="section"><h2>Replica timeline</h2>{timeline(events)}</section>
<section class="section"><h2>Important events</h2><table><thead><tr><th>#</th><th>View</th><th>Replica</th><th>Event</th><th>Node</th><th>Command</th><th>Detail</th></tr></thead><tbody>{table(events)}</tbody></table></section>
<section class="section"><h2>Final state</h2><pre>{final}</pre></section>
</main></body></html>'''


def main() -> None:
    parser = argparse.ArgumentParser(description='Create an HTML timeline from a HotStuff simulator log.')
    parser.add_argument('--log', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args()
    lines = args.log.read_text(encoding='utf-8').splitlines(keepends=True)
    events = parse_events(lines)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(args.log.stem, args.log, lines, events), encoding='utf-8')
    print(f'wrote {args.out}')


if __name__ == '__main__':
    main()
