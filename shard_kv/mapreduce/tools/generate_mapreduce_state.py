#!/usr/bin/env python3
"""Generate the editable MapReduce node/map/reduce state-transition SVG."""

from pathlib import Path
import html
import sys

W, H = 1600, 1040
OUT = Path(__file__).resolve().parents[1] / "docs" / "mapreduce_failure_state.svg"

COLORS = {
    "paper": "#f8fafc", "ink": "#172b3a", "muted": "#526778",
    "line": "#607588", "blue": "#dcecff", "blue_line": "#3476b8",
    "green": "#dff3e8", "green_line": "#31845d", "red": "#fde4e2",
    "red_line": "#bd4b43", "amber": "#fff1cf", "amber_line": "#b27a12",
    "white": "#ffffff", "panel": "#eef3f7", "soft_red": "#fff5f4",
}
FONT = "Arial, 'Microsoft YaHei', 'PingFang SC', 'Noto Sans CJK SC', sans-serif"


def esc(value: str) -> str:
    return html.escape(value, quote=True)


def text(x, y, value, size=18, color=None, weight=400, anchor="start"):
    color = color or COLORS["ink"]
    return (f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" '
            f'font-weight="{weight}" fill="{color}" text-anchor="{anchor}">{esc(value)}</text>')


def multiline(x, y, lines, size=15, color=None, line_height=22, anchor="middle", weight=400):
    color = color or COLORS["muted"]
    spans = "".join(f'<tspan x="{x}" dy="{0 if i == 0 else line_height}">{esc(line)}</tspan>'
                    for i, line in enumerate(lines))
    return (f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" '
            f'font-weight="{weight}" fill="{color}" text-anchor="{anchor}">{spans}</text>')


def box(x, y, w, h, title, lines, fill, stroke, title_color=None):
    title_color = title_color or COLORS["ink"]
    return [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" stroke="{stroke}" stroke-width="2.2"/>',
        text(x + w / 2, y + 31, title, 18, title_color, 700, "middle"),
        multiline(x + w / 2, y + 56, lines, 14, COLORS["muted"], 19),
    ]


def arrow(d, label=None, lx=None, ly=None, color=None, dashed=False, label_w=0):
    color = color or COLORS["line"]
    dash = ' stroke-dasharray="8 7"' if dashed else ""
    result = [f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2.4" marker-end="url(#arrow)"{dash}/>']
    if label:
        width = label_w or max(100, len(label) * 8 + 18)
        result.append(f'<rect x="{lx-width/2}" y="{ly-19}" width="{width}" height="28" rx="8" fill="{COLORS["paper"]}"/>')
        result.append(text(lx, ly, label, 13, color, 600, "middle"))
    return result


def build_svg():
    e = []
    e.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">')
    e.append(f'<rect width="{W}" height="{H}" fill="{COLORS["paper"]}"/>')
    e.append('<defs><marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L10,5 L0,10 z" fill="#607588"/></marker></defs>')
    e.append(text(70, 66, "MapReduce：节点本地中间结果的失效与重拉", 30, COLORS["ink"], 700))
    e.append(text(70, 101, "worker 进程模拟节点；manifest 指向 owner 的数据 socket；节点失效后 map 重算，reduce attempt 重启", 16, COLORS["muted"]))

    # Row backgrounds and labels.
    rows = [(145, 155, "NODE / DATA OWNER"), (335, 260, "MAP TASK + MANIFEST"), (630, 330, "REDUCE TASK ATTEMPT")]
    for y, h, label in rows:
        e.append(f'<rect x="48" y="{y}" width="1504" height="{h}" rx="20" fill="{COLORS["white"]}" stroke="#d8e1e8" stroke-width="1.3"/>')
        e.append(text(78, y + 31, label, 13, COLORS["muted"], 700))

    # Node states.
    e += box(270, 205, 250, 78, "ALIVE", ["heartbeats renew the lease", "data socket serves local files"], COLORS["blue"], COLORS["blue_line"])
    e += box(850, 205, 250, 78, "DOWN", ["lease expires or data fetch fails", "local disk is unreachable"], COLORS["red"], COLORS["red_line"], COLORS["red_line"])
    e += arrow("M 520 244 H 850", "failure detector", 685, 229, COLORS["red_line"], label_w=145)
    e.append(text(1190, 242, "A new process gets a new worker/node ID", 15, COLORS["muted"], 500))

    # Map states.
    e += box(120, 430, 205, 92, "PENDING", ["queued for a worker"], COLORS["panel"], "#8293a1")
    e += box(405, 430, 215, 92, "RUNNING", ["owner + attempt + lease"], COLORS["blue"], COLORS["blue_line"])
    e += box(735, 415, 300, 122, "COMMITTED MANIFEST", ["owner node + version", "R local partition files", "visible only after CompleteMap"], COLORS["green"], COLORS["green_line"], COLORS["green_line"])
    e += box(1190, 430, 220, 92, "INVALIDATED", ["owner's files unavailable"], COLORS["red"], COLORS["red_line"], COLORS["red_line"])
    e += arrow("M 325 476 H 405", "assign", 365, 456, COLORS["blue_line"], label_w=76)
    e += arrow("M 620 476 H 735", "R files + valid attempt", 678, 449, COLORS["green_line"], label_w=164)
    e += arrow("M 1035 480 H 1190", "node down", 1112, 458, COLORS["red_line"], label_w=112)
    e += arrow("M 510 522 V 574 H 220 V 522", "worker/lease failure: retry same map task", 365, 567, COLORS["amber_line"], label_w=275)
    e += arrow("M 1300 522 V 576 H 300 V 522", "clear location; queue map for recompute", 795, 568, COLORS["red_line"], label_w=270)
    # Node-down invalidation points to the old manifest.
    e += arrow("M 975 283 V 370 H 1320 V 430", "owner node's entire manifest becomes unavailable", 1150, 357, COLORS["red_line"], dashed=True, label_w=315)

    # Reduce states.
    e += box(90, 750, 205, 86, "PENDING", ["new attempt"], COLORS["panel"], "#8293a1")
    e += box(355, 750, 235, 86, "PULLING", ["fetch each map partition", "track map ID + version"], COLORS["blue"], COLORS["blue_line"])
    e += box(680, 750, 215, 86, "REDUCING", ["all maps ready", "sort, group, reduce"], COLORS["amber"], COLORS["amber_line"])
    e += box(1005, 750, 215, 86, "COMMITTED", ["master publishes mr-out-R"], COLORS["green"], COLORS["green_line"], COLORS["green_line"])
    e += box(1320, 750, 190, 86, "REVOKED", ["stale attempt cannot commit"], COLORS["red"], COLORS["red_line"], COLORS["red_line"])
    e += arrow("M 295 793 H 355", "assign reducer", 325, 775, COLORS["blue_line"], label_w=105)
    e += arrow("M 590 793 H 680", "all map outputs received", 635, 775, COLORS["green_line"], label_w=180)
    e += arrow("M 895 793 H 1005", "fetch + atomic publish", 950, 775, COLORS["green_line"], label_w=155)
    e += arrow("M 472 836 V 900 H 1415 V 836", "source manifest lost while pulling", 930, 894, COLORS["red_line"], label_w=220)
    e += arrow("M 1110 836 V 933 H 1415 V 836", "source node lost during reduce; completion rejected", 1260, 927, COLORS["red_line"], label_w=310)
    e += arrow("M 1320 792 H 295", "master reassigns; new attempt pulls current manifests", 805, 721, COLORS["blue_line"], label_w=360)

    # Cross-row event: committed map data is available to reducers immediately.
    e += arrow("M 885 537 V 660 H 472 V 750", "publish manifest; reducer pulls before all maps finish", 690, 650, COLORS["blue_line"], dashed=True, label_w=330)
    # Cross-row event: invalidated map manifests revoke reducer attempts.
    e += arrow("M 1400 522 V 615 H 1415 V 750", "revoke in-flight reducers", 1325, 613, COLORS["red_line"], dashed=True, label_w=190)

    e.append(text(72, 1000, "重算策略：不复制 map 中间结果；失去 owner 节点后重跑对应 map task。旧 attempt 文件不再出现在 manifest 中。", 15, COLORS["muted"]))
    e.append('</svg>')
    return "\n".join(e) + "\n"


def main():
    target = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else OUT
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(build_svg(), encoding="utf-8")
    print(target)


if __name__ == "__main__":
    main()
