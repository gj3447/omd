"""OMD CLI — MCP 툴과 동일 동사 (얇은 클라이언트). `omd <verb> ...`."""

from __future__ import annotations

import argparse
import json

from .core import Coordinator


def main(argv=None):
    p = argparse.ArgumentParser(prog="omd", description="OMD 입체운행물방울 군단장 CLI")
    p.add_argument("--db", default="omd.db")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("claim"); c.add_argument("agent"); c.add_argument("paths", nargs="+")
    c.add_argument("--mode", default="write"); c.add_argument("--ttl", type=float, default=600.0)
    c.add_argument("--task"); c.add_argument("--priority", type=int, default=0)

    for verb in ("release", "renew"):
        s = sub.add_parser(verb)
        s.add_argument("orbit_id"); s.add_argument("agent"); s.add_argument("fence", type=int)
        if verb == "renew":
            s.add_argument("--ttl", type=float, default=600.0)

    bl = sub.add_parser("bail"); bl.add_argument("agent")

    d = sub.add_parser("declare"); d.add_argument("task"); d.add_argument("--name", default="")
    d.add_argument("--writes", nargs="*", default=[]); d.add_argument("--reads", nargs="*", default=[])
    d.add_argument("--deps", nargs="*", default=[]); d.add_argument("--priority", type=int, default=0)

    # depend: task가 after 다음에 오도록 의존 엣지 추가 — 사이클이면 거부(P0-10).
    dep = sub.add_parser("depend"); dep.add_argument("task"); dep.add_argument("after")

    n = sub.add_parser("next"); n.add_argument("agent")
    st = sub.add_parser("start"); st.add_argument("task"); st.add_argument("agent")
    fi = sub.add_parser("finish"); fi.add_argument("task")
    cn = sub.add_parser("connect"); cn.add_argument("task")
    cn.add_argument("--agent"); cn.add_argument("--fence", type=int)
    sub.add_parser("sweep")
    sub.add_parser("status")

    a = p.parse_args(argv)
    omd = Coordinator(a.db)
    out = {
        "claim": lambda: omd.claim(a.agent, a.paths, a.mode, ttl=a.ttl, task_id=a.task,
                                   priority=a.priority),
        "release": lambda: omd.release(a.orbit_id, a.agent, a.fence),
        "renew": lambda: omd.renew(a.orbit_id, a.agent, a.fence, a.ttl),
        "bail": lambda: omd.bail(a.agent),
        "declare": lambda: omd.declare(a.task, name=a.name, writes=a.writes,
                                       reads=a.reads, deps=a.deps, priority=a.priority),
        "depend": lambda: omd.depend(a.task, a.after),
        "next": lambda: omd.next_task(a.agent),
        "start": lambda: omd.start(a.task, a.agent),
        "finish": lambda: omd.finish(a.task),
        "connect": lambda: omd.connect(a.task, getattr(a, "agent", None),
                                       getattr(a, "fence", None)),
        "sweep": lambda: omd.sweep(),
        "status": lambda: omd.status(),
    }[a.cmd]()
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
