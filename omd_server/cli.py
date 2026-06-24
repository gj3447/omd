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
    c.add_argument("--request-id"); c.add_argument("--bail-epoch", type=int)

    for verb in ("release", "renew"):
        s = sub.add_parser(verb)
        s.add_argument("orbit_id"); s.add_argument("agent"); s.add_argument("fence", type=int)
        s.add_argument("--request-id"); s.add_argument("--bail-epoch", type=int)
        if verb == "renew":
            s.add_argument("--ttl", type=float, default=600.0)

    bl = sub.add_parser("bail"); bl.add_argument("agent"); bl.add_argument("--request-id")

    d = sub.add_parser("declare"); d.add_argument("task"); d.add_argument("--name", default="")
    d.add_argument("--writes", nargs="*", default=[]); d.add_argument("--reads", nargs="*", default=[])
    d.add_argument("--deps", nargs="*", default=[]); d.add_argument("--priority", type=int, default=0)

    # depend: task가 after 다음에 오도록 의존 엣지 추가 — 사이클이면 거부(P0-10).
    dep = sub.add_parser("depend"); dep.add_argument("task"); dep.add_argument("after")

    n = sub.add_parser("next"); n.add_argument("agent")
    st = sub.add_parser("start"); st.add_argument("task"); st.add_argument("agent")
    st.add_argument("--request-id"); st.add_argument("--bail-epoch", type=int)
    cm = sub.add_parser("commit"); cm.add_argument("task"); cm.add_argument("msg")
    cm.add_argument("--agent"); cm.add_argument("--fence", type=int)
    cm.add_argument("--request-id"); cm.add_argument("--bail-epoch", type=int)
    fi = sub.add_parser("finish"); fi.add_argument("task")
    fi.add_argument("--agent"); fi.add_argument("--fence", type=int)
    fi.add_argument("--request-id"); fi.add_argument("--bail-epoch", type=int)
    cn = sub.add_parser("connect"); cn.add_argument("task")
    cn.add_argument("--agent"); cn.add_argument("--fence", type=int)
    cn.add_argument("--request-id"); cn.add_argument("--bail-epoch", type=int)
    hb = sub.add_parser("heartbeat"); hb.add_argument("agent")

    # D4 세마포어: permit=lease, 가용=max−count(ACTIVE), no-overtaking.
    sd = sub.add_parser("sem-declare"); sd.add_argument("sem")
    sd.add_argument("max_permits", type=int)
    aq = sub.add_parser("acquire"); aq.add_argument("agent"); aq.add_argument("sem")
    aq.add_argument("--ttl", type=float, default=300.0)
    aq.add_argument("--no-wait", action="store_true"); aq.add_argument("--priority", type=int, default=0)
    aq.add_argument("--request-id"); aq.add_argument("--bail-epoch", type=int)
    ap = sub.add_parser("acquire-poll"); ap.add_argument("waiter_id")
    sr = sub.add_parser("sem-release"); sr.add_argument("permit_id"); sr.add_argument("agent")
    sr.add_argument("fence", type=int)
    sr.add_argument("--request-id"); sr.add_argument("--bail-epoch", type=int)
    ss = sub.add_parser("sem-status"); ss.add_argument("sem")

    # D3 플래그: EPHEMERAL(소유 신호) vs LATCH(영속·단조) + register→poll wait.
    fs = sub.add_parser("flag-set"); fs.add_argument("key"); fs.add_argument("value")
    fs.add_argument("--agent"); fs.add_argument("--type", dest="flag_type", default="LATCH")
    fs.add_argument("--ttl", type=float); fs.add_argument("--request-id")
    fs.add_argument("--bail-epoch", type=int)
    fc = sub.add_parser("flag-clear"); fc.add_argument("key"); fc.add_argument("--agent")
    fc.add_argument("--request-id"); fc.add_argument("--bail-epoch", type=int)
    fg = sub.add_parser("flag-get"); fg.add_argument("key")
    fw = sub.add_parser("flag-wait"); fw.add_argument("key"); fw.add_argument("want")
    fw.add_argument("timeout", type=float); fw.add_argument("--agent")
    fp = sub.add_parser("flag-wait-poll"); fp.add_argument("waiter_id")

    sub.add_parser("sweep")
    sub.add_parser("status")

    a = p.parse_args(argv)
    omd = Coordinator(a.db)
    rid = lambda: getattr(a, "request_id", None)
    be = lambda: getattr(a, "bail_epoch", None)
    out = {
        "claim": lambda: omd.claim(a.agent, a.paths, a.mode, ttl=a.ttl, task_id=a.task,
                                   priority=a.priority, request_id=rid(), bail_epoch=be()),
        "release": lambda: omd.release(a.orbit_id, a.agent, a.fence,
                                       request_id=rid(), bail_epoch=be()),
        "renew": lambda: omd.renew(a.orbit_id, a.agent, a.fence, a.ttl,
                                   request_id=rid(), bail_epoch=be()),
        "bail": lambda: omd.bail(a.agent, request_id=rid()),
        "declare": lambda: omd.declare(a.task, name=a.name, writes=a.writes,
                                       reads=a.reads, deps=a.deps, priority=a.priority),
        "depend": lambda: omd.depend(a.task, a.after),
        "next": lambda: omd.next_task(a.agent),
        "start": lambda: omd.start(a.task, a.agent, request_id=rid(), bail_epoch=be()),
        "commit": lambda: omd.commit(a.task, a.msg, getattr(a, "agent", None),
                                     getattr(a, "fence", None),
                                     request_id=rid(), bail_epoch=be()),
        "finish": lambda: omd.finish(a.task, getattr(a, "agent", None),
                                     getattr(a, "fence", None),
                                     request_id=rid(), bail_epoch=be()),
        "connect": lambda: omd.connect(a.task, getattr(a, "agent", None),
                                       getattr(a, "fence", None),
                                       request_id=rid(), bail_epoch=be()),
        "heartbeat": lambda: omd.heartbeat(a.agent),
        "sem-declare": lambda: omd.sem_declare(a.sem, a.max_permits),
        "acquire": lambda: omd.acquire(a.agent, a.sem, ttl=a.ttl, no_wait=a.no_wait,
                                       priority=a.priority, request_id=rid(), bail_epoch=be()),
        "acquire-poll": lambda: omd.acquire_poll(a.waiter_id),
        "sem-release": lambda: omd.sem_release(a.permit_id, a.agent, a.fence,
                                               request_id=rid(), bail_epoch=be()),
        "sem-status": lambda: omd.sem_status(a.sem),
        "flag-set": lambda: omd.flag_set(a.key, a.value, getattr(a, "agent", None),
                                         flag_type=a.flag_type, ttl=a.ttl,
                                         request_id=rid(), bail_epoch=be()),
        "flag-clear": lambda: omd.flag_clear(a.key, getattr(a, "agent", None),
                                             request_id=rid(), bail_epoch=be()),
        "flag-get": lambda: omd.flag_get(a.key),
        "flag-wait": lambda: omd.flag_wait(a.key, a.want, a.timeout,
                                           getattr(a, "agent", None)),
        "flag-wait-poll": lambda: omd.flag_wait_poll(a.waiter_id),
        "sweep": lambda: omd.sweep(),
        "status": lambda: omd.status(),
    }[a.cmd]()
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
