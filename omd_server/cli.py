"""OMD CLI — MCP 툴과 동일 동사 (얇은 클라이언트). `omd <verb> ...`."""

from __future__ import annotations

import argparse
import json
import os

from .admission_config import (
    parse_admission_aging_quantum,
    parse_admission_max_age_boost,
    parse_admission_queue_capacity,
)
from .core import Coordinator


def main(argv=None):
    p = argparse.ArgumentParser(prog="omd", description="OMD 입체운행물방울 군단장 CLI")
    p.add_argument("--db", default="omd.db")
    p.add_argument("--admission-queue-capacity")
    p.add_argument("--admission-aging-quantum-seconds")
    p.add_argument("--admission-max-age-boost")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("claim"); c.add_argument("agent"); c.add_argument("paths", nargs="+")
    c.add_argument("--mode", default="write"); c.add_argument("--ttl", type=float, default=600.0)
    c.add_argument("--task"); c.add_argument("--priority", type=int, default=0)
    c.add_argument("--request-id"); c.add_argument("--bail-epoch", type=int)

    rc = sub.add_parser("rollover-claim")
    rc.add_argument("prior_orbit_id"); rc.add_argument("agent")
    rc.add_argument("expected_generation", type=int)
    rc.add_argument("--bail-epoch", type=int, required=True)
    rc.add_argument("--request-id", required=True)

    for verb in ("release", "renew"):
        s = sub.add_parser(verb)
        s.add_argument("orbit_id"); s.add_argument("agent"); s.add_argument("fence", type=int)
        s.add_argument("--request-id"); s.add_argument("--bail-epoch", type=int)
        if verb == "renew":
            s.add_argument("--ttl", type=float, default=600.0)

    cw = sub.add_parser("cancel-wait")
    cw.add_argument("orbit_id"); cw.add_argument("agent")
    cw.add_argument("request_generation", type=int)
    cw.add_argument("--bail-epoch", type=int, required=True)
    cw.add_argument("--request-id")

    # §D12: rebase/재독 후 task 의 read-set 재앵커(유령 읽기 청산).
    rr = sub.add_parser("read-refresh")
    rr.add_argument("task"); rr.add_argument("agent"); rr.add_argument("fence", type=int)
    rr.add_argument("--request-id"); rr.add_argument("--bail-epoch", type=int)

    bl = sub.add_parser("bail"); bl.add_argument("agent"); bl.add_argument("--request-id")

    d = sub.add_parser("declare"); d.add_argument("task"); d.add_argument("--name", default="")
    d.add_argument("--writes", nargs="*", default=[]); d.add_argument("--reads", nargs="*", default=[])
    d.add_argument("--shared", nargs="*", default=[]); d.add_argument("--deps", nargs="*", default=[])
    d.add_argument("--priority", type=int, default=0)

    # depend: task가 after 다음에 오도록 의존 엣지 추가 — 사이클이면 거부(P0-10).
    dep = sub.add_parser("depend"); dep.add_argument("task"); dep.add_argument("after")

    n = sub.add_parser("next"); n.add_argument("agent")
    tc = sub.add_parser("task-conditions"); tc.add_argument("task")
    st = sub.add_parser("start"); st.add_argument("task"); st.add_argument("agent")
    st.add_argument("--request-id"); st.add_argument("--bail-epoch", type=int)
    bg = sub.add_parser("begin"); bg.add_argument("task"); bg.add_argument("agent")
    bg.add_argument("--writes", nargs="+", required=True)
    bg.add_argument("--reads", nargs="*", default=[]); bg.add_argument("--shared", nargs="*", default=[])
    bg.add_argument("--deps", nargs="*", default=[]); bg.add_argument("--priority", type=int, default=0)
    bg.add_argument("--name", default=""); bg.add_argument("--ttl", type=float, default=600.0)
    bg.add_argument("--liveness-ttl", type=float)
    bg.add_argument("--request-id"); bg.add_argument("--bail-epoch", type=int)
    cm = sub.add_parser("commit"); cm.add_argument("task"); cm.add_argument("msg")
    cm.add_argument("--agent"); cm.add_argument("--fence", type=int)
    cm.add_argument("--request-id"); cm.add_argument("--bail-epoch", type=int)
    fi = sub.add_parser("finish"); fi.add_argument("task")
    fi.add_argument("--agent"); fi.add_argument("--fence", type=int)
    fi.add_argument("--request-id"); fi.add_argument("--bail-epoch", type=int)
    cn = sub.add_parser("connect"); cn.add_argument("task")
    cn.add_argument("--agent"); cn.add_argument("--fence", type=int)
    cn.add_argument("--push")
    cn.add_argument("--request-id"); cn.add_argument("--bail-epoch", type=int)
    ct = sub.add_parser("complete-task"); ct.add_argument("task"); ct.add_argument("msg", nargs="?")
    ct.add_argument("--agent"); ct.add_argument("--fence", type=int); ct.add_argument("--push")
    ct.add_argument("--request-id"); ct.add_argument("--bail-epoch", type=int)
    ca = sub.add_parser("cancel"); ca.add_argument("task"); ca.add_argument("--reason", default="")
    ca.add_argument("--request-id")
    hb = sub.add_parser("heartbeat"); hb.add_argument("agent"); hb.add_argument("--ttl", type=float)

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

    # D5 배리어: 세대-스탬프 응결 랑데부. 멤버십=task 집합, 사망/타임아웃→BROKEN.
    bd = sub.add_parser("barrier-declare"); bd.add_argument("name")
    bd.add_argument("task_ids", nargs="+"); bd.add_argument("--kind", default="connect")
    bd.add_argument("--policy", default="break"); bd.add_argument("--timeout", type=float)
    ba = sub.add_parser("barrier-arrive"); ba.add_argument("name"); ba.add_argument("agent")
    ba.add_argument("task"); ba.add_argument("--fence", type=int)
    ba.add_argument("--request-id"); ba.add_argument("--bail-epoch", type=int)
    bab = sub.add_parser("barrier-abort"); bab.add_argument("name"); bab.add_argument("--agent")
    bab.add_argument("--request-id"); bab.add_argument("--bail-epoch", type=int)
    bs = sub.add_parser("barrier-status"); bs.add_argument("name")
    bc = sub.add_parser("barrier-consume"); bc.add_argument("name"); bc.add_argument("--agent")
    bc.add_argument("--request-id"); bc.add_argument("--bail-epoch", type=int)

    sub.add_parser("sweep")
    sub.add_parser("status")

    a = p.parse_args(argv)
    capacity = parse_admission_queue_capacity(
        a.admission_queue_capacity
        if a.admission_queue_capacity is not None
        else os.environ.get("OMD_ADMISSION_QUEUE_CAPACITY")
    )
    aging_quantum = parse_admission_aging_quantum(
        a.admission_aging_quantum_seconds
        if a.admission_aging_quantum_seconds is not None
        else os.environ.get("OMD_ADMISSION_AGING_QUANTUM_SECONDS")
    )
    max_age_boost = parse_admission_max_age_boost(
        a.admission_max_age_boost
        if a.admission_max_age_boost is not None
        else os.environ.get("OMD_ADMISSION_MAX_AGE_BOOST")
    )
    omd = Coordinator(
        a.db,
        # One-shot commands own no background lifecycle. Every verb already
        # performs inline reconciliation, and cleanup happens before resign.
        sweep_interval=None,
        autostart_background_workers=False,
        admission_queue_capacity=capacity,
        admission_aging_quantum=aging_quantum,
        admission_max_age_boost=max_age_boost,
    )
    rid = lambda: getattr(a, "request_id", None)
    be = lambda: getattr(a, "bail_epoch", None)
    try:
        out = {
            "claim": lambda: omd.claim(a.agent, a.paths, a.mode, ttl=a.ttl, task_id=a.task,
                                       priority=a.priority, request_id=rid(), bail_epoch=be()),
            "rollover-claim": lambda: omd.rollover_claim(
                a.prior_orbit_id,
                a.agent,
                a.expected_generation,
                bail_epoch=a.bail_epoch,
                request_id=a.request_id,
            ),
            "release": lambda: omd.release(a.orbit_id, a.agent, a.fence,
                                           request_id=rid(), bail_epoch=be()),
            "renew": lambda: omd.renew(a.orbit_id, a.agent, a.fence, a.ttl,
                                       request_id=rid(), bail_epoch=be()),
            "cancel-wait": lambda: omd.cancel_wait(
                a.orbit_id, a.agent, a.request_generation,
                bail_epoch=a.bail_epoch, request_id=rid(),
            ),
            "read-refresh": lambda: omd.read_refresh(a.task, a.agent, a.fence,
                                                     request_id=rid(), bail_epoch=be()),
            "bail": lambda: omd.bail(a.agent, request_id=rid()),
            "declare": lambda: omd.declare(a.task, name=a.name, writes=a.writes,
                                           reads=a.reads, deps=a.deps, priority=a.priority,
                                           shared=a.shared),
            "depend": lambda: omd.depend(a.task, a.after),
            "next": lambda: omd.next_task(a.agent),
            "task-conditions": lambda: omd.task_conditions(a.task),
            "start": lambda: omd.start(a.task, a.agent, request_id=rid(), bail_epoch=be()),
            "begin": lambda: omd.begin(
                a.task, a.agent, a.writes, reads=a.reads, shared=a.shared, deps=a.deps,
                priority=a.priority, name=a.name, ttl=a.ttl, liveness_ttl=a.liveness_ttl,
                request_id=rid(), bail_epoch=be(),
            ),
            "commit": lambda: omd.commit(a.task, a.msg, getattr(a, "agent", None),
                                         getattr(a, "fence", None),
                                         request_id=rid(), bail_epoch=be()),
            "finish": lambda: omd.finish(a.task, getattr(a, "agent", None),
                                         getattr(a, "fence", None),
                                         request_id=rid(), bail_epoch=be()),
            "connect": lambda: omd.connect(a.task, getattr(a, "agent", None),
                                           getattr(a, "fence", None), push=a.push,
                                           request_id=rid(), bail_epoch=be()),
            "complete-task": lambda: omd.complete_task(
                a.task, a.msg, getattr(a, "agent", None), getattr(a, "fence", None),
                push=a.push, request_id=rid(), bail_epoch=be(),
            ),
            "cancel": lambda: omd.cancel(a.task, reason=a.reason, request_id=rid()),
            "heartbeat": lambda: omd.heartbeat(a.agent, ttl=a.ttl),
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
            "barrier-declare": lambda: omd.barrier_declare(a.name, a.task_ids, kind=a.kind,
                                                           policy=a.policy, timeout=a.timeout),
            "barrier-arrive": lambda: omd.barrier_arrive(a.name, a.agent, a.task,
                                                         fence=getattr(a, "fence", None),
                                                         request_id=rid(), bail_epoch=be()),
            "barrier-abort": lambda: omd.barrier_abort(a.name, getattr(a, "agent", None),
                                                       request_id=rid(), bail_epoch=be()),
            "barrier-status": lambda: omd.barrier_status(a.name),
            "barrier-consume": lambda: omd.barrier_consume(
                a.name, getattr(a, "agent", None), request_id=rid(), bail_epoch=be(),
            ),
            "sweep": lambda: omd.sweep(),
            "status": lambda: omd.status(),
        }[a.cmd]()
        print(json.dumps(out, ensure_ascii=False, indent=2))
    finally:
        # Join every accepted writer/effect before handing leadership off.
        # If close fails, resign must not expose the DB to a second writer.
        omd.close()
        omd.resign()


if __name__ == "__main__":
    main()
