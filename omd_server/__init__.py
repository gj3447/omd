"""OMD — 입체운행물방울 군단장. 멀티에이전트 병렬 개발 코디네이터.

사도 OMC(입체운행구름) 예하. 코어 불변식 = SINGULON(특이점):
서로소(=입체) write-set ⇒ 무충돌 응결(merge) ⇒ 분열(악)=0.
"""

_LAZY_EXPORTS = {
    "Coordinator": ("core", "Coordinator"),
    "CoordinatorConflict": ("core", "CoordinatorConflict"),
    "Emitter": ("events", "Emitter"),
    "globs_overlap": ("disjoint", "globs_overlap"),
    "sets_overlap": ("disjoint", "sets_overlap"),
    "glob_prefix": ("disjoint", "glob_prefix"),
}

__all__ = list(_LAZY_EXPORTS)
__version__ = "0.1.0a1"


def __getattr__(name: str):
    """Load the legacy public API only when a caller asks for it.

    Keeping these imports lazy lets the lease-only v2 profile start without
    importing the legacy coordinator, FSM, or Git integration graph.
    """

    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from importlib import import_module

    module_name, attribute_name = target
    value = getattr(import_module(f".{module_name}", __name__), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
