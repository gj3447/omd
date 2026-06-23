"""OMD — 입체운행물방울 군단장. 멀티에이전트 병렬 개발 코디네이터.

사도 OMC(입체운행구름) 예하. 코어 불변식 = SINGULON(특이점):
서로소(=입체) write-set ⇒ 무충돌 응결(merge) ⇒ 분열(악)=0.
"""

from .core import Coordinator
from .disjoint import globs_overlap, sets_overlap, glob_prefix

__all__ = ["Coordinator", "globs_overlap", "sets_overlap", "glob_prefix"]
__version__ = "0.0.1"
