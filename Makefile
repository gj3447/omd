# OMD — 편의 진입점. CI 정본은 .github/workflows/ci.yml(pytest + 하네스 게이트).
PY ?= .venv/bin/python
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
SINCE ?= $(shell git rev-list --max-parents=0 HEAD | head -1)

.PHONY: test harness conformance verify

test:           ## 전체 테스트(하네스 테스트 포함)
	$(PY) -m pytest -q

conformance:    ## P4 스펙↔구현 적합성 게이트(must 회귀 시 NO_GO)
	$(PY) -m omd_server.conformance

harness:        ## P1 우회 + P2 hot + P4 적합성 종합(P1/P2는 정보성)
	$(PY) -m omd_server.harness --repo . --branch $(BRANCH) --since $(SINCE) --max-hot 999

verify: test conformance   ## 게이트 강제(test + 적합성 회귀가드)
	@echo "verify: GREEN (pytest + 적합성 게이트 통과)"
