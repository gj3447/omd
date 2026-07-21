# Option C — admission policy envelope heal-forward (재발방지 근본수정)

> 2026-07-21 PROM(omd-db-fix-prom, 13-agent 적대검증) 산출. **재배포게이트, 핫픽스 아님·HIGH-risk.**
> 배경: coord DB non-canonical 크래시를 Option B(move-aside→re-init)로 즉시 해소했으나, over-strict
> equality 결함은 남음 — 다음 envelope 진화(additive 필드) 시 재발. 이 문서 = 그 근본수정 스펙.

## 결함 (실측 확정)
`core.py:587-608` — 저장 envelope 를 `{aging_quantum,max_age_boost}` **2필드로만** `QueuePolicy` 재구성 후:
- `:599` `canonical_json(durable_policy.envelope) != persisted_policy_envelope` — **현 바이너리 재직렬화(5키)를 저장본(옛 4키)과 byte 비교**. additive descriptive 필드(`priority_domain`, admission.py:85) 1개 추가만으로 실패.
- `:603` `durable_policy.version != persisted_policy_version` — `version=schema/sha256(전체 envelope)`(admission.py:90-92)라 서술필드 1개가 모든 과거 pin 을 orphan.
- `:607` `durable_policy != admission_policy` — **frozen dataclass 2필드 동등성 = 이미 올바른 파라미터 불변식.** `:599`/`:603` 는 이 위 redundant+harmful 층.

`priority_domain` 은 `accepts_base_priority`(admission.py:94-113)가 이미 강제하는 ceiling 규칙의 *문서화*일 뿐 rank 산술 미사용(grep: 문자열이 admission.py:84-85 에만). → additive-진화 취약성의 구조적 원인.

## 올바른 불변식 (고칠 것 / 살릴 것)
- **살릴 것**: `:607` 파라미터 동등성(진짜 정책변경 gate). `accepts_base_priority` ceiling.
- **고칠 것**: `:599`/`:603` 를 **저장 바이트 self-consistency** 로 — 재직렬화-비교 금지, 대신
  `sha256_json(decoded 저장 envelope) == 저장 version 의 hash부분`(저장본이 자기 write 시점과 일관).
  tampered(envelope는 고쳤는데 version 미갱신) → fail-closed. genuine 옛/새 shape → pass.
- **content-address 축소**: `version` 을 authoritative projection `{aging_quantum,max_age_boost,rank-algo-id}`
  에만. `schema`/`priority_domain` = descriptive metadata(호환 allowlist 검증).

## Heal-forward (재발방지 규율)
같은 파라미터 + 옛 shape 감지 시 **제자리 수용에 그치지 말고 현 canonical 로 forward-migrate**(안 그러면
옛 version 태그 orbit 행이 `accepts_base_priority :111 version==self.version` 에서 reject됨):
1. semantic-param 동등(`:607`) gate 통과 시에만 heal.
2. 단일 `BEGIN IMMEDIATE`: meta envelope+version 을 현 canonical 로 UPDATE **+ 옛 version 보유
   orbit 행 policy_version 을 신 version 으로 repin**(둘 다 같은 tx — 부분수정 금지).
3. **파라미터 변경(예: aging_quantum 60→30)은 절대 heal 금지 = fail-closed**(이미 큐잉된 v2 행 조용한 재해석 차단).
4. `store.py`: `MIGRATABLE_ENVELOPE_VERSIONS`(=`KNOWN_ENVELOPE_PREDECESSORS`) allowlist(`store.py:24
   MIGRATABLE_SCHEMA_VERSIONS` 미러) + `_migrate()` 에 **meta-VALUE 마이그레이션 채널** 신설(현재
   `_MIGRATIONS` 는 column-ADD 전용). idempotent.

## 테스트 (TDD)
- `tests/test_scheduler_m1_aging.py`: **옛-shape 픽스처 DB**(4키·유효파라미터·descriptive-only 차이 +
  옛 version 태그 v2 orbit 행) seed → Coordinator init **clean upgrade** assert(meta+orbit 모두 신 version).
- **음성오라클(fail-closed teeth)**: `aging_quantum 60→30` 저장본 → init **반드시 실패**(heal 금지).
- 회귀: 현재 `non-canonical` 경로 커버리지 0 → 명시 추가. `test_scheduler_m1_aging.py:741` 은 current shape 만 pin.

## 검증
`dev123` 하네스로 착지(coord DB 이제 작동 → coordination=HELD 완전 사이클). 완화는 **additive-only** —
`git diff`로 파라미터 gate 보존 눈검증 필수.

## 정직 caveat
- "파라미터 2개만 authoritative" 는 rank 산술엔 참이나 version 정체성엔 아님 → projection-version 으로 함께 처리.
- HIGH-risk(정지 hash 행 mis-rank)는 현 DB 엔 nil(전부 terminal)이나 live DB 엔 material → 완화 additive-only 로 국한하는 이유.
