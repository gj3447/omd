# 우선권 증명 (Priority proof)

이 폴더는 **이 저장소의 코드가 특정 시각 이전에 존재했다**는 것을 *신뢰 없이* 증명한다.

- 저자: **Gyeongjun Ra (라경준)** — METAHUMOTONIC / SYMPOSIUM
- 앵커: **Bitcoin block 958111**, 블록 시각 **2026-07-15T06:58:20Z**

## 왜 git 시각이 아니라 이것인가

git 의 커밋 시각은 `GIT_AUTHOR_DATE` 로 얼마든지 위조된다. GitHub 의 push 시각은 GitHub 을
믿어야 한다. 아래 증명은 **아무도 믿을 필요가 없다** — 비트코인 블록만 보면 된다.

## 증명 사슬

```
priority-20260715.manifest       ← HEAD 커밋 sha + 추적 파일 전체의 sha256 을 고정
        │  (sha256)
        ▼
priority-20260715.manifest.ots   ← 그 해시를 Bitcoin block 958111 에 고정
```

커밋 sha 는 트리 전체의 머클 루트다. 파일 하나만 달라도 sha 가 달라진다 ⇒ 사후 조작 불가.

## 직접 검증하는 법

```bash
pip install opentimestamps-client

# 1) 매니페스트가 블록 958111 에 앵커됐음을 확인
ots info PROVENANCE/priority-20260715.manifest.ots | grep BitcoinBlockHeaderAttestation

# 2) 비트코인 노드가 있으면 완전 검증
ots verify PROVENANCE/priority-20260715.manifest.ots

# 3) 노드 없이도 확인 가능: ots info 가 출력하는 merkle root 를
#    공개 익스플로러의 블록 958111 merkle_root 와 대조한다
curl -s https://blockstream.info/api/block/$(curl -s https://blockstream.info/api/block-height/958111) | jq -r .merkle_root
# → 0242581b63900665a4a682af584bebb86e05021d5f6921015e6ce518e2d4bea0

# 4) 매니페스트가 가리키는 커밋이 실재하는지
grep '^commit' PROVENANCE/priority-20260715.manifest
git cat-file -p <그 sha>
```

## 중요한 성질

**스탬프 시점에 이 저장소는 아직 비공개였다.** OpenTimestamps 는 캘린더 서버에 *해시만*
제출하므로 내용이 새지 않는다. 따라서 이 증명은 *"공개해서 남들이 볼 수 있게 되기 전에
이미 이 코드를 갖고 있었다"* 를 성립시킨다 — 우선권 증명으로서 가장 강한 형태다.

생성 도구: [`SYMPOSIUM/scripts/priority_stamp.sh`](https://github.com/gj3447/symposium) (비공개 모노레포)
