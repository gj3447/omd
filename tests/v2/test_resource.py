from __future__ import annotations

import unicodedata

import pytest

from omd_server.v2.errors import ErrorCode, ResourceValidationError
from omd_server.v2.resource import (
    CaseMode,
    RepoPolicy,
    SelectorKind,
    canonicalize_resource,
    overlaps,
)


DOMAIN = "symposium"


def policy(
    *,
    case_mode: CaseMode = CaseMode.SENSITIVE,
    forbidden_symlink_prefixes: tuple[tuple[str, ...], ...] = (),
) -> RepoPolicy:
    return RepoPolicy(
        repo_id="omd",
        case_mode=case_mode,
        forbidden_symlink_prefixes=forbidden_symlink_prefixes,
    )


def resource(
    path: str,
    *,
    selector: SelectorKind = SelectorKind.EXACT,
    repo_policy: RepoPolicy | None = None,
):
    return canonicalize_resource(
        domain_id=DOMAIN,
        policy=repo_policy or policy(),
        raw_path=path,
        selector=selector,
    )


def test_case_insensitive_repository_has_one_resource_identity() -> None:
    repo_policy = policy(case_mode=CaseMode.INSENSITIVE)

    assert resource("Src/Foo.py", repo_policy=repo_policy) == resource(
        "src/foo.PY", repo_policy=repo_policy
    )


def test_unicode_paths_are_normalized_to_nfc() -> None:
    nfc = "docs/caf\u00e9.md"
    nfd = unicodedata.normalize("NFD", nfc)

    assert resource(nfc) == resource(nfd)
    assert resource(nfd).segments[-1] == "caf\u00e9.md"


def test_dotfiles_are_not_stripped() -> None:
    assert resource(".env") != resource("env")
    assert resource("config/.env").segments == ("config", ".env")


@pytest.mark.parametrize(
    ("raw_path", "code"),
    [
        ("/absolute", ErrorCode.ABSOLUTE_PATH),
        ("../escape", ErrorCode.PARENT_TRAVERSAL),
        ("a/../escape", ErrorCode.PARENT_TRAVERSAL),
        ("./relative", ErrorCode.CURRENT_DIRECTORY_SEGMENT),
        ("a\\b", ErrorCode.NON_POSIX_SEPARATOR),
        ("src/*.py", ErrorCode.UNSUPPORTED_SELECTOR),
        ("src/a?.py", ErrorCode.UNSUPPORTED_SELECTOR),
        ("src/[ab].py", ErrorCode.UNSUPPORTED_SELECTOR),
        ("a//b", ErrorCode.EMPTY_PATH_SEGMENT),
        ("a/", ErrorCode.EMPTY_PATH_SEGMENT),
        ("a\x00b", ErrorCode.INVALID_RESOURCE),
        ("", ErrorCode.INVALID_RESOURCE),
    ],
)
def test_unsafe_or_ambiguous_paths_are_rejected(
    raw_path: str, code: ErrorCode
) -> None:
    with pytest.raises(ResourceValidationError) as caught:
        resource(raw_path)

    assert caught.value.error.code is code


def test_registered_symlink_boundary_is_rejected() -> None:
    repo_policy = policy(forbidden_symlink_prefixes=(("vendor", "external"),))

    with pytest.raises(ResourceValidationError) as caught:
        resource("vendor/external/pkg/file.py", repo_policy=repo_policy)

    assert caught.value.error.code is ErrorCode.SYMLINK_BOUNDARY


def test_overlap_uses_segments_not_string_prefixes() -> None:
    src_a = resource("src/a", selector=SelectorKind.SUBTREE)
    src_ab = resource("src/ab/file.py")
    src_a_child = resource("src/a/file.py")

    assert not overlaps(src_a, src_ab)
    assert overlaps(src_a, src_a_child)


def test_resources_from_different_domains_or_repositories_do_not_overlap() -> None:
    left = resource("src/a.py")
    other_domain = canonicalize_resource(
        domain_id="other",
        policy=policy(),
        raw_path="src/a.py",
        selector=SelectorKind.EXACT,
    )
    other_repo = canonicalize_resource(
        domain_id=DOMAIN,
        policy=RepoPolicy(repo_id="other-repo"),
        raw_path="src/a.py",
        selector=SelectorKind.EXACT,
    )

    assert not overlaps(left, other_domain)
    assert not overlaps(left, other_repo)
