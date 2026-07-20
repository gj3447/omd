"""Shared operator parsing for durable admission policy authority."""

from __future__ import annotations

import math

from .admission import (
    DEFAULT_ADMISSION_AGING_QUANTUM,
    DEFAULT_ADMISSION_MAX_AGE_BOOST,
    MAX_ADMISSION_PRIORITY,
)


DEFAULT_ADMISSION_QUEUE_CAPACITY = 1024


def parse_admission_queue_capacity(raw: str | None) -> int:
    """Parse a bounded repository wait capacity before opening the DB."""
    if raw is None or not raw.strip():
        return DEFAULT_ADMISSION_QUEUE_CAPACITY
    try:
        capacity = int(raw)
    except ValueError as exc:
        raise ValueError(
            "OMD_ADMISSION_QUEUE_CAPACITY must be a non-negative integer"
        ) from exc
    if capacity < 0:
        raise ValueError(
            "OMD_ADMISSION_QUEUE_CAPACITY must be a non-negative integer"
        )
    return capacity


def parse_admission_aging_quantum(raw: str | None) -> float:
    """Parse the positive step width for the durable v2 aging policy."""
    if raw is None or not raw.strip():
        return DEFAULT_ADMISSION_AGING_QUANTUM
    try:
        quantum = float(raw)
    except ValueError as exc:
        raise ValueError(
            "OMD_ADMISSION_AGING_QUANTUM_SECONDS must be a positive finite number"
        ) from exc
    if not math.isfinite(quantum) or quantum <= 0:
        raise ValueError(
            "OMD_ADMISSION_AGING_QUANTUM_SECONDS must be a positive finite number"
        )
    return quantum


def parse_admission_max_age_boost(raw: str | None) -> int:
    """Parse the signed-64-safe saturation ceiling for the durable v2 policy."""
    if raw is None or not raw.strip():
        return DEFAULT_ADMISSION_MAX_AGE_BOOST
    try:
        ceiling = int(raw)
    except ValueError as exc:
        raise ValueError(
            "OMD_ADMISSION_MAX_AGE_BOOST must be a non-negative "
            "signed-64-bit integer"
        ) from exc
    if ceiling < 0 or ceiling > MAX_ADMISSION_PRIORITY:
        raise ValueError(
            "OMD_ADMISSION_MAX_AGE_BOOST must be a non-negative "
            "signed-64-bit integer"
        )
    return ceiling
