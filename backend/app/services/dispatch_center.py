"""命名兼容模块：分发登记中心。"""
from __future__ import annotations

from app.services.dispatch_registry import (
    DispatchRegistry,
    dispatch_center,
    dispatch_registry,
    registry,
)

DispatchCenter = DispatchRegistry

__all__ = [
    "DispatchRegistry",
    "DispatchCenter",
    "registry",
    "dispatch_registry",
    "dispatch_center",
]
