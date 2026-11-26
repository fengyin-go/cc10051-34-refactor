"""命名兼容模块：统一分发登记处的另一种常见英文命名。"""
from __future__ import annotations

from app.services.dispatch_registry import (
    DispatchRegistry,
    dispatch_center,
    dispatch_registry,
    distribution_registry,
    registry,
)

DistributionRegistry = DispatchRegistry
DispatchCenter = DispatchRegistry

__all__ = [
    "DispatchRegistry",
    "DistributionRegistry",
    "DispatchCenter",
    "registry",
    "dispatch_registry",
    "distribution_registry",
    "dispatch_center",
]
