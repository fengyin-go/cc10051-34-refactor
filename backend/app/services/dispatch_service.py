"""命名兼容模块：分发登记服务。"""
from __future__ import annotations

from app.services.dispatch_registry import (
    DispatchRegistry,
    dispatch_registry,
    registry,
)

DispatchService = DispatchRegistry
DistributionService = DispatchRegistry

__all__ = [
    "DispatchRegistry",
    "DispatchService",
    "DistributionService",
    "registry",
    "dispatch_registry",
]
