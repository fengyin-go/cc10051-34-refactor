"""命名兼容模块：分发登记/分配服务。"""
from __future__ import annotations

from app.services.dispatch_registry import DispatchRegistry, registry

DistributionService = DispatchRegistry
DistributionRegistry = DispatchRegistry

__all__ = ["DispatchRegistry", "DistributionService", "DistributionRegistry", "registry"]
