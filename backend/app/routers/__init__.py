"""业务路由汇总：分发专用路由先注册，避免被通用 /api/{module} 动态路由匹配。"""
from __future__ import annotations

from app.routers import dispatch, resource

ROUTERS = [dispatch, resource]
