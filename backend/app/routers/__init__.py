"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline_router, group_ingest_router

# 专用路由必须排在通用资源路由之前，否则 /api/{module}/{entry_id}
# 会抢先匹配 /api/group-ingest/pending 这类固定子路径。
ROUTERS = [group_ingest_router, pipeline_router, resource]
