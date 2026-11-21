from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from .database import Session
from .security import current, allow
from .claim_service import audit
from .shadow_models import ShadowBatch, ShadowItem
from .shadow_schemas import ShadowBatchIn
from . import shadow_service as shadow
import os
from redis import Redis

router = APIRouter()
r = Redis.from_url(os.getenv('REDIS_URL', 'redis://redis:6379/0'), decode_responses=True)


@router.get('/shadow-batches')
def batches(u=Depends(current)):
    with Session() as s:
        return shadow.list_batches(s)


@router.get('/shadow-batches/{id}')
def batch_detail(id: int, u=Depends(current)):
    with Session() as s:
        b = s.get(ShadowBatch, id)
        if not b:
            raise HTTPException(404, '影子复评批次不存在')
        return shadow.batch_view(s, b)


@router.post('/shadow-batches')
def create_batch(d: ShadowBatchIn, u=Depends(current)):
    # 仅主管可在分流页面发起影子复评
    allow(u, 'admin')
    with Session.begin() as s:
        view = shadow.create_batch(s, u, d.code)
        bid = view['id']
        total = view['total_count']
    # 每单一条复评消息；消息重复也只会被 worker 幂等跳过，不会重复计入
    for _ in range(total):
        r.rpush('shadow-risk', bid)
    return view


@router.post('/shadow-batches/{id}/retry')
def retry_batch(id: int, u=Depends(current)):
    # 复评中断/单条失败后恢复：接着算原名单，只补投待办项数量的消息
    allow(u, 'admin', 'assessor')
    with Session.begin() as s:
        b = s.get(ShadowBatch, id)
        if not b:
            raise HTTPException(404, '影子复评批次不存在')
        if b.state == 'published':
            raise HTTPException(409, '批次已公布，无需重试')
        todo = s.scalars(select(ShadowItem).where(
            ShadowItem.batch_id == id,
            ShadowItem.state.in_(['pending', 'failed']))).all()
        remaining = len(todo)
        audit(s, u, 'shadow_batch_retry', f'{b.code}:待办{remaining}单')
    if remaining:
        for _ in range(remaining):
            r.rpush('shadow-risk', id)
    with Session() as s:
        return shadow.batch_view(s, s.get(ShadowBatch, id))
