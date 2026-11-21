import json
from datetime import datetime, timezone
from sqlalchemy import select, func
from fastapi import HTTPException
from .models import Claim, Audit
from .shadow_models import ShadowBatch, ShadowItem
from .risk_model import HIGH_RISK_THRESHOLD
from .claim_service import audit, obj


def now():
    return datetime.now(timezone.utc).isoformat()


def tier_of(risk, threshold=HIGH_RISK_THRESHOLD):
    # 拆检档：高风险（需完整证据）/ 常规
    if risk is None:
        return ''
    return 'high' if risk >= threshold else 'standard'


def tier_label(tier):
    return {'high': '高风险拆检档', 'standard': '常规拆检档', '': '无评分'}.get(tier, tier)


def _retained_input(claim):
    # 今后评分在 assessment_note 留存原输入；旧单是纯指标/纯文本，解析不到即视为缺输入
    try:
        payload = json.loads(claim.assessment_note) if claim.assessment_note else None
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or 'input' not in payload:
        return None
    snap = payload['input']
    required = ('machine_model', 'part', 'hours_at_failure', 'age_days', 'leak_flag')
    if not all(k in snap for k in required):
        return None
    return snap


def _input_snapshot(s, claim):
    # 沿用每单原评分的输入：优先读取评分时留存的输入快照
    snap = _retained_input(claim)
    if snap is not None:
        return snap
    # 旧单没有留存输入：领域表当前值无法证明与原评分输入一致，记为无法比较
    return None


def create_batch(s, u, code=None):
    running = s.scalar(select(ShadowBatch).where(ShadowBatch.state == 'running').with_for_update())
    if running:
        raise HTTPException(409, f'已有进行中的影子复评批次 {running.code}，须等其算齐公布后再发起')
    # 待签单：非历史、尚未结案（未签赔/拒赔）；过保单没有分流线，不进复评名单
    pending = s.scalars(select(Claim).where(
        Claim.historical == False,  # noqa: E712
        Claim.status.in_(['submitted', 'queued', 'assessed', 'failed'])
    ).order_by(Claim.id)).all()
    if not pending:
        raise HTTPException(409, '当前没有待签单可纳入影子复评')
    batch_no = (s.scalar(select(func.max(ShadowBatch.id))) or 0) + 1
    batch = ShadowBatch(
        code=code or f'SHADOW-{batch_no:03}', state='running',
        threshold=HIGH_RISK_THRESHOLD, created_by=u['email'], created_at=now())
    s.add(batch); s.flush()
    total = 0
    for c in pending:
        snap = _input_snapshot(s, c)
        available = snap is not None and c.risk is not None
        original_tier = tier_of(c.risk, HIGH_RISK_THRESHOLD) if c.risk is not None else ''
        item = ShadowItem(
            batch_id=batch.id, claim_id=c.id, claim_code=c.code,
            state='pending' if available else 'incomparable',
            result='' if available else 'incomparable',
            original_risk=c.risk, original_tier=original_tier,
            input_snapshot=json.dumps(snap, ensure_ascii=False) if snap else '',
            input_available=available,
            note='' if available else '旧单缺少原评分输入或原评分，记为无法比较')
        s.add(item)
        total += 1
        if not available:
            batch.incomparable_count += 1
    batch.total_count = total
    audit(s, u, 'shadow_batch_created', f'{batch.code}:{total}单')
    s.flush()
    return batch_view(s, batch)


def compare_tiers(original_tier, shadow_tier):
    if original_tier == shadow_tier:
        return 'unchanged'
    if original_tier == 'standard' and shadow_tier == 'high':
        return 'escalate'
    if original_tier == 'high' and shadow_tier == 'standard':
        return 'downgrade'
    return 'incomparable'


def item_view(s, item):
    d = obj(item)
    try:
        d['input_snapshot'] = json.loads(item.input_snapshot) if item.input_snapshot else None
    except (ValueError, TypeError):
        d['input_snapshot'] = None
    d['original_tier_label'] = tier_label(item.original_tier)
    d['shadow_tier_label'] = tier_label(item.shadow_tier)
    d['result_label'] = {'unchanged': '拆检档不变', 'escalate': '升入高风险拆检档',
                         'downgrade': '降为常规拆检档', 'incomparable': '无法比较',
                         '': '待复评'}.get(item.result, item.result)
    claim = s.get(Claim, item.claim_id)
    d['claim_status'] = claim.status if claim else ''
    return d


def batch_view(s, batch):
    d = obj(batch)
    try:
        d['metrics'] = json.loads(batch.metrics) if batch.metrics else None
    except (ValueError, TypeError):
        d['metrics'] = None
    items = s.scalars(select(ShadowItem).where(ShadowItem.batch_id == batch.id).order_by(ShadowItem.id)).all()
    d['items'] = [item_view(s, x) for x in items]
    d['remaining'] = batch.total_count - batch.done_count - batch.incomparable_count
    d['change_count'] = sum(1 for x in items if x.result in ('escalate', 'downgrade'))
    return d


def list_batches(s):
    return [batch_view(s, b) for b in s.scalars(select(ShadowBatch).order_by(ShadowBatch.id.desc())).all()]


def publish_if_complete(s, batch):
    # 整批算齐（含无法比较）才公布换档及无法比较的单子
    pending_left = s.scalar(select(ShadowItem.id).where(
        ShadowItem.batch_id == batch.id,
        ShadowItem.state.in_(['pending', 'failed'])).limit(1))
    if pending_left:
        return False
    if batch.state == 'published':
        return True
    batch.state = 'published'
    batch.published_at = now()
    changes = s.scalar(select(func.count()).select_from(ShadowItem).where(
        ShadowItem.batch_id == batch.id, ShadowItem.result.in_(['escalate', 'downgrade'])))
    incomparables = s.scalar(select(func.count()).select_from(ShadowItem).where(
        ShadowItem.batch_id == batch.id, ShadowItem.result == 'incomparable'))
    s.add(Audit(actor=batch.created_by, action='shadow_batch_published',
                detail=f'{batch.code}:换档{changes}单,无法比较{incomparables}单', at=now()))
    return True
