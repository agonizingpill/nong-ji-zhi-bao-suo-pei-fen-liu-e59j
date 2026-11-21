from sqlalchemy import String, Integer, Float, Boolean, ForeignKey, CheckConstraint, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .database import Base

SHADOW_BATCH_STATES = ('running', 'published')
SHADOW_ITEM_STATES = ('pending', 'done', 'incomparable', 'failed')
SHADOW_ITEM_RESULTS = ('unchanged', 'escalate', 'downgrade', 'incomparable')


class ShadowBatch(Base):
    __tablename__ = 'shadow_batches'
    __table_args__ = (CheckConstraint("state in ('running','published')"),
                      CheckConstraint('total_count >= 0 AND done_count >= 0 AND incomparable_count >= 0'))
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), unique=True)
    state: Mapped[str] = mapped_column(String(20), default='running')
    training_sample_count: Mapped[int] = mapped_column(Integer, default=0)
    total_count: Mapped[int] = mapped_column(Integer, default=0)
    done_count: Mapped[int] = mapped_column(Integer, default=0)
    incomparable_count: Mapped[int] = mapped_column(Integer, default=0)
    threshold: Mapped[float] = mapped_column(Float, default=0.5)
    metrics: Mapped[str] = mapped_column(Text, default='')
    created_by: Mapped[str] = mapped_column(String(120))
    created_at: Mapped[str] = mapped_column(String(40))
    published_at: Mapped[str | None] = mapped_column(String(40), nullable=True)


class ShadowItem(Base):
    __tablename__ = 'shadow_items'
    __table_args__ = (UniqueConstraint('batch_id', 'claim_id', name='uq_shadow_batch_claim'),
                      CheckConstraint("state in ('pending','done','incomparable','failed')"),
                      CheckConstraint("result in ('','unchanged','escalate','downgrade','incomparable')"))
    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey('shadow_batches.id'))
    claim_id: Mapped[int] = mapped_column(ForeignKey('claims.id'))
    claim_code: Mapped[str] = mapped_column(String(60))
    state: Mapped[str] = mapped_column(String(20), default='pending')
    result: Mapped[str] = mapped_column(String(20), default='')
    # 受理时圈定：原评分输入快照与原分流线，后续不随原单变化
    original_risk: Mapped[float | None] = mapped_column(Float, nullable=True)
    original_tier: Mapped[str] = mapped_column(String(20), default='')
    input_snapshot: Mapped[str] = mapped_column(Text, default='')
    input_available: Mapped[bool] = mapped_column(Boolean, default=True)
    shadow_risk: Mapped[float | None] = mapped_column(Float, nullable=True)
    shadow_tier: Mapped[str] = mapped_column(String(20), default='')
    note: Mapped[str] = mapped_column(Text, default='')
    attempts: Mapped[int] = mapped_column(Integer, default=0)
