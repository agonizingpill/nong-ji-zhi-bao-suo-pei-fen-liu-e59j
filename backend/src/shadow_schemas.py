from pydantic import BaseModel, Field


class ShadowBatchIn(BaseModel):
    # 受理时圈定名单和训练样本：名单由系统按"待签单"规则圈定，主管可给批次备注名
    code: str | None = Field(default=None, min_length=2, max_length=60)
