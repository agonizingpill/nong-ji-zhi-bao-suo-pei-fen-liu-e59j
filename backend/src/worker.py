import json,time,os
from pathlib import Path
from redis import Redis
from sqlalchemy import select
from .database import Session
from .models import Claim,Machine
from .risk_model import assess,train_on_history,predict,feature_row
from .shadow_models import ShadowBatch,ShadowItem
from .shadow_service import tier_of,compare_tiers,publish_if_complete
r=Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)
ARTIFACTS=Path(os.getenv('ARTIFACTS_DIR','/artifacts'))
ARTIFACTS.mkdir(parents=True,exist_ok=True)

def _retained_input(s,c):
    row=feature_row(s,c)
    return {'machine_model':row[0],'part':row[1],'hours_at_failure':row[2],
            'age_days':row[3],'leak_flag':bool(row[4]),
            'machine_serial':s.get(Machine,c.machine_id).serial}

def run_claim_risk(cid):
    with Session.begin() as s:
        c=s.get(Claim,cid)
        if not c or c.status!='queued':return
        m=s.get(Machine,c.machine_id)
        if c.hours_at_failure>m.warranty_hours:
            c.status='out_of_warranty';c.assessment_note='超出质保工时';return
        c.risk,metrics=assess(s,c);c.status='assessed'
        # 今后评分留存原输入，供日后影子复评沿用同一输入比较
        c.assessment_note=json.dumps(
            {'metrics':metrics,'input':_retained_input(s,c)},ensure_ascii=False)
    (ARTIFACTS/f'claim-{cid}-model.json').write_text(json.dumps(metrics,ensure_ascii=False))

def process_shadow_item(bid):
    # 复评中断后恢复：接着算原名单里的待办项；done/incomparable 永不重算，重复消息幂等跳过
    with Session.begin() as s:
        batch=s.get(ShadowBatch,bid)
        if not batch or batch.state=='published':return
        item=s.scalars(select(ShadowItem).where(
            ShadowItem.batch_id==bid,ShadowItem.state.in_(['pending','failed'])
        ).order_by(ShadowItem.id).with_for_update().limit(1)).first()
        if not item:
            publish_if_complete(s,batch);return
        item.attempts+=1
        if not item.input_available:
            item.state='incomparable';item.result='incomparable'
            item.note='旧单缺少原评分输入或原评分，记为无法比较'
            publish_if_complete(s,batch);return
        try:
            snap=json.loads(item.input_snapshot)
            row=[snap['machine_model'],snap['part'],snap['hours_at_failure'],
                 snap['age_days'],int(snap['leak_flag'])]
            # 用当前历史样本重训后的模型，对受理时圈定的原输入重新打分
            model,metrics=train_on_history(s)
            shadow_risk=predict(model,row)
            shadow_tier=tier_of(shadow_risk,batch.threshold)
            item.shadow_risk=shadow_risk;item.shadow_tier=shadow_tier
            item.result=compare_tiers(item.original_tier,shadow_tier)
            item.state='done';item.note=''
            batch.done_count+=1
            if not batch.metrics:
                batch.training_sample_count=metrics['training_rows']+metrics['holdout_rows']
                batch.metrics=json.dumps(metrics,ensure_ascii=False)
                (ARTIFACTS/f'shadow-{bid}-model.json').write_text(json.dumps(metrics,ensure_ascii=False))
        except Exception as e:
            # 单条失败保留 failed 可重试，不计入 done，整批不会提前公布
            item.state='failed';item.note=str(e)[:500];return
        publish_if_complete(s,batch)

def main():
    ARTIFACTS.mkdir(parents=True,exist_ok=True)
    while True:
        (ARTIFACTS/'worker-heartbeat').write_text(str(time.time()))
        # 影子复评优先；两类任务互不修改对方数据，复评期间原单照常签赔
        queued=r.blpop(['shadow-risk','claim-risk'],timeout=2)
        if not queued:continue
        queue,payload=queued
        try:
            if queue=='claim-risk':run_claim_risk(int(payload))
            else:process_shadow_item(int(payload))
        except Exception:
            if queue=='claim-risk':
                with Session.begin() as s:
                    c=s.get(Claim,int(payload))
                    if c:c.status='failed';c.assessment_note='分流计算失败，可重试'
            # 影子消息进程级异常：条目仍为 pending/failed，由重试接口或下一条消息接着算，不丢原名单

if __name__=='__main__':
    main()
