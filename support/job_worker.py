"""外部作业协议演练，仅生成回执，不分析依赖或发布仓表。"""
import argparse,hashlib,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--request',required=True);p.add_argument('--directory',required=True);p.add_argument('--mode',choices=['ok','fail-before','exit-after'],default='ok');a=p.parse_args()
r=json.loads(Path(a.request).read_text(encoding='utf-8'))
assert set(r)=={'run_token','job_id','scope','window','input_fingerprints'}
if a.mode=='fail-before':raise SystemExit(23)
out=Path(a.directory);out.mkdir(parents=True,exist_ok=True)
payload=json.dumps(r,ensure_ascii=False,sort_keys=True).encode();artifact=out/'artifact.json';artifact.write_bytes(payload)
receipt={'run_token':r['run_token'],'sha256':hashlib.sha256(payload).hexdigest(),'artifact':str(artifact.resolve()),'input_fingerprints':r['input_fingerprints']}
(out/'receipt.json').write_text(json.dumps(receipt,ensure_ascii=False),encoding='utf-8')
if a.mode=='exit-after':raise SystemExit(24)
print(json.dumps(receipt,ensure_ascii=False))
