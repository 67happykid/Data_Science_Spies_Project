from __future__ import annotations
import argparse,csv,hashlib,io,json,logging,math,secrets,sys,threading,time
from collections import Counter,OrderedDict
from datetime import date,datetime
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse,parse_qs

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent))
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from evidence import build_evidence
from pipelines.filter_profiles import load_features
from pipelines.travel_analysis.criteria import CRITERIA,DEFAULT_CRITERIA,select_profiles,validate_config


def digest(path):
 with Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def safe(value):
 if value is None:return None
 if isinstance(value,(date,datetime,pd.Timestamp)):return value.isoformat()
 if isinstance(value,float) and not math.isfinite(value):return None
 if hasattr(value,'item'):return safe(value.item())
 if isinstance(value,dict):return {k:safe(v) for k,v in value.items()}
 if isinstance(value,(list,tuple)):return [safe(v) for v in value]
 return value

def serialize(row,reasons):
 names=json.loads(row['names_json']);display=[]
 for n in names:
  value=n.get('full_name') or ' '.join(str(n[k]) for k in ['first_name','middle_name','last_name'] if n.get(k))
  if value and value not in display:display.append(value)
 return safe({**{k:row.get(k) for k in ['unit_id','unit_kind','source_signature','flight_days','active_dates','distinct_routes','top_route_flight_days','top_route_share','peak7_flight_days','peak7_share','route_rarity','route_coverage','first_date','last_date','reference_eligible','reference_units','candidate_document_count','peak_day_flight_bins','criteria_count','matched_criteria']},'display_name':display[0] if display else 'Профиль '+row['unit_id'].split(':')[-1][:8],'aliases':display,'names':names,'reasons':reasons})

class Workbench:
 def __init__(self,input_file,cache_dir):
  self.input=Path(input_file).resolve();self.cache=Path(cache_dir);self.frame=None;self.lock=threading.Lock();self.results=OrderedDict();self.evidence_cache=OrderedDict();self.status={'state':'loading','message':'Подготавливаем данные для отбора…'};self.version=None
 def load(self):
  try:
   fingerprint=digest(self.input)
   codehash=hashlib.sha256(''.join(digest(HERE.parent/n) for n in ['pipelines/filter_profiles.py','pipelines/features.py','pipelines/travel_analysis/criteria.py','pipelines/build_common_parquet.py']).encode()).hexdigest()
   key=fingerprint+'-'+codehash;self.cache.mkdir(parents=True,exist_ok=True);path=self.cache/'features.parquet';cache_used=False
   if path.exists():
    table=pq.read_table(path)
    if (table.schema.metadata or {}).get(b'workbench.key',b'').decode()==key:self.frame=table.to_pandas();cache_used=True
   if self.frame is None:
    self.frame,manifest=load_features(self.input)
    t=pa.Table.from_pandas(self.frame,preserve_index=False);t=t.replace_schema_metadata({**(t.schema.metadata or {}),b'workbench.key':key.encode()})
    tmp=self.cache/'features.parquet.partial';pq.write_table(t,tmp,compression='zstd');tmp.replace(path)
   self.version=fingerprint
   manifest=json.loads(pq.ParquetFile(self.input).schema_arrow.metadata[b'travel.manifest'])
   self.status={'state':'ready','message':'Данные готовы','profiles':int(self.frame.unit_kind.eq('source_profile').sum()),'groups':int(self.frame.unit_kind.eq('preliminary_group').sum()),'observations':manifest['observations'],'reference_profiles':int((self.frame.unit_kind.eq('source_profile')&self.frame.reference_eligible).sum()),'cache_used':cache_used,'dataset_version':fingerprint[:12]}
  except Exception:
   logging.exception('Feature preparation failed');self.status={'state':'error','message':'Не удалось подготовить данные. Подробности — в журнале сервера.'}
 def query(self,config):
  if self.frame is None or self.status['state']!='ready':raise RuntimeError('Данные ещё подготавливаются')
  config=validate_config(config);start=time.monotonic();selected,reasons,config=select_profiles(self.frame,config)
  rows=[serialize(row,reasons[idx]) for idx,row in selected.iterrows()]
  rows.sort(key=lambda r:(-r['criteria_count'],-r['flight_days'],r['display_name'],r['unit_id']))
  token=secrets.token_urlsafe(16);record={'rows':rows,'config':config,'dataset_sha256':self.version,'elapsed_ms':round((time.monotonic()-start)*1000),'by_source':dict(Counter(r['source_signature'] for r in rows)),'by_criterion':dict(Counter(c for r in rows for c in r['matched_criteria']))}
  with self.lock:
   self.results[token]=record
   while len(self.results)>8:self.results.popitem(last=False)
  return {'query_id':token,'total':len(rows),'elapsed_ms':record['elapsed_ms'],'by_source':record['by_source'],'by_criterion':record['by_criterion'],'config':config}
 def get(self,token):
  with self.lock:
   if token not in self.results:raise KeyError('Выборка устарела. Запустите отбор ещё раз.')
   return self.results[token]
 def evidence(self,token,unit_id):
  record=self.get(token)
  selected=next((r for r in record['rows'] if r['unit_id']==unit_id),None)
  if selected is None:raise KeyError('Персонаж не найден в этой выборке')
  with self.lock:base=self.evidence_cache.get(unit_id)
  if base is None:
   feature=self.frame.loc[self.frame.unit_id.eq(unit_id)].iloc[0]
   base=build_evidence(self.input,[n['entity_id'] for n in selected['names']],json.loads(feature['document_context_components_json']))

   assert base['route_conflicts']['count']==int(feature['route_conflict_flight_days'])
   assert base['busy_days']['peak_distinct_flights']==int(feature['peak_day_flight_bins'])
   if selected['candidate_document_count']>1:
    assert base['documents']['max_distinct_document_count']==int(selected['candidate_document_count'])
   with self.lock:
    self.evidence_cache[unit_id]=base
    while len(self.evidence_cache)>32:self.evidence_cache.popitem(last=False)
  return {'unit_id':unit_id,'display_name':selected['display_name'],'dataset_sha256':self.version,
    'reasons':selected['reasons'],**base}
 def filtered(self,token,params):
  record=self.get(token);rows=record['rows'];search=params.get('search',[''])[0].strip().casefold();source=params.get('source',[''])[0]
  if search:rows=[r for r in rows if search in (r['display_name']+' '+' '.join(r['aliases'])+' '+r['unit_id']).casefold()]
  if source:rows=[r for r in rows if source in r['source_signature'].split('+')]
  sort=params.get('sort',['matches'])[0]
  if sort=='flights':rows=sorted(rows,key=lambda r:(-r['flight_days'],r['unit_id']))
  elif sort=='routes':rows=sorted(rows,key=lambda r:(-r['distinct_routes'],r['unit_id']))
  elif sort=='name':rows=sorted(rows,key=lambda r:(r['display_name'],r['unit_id']))
  elif sort!='matches':raise ValueError('Неизвестный порядок сортировки')
  return rows
 def page(self,token,params):
  record=self.get(token);rows=self.filtered(token,params)
  page=max(1,int(params.get('page',['1'])[0]));size=min(100,max(1,int(params.get('size',['20'])[0])));total=len(rows);page=min(page,max(1,math.ceil(total/size)))
  return {'rows':rows[(page-1)*size:page*size],'total':total,'selection_total':len(record['rows']),'page':page,'size':size,'pages':max(1,math.ceil(total/size))}

class Handler(BaseHTTPRequestHandler):
 def reply(self,status,data,kind='application/json; charset=utf-8',download=None):
  payload=json.dumps(safe(data),ensure_ascii=False,allow_nan=False).encode() if kind.startswith('application/json') else data
  self.send_response(status);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(payload)));self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff')
  if download:self.send_header('Content-Disposition',f'attachment; filename="{download}"')
  self.end_headers();self.wfile.write(payload)
 def do_GET(self):
  try:
   parsed=urlparse(self.path);route=parsed.path;params=parse_qs(parsed.query)
   if route=='/api/status':return self.reply(200,self.server.workbench.status)
   if route=='/api/criteria':return self.reply(200,{'criteria':CRITERIA,'defaults':DEFAULT_CRITERIA})
   if route.startswith('/api/evidence/'):
    token=route.split('/')[-1];unit_id=params.get('unit_id',[''])[0]
    if not unit_id:raise ValueError('Нужен ID персонажа')
    data=self.server.workbench.evidence(token,unit_id)
    return self.reply(200,data,download='profile_evidence.json' if params.get('download',['0'])[0]=='1' else None)
   if route.startswith('/api/results/'):
    token=route.split('/')[-1];return self.reply(200,self.server.workbench.page(token,params))
   if route.startswith('/api/export/'):
    token=route.split('/')[-1];record=self.server.workbench.get(token);rows=self.server.workbench.filtered(token,params);fmt=params.get('format',['json'])[0]
    if fmt=='json':return self.reply(200,{**record,'rows':rows,'filtered_total':len(rows),'selection_total':len(record['rows'])},download='selected_profiles.json')
    if fmt!='csv':raise ValueError('Доступны CSV и JSON')
    buf=io.StringIO();writer=csv.writer(buf);writer.writerow(['profile_id','name','source','flights','routes','criteria_count','criteria','reasons_json'])
    for r in rows:

     name=r['display_name'];name="'"+name if name[:1] in '=+-@' else name
     writer.writerow([r['unit_id'],name,r['source_signature'],r['flight_days'],r['distinct_routes'],r['criteria_count'],'; '.join(r['matched_criteria']),json.dumps(r['reasons'],ensure_ascii=False)])
    return self.reply(200,('\ufeff'+buf.getvalue()).encode(),'text/csv; charset=utf-8','selected_profiles.csv')
   assets={'/':('index.html','text/html; charset=utf-8'),'/app.js':('app.js','application/javascript; charset=utf-8'),'/evidence.js':('evidence.js','application/javascript; charset=utf-8'),'/style.css':('style.css','text/css; charset=utf-8')}
   if route not in assets:return self.reply(404,{'error':'Не найдено'})
   name,kind=assets[route];return self.reply(200,(HERE/'static'/name).read_bytes(),kind)
  except KeyError as exc:self.reply(404,{'error':str(exc)})
  except (ValueError,TypeError) as exc:self.reply(400,{'error':str(exc)})
  except Exception:logging.exception('GET failed');self.reply(500,{'error':'Ошибка сервера'})
 def do_POST(self):
  try:
   if self.path!='/api/query':return self.reply(404,{'error':'Не найдено'})
   origin=self.headers.get('Origin')
   if origin and origin!=f'http://{self.headers.get("Host")}':return self.reply(403,{'error':'Запрос разрешён только из локального интерфейса'})
   length=int(self.headers.get('Content-Length','0'))
   if not 0<length<=65536:raise ValueError('Неверный размер запроса')
   config=json.loads(self.rfile.read(length));return self.reply(200,self.server.workbench.query(config))
  except RuntimeError as exc:self.reply(503,{'error':str(exc)})
  except (ValueError,TypeError,KeyError) as exc:self.reply(400,{'error':str(exc)})
  except Exception:logging.exception('Query failed');self.reply(500,{'error':'Не удалось выполнить отбор'})
 def log_message(self,format,*args):logging.info(format,*args)

def main():
 p=argparse.ArgumentParser();p.add_argument('--input',type=Path,default=HERE.parent/'data/common/travel.parquet');p.add_argument('--port',type=int,default=8765);p.add_argument('--cache',type=Path,default=HERE.parent/'data/cache');a=p.parse_args()
 if not a.input.is_file():p.error('Не найден общий Parquet: '+str(a.input))
 logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
 server=ThreadingHTTPServer(('127.0.0.1',a.port),Handler);server.workbench=Workbench(a.input,a.cache)
 threading.Thread(target=server.workbench.load,daemon=True).start();print(f'Local: http://127.0.0.1:{a.port}',flush=True)
 try:server.serve_forever()
 except KeyboardInterrupt:pass
 finally:server.server_close()
if __name__=='__main__':main()
