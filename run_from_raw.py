import argparse,concurrent.futures,json,os,subprocess,sys,time
from pathlib import Path

def main():
 p=argparse.ArgumentParser();p.add_argument('--raw',type=Path,required=True);p.add_argument('--out',type=Path,required=True);p.add_argument('--workers',type=int,default=3);a=p.parse_args()
 if not 1<=a.workers<=3:p.error('--workers must be 1..3')
 raw=a.raw.resolve();out=a.out.resolve();code=Path(__file__).resolve().parent
 jobs={
 'csv':('CSV_Parser/parse_csv.py','BoardingData.csv','--out','csv'),
 'tab':('Tab_Parser/tab_parser.py','Sirena-export-fixed.tab','--output','tab'),
 'json':('JSON_Parser/json_parser.py','FrequentFlyerForum-Profiles.json','--output','json'),
 'xml':('XML_Parser/parse_ds_parquet.py','PointzAggregator-AirlinesData.xml','--output','xml'),
 'yaml':('YAML_Parser/parse_yaml.py','SkyTeam-Exchange.yaml','--output','yaml'),
 'xlsx':('Excel_Tickets/parse_excel_tickets.py','YourBoardingPassDotAero.zip','--output','xlsx/tickets.parquet'),
 'pdf_intro':('PDF_Parser/parse_intro.py','Skyteam_Timetable.pdf','--out','pdf_intro'),
 'pdf_schedule':('PDF_Parser/parse_schedule.py','Skyteam_Timetable.pdf','--out','pdf_schedule')}
 for _,name,_,_ in jobs.values():
  if not (raw/name).is_file():raise FileNotFoundError(raw/name)
 out.mkdir(parents=True,exist_ok=False);logs=out/'logs';logs.mkdir();status={'status':'running','parsers':{}}
 def save():(out/'run_status.json').write_text(json.dumps(status,indent=2))
 def call(name,cmd):
  with (logs/(name+'.log')).open('w') as log:r=subprocess.run(list(map(str,cmd)),stdout=log,stderr=subprocess.STDOUT,env=env)
  return name,r.returncode
 env=dict(os.environ);env['PYTHONPATH']=str(code)+os.pathsep+env.get('PYTHONPATH','');save()
 with concurrent.futures.ThreadPoolExecutor(max_workers=a.workers) as pool:
  futures=[pool.submit(call,k,[sys.executable,code/'parsers'/script,raw/name,flag,out/'parsed'/dest]) for k,(script,name,flag,dest) in jobs.items()]
  for f in concurrent.futures.as_completed(futures):
   name,rc=f.result();status['parsers'][name]=rc;save();print(name,rc,flush=True)
 if any(status['parsers'].values()):status['status']='failed';save();raise SystemExit('Parser failed; see logs')
 commands=[('build',[sys.executable,'-m','pipelines.build_common_parquet','--input-dir',out/'parsed','--output',out/'common/travel.parquet']),('filter',[sys.executable,'-m','pipelines.filter_profiles','--input',out/'common/travel.parquet','--output',out/'selection/profiles.parquet','--json-output',out/'selection/profiles.json'])]
 for name,cmd in commands:
  _,rc=call(name,cmd);status[name]=rc;save()
  if rc:status['status']='failed';save();raise SystemExit(name+' failed; see logs')
 status['status']='complete';save();print('Complete:',out)
if __name__=='__main__':main()
