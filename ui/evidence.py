from __future__ import annotations
import json
from collections import defaultdict
from pathlib import Path
import duckdb
import pyarrow as pa
from pipelines.research.common import literal

SOURCE_FILES={
 'csv':'BoardingData.csv','tab':'Sirena-export-fixed.tab',
 'json':'FrequentFlyerForum-Profiles.json','xml':'PointzAggregator-AirlinesData.xml',
 'yaml':'SkyTeam-Exchange.yaml','excel':'YourBoardingPassDotAero.zip',
}

def name_of(row):
 return row.get('full_name') or ' '.join(row[k] for k in ['first_name','middle_name','last_name'] if row.get(k)) or 'Запись '+row['entity_id'][:8]

def records(db,query):
 result=db.execute(query);fields=[c[0] for c in result.description]
 return [dict(zip(fields,r)) for r in result.fetchall()]

def member_record(row,targets):
 return {'entity_id':row['entity_id'],'name':name_of(row),'source':row['source'],
         'source_file':SOURCE_FILES.get(row['source'],row['source']),
         'source_record_id':row['entity_record_id'],'source_locator':row['entity_locator'],
         'source_sha256':row['entity_source_sha256'],
         'belongs_to_selected_unit':row['entity_id'] in targets}

def build_evidence(input_file,entity_ids,component_ids):

 targets=set(entity_ids);components=set(component_ids)
 db=duckdb.connect()
 try:
  db.execute("SET memory_limit='512MB'");db.execute('SET threads=1')
  db.execute(f'CREATE VIEW dataset AS SELECT * FROM read_parquet({literal(Path(input_file))})')
  db.register('targets',pa.table({'entity_id':pa.array(sorted(targets),type=pa.string())}))
  db.register('requested_components',pa.table({'component_id':pa.array(sorted(components),type=pa.string())}))
  db.execute('''CREATE TEMP TABLE unit_observations AS SELECT observation_id,entity_id,source,source_record_id,
    source_sha256,entity_locator,record_kind,date_raw,date_key,flight_raw,flight_key,origin_raw,origin_key,
    destination_raw,destination_key,local_time_raw,local_time_key FROM dataset
    WHERE entity_id IN (SELECT entity_id FROM targets) AND observation_id IS NOT NULL''')
  observations=records(db,'''SELECT * FROM unit_observations
    WHERE date_key IS NOT NULL AND flight_key IS NOT NULL ORDER BY date_key,flight_key,source,observation_id''')
  bins=defaultdict(list)
  for row in observations:bins[(str(row['date_key']),row['flight_key'])].append(row)
  slots=[];by_day=defaultdict(list)
  for (date,flight),rows in sorted(bins.items()):
   routes=defaultdict(list)
   for row in rows:
    if row['origin_key'] is not None and row['destination_key'] is not None:
     routes[(row['origin_key'],row['destination_key'])].append(row)
   def observation_ref(r):
    return {'observation_id':r['observation_id'],'source_record_id':r['source_record_id'],
            'entity_id':r['entity_id'],'source':r['source'],'source_file':SOURCE_FILES.get(r['source'],r['source']),
            'source_locator':r['entity_locator'],'source_sha256':r['source_sha256'],
            'date_raw':r['date_raw'],'flight_raw':r['flight_raw'],
            'origin_raw':r['origin_raw'],'destination_raw':r['destination_raw'],
            'local_time_raw':r['local_time_raw'],'local_time':r['local_time_key']}
   slot={'date':date,'flight':flight,'route_variant_count':len(routes),
         'routes':[{'origin':origin,'destination':destination,'sources':sorted({r['source'] for r in refs}),
                    'observations':[observation_ref(r) for r in refs]} for (origin,destination),refs in sorted(routes.items())],
         'observation_count':len(rows),'incomplete_route_observations':sum(r['origin_key'] is None or r['destination_key'] is None for r in rows),
         'observations':[observation_ref(r) for r in rows]}
   slots.append(slot);by_day[date].append(slot)
  conflicts=[s for s in slots if s['route_variant_count']>1]
  peak=max((len(v) for v in by_day.values()),default=0)
  busy=[{'date':d,'distinct_flights':len(v),'flights':v} for d,v in sorted(by_day.items()) if len(v)>=2]
  document_components=[]
  if components:
   members=records(db,'''SELECT candidate_component_id,entity_id,source,first_name,last_name,middle_name,full_name,
      document_key,document_raw,entity_record_id,entity_locator,entity_source_sha256,linkage_status,
      component_reasons_json,max(identifier_assertions_json) identifiers_json
      FROM dataset WHERE candidate_component_id IN (SELECT component_id FROM requested_components)
      GROUP BY ALL ORDER BY candidate_component_id,source,entity_id''')
   groups=defaultdict(list)
   for member in members:groups[member['candidate_component_id']].append(member)
   for component_id,group in sorted(groups.items()):
    docs=defaultdict(list);assertions=defaultdict(dict);group_reasons=set()
    for member in group:
     group_reasons.update(json.loads(member['component_reasons_json'] or '[]'))
     if member['document_key'] is not None:docs[member['document_key']].append(member)
     for assertion in json.loads(member['identifiers_json'] or '[]'):
      if assertion.get('usable') and assertion.get('namespace') is not None and assertion.get('value') is not None:
       key=(assertion['identifier_type'],assertion['namespace'],assertion['value'])
       assertions[key][member['entity_id']]={'entity_id':member['entity_id'],'source':member['source'],
         'name':name_of(member),'assertion_id':assertion.get('assertion_id'),
         'source_record_id':assertion.get('source_record_id'),'source_sha256':assertion.get('source_sha256')}
    document_components.append({'component_id':component_id,'linkage_status':group[0]['linkage_status'],
      'member_count':len(group),'distinct_document_count':len(docs),'component_reasons':sorted(group_reasons),
      'documents':[{'document_key':key,'raw_values':sorted({r['document_raw'] for r in refs if r['document_raw'] is not None}),
         'sources':sorted({r['source'] for r in refs}),'record_count':len(refs),
         'records':[{**member_record(r,targets),'document_raw':r['document_raw']} for r in refs]} for key,refs in sorted(docs.items())],
      'matching_keys':[{'type':kind,'namespace':namespace,'value':value,
         'sources':sorted({r['source'] for r in refs.values()}),'records':list(refs.values())}
         for (kind,namespace,value),refs in sorted(assertions.items()) if len({r['source'] for r in refs.values()})>=2],
      'selected_members':[member_record(r,targets) for r in group if r['entity_id'] in targets],
      'ownership':'candidate_component_context_not_confirmed_profile_ownership'})
  return {'documents':{'components':document_components,
            'max_distinct_document_count':max((g['distinct_document_count'] for g in document_components),default=0)},
          'route_conflicts':{'count':len(conflicts),'slots':conflicts},
          'busy_days':{'peak_distinct_flights':peak,'days':busy},
          'source_observations':len(observations),'date_flight_bins':len(slots),
          'counting_rule':'Distinct calendar-date/flight bins; repeated source observations do not add flights.'}
 finally:db.close()
