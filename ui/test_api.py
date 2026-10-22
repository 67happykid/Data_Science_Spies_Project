import csv,io,json,unittest
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError
import pyarrow.parquet as pq
BASE='http://127.0.0.1:8765'
def request(path,data=None,headers=None):
 body=json.dumps(data).encode() if data is not None else None
 req=Request(BASE+path,data=body,headers={'Content-Type':'application/json',**(headers or {})})
 with urlopen(req,timeout=30) as response:return response.read(),dict(response.headers)
def query(config):return json.loads(request('/api/query',config)[0])
class WorkbenchTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  meta=json.loads(request('/api/criteria')[0]);cls.defaults={'criteria':{k:{} for k in meta['defaults']},'match':'any','unit_kinds':['source_profile']};cls.default=query(cls.defaults);cls.token=cls.default['query_id']
 def test_01_default_matches_previous_full_pipeline(self):
  data=json.loads(request('/api/export/'+self.token)[0]);previous=pq.read_table(Path(__file__).resolve().parents[1]/'data/selection/profiles.parquet',columns=['unit_id']).column(0).to_pylist()
  self.assertEqual({r['unit_id'] for r in data['rows']},set(previous));self.assertEqual(len(data['rows']),2386)
 def test_02_any_all_and_absolute_thresholds(self):
  config={'criteria':{'high_frequency':{'minimum':30},'many_routes':{'minimum':25}},'match':'any'};a=query(config);config['match']='all';b=query(config)
  ar=json.loads(request('/api/export/'+a['query_id'])[0])['rows'];br=json.loads(request('/api/export/'+b['query_id'])[0])['rows']
  self.assertLessEqual({r['unit_id'] for r in br},{r['unit_id'] for r in ar});self.assertGreater(len(br),0)
  for r in br:self.assertGreaterEqual(r['flight_days'],30);self.assertGreaterEqual(r['distinct_routes'],25)
 def test_03_empty_result_is_valid(self):
  q=query({'criteria':{'high_frequency':{'minimum':999999}}});self.assertEqual(q['total'],0);p=json.loads(request('/api/results/'+q['query_id'])[0]);self.assertEqual(p['pages'],1);self.assertEqual(p['rows'],[])
 def test_04_pagination_no_duplicate_profiles(self):
  a=json.loads(request('/api/results/'+self.token+'?page=1')[0]);b=json.loads(request('/api/results/'+self.token+'?page=2')[0]);self.assertEqual(len(a['rows']),20);self.assertTrue({r['unit_id'] for r in a['rows']}.isdisjoint(r['unit_id'] for r in b['rows']))
 def test_05_source_filter_also_applies_to_exports(self):
  a=json.loads(request('/api/results/'+self.token+'?source=xml')[0]);data=json.loads(request('/api/export/'+self.token+'?source=xml')[0]);self.assertEqual(a['total'],722);self.assertEqual(len(data['rows']),a['total']);self.assertTrue(all(r['source_signature']=='xml' for r in data['rows']))
 def test_06_search_and_csv_export(self):
  rows=json.loads(request('/api/results/'+self.token+'?search=VARVARA')[0]);raw,headers=request('/api/export/'+self.token+'?format=csv&search=VARVARA');csvrows=list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))));self.assertEqual(len(csvrows),rows['total']);self.assertIn('attachment',headers['Content-Disposition']);self.assertTrue(all('VARVARA' in r['name'] for r in csvrows))
 def test_07_unknown_criterion_rejected(self):
  with self.assertRaises(HTTPError) as ctx:query({'criteria':{'bogus':{}}})
  self.assertEqual(ctx.exception.code,400)
 def test_08_bool_and_out_of_range_threshold_rejected(self):
  for value in [True,-1]:
   with self.assertRaises(HTTPError) as ctx:query({'criteria':{'high_frequency':{'minimum':value}}})
   self.assertEqual(ctx.exception.code,400)
 def test_09_unknown_result_is_clear_404(self):
  with self.assertRaises(HTTPError) as ctx:request('/api/results/unknown')
  self.assertEqual(ctx.exception.code,404)
 def test_10_criteria_empty_rejected(self):
  with self.assertRaises(HTTPError) as ctx:query({'criteria':{}})
  self.assertEqual(ctx.exception.code,400)
 def test_11_full_explanations_and_finite_json(self):
  rows=json.loads(request('/api/export/'+self.token)[0])['rows']
  for row in rows:
   self.assertEqual(row['criteria_count'],len(row['reasons']))
   for r in row['reasons']:self.assertTrue(r['observed']>=r['threshold'] if r['operator']=='>=' else r['observed']>r['threshold'])
 def test_12_local_page_ready_and_cached(self):
  body,_=request('/');self.assertIn('TRACE'.encode(),body);s=json.loads(request('/api/status')[0]);self.assertEqual(s['state'],'ready');self.assertTrue(s['cache_used']);self.assertEqual(s['observations'],4358805)
class AdditionalEvidenceAPITests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.docs=query({'criteria':{'document_discrepancy':{'minimum':2}}})
  cls.person=json.loads(request('/api/results/'+cls.docs['query_id']+'?size=1')[0])['rows'][0]
  from urllib.parse import urlencode
  cls.url='/api/evidence/'+cls.docs['query_id']+'?'+urlencode({'unit_id':cls.person['unit_id']})
 def test_documents_expose_actual_numbers_and_source_records(self):
  data=json.loads(request(self.url)[0]);components=data['documents']['components']
  self.assertEqual(data['documents']['max_distinct_document_count'],self.person['candidate_document_count'])
  self.assertTrue(components)
  for c in components:
   self.assertEqual(c['distinct_document_count'],len({d['document_key'] for d in c['documents']}))
   for doc in c['documents']:
    self.assertTrue(doc['document_key']);self.assertTrue(doc['raw_values'])
    for ref in doc['records']:self.assertTrue(ref['source_record_id']);self.assertTrue(ref['source_locator']);self.assertTrue(ref['source_sha256'])
 def test_component_context_not_misattributed_as_profile_documents(self):
  data=json.loads(request(self.url)[0])
  for c in data['documents']['components']:
   self.assertEqual(c['ownership'],'candidate_component_context_not_confirmed_profile_ownership')
   self.assertEqual(c['linkage_status'],'held_for_review')
   self.assertTrue(c['matching_keys'])
   self.assertFalse(any(r['belongs_to_selected_unit'] for d in c['documents'] for r in d['records']))
 def test_evidence_download_is_complete_and_versioned(self):
  raw,headers=request(self.url+'&download=1');data=json.loads(raw)
  self.assertIn('attachment',headers['Content-Disposition']);self.assertEqual(data['unit_id'],self.person['unit_id']);self.assertEqual(len(data['dataset_sha256']),64);self.assertIn('reasons',data)
 def test_unselected_profile_evidence_rejected(self):
  from urllib.parse import urlencode
  with self.assertRaises(HTTPError) as ctx:request('/api/evidence/'+self.docs['query_id']+'?'+urlencode({'unit_id':'not-selected'}))
  self.assertEqual(ctx.exception.code,404)
 def test_route_evidence_reproduces_count_and_contains_variants(self):
  q=query({'criteria':{'route_conflict':{'minimum':1}},'unit_kinds':['preliminary_group']})
  person=json.loads(request('/api/results/'+q['query_id']+'?size=1')[0])['rows'][0]
  from urllib.parse import urlencode
  evidence=json.loads(request('/api/evidence/'+q['query_id']+'?'+urlencode({'unit_id':person['unit_id']}))[0])
  observed=next(r['observed'] for r in person['reasons'] if r['criterion']=='route_conflict')
  self.assertEqual(evidence['route_conflicts']['count'],observed)
  for slot in evidence['route_conflicts']['slots']:
   self.assertGreater(slot['route_variant_count'],1);self.assertEqual(slot['route_variant_count'],len(slot['routes']))
   self.assertTrue(all(route['observations'] for route in slot['routes']))
if __name__=='__main__':unittest.main(verbosity=2)
