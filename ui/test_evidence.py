import sys,tempfile,json,unittest
from pathlib import Path
from datetime import date
import pyarrow as pa
import pyarrow.parquet as pq
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from evidence import build_evidence

class EvidenceTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)/'data.parquet'
  def row(entity,source,obs=None,day=None,flight=None,origin=None,dest=None,document=None,raw_document=None):
   return {'entity_id':entity,'source':source,'observation_id':obs,'record_kind':'observation' if obs else 'entity_only',
    'source_record_id':obs or entity,'source_sha256':'source-hash','entity_record_id':entity+'-record',
    'entity_locator':entity+'-locator','entity_source_sha256':'entity-source-hash',
    'first_name':entity.upper(),'last_name':'TEST','middle_name':None,'full_name':None,
    'candidate_component_id':'component-1','linkage_status':'held_for_review','component_reasons_json':'["different_known_documents"]',
    'document_key':document,'document_raw':raw_document,
    'identifier_assertions_json':json.dumps([{'identifier_type':'loyalty','namespace':'SU','value':'000123','usable':True,'assertion_id':entity+'-card','source_record_id':entity+'-card-record','source_sha256':'source-hash'}]),
    'date_raw':str(day) if day else None,'date_key':day,'flight_raw':flight,'flight_key':flight,
    'origin_raw':origin,'origin_key':origin,'destination_raw':dest,'destination_key':dest,
    'local_time_raw':'10:00','local_time_key':'10:00'}
  rows=[row('profile','json','o1',date(2017,1,1),'SU1','SVO','CDG'),
   row('profile','json','o2',date(2017,1,1),'SU1','SVO','CDG'),
   row('profile','json','o3',date(2017,1,1),'SU1','LED','CDG'),
   row('profile','json','o4',date(2017,1,1),'SU2','CDG','SVO'),
   row('profile','json','o5',date(2017,1,2),'SU3',None,'CDG'),
   row('doc-a','csv',document='00123',raw_document='00 123'),
   row('doc-a2','tab',document='00123',raw_document='00123'),
   row('doc-b','tab',document='00124',raw_document='00 124'),
   row('unrelated','xml','x1',date(2017,1,1),'SU99','SVO','DME')]
  pq.write_table(pa.Table.from_pylist(rows),self.path)
 def tearDown(self):self.temp.cleanup()
 def get(self):return build_evidence(self.path,['profile'],['component-1'])
 def test_documents_group_by_normalized_key_keep_raw_and_lineage(self):
  d=self.get()['documents']['components'][0]
  self.assertEqual(d['distinct_document_count'],2)
  a=d['documents'][0];self.assertEqual(a['document_key'],'00123');self.assertEqual(a['raw_values'],['00 123','00123']);self.assertEqual(a['sources'],['csv','tab'])
  self.assertEqual(a['records'][0]['source_locator'],'doc-a-locator');self.assertEqual(a['records'][0]['source_record_id'],'doc-a-record')
 def test_linked_document_records_not_assigned_to_profile(self):
  d=self.get()['documents']['components'][0]
  self.assertEqual(d['ownership'],'candidate_component_context_not_confirmed_profile_ownership')
  self.assertFalse(any(r['belongs_to_selected_unit'] for doc in d['documents'] for r in doc['records']))
  self.assertEqual([r['entity_id'] for r in d['selected_members']],['profile'])
 def test_shared_loyalty_key_retains_leading_zeroes_and_provenance(self):
  keys=self.get()['documents']['components'][0]['matching_keys']
  self.assertEqual(keys[0]['value'],'000123');self.assertEqual(keys[0]['namespace'],'SU');self.assertEqual(keys[0]['sources'],['csv','json','tab','xml'])
  self.assertTrue(all(r['assertion_id'] and r['source_record_id'] for r in keys[0]['records']))
 def test_conflicting_route_variants_keep_each_source_observation(self):
  e=self.get();self.assertEqual(e['route_conflicts']['count'],1)
  slot=e['route_conflicts']['slots'][0];self.assertEqual(slot['flight'],'SU1');self.assertEqual(slot['route_variant_count'],2);self.assertEqual(slot['observation_count'],3)
  self.assertEqual(sum(len(r['observations']) for r in slot['routes']),3)
 def test_busy_day_counts_unique_date_flight_bins_not_duplicate_rows(self):
  e=self.get();self.assertEqual(e['busy_days']['peak_distinct_flights'],2);self.assertEqual(e['busy_days']['days'][0]['distinct_flights'],2)
  self.assertEqual({r['flight'] for r in e['busy_days']['days'][0]['flights']},{'SU1','SU2'});self.assertEqual(e['date_flight_bins'],3)
 def test_incomplete_route_kept_without_inventing_conflict(self):
  e=self.get();self.assertEqual(e['source_observations'],5);self.assertEqual(e['route_conflicts']['count'],1)
 def test_unknown_component_and_unit_do_not_leak_other_records(self):
  e=build_evidence(self.path,['absent'],['absent'])
  self.assertEqual(e['documents']['components'],[]);self.assertEqual(e['date_flight_bins'],0);self.assertEqual(e['busy_days']['peak_distinct_flights'],0)
if __name__=='__main__':unittest.main(verbosity=2)
