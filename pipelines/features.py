def build_tables(db):
    db.execute("""CREATE TABLE unit_members AS
      SELECT 'profile:'||e.entity_id unit_id,e.entity_id,'source_profile' unit_kind,
        e.source profile_source,a.status linkage_status
      FROM entities e JOIN assignments a USING(entity_id)
      WHERE e.entity_kind IN ('native_profile','native_user')
      UNION ALL
      SELECT 'group:'||a.provisional_group_id,a.entity_id,'preliminary_group',NULL,a.status
      FROM assignments a WHERE a.status='provisional_consistent'
        AND a.provisional_group_id IS NOT NULL""")
    db.execute("""CREATE TABLE units AS SELECT unit_id,unit_kind,
      coalesce(min(profile_source),string_agg(DISTINCT e.source,'+' ORDER BY e.source)) source_signature,
      count(*) entity_members, min(linkage_status) linkage_status
      FROM unit_members m JOIN entities e USING(entity_id) GROUP BY unit_id,unit_kind""")
    db.execute("""CREATE TABLE unit_observations AS SELECT m.unit_id,o.*,
      CASE WHEN date_key IS NOT NULL AND flight_key IS NOT NULL THEN
        sha256(to_json([m.unit_id,cast(date_key AS VARCHAR),flight_key])) END slot_id
      FROM observations o JOIN unit_members m USING(entity_id)""")
    db.execute("""CREATE TABLE slots AS SELECT unit_id,slot_id,date_key,flight_key,
      count(*) raw_observations,count(DISTINCT source) sources,
      count(DISTINCT to_json([origin_key,destination_key])) FILTER(
        WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL) route_variants,
      CASE WHEN route_variants=1 THEN min(origin_key) FILTER(
        WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL) END origin,
      CASE WHEN route_variants=1 THEN min(destination_key) FILTER(
        WHERE origin_key IS NOT NULL AND destination_key IS NOT NULL) END destination,
      count(DISTINCT local_time_key) known_time_variants
      FROM unit_observations WHERE slot_id IS NOT NULL GROUP BY unit_id,slot_id,date_key,flight_key""")
    db.execute("""CREATE TABLE unit_routes AS SELECT unit_id,origin,destination,
      count(*) flight_days,count(DISTINCT date_key) dates
      FROM slots WHERE route_variants=1 GROUP BY ALL""")
    db.execute("""CREATE TABLE pattern_features AS WITH counts AS (
      SELECT unit_id,count(*) flight_days,count(DISTINCT date_key) active_dates,
        min(date_key) first_date,max(date_key) last_date,sum(raw_observations) raw_observations,
        count(*) FILTER(WHERE route_variants=1) routed_flight_days,
        count(*) FILTER(WHERE route_variants>1) route_conflict_flight_days,
        count(*) FILTER(WHERE known_time_variants>1) different_known_times
      FROM slots GROUP BY unit_id),
      by_day AS (SELECT unit_id,date_key,count(*) n FROM slots GROUP BY ALL),
      rolling AS (SELECT unit_id,sum(n) OVER(PARTITION BY unit_id ORDER BY date_key
        RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW) peak FROM by_day),
      peaks AS (SELECT unit_id,max(peak) peak7_flight_days FROM rolling GROUP BY unit_id),
      by_month AS (SELECT unit_id,date_trunc('month',date_key) period_month,count(*) n FROM slots GROUP BY ALL),
      months AS (SELECT unit_id,max(n) max_month_flight_days FROM by_month GROUP BY unit_id),
      routes AS (SELECT unit_id,count(*) distinct_routes,max(flight_days) top_route_flight_days
        FROM unit_routes GROUP BY unit_id)
      SELECT u.*,coalesce(c.flight_days,0) flight_days,coalesce(c.active_dates,0) active_dates,
        c.first_date,c.last_date,coalesce(c.raw_observations,0) raw_observations,
        coalesce(c.routed_flight_days,0) routed_flight_days,
        coalesce(c.route_conflict_flight_days,0) route_conflict_flight_days,
        coalesce(c.different_known_times,0) different_known_times,
        coalesce(r.distinct_routes,0) distinct_routes,coalesce(r.top_route_flight_days,0) top_route_flight_days,
        coalesce(p.peak7_flight_days,0) peak7_flight_days,coalesce(m.max_month_flight_days,0) max_month_flight_days,
        c.routed_flight_days::DOUBLE/nullif(c.flight_days,0) route_coverage,
        r.top_route_flight_days::DOUBLE/nullif(c.routed_flight_days,0) top_route_share,
        p.peak7_flight_days::DOUBLE/nullif(c.flight_days,0) peak7_share,
        m.max_month_flight_days::DOUBLE/nullif(c.flight_days,0) max_month_share,
        u.unit_kind||':'||u.source_signature cohort,
        coalesce(c.flight_days>=5 AND c.active_dates>=3 AND
          c.routed_flight_days::DOUBLE/c.flight_days>=0.8,false) eligible
      FROM units u LEFT JOIN counts c USING(unit_id) LEFT JOIN peaks p USING(unit_id)
      LEFT JOIN months m USING(unit_id) LEFT JOIN routes r USING(unit_id)""")
    db.execute("""CREATE TABLE cohort_routes AS SELECT f.cohort,r.origin,r.destination,
      count(*) units_with_route FROM unit_routes r JOIN pattern_features f USING(unit_id)
      WHERE f.eligible GROUP BY ALL""")
    db.execute("""CREATE TABLE route_rarity AS WITH cohorts AS (
      SELECT cohort,count(*) eligible_units FROM pattern_features WHERE eligible GROUP BY cohort)
      SELECT f.unit_id,sum(r.flight_days * -ln(c.units_with_route::DOUBLE/(n.eligible_units+1)))/sum(r.flight_days) route_rarity,
        sum(r.flight_days) FILTER(WHERE (c.units_with_route-1)::DOUBLE/nullif(n.eligible_units-1,0)<0.01)::DOUBLE/
          sum(r.flight_days) rare_route_share,n.eligible_units
      FROM pattern_features f JOIN unit_routes r USING(unit_id)
      JOIN cohort_routes c ON f.cohort=c.cohort AND r.origin=c.origin AND r.destination=c.destination
      JOIN cohorts n ON f.cohort=n.cohort WHERE f.eligible GROUP BY f.unit_id,n.eligible_units""")
    db.execute("""CREATE TABLE all_features AS SELECT f.*,r.route_rarity,
      CASE WHEN f.eligible THEN coalesce(r.rare_route_share,0) END rare_route_share,r.eligible_units,
      coalesce(f.eligible AND r.eligible_units>=200,false) model_eligible,
      ln(1+f.flight_days) log_flight_days,ln(1+f.distinct_routes) log_routes
      FROM pattern_features f LEFT JOIN route_rarity r USING(unit_id)""")
