import argparse
import csv
import hashlib
import io
import json
import math
import os
import platform
import re
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from itertools import combinations, islice
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
BUCKET = 'hh-lake'
KEY = 'it_vacancies_2024.csv'
REGIONS = ['Москва', 'Санкт-Петербург', 'Новосибирск']
ROLE = 'Программист, разработчик'
PATTERN = r'(^|[^a-z0-9а-яё])java([^a-z0-9а-яё]|$)'
CHUNK_ROWS = 2000
WORKERS = 2


def save(path, obj):
    p = ROOT / path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding='utf-8')


def load(path):
    return json.loads((ROOT / path).read_text(encoding='utf-8'))


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda: f.read(1024*1024), b''):
            h.update(b)
    return h.hexdigest()


def credentials():
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
    return (os.getenv('S3_ENDPOINT', 'http://127.0.0.1:19000'),
            os.environ['RUSTFS_ACCESS_KEY'], os.environ['RUSTFS_SECRET_KEY'])


def s3():
    import boto3
    from botocore.config import Config
    endpoint, access, secret = credentials()
    return boto3.client('s3', endpoint_url=endpoint, aws_access_key_id=access,
         aws_secret_access_key=secret, region_name='us-east-1',
         config=Config(signature_version='s3v4', s3={'addressing_style':'path'},
                       request_checksum_calculation='when_required',
                       response_checksum_validation='when_required', read_timeout=180))


def duck():
    import duckdb
    c = duckdb.connect()
    c.execute("SET threads=2; SET memory_limit='2GB'; LOAD httpfs")
    endpoint, access, secret = credentials()
    u = urlparse(endpoint)
    literal = lambda x: "'"+x.replace("'", "''")+"'"
    c.execute(f"""CREATE SECRET local_s3 (TYPE s3,KEY_ID {literal(access)},
       SECRET {literal(secret)},ENDPOINT {literal(u.netloc)},
       USE_SSL {str(u.scheme=='https').lower()},URL_STYLE 'path',REGION 'us-east-1')""")
    return c


def setup():
    p = ROOT / 'data' / KEY
    client = s3()
    if BUCKET not in {b['Name'] for b in client.list_buckets()['Buckets']}:
        client.create_bucket(Bucket=BUCKET)
    expected = digest(p)
    client.upload_file(str(p), BUCKET, KEY, ExtraArgs={'Metadata':{'sha256':expected}})
    head = client.head_object(Bucket=BUCKET, Key=KEY)
    h = hashlib.sha256()
    r = client.get_object(Bucket=BUCKET, Key=KEY)
    with r['Body'] as body:
        for b in iter(lambda: body.read(1024*1024),b''):
            h.update(b)
    assert h.hexdigest() == expected and head['ContentLength'] == p.stat().st_size
    save('evidence/source.json', dict(
        url='https://drive.google.com/file/d/1rLEu3N2isIBhAkK1EcSJ2482ouOPAots/view',
        filename=KEY, bytes=p.stat().st_size, sha256=expected, s3_get_hash_equal=True,
        etag=head['ETag'], bucket=BUCKET, variant=ROLE+' на Java'))
    import duckdb
    duckdb.connect().execute('INSTALL httpfs')
    print(json.dumps(load('evidence/source.json'),ensure_ascii=False,indent=2),flush=True)


def map_chunk(rows, fail=False):
    if fail:
        raise RuntimeError('Test: mapper failed before emitting its result')
    counts, sizes, ids = Counter(), Counter(), []
    for row in rows:
        if row['role'] != ROLE or not re.search(PATTERN,row['name'],re.I):
            continue
        ids.append(row['source']+':'+row['vacancy_id'])
        skills = sorted({s.strip().lower() for s in row['key_skills'].split(';') if s.strip()})
        sizes[len(skills)] += 1
        for k in (1,2,3):
            counts.update(combinations(skills,k))
    return counts,sizes,ids


def mapreduce(test_failure=False):
    csv.field_size_limit(100_000_000)
    response = s3().get_object(Bucket=BUCKET,Key=KEY)
    total, sizes, ids = Counter(), Counter(), []
    chunks = retries = rows_read = 0
    with (io.TextIOWrapper(response['Body'],encoding='utf-8-sig',newline='') as text,
          ProcessPoolExecutor(max_workers=WORKERS) as pool):
        reader = csv.DictReader(text)
        while True:
            batch = []
            for _ in range(WORKERS*2):
                rows = list(islice(reader,CHUNK_ROWS))
                if not rows:
                    break
                rows_read += len(rows)
                batch.append([{k:r[k] for k in ('source','vacancy_id','name','role','key_skills')} for r in rows])
            if not batch:
                break
            futures = [pool.submit(map_chunk,r,test_failure and chunks==0 and i==0) for i,r in enumerate(batch)]
            for rows,future in zip(batch,futures):
                try:
                    counts,hist,selected = future.result()
                except RuntimeError as exc:
                    if not test_failure or retries:
                        raise
                    print(str(exc)+'; retry the uncommitted chunk',flush=True)
                    retries += 1
                    counts,hist,selected = pool.submit(map_chunk,rows).result()
                total.update(counts)
                sizes.update(hist)
                ids.extend(selected)
                chunks += 1
    assert len(ids)==len(set(ids)), 'Duplicate vacancy IDs: define a deduplication policy before counting'
    result = {str(k):[dict(skills=list(s),frequency=n) for s,n in sorted(total.items()) if len(s)==k]
              for k in (1,2,3)}
    return dict(counts=result,ids=sorted(ids),stats=dict(
        source_rows=rows_read,chunks=chunks,chunk_rows=CHUNK_ROWS,workers=WORKERS,
        selected_rows=len(ids),stack_sizes=dict(sorted(sizes.items())),retries=retries,
        input_etag=response['ETag'],input_bytes=response['ContentLength']))


def stack_sql(k, with_id=False):
    names = 'abc'[:k]
    joins = ' '.join(f'JOIN expanded {n} ON {n}.id=a.id AND {n}.pos>{names[i-1]}.pos'
                     for i,n in enumerate(names) if i)
    stack = '['+','.join(n+'.skill' for n in names)+']'
    start = 'WITH expanded AS (SELECT id,unnest(skills) AS skill,generate_subscripts(skills,1) AS pos FROM jobs) '
    if with_id:
        return start+f'SELECT a.id,{stack} AS skills,{k} AS k FROM expanded a '+joins
    return start+f'SELECT {stack} AS skills,count(*) AS frequency FROM expanded a '+joins+' GROUP BY ALL ORDER BY skills'


def sql_counts(materialize=False):
    c = duck()
    try:
        c.execute(f"CREATE {'TABLE' if materialize else 'VIEW'} jobs AS "+(ROOT/'java.sql').read_text(encoding='utf-8'))
        ids = [r[0] for r in c.execute('SELECT id FROM jobs ORDER BY id').fetchall()]
        assert len(ids)==len(set(ids))
        counts = {}
        for k in (1,2,3):
            counts[str(k)] = [dict(skills=s,frequency=n) for s,n in c.execute(stack_sql(k)).fetchall()]
        return dict(counts=counts,ids=ids,stats=dict(selected_rows=len(ids)))
    finally:
        c.close()


def worker(method, output):
    start = time.perf_counter()
    if method in ('mapreduce','failure'):
        r = mapreduce(method=='failure')
    else:
        r = sql_counts(materialize=method=='duckdb_optimized')
    r['compute_seconds'] = time.perf_counter()-start
    save(output,r)
    print(f'{method}: {len(r["ids"])} Java vacancies',flush=True)
    for k,rows in r['counts'].items():
        print(f'k={k}: unique={len(rows)}, occurrences={sum(x["frequency"] for x in rows)}',flush=True)


def measure(method, attempt):
    import psutil
    output=f'evidence/{method}-{attempt}.json'
    logfile=ROOT/f'evidence/{method}-{attempt}.log'
    start=time.perf_counter()
    peak=samples=0
    with logfile.open('w',encoding='utf-8') as log:
        p=subprocess.Popen([sys.executable,'-X','utf8','lab.py','worker',method,output],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
        process=psutil.Process(p.pid)
        while p.poll() is None:
            rss=0
            try:
                for item in [process]+process.children(recursive=True):
                    try:rss+=item.memory_info().rss
                    except psutil.NoSuchProcess:pass
            except psutil.NoSuchProcess:pass
            peak=max(peak,rss)
            samples+=1
            time.sleep(.01)
    seconds=time.perf_counter()-start
    print(logfile.read_text(encoding='utf-8'),end='',flush=True)
    if p.returncode:
        raise RuntimeError(f'{method} failed; see {logfile}')
    row=dict(method=method,attempt=attempt,seconds=seconds,peak_rss_bytes=peak,samples=samples)
    print(json.dumps(row),flush=True)
    return row,load(output)


def benchmark():
    import psutil,duckdb
    expected_input=load('evidence/source.json')
    head=s3().head_object(Bucket=BUCKET,Key=KEY)
    assert head['ETag']==expected_input['etag'] and head['ContentLength']==expected_input['bytes']
    save('evidence/environment.json',dict(os=platform.platform(),python=sys.version,
        cpu=platform.processor(),physical_cores=psutil.cpu_count(logical=False),
        logical_cores=psutil.cpu_count(),ram_bytes=psutil.virtual_memory().total,duckdb=duckdb.__version__,
        memory='Peak sampled sum of Python worker + mapper RSS; polling 10 ms; RustFS not included',
        timing='Fresh process wall time: imports, S3 reads, calculation, JSON serialization',
        cache='OS and RustFS caches not cleared; methods sequential; rotated order; first trial included'))
    methods=['mapreduce','duckdb','duckdb_optimized']
    runs=[]
    reference=None
    for attempt in range(1,4):
        for method in methods[attempt-1:]+methods[:attempt-1]:
            metric,r=measure(method,attempt)
            runs.append(metric)
            if reference is None:reference=r
            assert r['ids']==reference['ids'], 'Different selected vacancy IDs'
            assert r['counts']==reference['counts'], 'Frequency dictionaries are not equal'
            save('results/'+method+'.json',r)
            save('evidence/profiling.json',runs)
    failure_metric,failure=measure('failure',1)
    assert failure['stats']['retries']==1
    assert failure['ids']==reference['ids'] and failure['counts']==reference['counts']
    hist=load('results/mapreduce.json')['stats']['stack_sizes']
    checks=[]
    for k in (1,2,3):
        occurrences=sum(row['frequency'] for row in reference['counts'][str(k)])
        expected=sum(n*math.comb(int(m),k) for m,n in hist.items() if int(m)>=k)
        assert occurrences==expected
        checks.append(dict(k=k,unique=len(reference['counts'][str(k)]),occurrences=occurrences,
                           combinations_formula=expected,full_dictionary_equal=True))
    save('evidence/verification.json',dict(selected_rows=len(reference['ids']),successful_trials=9,
        selected_ids_equal=True,checks=checks,mapper_failure_retried=True,
        retry_result_equal=True,failure_measurement=failure_metric))
    client=s3()
    uploaded=[]
    for method in ('mapreduce','duckdb','duckdb_optimized'):
        for k,rows in load('results/'+method+'.json')['counts'].items():
            key=f'results/java/{method}/skills_{k}.json'
            body=json.dumps(rows,ensure_ascii=False).encode('utf-8')
            client.put_object(Bucket=BUCKET,Key=key,Body=body,ContentType='application/json')
            with client.get_object(Bucket=BUCKET,Key=key)['Body'] as stored:
                assert hashlib.sha256(stored.read()).digest()==hashlib.sha256(body).digest()
            uploaded.append(dict(key=key,bytes=len(body),sha256=hashlib.sha256(body).hexdigest(),get_verified=True))
    save('evidence/s3-results.json',uploaded)
    assert client.head_object(Bucket=BUCKET,Key=KEY)['ETag']==expected_input['etag']
    print(json.dumps(load('evidence/verification.json'),ensure_ascii=False,indent=2),flush=True)


def records(c,query,parameters=None):
    cursor=c.execute(query,parameters or [])
    names=[x[0] for x in cursor.description]
    return [dict(zip(names,row)) for row in cursor.fetchall()]


def export_csv(name,rows):
    if not rows:return
    with (ROOT/'results'/name).open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow({k:' + '.join(v) if isinstance(v,list) else v for k,v in row.items()})


def analyze():
    c=duck()
    try:
        c.execute('CREATE TABLE jobs AS '+(ROOT/'java.sql').read_text(encoding='utf-8'))
        c.execute("""CREATE TABLE salary_jobs AS SELECT *,
           CASE WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from+salary_to)/2
                ELSE coalesce(salary_from,salary_to) END AS salary
           FROM jobs WHERE currency IN ('RUR','RUB') AND gross='false'
           AND coalesce(salary_from,salary_to)>0
           AND (salary_from IS NULL OR salary_to IS NULL OR salary_from<=salary_to)""")
        c.execute('CREATE TABLE stacks AS '+ ' UNION ALL '.join('('+stack_sql(k,True)+')' for k in (1,2,3)))
        frequencies={tuple(r['skills']):r['frequency'] for rows in load('results/mapreduce.json')['counts'].values() for r in rows}
        salaries=records(c,"""SELECT s.skills,count(*) AS n,median(j.salary) AS median_salary
             FROM stacks s JOIN salary_jobs j USING(id) GROUP BY s.skills""")
        by_stack={tuple(r['skills']):r for r in salaries}
        effects=[]
        for target,row in by_stack.items():
            if len(target)==1:continue
            base=min(combinations(target,len(target)-1),key=lambda x:(-frequencies[x],x))
            b=by_stack.get(base)
            if b is None or min(row['n'],b['n'])<5:continue
            effects.append(dict(k=len(target),skills=list(target),base=list(base),
                base_frequency=frequencies[base],frequency=frequencies[target],
                base_salary_n=b['n'],salary_n=row['n'],base_median=b['median_salary'],
                median=row['median_salary'],delta=row['median_salary']-b['median_salary'],
                delta_percent=100*(row['median_salary']/b['median_salary']-1)))
        effects.sort(key=lambda r:(r['k'],-r['delta_percent'],r['skills']))
        summary=records(c,"""SELECT j.area AS region,count(*) AS vacancies,
             count(*) FILTER (WHERE len(j.skills)=0) AS without_skills,
             count(p.salary) AS salary_n,median(p.salary) AS median_salary
             FROM jobs j LEFT JOIN salary_jobs p USING(id) GROUP BY j.area ORDER BY vacancies DESC""")
        selected_regions=[r for r in summary if r['region'] in REGIONS]
        regional=records(c,"""SELECT j.area AS region,s.k,s.skills,count(*) AS vacancies,
             count(p.salary) AS salary_n,median(p.salary) AS median_salary
             FROM stacks s JOIN jobs j USING(id) LEFT JOIN salary_jobs p USING(id)
             GROUP BY ALL ORDER BY j.area,s.k,vacancies DESC,s.skills""")
        region_top=[]
        for region in REGIONS:
            for k in (1,2,3):
                region_top.extend([r for r in regional if r['region']==region and r['k']==k][:5])
        counts=records(c,"""SELECT j.area AS region,s.k,count(*) AS occurrences,count(DISTINCT s.skills) AS unique_stacks
           FROM stacks s JOIN jobs j USING(id) GROUP BY ALL ORDER BY j.area,s.k""")
        counts=[r for r in counts if r['region'] in REGIONS]
        overall=records(c,'SELECT count(*) AS n,median(salary) AS median_salary FROM salary_jobs')[0]
        salary_breakdown=records(c,'SELECT currency,gross,count(*) AS vacancies FROM jobs GROUP BY ALL ORDER BY vacancies DESC')
        extremes={}
        for k in (2,3):
            group=[r for r in effects if r['k']==k]
            extremes[str(k)]=dict(increase=sorted([r for r in group if r['delta']>0],key=lambda r:(-r['delta_percent'],r['skills']))[:5],
                                 decrease=sorted([r for r in group if r['delta']<0],key=lambda r:(r['delta_percent'],r['skills']))[:5])
        cohorts=[]
        for k in (2,3):
            for direction in ('increase','decrease'):
                if not extremes[str(k)][direction]:continue
                item=extremes[str(k)][direction][0]
                groups={label:records(c,"""SELECT experience,count(*) AS n,median(salary) AS median_salary
                         FROM salary_jobs WHERE list_has_all(skills,?) GROUP BY experience ORDER BY n DESC""",[skills])
                        for label,skills in [('base',item['base']),('target',item['skills'])]}
                cohorts.append(dict(k=k,direction=direction,skills=item['skills'],groups=groups))
        result=dict(overall=overall,salary_breakdown=salary_breakdown,extremes=extremes,
                    regions=selected_regions,regional_stacks=region_top,regional_counts=counts,cohorts=cohorts)
        save('results/analysis.json',result)
        export_csv('salary_effects.csv',effects)
        export_csv('regions.csv',selected_regions)
        export_csv('regional_stacks.csv',region_top)
        export_csv('regional_counts.csv',counts)
        print('Salary sample:',overall,'\nRegions:',selected_regions,flush=True)
    finally:c.close()


def main():
    p=argparse.ArgumentParser()
    p.add_argument('stage',choices=['setup','benchmark','analyze','worker','all'])
    p.add_argument('method',nargs='?')
    p.add_argument('output',nargs='?')
    a=p.parse_args()
    (ROOT/'evidence').mkdir(exist_ok=True)
    (ROOT/'results').mkdir(exist_ok=True)
    if a.stage=='worker':worker(a.method,a.output);return
    if a.stage in ('setup','all'):setup()
    if a.stage in ('benchmark','all'):benchmark()
    if a.stage in ('analyze','all'):analyze()


if __name__=='__main__':
    main()
