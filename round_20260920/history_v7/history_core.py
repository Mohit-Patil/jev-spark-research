"""Prospective, pre-execution selection with frozen historical measurements.
No new runtime AQE hook. History contains previous full-query observations only.
All candidate SQL retains AQE. Model inputs never contain evaluation results.
"""
from pathlib import Path
import asyncio, copy, hashlib, json, math, re, statistics, sys, time
import httpx
R=Path(__file__).resolve().parent;ROUND=R.parent;WS=ROUND.parent
sys.path[:0]=[str(ROUND/'production_v3'),str(ROUND/'production_v4'),str(ROUND)]
import parquet_corpus as corpus
from baseline import plan_info, normalize, append
from safe_jev import ENDPOINT, MODEL, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, load_key, validate_response, BudgetError
from authorized_budget import AuthorizedBudget, BatchBudget
ACTIONS=('NATIVE','BROADCAST','MERGE','SHUFFLE_HASH')
POLICIES=('NATIVE','TUNED64','LOCAL_HISTORY','JEV_ZERO','JEV_HISTORY')
TRAIN_BATCHES=('parquet_narrow_v1','parquet_wide_small_v1','parquet_wide_large_v1','parquet_wide_selective_v1')
CASES={
 'lookup_narrow':dict(n=1600000,m=120000,width=128,keep=1000,seed=947,family='LEFT_LOOKUP'),
 'lookup_wide':dict(n=160000,m=320000,width=384,keep=1000,seed=953,family='LEFT_LOOKUP'),
 'lookup_selective':dict(n=2800000,m=320000,width=384,keep=20,seed=967,family='LEFT_LOOKUP'),
 'residual_transfer':dict(n=2200000,m=280000,width=384,keep=30,seed=971,family='LEFT_RESIDUAL')}
CRITERIA={'NATIVE':'Native Spark planning with AQE, initial and adaptive broadcast thresholds 10 MiB.',
 'BROADCAST':'Explicit broadcast-right join hint with native AQE otherwise unchanged.',
 'MERGE':'Explicit sort-merge join hints with native AQE otherwise unchanged.',
 'SHUFFLE_HASH':'Explicit shuffle-hash build-right hint with native AQE otherwise unchanged.',
 'ABSTAIN':'Use NATIVE when the supplied evidence does not justify preferring another strategy.'}
INSTRUCTIONS=('Choose the supplied strategy expected to minimize this entire query execution time on the described local machine. '
 'The decision is BEFORE execution, not a mid-query switch. Consider any supplied historical observations from other data snapshots; '
 'they are noisy measurements, not guarantees. File/footer bytes are not hash-table peak memory; filter fractions are estimates. '
 'Use only the supplied evidence. Return ABSTAIN when evidence is insufficient. No candidate execution outcome for this query is supplied. '
 'The request cost is accounted separately in the measured workflow. Do not assume a hinted strategy is always faster.')

def dump(path,obj):
    path.write_text(json.dumps(obj,indent=2,sort_keys=True,allow_nan=False))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def rows(path):return [json.loads(x) for x in path.read_text().splitlines() if x]
def query(c,arm):
    sql=corpus.query(arm)
    if c.get('family','LEFT_LOOKUP')=='LEFT_RESIDUAL':
        sql=sql.replace('ON f.k=d.k GROUP BY','ON f.k=d.k AND pmod(f.v + d.k, 5) < 3 GROUP BY')
    elif c.get('family','LEFT_LOOKUP')!='LEFT_LOOKUP':raise ValueError('unsupported_family')
    return sql

def oracle(c):
    if c.get('family','LEFT_LOOKUP')=='LEFT_LOOKUP':return corpus.oracle(c)
    result={};cache={}
    for i in range(c['n']):
        k=None if i%31==0 else (i*97+c['seed'])%c['m'];v=(i*13+c['seed'])%997;b=i%7
        match=k is not None and (k*17+c['seed'])%1000<c['keep'] and (v+k)%5<3
        score=0
        if match:
            seg,off=divmod(v%c['width'],64);key=(k,seg)
            if key not in cache:cache[key]=hashlib.sha256(f'{k}:{seg}:{c["seed"]}'.encode()).hexdigest()
            score=ord(cache[key][off])
        a=result.setdefault(b,[0,0,0,0,0]);a[0]+=1;a[1]+=k or 0;a[2]+=v;a[3]+=score;a[4]+=int(match)
    return [[b,*result[b]] for b in sorted(result)]

def footer(spark,directory):
    """Read metadata only, using already-installed Parquet JVM classes."""
    directory=Path(directory).resolve()
    if not directory.is_relative_to(WS.resolve()):raise ValueError('outside_workspace')
    files=sorted(directory.glob('*.parquet'))
    if not 1<=len(files)<=32:raise ValueError('footer_file_bound')
    out={'file_rows':0,'file_bytes':0,'column_uncompressed_bytes':0,'files':len(files),'row_groups':0}
    j=spark._jvm;conf=spark._jsc.hadoopConfiguration()
    for p in files:
        if p.is_symlink():raise ValueError('data_symlink')
        src=j.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(j.org.apache.hadoop.fs.Path(str(p)),conf)
        reader=j.org.apache.parquet.hadoop.ParquetFileReader.open(src)
        try:
            blocks=reader.getFooter().getBlocks().iterator()
            while blocks.hasNext():
                b=blocks.next();out['row_groups']+=1;out['file_rows']+=int(b.getRowCount())
                cols=b.getColumns().iterator()
                while cols.hasNext():out['column_uncompressed_bytes']+=int(cols.next().getTotalUncompressedSize())
        finally:reader.close()
        out['file_bytes']+=p.stat().st_size
    return out

def metadata_features(spark,c,data):
    a=footer(spark,data/'fact');b=footer(spark,data/'dim')
    fraction=c['keep']/1000
    # Estimate from the SQL predicate, NOT an exact post-filter count/scan.
    return {'family':c.get('family','LEFT_LOOKUP'),'left':a,'right':b,
      'filter_sql':f'(k*17+{c["seed"]}) % 1000 < {c["keep"]}',
      'estimated_retained_fraction':fraction,'estimated_build_rows':b['file_rows']*fraction,
      'estimated_build_encoded_bytes':b['column_uncompressed_bytes']*fraction,
      'residual_condition':'pmod(f.v + d.k, 5) < 3' if c.get('family')=='LEFT_RESIDUAL' else None,
      'semantics':'Parquet footer rows/encoded bytes available before execution. Retained fraction is a heuristic for the SQL modular predicate. No post-filter scan. Encoded bytes are not UnsafeRow size or peak memory.'}

def vector(f):
    vals=[f['left']['file_rows'],f['estimated_build_rows'],f['estimated_build_encoded_bytes'],
          f['right']['column_uncompressed_bytes']/max(1,f['right']['file_rows'])]
    if any(not math.isfinite(x) or x<0 for x in vals):raise ValueError('invalid_features')
    return [math.log1p(x) for x in vals]

def local_select(f,history):
    compatible=[e for e in history if e['features']['family']==f['family']]
    if not compatible:return {'choice':'NATIVE','reason':'unseen_family'}
    x=vector(f)
    nearest=min(compatible,key=lambda e:sum((a-b)**2 for a,b in zip(x,vector(e['features']))))
    dist=math.sqrt(sum((a-b)**2 for a,b in zip(x,vector(nearest['features']))))
    times=nearest['candidate_median_s'];best=min(ACTIONS,key=lambda k:times[k]);native=times['NATIVE']
    if dist>4 or native-times[best]<=max(0.05,0.10*native):best='NATIVE'
    return {'choice':best,'reason':'nearest_historical_regime_with_10pct_50ms_margin',
      'historical_id':nearest['historical_id'],'log_distance':dist}

def payload(f,plans,history=None):
    state={'decision':'whole-query strategy BEFORE execution; AQE remains enabled',
      'engine':'Apache Spark 4.0.1','resources':{'master':'local[2]','jvm_heap_max_bytes':2147483648,'shuffle_partitions':32},
      'metadata':copy.deepcopy(f),'candidate_initial_plans':{k:normalize(plans[k]['initial_plan']).replace(str(WS),'<workspace>') for k in ACTIONS},
      'unknown':{'actual_post_filter_rows':None,'peak_hash_table_memory':None,'current_candidate_runtimes':None}}
    if history is not None:
        state['historical_observations']=copy.deepcopy(history)
        state['history_scope']='Four previously completed development snapshots. Full-query medians from three measurements each; no current or evaluation-family outcomes. Not runtime-switch counterfactual labels.'
    return {'model':MODEL,'state':state,'questions':{'plan':{'type':'choice','instructions':INSTRUCTIONS,'criteria':CRITERIA}}}

def encode(p):
    if p.get('model')!=MODEL or set(p.get('questions',{}))!={'plan'}:raise ValueError('payload_shape')
    q=p['questions']['plan']
    if q.get('type')!='choice' or q.get('criteria')!=CRITERIA:raise ValueError('invalid_options')
    b=json.dumps(p,separators=(',',':'),sort_keys=True,allow_nan=False).encode()
    if not 0<len(b)<=MAX_REQUEST_BYTES:raise BudgetError('request_size_bound')
    return b

class DeadlineClient:
    """One event loop and persistent async client per batch. No detached tasks."""
    def __init__(self,budget,log_path,deadline=1.25,transport=None,key_loader=load_key):
        self.budget=budget;self.log_path=log_path;self.deadline=deadline;self.key_loader=key_loader
        limits=httpx.Limits(max_connections=1,max_keepalive_connections=1,keepalive_expiry=60)
        self.runner=asyncio.Runner()
        self.client=httpx.AsyncClient(transport=transport or httpx.AsyncHTTPTransport(retries=0,limits=limits),
          timeout=httpx.Timeout(0.9,connect=0.9,write=0.9,pool=0.9),follow_redirects=False,trust_env=False)
    def close(self):
        self.runner.run(self.client.aclose());self.runner.close()
    def request(self,p,purpose):
        wall=time.perf_counter();b=encode(p)
        if not re.fullmatch('[A-Za-z0-9_-]{1,80}',purpose):raise ValueError('purpose')
        key,meta=self.key_loader()
        if key.encode() in b:raise ValueError('secret_in_payload')
        digest=hashlib.sha256(b).hexdigest();attempt=self.budget.reserve(len(b),digest,purpose)
        rec={'attempt':attempt,'purpose':purpose,'request_sha256':digest,'request_bytes':len(b),
          'deadline_s':self.deadline,'transport':'pooled_async_http1_total_network_deadline'}
        start=time.perf_counter()
        try:
            obj=self.runner.run(self._send(b,key,rec))
            rec.update(validate_response(obj,'plan',CRITERIA));rec['status']='succeeded'
        except Exception as e:
            rec.update(status='failed',choice='ABSTAIN',error_type=type(e).__name__)
        finally:key=None
        rec.update(latency_s=time.perf_counter()-start,adapter_wall_s=time.perf_counter()-wall,completed_unix=time.time())
        if self.log_path is not None:append(Path(self.log_path),rec)
        return rec
    async def _send(self,b,key,rec):
        async with asyncio.timeout(self.deadline):
            async with self.client.stream('POST',ENDPOINT,content=b,
              headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'}) as response:
                rec['http_status']=response.status_code
                if response.status_code!=200:raise ValueError('http_failure')
                buf=bytearray()
                async for chunk in response.aiter_bytes():
                    buf.extend(chunk)
                    if len(buf)>MAX_RESPONSE_BYTES:raise ValueError('response_size_bound')
                return json.loads(buf)

def effective(rec):
    return rec.get('choice') if rec.get('status')=='succeeded' and rec.get('choice') in ACTIONS else 'NATIVE'
