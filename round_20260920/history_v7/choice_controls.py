"""Small synthetic routing/numeric controls, NOT Spark performance evidence.
Permutation of answer labels and option order is fixed before requests.
All new calls use the existing cumulative ledger and bounded async transport.
"""
import asyncio, hashlib, itertools, json, random, statistics, time
from html.parser import HTMLParser
from history_core import R,ROUND,MODEL,ENDPOINT,MAX_REQUEST_BYTES,MAX_RESPONSE_BYTES,load_key,validate_response,AuthorizedBudget,BatchBudget,append,dump,sha
import httpx

class Text(HTMLParser):
    def __init__(self):super().__init__();self.ignore=0;self.parts=[]
    def handle_starttag(self,tag,attrs):
        if tag in ('script','style'):self.ignore+=1
    def handle_endtag(self,tag):
        if tag in ('script','style'):self.ignore=max(0,self.ignore-1)
    def handle_data(self,data):
        if not self.ignore and data.strip():self.parts.append(data.strip())

def cases():
    rng=random.Random(7291);out=[]
    for name in ('A','B','C'):
        options=['A','B','C','ABSTAIN'];rng.shuffle(options)
        out.append({'kind':'literal_route','expected':name,'options':options,
          'state':{'required_choice':name},'instructions':'This is an exact routing control. Return the required_choice supplied in state. Do not predict anything.'})
    for kind,labels in [('generic_minimum',['A','B','C']),('strategy_minimum',['BROADCAST','MERGE','NATIVE'])]:
        for i,values in enumerate(itertools.permutations([0.2,0.8,1.5])):
            options=labels+['ABSTAIN'];rng.shuffle(options)
            timings=dict(zip(labels,values));items=[{'candidate':k,'duration_seconds':v} for k,v in timings.items()];rng.shuffle(items)
            out.append({'kind':kind,'expected':min(timings,key=timings.get),'options':options,
              'state':{'control':'Select the minimum of these supplied exact numbers, not a future runtime prediction.','measurements':items},
              'instructions':'Synthetic numeric control, not a Spark optimization prediction. Select the candidate with the smallest supplied duration_seconds. All numbers are exact and complete. Use ABSTAIN only when a number is missing. Ignore generic assumptions about strategy names.'})
    rng.shuffle(out);return out

async def request(client,p,budget,purpose):
    data=json.dumps(p,separators=(',',':'),allow_nan=False).encode()
    assert 0<len(data)<=MAX_REQUEST_BYTES
    key,_=load_key();assert key.encode() not in data
    options=p['questions']['plan']['criteria'];assert 2<=len(options)<=5 and 'ABSTAIN' in options
    h=hashlib.sha256(data).hexdigest();n=budget.reserve(len(data),h,purpose)
    rec={'attempt':n,'purpose':purpose,'request_sha256':h,'request_bytes':len(data),'deadline_s':3.0}
    start=time.perf_counter()
    try:
        async with asyncio.timeout(3.0):
            async with client.stream('POST',ENDPOINT,content=data,headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'}) as res:
                rec['http_status']=res.status_code
                if res.status_code!=200:raise ValueError('http_status')
                buf=bytearray()
                async for chunk in res.aiter_bytes():
                    buf.extend(chunk)
                    if len(buf)>MAX_RESPONSE_BYTES:raise ValueError('response_bound')
                obj=json.loads(buf)
        rec.update(validate_response(obj,'plan',options));rec['status']='succeeded'
    except Exception as e:rec.update(status='failed',choice='ABSTAIN',error_type=type(e).__name__)
    finally:key=None
    rec.update(latency_s=time.perf_counter()-start,completed_unix=time.time());append(ROUND/'jev_calls.jsonl',rec)
    return rec

async def main_async():
    out=R/'choice_controls_v1';out.mkdir(exist_ok=False);before=AuthorizedBudget().count();budget=BatchBudget(15)
    spec=cases();dump(out/'FROZEN.json',{'created_unix':time.time(),'source_sha256':sha(R/'choice_controls.py'),'cases':spec,'max_calls':15,'interpretation':'Known-answer controls only, no Spark queries or ranking accuracy claim.'})
    records=[];limits=httpx.Limits(max_connections=1,max_keepalive_connections=1,keepalive_expiry=60)
    async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=0,limits=limits),timeout=httpx.Timeout(2.0),trust_env=False,follow_redirects=False) as client:
        for i,c in enumerate(spec):
            criteria={k:('Abstain if required information is absent.' if k=='ABSTAIN' else 'Select candidate '+k+'.') for k in c['options']}
            p={'model':MODEL,'state':c['state'],'questions':{'plan':{'type':'choice','instructions':c['instructions'],'criteria':criteria}}}
            rec=await request(client,p,budget,'h7_control_'+str(i))
            entry={'index':i,'kind':c['kind'],'expected':c['expected'],'correct':rec['status']=='succeeded' and rec['choice']==c['expected'],'record':rec,'payload':p}
            records.append(entry);append(out/'raw.jsonl',entry)
    report={'status':'succeeded','calls':len(records),'new_api_attempts':budget.used,'budget_before':before,'budget':AuthorizedBudget().status(),
      'groups':{kind:{'total':sum(r['kind']==kind for r in records),'valid':sum(r['kind']==kind and r['record']['status']=='succeeded' for r in records),
        'correct':sum(r['kind']==kind and r['correct'] for r in records)} for kind in ('literal_route','generic_minimum','strategy_minimum')},
      'records':[{'kind':r['kind'],'expected':r['expected'],'choice':r['record']['choice'],'status':r['record']['status'],'correct':r['correct']} for r in records],
      'spark_queries':0,'scope':'Controls do not prove future plan-selection quality or general model ability.'}
    dump(out/'COMPLETED.json',report);print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=Text();parser.feed((R/'official_choice.html').read_text());text='\n'.join(parser.parts)
    (R/'official_choice_text.txt').write_text(text)
    pos=text.lower().find('criteria')
    dump(R/'DOC_INSPECTION.json',{'source_sha256':sha(R/'official_choice.html'),'text_characters':len(text),'criteria_excerpt':text[max(0,pos-1000):pos+3500]})
    asyncio.run(main_async())
