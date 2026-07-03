import { PGlite } from '@electric-sql/pglite'
import { installChangeTracking } from '../../src/replication/change-tracker.ts'
const BODY = 'x'.repeat(1024)
function pct(a:number[],p:number){const s=[...a].sort((x,y)=>x-y);return s[Math.min(s.length-1,Math.floor(p*s.length))]}
function stat(n:string,a:number[]){const m=a.reduce((x,y)=>x+y,0)/a.length;console.log(`${n.padEnd(40)} mean=${m.toFixed(3)}ms p50=${pct(a,0.5).toFixed(3)} p95=${pct(a,0.95).toFixed(3)} n=${a.length}`);return m}
async function freshDb(withTriggers:boolean){const db=new PGlite({dataDir:'memory://',relaxedDurability:true});await db.waitReady;await db.exec('CREATE EXTENSION IF NOT EXISTS plpgsql');await db.exec(`CREATE TABLE message(id text primary key, session text, role text, body text, updated_at bigint)`);if(withTriggers)await installChangeTracking(db);return db}

console.log('=== BENCH 1: CDC trigger overhead (direct PGlite, memory://) ===')
const res:Record<string,number>={}
for (const withTriggers of [false, true]) {
  const db = await freshDb(withTriggers)
  const tag = withTriggers ? 'WITH ' : 'NO   '
  { const t:number[]=[]; for(let i=0;i<1500;i++){const s=performance.now();await db.query(`INSERT INTO message(id,session,role,body,updated_at) VALUES($1,$2,$3,$4,$5)`,[`m${i}`,'s','assistant',BODY,Date.now()]);t.push(performance.now()-s)} res[tag+'INSERT']=stat(`${tag}INSERT`,t.slice(100)) }
  { const t:number[]=[]; for(let i=0;i<1500;i++){const s=performance.now();await db.query(`INSERT INTO message(id,session,role,body,updated_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body,updated_at=EXCLUDED.updated_at`,[`m${i}`,'s','assistant',BODY+i,Date.now()]);t.push(performance.now()-s)} res[tag+'UPSERT-change']=stat(`${tag}UPSERT (real change)`,t.slice(100)) }
  { const t:number[]=[]; for(let i=0;i<1500;i++){const s=performance.now();await db.query(`INSERT INTO message(id,session,role,body,updated_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body`,[`m${i}`,'s','assistant',BODY+i,Date.now()]);t.push(performance.now()-s)} res[tag+'UPSERT-noop']=stat(`${tag}UPSERT (no-op)`,t.slice(100)) }
  { const t:number[]=[]; for(let i=0;i<1500;i++){const s=performance.now();await db.query(`SELECT * FROM message WHERE id=$1`,[`m${i%1000}`]);t.push(performance.now()-s)} res[tag+'SELECT']=stat(`${tag}SELECT point`,t.slice(100)) }
  await db.close()
}
console.log('\n--- trigger overhead (WITH - NO) ---')
for (const k of ['INSERT','UPSERT-change','UPSERT-noop','SELECT']) {
  const w=res['WITH '+k], n=res['NO   '+k]
  console.log(`  ${k.padEnd(14)} +${(w-n).toFixed(3)}ms (${((w/n-1)*100).toFixed(0)}% over base ${n.toFixed(3)}ms)`)
}
console.log('DONE')
