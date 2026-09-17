import path from 'node:path';
import fsp from 'node:fs/promises';
import {DATA_DIR} from './config.mjs';

const CUTOFF_MS=1789469100000;
let deletedFiles=0,deletedBytes=0,keptFiles=0;

async function walk(dir){
  let entries;
  try{entries=await fsp.readdir(dir,{withFileTypes:true});}catch(e){if(e?.code==='ENOENT')return;throw e;}
  for(const ent of entries){
    const p=path.join(dir,ent.name);
    if(ent.isDirectory()){await walk(p);continue;}
    if(!ent.isFile())continue;
    if(!(ent.name.endsWith('.spdb.gz')||ent.name.endsWith('.spdb.gz.sha256')))continue;
    const tsText=ent.name.split('.')[0];
    if(!/^\d{13}$/.test(tsText)){keptFiles++;continue;}
    const ts=Number(tsText);
    if(ts<=CUTOFF_MS){
      let size=0;try{size=(await fsp.stat(p)).size;}catch{}
      await fsp.unlink(p);
      deletedFiles++;deletedBytes+=size;
    }else keptFiles++;
  }
}

console.log(JSON.stringify({kind:'prune-start',dataDir:DATA_DIR,cutoffMs:CUTOFF_MS,cutoffIso:new Date(CUTOFF_MS).toISOString()}));
await walk(DATA_DIR);
console.log(JSON.stringify({kind:'prune-done',deletedFiles,deletedBytes,keptFiles,cutoffMs:CUTOFF_MS}));
