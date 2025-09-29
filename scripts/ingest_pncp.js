// scripts/ingest_pncp.js — coleta via PNCP (consulta pública)

const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

const PNCP_BASE = 'https://pncp.gov.br/pncp-api/consultas/v1';
const PATH = '/compras';

const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const pick  = (...vals)=>{ for (const v of vals){ if (v!==undefined && v!==null && v!=='') return v; } return null; };

async function upsert(o){
  const { error } = await sb.from('opportunities').upsert(o, { onConflict:'portal,notice_number,agency' });
  if (error) throw error;
}

function mapItem(it){
  const title    = pick(it.objetoCompra, it.objeto, it.descricao, 'Sem título');
  const notice   = pick(it.numeroCompra, it.numeroEdital, it.identificador, null);
  const agency   = pick(it.orgaoNome, it.orgao?.nome, it.unidadeGestoraNome);
  const state    = pick(it.uf, it.orgao?.uf, it.unidadeGestoraUf);
  const modality = pick(it.modalidade, it.modalidadeNome);
  const link     = pick(it.urlPncp, it.urlPortal, null);
  const deadline = pick(it.dataAberturaProposta, it.dataLimite, it.dataSessaoPublica);

  return {
    title,
    portal: 'PNCP',
    agency,
    state,
    city: null,
    modality,
    notice_number: notice ? String(notice) : null,
    link,
    deadline_date: deadline ? String(deadline).slice(0,10) : null,
    status: 'monitorando',
    updated_at: new Date().toISOString()
  };
}

// >>> ESTE É O fetchPage que você pediu <<<
async function fetchPage(pagina=0){
  const url = new URL(PNCP_BASE + PATH);
  url.searchParams.set('pagina', String(pagina));
  url.searchParams.set('tamanho', '50');
  url.searchParams.set('ano', String(new Date().getFullYear()));
  // filtros extras (opcional):
  // url.searchParams.set('uf', 'SP');
  // url.searchParams.set('palavraChave', 'livro');
  const resp = await axios.get(url.toString(), {
    timeout: 30000,
    headers:{ 'Accept':'application/json', 'User-Agent':'Radar-Inteligente-MVP/1.0' }
  });
  return resp.data;
}

(async ()=>{
  let page = 0, inserted = 0, updated = 0;

  try {
    while (true){
      const data = await fetchPage(page);
      const items = Array.isArray(data?.content) ? data.content :
                    Array.isArray(data?.items)   ? data.items   : [];
      if (!items.length) break;

      for (const it of items){
        const o = mapItem(it);
        if ((!o.title || o.title === 'Sem título') && !o.notice_number) continue;

        const before = await sb.from('opportunities')
          .select('id')
          .eq('portal', o.portal)
          .eq('notice_number', o.notice_number)
          .eq('agency', o.agency)
          .maybeSingle();

        await upsert(o);
        if (before.data?.id) updated++; else inserted++;
      }

      const hasNext = (data?.last === false) || (items.length === 50);
      if (!hasNext) break;
      page++;
      await sleep(400);
    }

    if (!inserted && !updated){
      await upsert({
        title:'TESTE — PNCP (validação de escrita)',
        portal:'PNCP',
        agency:'Orgão Exemplo',
        state:'DF',
        modality:'Pregão',
        notice_number:'PNCP-TESTE-0001',
        link:null,
        deadline_date:null,
        status:'monitorando',
        updated_at:new Date().toISOString()
      });
      inserted = 1;
      console.log('PNCP sem retorno útil; inserida linha de TESTE.');
    }

    await sb.from('ingestion_logs').insert({
      source:'PNCP',
      params:{ pageStart:0 },
      inserted_count: inserted,
      updated_count: updated
    });

    console.log(`PNCP — inseridos ${inserted}, atualizados ${updated}`);
  } catch (e){
    console.error('PNCP erro:', e?.response?.status, e?.message);
    await sb.from('ingestion_logs').insert({
      source:'PNCP',
      params:{ fail:true },
      error:String(e?.message||e)
    });
    process.exit(1);
  }
})();
