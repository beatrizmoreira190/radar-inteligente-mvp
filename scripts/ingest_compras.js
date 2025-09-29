// scripts/ingest_compras.js — coleta via Compras.gov.br (catálogo v1) + fallback
// Usa .json nos endpoints e pagina por offset/limit. Faz UPSERT no Supabase.

const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

// Alvos (ordem de tentativa)
const TARGETS = [
  { base: 'https://compras.dados.gov.br', path: '/licitacoes/v1/licitacoes.json', kind: 'LICITACOES' },
  { base: 'https://compras.dados.gov.br', path: '/pregoes/v1/pregoes.json',       kind: 'PREGOES'    },
];

const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const pick  = (...vals)=>{ for (const v of vals){ if (v!==undefined && v!==null && v!=='') return v; } return null; };

async function upsert(o){
  const { error } = await sb
    .from('opportunities')
    .upsert(o, { onConflict: 'portal,notice_number,agency' });
  if (error) throw error;
}

function mapItem(it){
  // Tenta várias chaves comuns do catálogo v1 (e _embedded)
  const title = pick(
    it.objeto, it.descricao, it.informacoes_gerais,
    it._embedded?.objeto, it._embedded?.descricao,
    'Sem título'
  );

  const modality = pick(it.modalidade, it.nome_modalidade, it._embedded?.modalidade_nome);

  const notice = String(pick(
    it.numero, it.numero_aviso, it.identificador, it.id_compra, it.codigo, it.numero_pregao, ''
  ));

  const agency = pick(
    it.uasg_nome, it.orgao_nome,
    it.uasg && `UASG ${it.uasg}`,
    it._embedded?.uasg_nome
  );

  const state = pick(it.uf, it.estado, it._embedded?.uf);
  const city  = pick(it.municipio_nome, it.cidade, it._embedded?.municipio_nome);

  const deadline = pick(
    it.data_abertura_proposta, it.data_entrega_proposta,
    it.data_abertura, it.data_sessao
  );

  const link = pick(it._links?.self?.href, it._links?.self, it.url_detalhe);

  return {
    title,
    portal: 'COMPRASGOV',
    agency,
    state,
    city,
    modality,
    notice_number: notice,
    link,
    deadline_date: deadline ? String(deadline).slice(0,10) : null,
    status: 'monitorando',
    updated_at: new Date().toISOString()
  };
}

async function fetchPage(base, path, offset=0, limit=50){
  const url = new URL(base + path);
  url.searchParams.set('offset', String(offset));
  url.searchParams.set('limit',  String(limit));
  const resp = await axios.get(url.toString(), {
    timeout: 30000,
    headers: { 'Accept': 'application/json', 'User-Agent': 'Radar-Inteligente-MVP/1.0 (contato@exemplo.com)' }
  });
  return resp.data;
}

(async ()=>{
  let inserted = 0, updated = 0;

  try {
    for (const t of TARGETS){
      let offset = 0, limit = 50, round = 0;

      try {
        while (round < 10) { // até 500 itens por alvo (MVP)
          const raw = await fetchPage(t.base, t.path, offset, limit);

          // Normaliza onde pode estar a lista
          const arr =
            Array.isArray(raw.licitacoes) ? raw.licitacoes :
            Array.isArray(raw.pregoes)    ? raw.pregoes    :
            Array.isArray(raw.items)      ? raw.items      :
            Array.isArray(raw.resultado)  ? raw.resultado  :
            Array.isArray(raw._embedded?.licitacoes) ? raw._embedded.licitacoes :
            Array.isArray(raw._embedded?.pregoes)    ? raw._embedded.pregoes    :
            [];

          if (!arr.length) break;

          for (const it of arr){
            const o = mapItem(it);
            // evita linhas totalmente vazias
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

          if (arr.length < limit) break;
          offset += limit; round++;
          await sleep(400);
        }

        if (inserted || updated) break; // já deu bom em um dos alvos
      } catch (e) {
        console.log(`Alvo ${t.path} falhou: ${e?.response?.status || ''} ${e?.message}`);
        continue; // tenta o próximo alvo
      }
    }

    // fallback: pelo menos 1 linha p/ validar escrita
    if (!inserted && !updated){
      await upsert({
        title: 'TESTE — Inserido pelo workflow (COMPRASGOV)',
        portal: 'COMPRASGOV',
        agency: 'UASG 000000',
        state: 'DF',
        modality: 'Pregão',
        notice_number: 'TESTE-0001',
        link: null,
        deadline_date: null,
        status: 'monitorando',
        updated_at: new Date().toISOString()
      });
      inserted = 1;
      console.log('Nenhum dado retornado dos endpoints. Inserida linha de TESTE para validar escrita.');
    }

    await sb.from('ingestion_logs').insert({
      source:'COMPRASGOV',
      params:{ targets: TARGETS.map(t=>t.path) },
      inserted_count: inserted,
      updated_count: updated
    });

    console.log(`FINAL: inseridos ${inserted}, atualizados ${updated}`);
  } catch (e){
    console.error('Erro geral:', e?.response?.status, e?.message);
    await sb.from('ingestion_logs').insert({
      source:'COMPRASGOV',
      params:{ fail:true },
      error: String(e?.message || e)
    });
    process.exit(1);
  }
})();
