// scripts/ingest_compras.js — coleta via Compras.gov.br (catálogo v1) + fallback
// Observação: muitos endpoints do Compras exigem sufixo .json.
// Este script usa dois alvos em sequência: (A) /licitacoes/v1/licitacoes.json e (B) /pregoes/v1/pregoes.json
// Ele pagina por offset/limit e faz UPSERT no Supabase.

const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

// Alvos (ordem de tentativa)
const TARGETS = [
  { base: 'https://compras.dados.gov.br', path: '/licitacoes/v1/licitacoes.json', kind: 'LICITACOES' },
  { base: 'https://compras.dados.gov.br', path: '/pregoes/v1/pregoes.json',       kind: 'PREGOES'    },
];

const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));

async function upsert(o){
  const { error } = await sb
    .from('opportunities')
    .upsert(o, { onConflict: 'portal,notice_number,agency' });
  if (error) throw error;
}

function pick(v1, ...rest){ for (const v of [v1, ...rest]) if (v !== undefined && v !== null && v !== '') return v; return null; }

function mapItem(it, kind){
  // Campos comuns observados no catálogo v1 do Compras:
  const title     = pick(it.objeto, it.descricao, it.informacoes_gerais, 'Sem título');
  const modality  = pick(it.modalidade, it.nome_modalidade);
  const notice    = String(pick(it.numero, it.numero_aviso, it.codigo, it.identificador, it.id_compra, ''));
  const agency    = pick(it.uasg_nome, it.uasg && `UASG ${it.uasg}`);
  const state     = pick(it.uf, it.estado);
  const deadline  = pick(it.data_abertura_proposta, it.data_entrega_proposta, it.data_abertura, it.data_sessao);
  const link      = it._links?.self?.href || it._links?.self || null;

  return {
    title,
    portal: 'COMPRASGOV',
    agency,
    state,
    city: null,
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
  // Cabeçalhos: aceitar JSON e definir um User-Agent "educado"
  const resp = await axios.get(url.toString(), {
    timeout: 30000,
    headers: { 'Accept': 'application/json', 'User-Agent': 'Radar-Inteligente-MVP/1.0 (contato@exemplo.com)' }
  });
  return resp.data;
}

(async ()=>{
  let inserted = 0, updated = 0;
  try {
    let ok = false;

    for (const t of TARGETS){
      let offset = 0, limit = 50;
      let round = 0;
      try {
        while (round < 10) { // até 500 itens por alvo (MVP)
          const data = await fetchPage(t.base, t.path, offset, limit);
          // Estruturas típicas: raiz pode ter "licitacoes" ou "pregoes", senão caímos em genéricos.
          const items = data.licitacoes || data.pregoes || data.items || data.resultado || data._embedded || [];
          // Em alguns catálogos, vem objeto com arrays dentro (ex.: data._embedded.licitacoes)
          const arr = Array.isArray(items) ? items :
                      Array.isArray(items?.licitacoes) ? items.licitacoes :
                      Array.isArray(items?.pregoes)     ? items.pregoes : [];

          if (!arr.length) break;

          for (const it of arr){
            const o = mapItem(it, t.kind);
            const before = await sb.from('opportunities')
              .select('id').eq('portal',o.portal).eq('notice_number',o.notice_number).eq('agency',o.agency)
              .maybeSingle();
            await upsert(o);
            if (before.data?.id) updated++; else inserted++;
          }

          if (arr.length < limit) break;
          offset += limit; round++;
          await sleep(400);
        }
        if (inserted || updated) { ok = true; break; }
      } catch (e) {
        // Continua para o próximo alvo
