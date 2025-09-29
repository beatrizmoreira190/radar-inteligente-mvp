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
  url.searchParams.set('limit',  String(lim
