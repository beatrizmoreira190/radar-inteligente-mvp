const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

// Dados Abertos Compras.gov.br (Módulo Legado)
const BASE = 'https://dadosabertos.compras.gov.br';
const ENDPOINT = '/modulo-legado/1_consultarLicitacao';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function upsertOpportunity(o) {
  const { error } = await sb
    .from('opportunities')
    .upsert(o, { onConflict: 'portal,notice_number,agency' });
  if (error) throw error;
}

function mapLicRow(row) {
  const title = row.objeto || row.informacoes_gerais || 'Sem título';
  const modality = row.nome_modalidade || null;
  const notice = String(row.numero_aviso ?? row.identificador ?? row.id_compra ?? '');
  const agency = row.uasg ? `UASG ${row.uasg}` : null;
  const state  = row.uf || null;
  const deadline = row.data_abertura_proposta || row.data_entrega_proposta || null;

  return {
    title,
    portal: 'COMPRASGOV',
    agency,
    state,
    city: null,
    modality,
    notice_number: notice,
    link: null,
    deadline_date: deadline ? String(deadline).slice(0,10) : null,
    status: 'monitorando',
    updated_at: new Date().toISOString()
  };
}

async function fetchPage(page = 1, params = {}) {
  const url = new URL(BASE + ENDPOINT);
  url.searchParams.set('pagina', String(page));
  url.searchParams.set('tamanhoPagina', '100');
  if (params.dt_ano_aviso) url.searchParams.set('dt_ano_aviso', String(params.dt_ano_aviso));
  const resp = await axios.get(url.toString(), { timeout: 30000, headers: { Accept: '*/*' } });
  return resp.data;
}

(async () => {
  const year = new Date().getFullYear();
  let page = 1, inserted = 0, updated = 0;

  try {
    while (true) {
      const data = await fetchPage(page, { dt_ano_aviso: year });
      const rows = data.resultado || [];
      for (const row of rows) {
        const o = mapLicRow(row);
        const before = await sb.from('opportunities')
          .select('id')
          .eq('portal', o.portal).eq('notice_number', o.notice_number).eq('agency', o.agency)
          .maybeSingle();
        await upsertOpportunity(o);
        if (before.data?.id) updated++; else inserted++;
      }
      const rest = data.paginasRestantes ?? 0;
      if (!rest || rest <= 0) break;
      page++;
      await sleep(500);
    }
    await sb.from('ingestion_logs').insert({
      source: 'COMPRASGOV', params: { year }, inserted_count: inserted, updated_count: updated
    });
    console.log(`OK: inseridos ${inserted}, atualizados ${updated}`);
  } catch (e) {
    console.error('Erro:', e?.message);
    await sb.from('ingestion_logs').insert({
      source: 'COMPRASGOV', params: { page }, error: String(e?.message || e)
    });
    process.exit(1);
  }
})();
