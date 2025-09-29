const { createClient } = require('@supabase/supabase-js');
const nodemailer = require('nodemailer');

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function getRecipients() {
  return []; // ex.: ['voce@seudominio.com'];
}
async function getRecentOpps(hours = 24) {
  const since = new Date(Date.now() - hours*3600*1000).toISOString();
  const { data, error } = await sb
    .from('opportunities')
    .select('*')
    .gte('updated_at', since)
    .order('deadline_date', { ascending: true });
  if (error) throw error;
  return data || [];
}

(async () => {
  const recipients = await getRecipients();
  if (!recipients.length) { console.log('Sem destinatários (MVP).'); return; }

  const tx = nodemailer.createTransport({
    host: process.env.SMTP_HOST, port: Number(process.env.SMTP_PORT || 587),
    secure: false, auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
  });

  const opps = await getRecentOpps(24);
  const lines = opps.slice(0, 30).map(o =>
    `• [${o.portal}] ${o.title} — prazo: ${o.deadline_date ?? 's/ data'}`
  );
  const text = lines.length ? lines.join('\n') : 'Sem novidades nas últimas 24h.';

  for (const to of recipients) {
    await tx.sendMail({
      from: `"${process.env.MAIL_FROM_NAME || 'Radar'}" <${process.env.MAIL_FROM}>`,
      to, subject: 'Novidades do Radar Inteligente', text
    });
    console.log('Email enviado para', to);
  }
})();
