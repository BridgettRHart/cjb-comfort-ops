// ═══════════════════════════════════════════════════════════════════════════
//  CJB Comfort — Cloudflare Worker
//  Deploy at: cjb-comfort-proxy.bridgettrhart.workers.dev
//
//  Environment secrets (set in Cloudflare dashboard → Settings → Variables):
//    STRIPE_SECRET_KEY  →  sk_live_...
// ═══════════════════════════════════════════════════════════════════════════

const AIRTABLE_BASE_ID = 'appPv1xZIck89RxL2';
const AIRTABLE_API_KEY = 'patwLtwCfaf4ctJu1.5e01386c6580cb6dae0a160c566563d287f4d54fa7489c73cf5a7a920fbcb17f';
const CALENDLY_TOKEN   = 'eyJraWQiOiIxY2UxZTEzNjE3ZGNmNzY2YjNjZWJjY2Y4ZGM1YmFmYThhNjVlNjg0MDIzZjdjMzJiZTgzNDliMjM4MDEzNWI0IiwidHlwIjoiUEFUIiwiYWxnIjoiRVMyNTYifQ.eyJqdGkiOiI3ZDNlNmY5Zi1lMjZkLTQ3MDktYWJiYS05ZWUyNTFhMTg0ZjMiLCJzdWIiOiI2YzUxNjNhMy1mNDMwLTQ0NmUtOGJhNy01NjcxNzhhZWFlZTAiLCJpYXQiOjE3NDU1NDUzMTMsImV4cCI6MTkwMzMxMTMxM30.YfFCqkT7WDZ8nfbFT3oSQlX0qk5yH2Mt8a0OMWBjxz-Tf6iHdJvZKKjuPik2-YygsLKQhGCXQTaYIDqhMaBaYd5c5FwOI0ZbJBCsaZug';

const ALLOWED_TABLES = [
  'Customers','Contacts','Properties','Equipment','Jobs',
  'Work Orders','Technicians','Product List',
  'Maintenance Contracts','Invoices','Companies'
];

const EVENT_TYPE_MAP = {
  'inspection':   'Inspection',
  'maintenance':  'Maintenance',
  'repair':       'Repair',
  'installation': 'Installation',
  'estimate':     'Estimate Only',
  'warranty':     'Warranty'
};

export default {
  async fetch(request, env) {
    const url  = new URL(request.url);
    const path = url.pathname;

    const corsHeaders = {
      'Access-Control-Allow-Origin':  '*',
      'Access-Control-Allow-Methods': 'GET,POST,PATCH,DELETE,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,Authorization'
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // ── Calendly webhook ──────────────────────────────────────────────────
    if (path === '/api/calendly-webhook' && request.method === 'POST') {
      try {
        const body    = await request.json();
        const event   = body.event;
        const payload = body.payload;

        if (event === 'invitee.created') {
          const inviteeName  = payload.name  || 'Unknown';
          const inviteeEmail = (payload.email || '').toLowerCase().trim();
          const inviteePhone = payload.text_reminder_number || '';

          const eventUri  = payload.event || '';
          const eventUuid = eventUri.split('/').pop();

          let scheduledDate = '';
          let eventTypeName = '';
          if (eventUuid) {
            const evRes = await fetch(`https://api.calendly.com/scheduled_events/${eventUuid}`, {
              headers: { Authorization: `Bearer ${CALENDLY_TOKEN}` }
            });
            if (evRes.ok) {
              const evData = await evRes.json();
              scheduledDate = evData.resource?.start_time || '';
              eventTypeName = (evData.resource?.name || '').toLowerCase();
            }
          }

          let workOrderType = 'Inspection';
          for (const [key, val] of Object.entries(EVENT_TYPE_MAP)) {
            if (eventTypeName.includes(key)) { workOrderType = val; break; }
          }

          let phone = inviteePhone, address = '', city = '', state = '', zip = '', unitCount = '', problemDesc = '';
          for (const qa of (payload.questions_and_answers || [])) {
            const q = (qa.question || '').toLowerCase();
            const a = (qa.answer   || '').trim();
            if (!a) continue;
            if (q.includes('phone')) {
              phone = a;
            } else if (q.includes('service address') || q.includes('address')) {
              const parts = a.split(',').map(s => s.trim());
              address = parts[0] || a;
              if (parts[1]) city = parts[1];
            } else if (q.includes('city'))  { city        = a; }
            else if (q.includes('state'))   { state       = a; }
            else if (q.includes('zip'))     { zip         = a; }
            else if (q.includes('how many') || q.includes('unit')) { unitCount = a; }
            else if (q.includes('known issues') || q.includes('notes') || q.includes('issue')) { problemDesc = a; }
          }

          // Find or create Customer
          let customerId = null;
          if (inviteeEmail) {
            const custSearch = await airtableGet('Customers', `{Email}="${inviteeEmail}"`);
            if (custSearch.records?.length > 0) {
              customerId = custSearch.records[0].id;
            } else {
              const nameParts = inviteeName.trim().split(' ');
              const newCust = await airtablePost('Customers', {
                'Customer Name': inviteeName,
                'First Name':    nameParts[0] || '',
                'Last Name':     nameParts.slice(1).join(' ') || '',
                'Email':         inviteeEmail,
                'Phone':         phone,
                'Type':          'Residential',
                'Active':        true
              });
              customerId = newCust.id;
            }
          }

          // Find or create Property
          let propertyId = null;
          if (address) {
            let propRecords = [];
            if (inviteeEmail) {
              const propSearch = await airtableGet('Properties', `{Customer Email}="${inviteeEmail}"`);
              propRecords = propSearch.records || [];
            }
            if (propRecords.length === 1) {
              propertyId = propRecords[0].id;
            } else if (propRecords.length === 0) {
              const propFields = {
                'Property Name':   inviteeName + (address ? ' — ' + address : ''),
                'Service Address': address,
                'Active':          true
              };
              if (city)         propFields['City']           = city;
              if (state)        propFields['State']          = state;
              if (zip)          propFields['Zip']            = zip;
              if (inviteeEmail) propFields['Customer Email'] = inviteeEmail;
              if (customerId)   propFields['Customer']       = [customerId];
              const newProp = await airtablePost('Properties', propFields);
              propertyId = newProp.id;
            }
            // Multiple properties → leave unlinked, Bridgett links manually
          }

          const noteParts = [];
          if (phone)     noteParts.push('Phone: ' + phone);
          if (unitCount) noteParts.push('Units: ' + unitCount);

          const woFields = {
            'Work Order Name': `${inviteeName} — ${workOrderType}`,
            'Status':          'Scheduled',
            'Work Order Type': workOrderType,
            'Notes':           noteParts.join(' | '),
            'Active':          true
          };
          if (scheduledDate) woFields['Scheduled Date']      = scheduledDate;
          if (problemDesc)   woFields['Problem Description'] = problemDesc;
          if (customerId)    woFields['Customer']            = [customerId];
          if (propertyId)    woFields['Property']            = [propertyId];
          if (eventUri)      woFields['Calendly ID']         = eventUri;

          await airtablePost('Work Orders', woFields);

          return new Response(JSON.stringify({ ok: true }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }

        if (event === 'invitee.canceled') {
          const calendlyId = payload.uri || payload.event || '';
          if (calendlyId) {
            const search = await airtableGet('Work Orders', `{Calendly ID}="${calendlyId}"`);
            if (search.records?.length > 0) {
              await airtablePatch('Work Orders', search.records[0].id, { 'Status': 'Cancelled' });
            }
          }
          return new Response(JSON.stringify({ ok: true }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }

        return new Response(JSON.stringify({ ok: true, skipped: true }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });

      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Calendly webhook registration (one-time) ───────────────────────────
    if (path === '/api/setup-calendly-webhook' && request.method === 'GET') {
      try {
        const meRes  = await fetch('https://api.calendly.com/users/me', {
          headers: { Authorization: `Bearer ${CALENDLY_TOKEN}` }
        });
        const meData = await meRes.json();
        const orgUri  = meData.resource?.current_organization;
        const userUri = meData.resource?.uri;

        const whRes = await fetch('https://api.calendly.com/webhook_subscriptions', {
          method: 'POST',
          headers: { Authorization: `Bearer ${CALENDLY_TOKEN}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            url:          'https://cjb-comfort-proxy.bridgettrhart.workers.dev/api/calendly-webhook',
            events:       ['invitee.created', 'invitee.canceled'],
            organization: orgUri,
            user:         userUri,
            scope:        'user'
          })
        });
        const whData = await whRes.json();
        return new Response(
          whRes.ok ? 'SUCCESS — Calendly webhook registered!' : 'Error: ' + JSON.stringify(whData),
          { headers: corsHeaders }
        );
      } catch (err) {
        return new Response('Error: ' + err.message, { status: 500, headers: corsHeaders });
      }
    }

    // ── Stripe Invoice ────────────────────────────────────────────────────
    if (path === '/api/invoice' && request.method === 'POST') {
      try {
        const { workOrderId, customerId, lineItems, notes, sendNow } = await request.json();

        if (!customerId) throw new Error('customerId is required');
        if (!lineItems?.length) throw new Error('At least one line item is required');

        const STRIPE_KEY = env.STRIPE_SECRET_KEY;
        if (!STRIPE_KEY) throw new Error('STRIPE_SECRET_KEY not configured in Worker environment');

        // 1. Get customer from Airtable
        const custRec  = await airtableGetById('Customers', customerId);
        const custEmail = (custRec.fields['Email']         || '').trim();
        const custName  = (custRec.fields['Customer Name'] || '').trim();
        if (!custEmail) throw new Error(`Customer "${custName}" has no email — required for Stripe invoicing`);

        // 2. Find or create Stripe customer
        let stripeCustId;
        const srchRes  = await fetch(
          `https://api.stripe.com/v1/customers?email=${encodeURIComponent(custEmail)}&limit=1`,
          { headers: { Authorization: `Bearer ${STRIPE_KEY}` } }
        );
        const srchData = await srchRes.json();
        if (srchData.data?.length > 0) {
          stripeCustId = srchData.data[0].id;
        } else {
          const cc = await stripePost(STRIPE_KEY, '/v1/customers', { email: custEmail, name: custName });
          stripeCustId = cc.id;
        }

        // 3. Create line items as pending for this customer (no invoice ID yet)
        //    Stripe collects all pending items when the invoice is created
        for (const item of lineItems) {
          await stripePost(STRIPE_KEY, '/v1/invoiceitems', {
            customer:    stripeCustId,
            unit_amount: String(Math.round((item.unitPrice || 0) * 100)),
            quantity:    String(Math.max(1, Math.round(item.quantity || 1))),
            currency:    'usd',
            description: item.productName || 'Service'
          });
        }

        // 4. Create invoice — automatically collects all pending items above
        const invParams = {
          customer:           stripeCustId,
          auto_advance:       'false',
          collection_method:  'send_invoice',
          days_until_due:     '30'
        };
        if (notes) invParams.description = notes;
        const inv = await stripePost(STRIPE_KEY, '/v1/invoices', invParams);

        // 5. Finalize + send only if sendNow — otherwise leave as Stripe draft
        let finalInv = inv;
        if (sendNow) {
          const finalized = await stripePost(STRIPE_KEY, `/v1/invoices/${inv.id}/finalize`, {});
          finalInv = await stripePost(STRIPE_KEY, `/v1/invoices/${inv.id}/send`, {});
        }

        // 6. Create Airtable Invoice record
        const subtotal = lineItems.reduce((s, li) => s + ((li.unitPrice || 0) * (li.quantity || 1)), 0);
        const today   = new Date().toISOString().split('T')[0];
        const dueDate = new Date(Date.now() + 30 * 86400000).toISOString().split('T')[0];
        const atFields = {
          'Invoice Name':   `${custName} — ${today}`,
          'Customers':      [customerId],
          'Status':         sendNow ? 'Sent' : 'Draft',
          'Invoice Type':   'Standard',
          'Invoice Date':   today,
          'Due Date':       dueDate,
          'Subtotal':       subtotal,
          'Total':          subtotal,
          'Internal Notes': `Stripe Invoice ID: ${finalInv.id}${finalInv.hosted_invoice_url ? '\n' + finalInv.hosted_invoice_url : ''}`
        };
        if (workOrderId) atFields['Work Orders'] = [workOrderId];
        if (notes)       atFields['Notes']        = notes;
        const atInv = await airtablePost('Invoices', atFields);

        // 7. Update Work Order:
        //    - sendNow  → status Invoiced + store hosted URL so office can find it
        //    - draft    → leave status as Complete so invoice button stays available
        if (workOrderId && sendNow) {
          const woUpdate = { 'Status': 'Invoiced' };
          if (finalInv.hosted_invoice_url) {
            woUpdate['Internal Notes'] = finalInv.hosted_invoice_url;
          }
          await airtablePatch('Work Orders', workOrderId, woUpdate);
        }

        return new Response(JSON.stringify({
          ok:              true,
          stripeInvoiceId: finalInv.id,
          hostedUrl:       finalInv.hosted_invoice_url || null,
          isDraft:         !sendNow,
          airtableId:      atInv.id
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Airtable proxy ────────────────────────────────────────────────────
    const match = path.match(/^\/api\/([^/]+)\/?([^/]*)$/);
    if (match) {
      const tableName = decodeURIComponent(match[1]);
      const recordId  = match[2] || '';

      if (!ALLOWED_TABLES.includes(tableName)) {
        return new Response(JSON.stringify({ error: 'Table not allowed' }), {
          status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }

      const atBase = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
      const atUrl  = recordId ? `${atBase}/${recordId}` : `${atBase}${url.search}`;
      const atHeaders = { Authorization: `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' };

      let atReqInit = { method: request.method, headers: atHeaders };
      if (['POST','PATCH','PUT'].includes(request.method)) {
        atReqInit.body = await request.text();
      }

      const atRes  = await fetch(atUrl, atReqInit);
      const atBody = await atRes.text();
      return new Response(atBody, {
        status: atRes.status,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    return new Response('Not found', { status: 404, headers: corsHeaders });
  }
};

// ── Airtable helpers ──────────────────────────────────────────────────────
async function airtableGet(table, formula) {
  const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(table)}` +
              `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=5`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
  if (!res.ok) throw new Error(`Airtable GET ${table}: ${res.status}`);
  return res.json();
}

async function airtableGetById(table, recordId) {
  const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
  if (!res.ok) throw new Error(`Airtable GET ${table}/${recordId}: ${res.status}`);
  return res.json();
}

async function airtablePost(table, fields) {
  const res = await fetch(
    `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(table)}`,
    {
      method: 'POST',
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ fields })
    }
  );
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Airtable POST ${table}: ${res.status} — ${err}`);
  }
  return res.json();
}

async function airtablePatch(table, recordId, fields) {
  const res = await fetch(
    `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(table)}/${recordId}`,
    {
      method: 'PATCH',
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ fields })
    }
  );
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Airtable PATCH ${table}/${recordId}: ${res.status} — ${err}`);
  }
  return res.json();
}

// ── Stripe helper ─────────────────────────────────────────────────────────
async function stripePost(apiKey, path, params) {
  const body = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) body.append(k, String(v));
  }
  const res = await fetch(`https://api.stripe.com${path}`, {
    method: 'POST',
    headers: {
      Authorization:  `Bearer ${apiKey}`,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: body.toString()
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Stripe ${path}: ${data.error?.message || JSON.stringify(data)}`);
  return data;
}
