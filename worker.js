// ═══════════════════════════════════════════════════════════════════════════
//  CJB Comfort — Cloudflare Worker
//  Deploy at: cjb-comfort-proxy.bridgettrhart.workers.dev
//
//  Environment secrets (set in Cloudflare dashboard → Settings → Variables):
//    STRIPE_SECRET_KEY  →  sk_live_...
// ═══════════════════════════════════════════════════════════════════════════

const WAVE_BUSINESS_ID       = 'QnVzaW5lc3M6ODQyOTljZjItODAyNy00NzFiLWE1NGUtOWVmYzZlZjRlNDY1';
const WAVE_INCOME_ACCOUNT_ID = 'QWNjb3VudDo2Mzg1NDYxMDc5MTUzNTU5MTg7QnVzaW5lc3M6ODQyOTljZjItODAyNy00NzFiLWE1NGUtOWVmYzZlZjRlNDY1';
let _waveServiceProductId = null; // cached per Worker instance

const R2_PUBLIC_URL = 'https://pub-53ca3c753a32459a8ecc3f361afc4ab2.r2.dev';

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

    // ── Wave setup diagnostic ─────────────────────────────────────────────
    if (path === '/api/wave-setup' && request.method === 'GET') {
      const WAVE_KEY = env.WAVE_API_KEY;
      if (!WAVE_KEY) return new Response(JSON.stringify({ error: 'WAVE_API_KEY not set' }), {
        status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
      try {
        const data = await waveQuery(WAVE_KEY, `{
          businesses(page: 1, pageSize: 5) {
            edges { node { id name } }
          }
        }`);
        const bizId = data.businesses.edges[0]?.node.id;
        const bizName = data.businesses.edges[0]?.node.name;

        const detail = await waveQuery(WAVE_KEY, `
          query($bizId: ID!) {
            business(id: $bizId) {
              products(page: 1, pageSize: 50) {
                edges { node { id name isArchived defaultSalesTaxes { id name } } }
              }
              accounts(subtypes: [INCOME]) {
                edges { node { id name subtype { value } } }
              }
            }
          }`, { bizId });

        return new Response(JSON.stringify({
          businessId: bizId,
          businessName: bizName,
          products: detail.business.products.edges.map(e => e.node),
          incomeAccounts: detail.business.accounts.edges.map(e => e.node)
        }, null, 2), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Equipment data-tag extraction (Claude Vision + R2 upload) ────────
    if (path === '/api/extract-tag' && request.method === 'POST') {
      try {
        const ANTHROPIC_KEY = env.ANTHROPIC_API_KEY;
        if (!ANTHROPIC_KEY) throw new Error('ANTHROPIC_API_KEY not configured');
        if (!env.PHOTOS_BUCKET) throw new Error('PHOTOS_BUCKET R2 binding not configured');

        const form        = await request.formData();
        const imageFile   = form.get('image');
        if (!imageFile) throw new Error('No image file provided');

        const arrayBuffer = await imageFile.arrayBuffer();
        const mediaType   = imageFile.type || 'image/jpeg';
        const ext         = mediaType.includes('png') ? 'png' : mediaType.includes('webp') ? 'webp' : 'jpg';
        const photoKey    = `equipment/${Date.now()}-${Math.random().toString(36).slice(2)}.${ext}`;

        // Upload to R2
        await env.PHOTOS_BUCKET.put(photoKey, arrayBuffer, {
          httpMetadata: { contentType: mediaType }
        });
        const photoUrl = `${R2_PUBLIC_URL}/${photoKey}`;

        // Convert to base64 for Claude
        const base64 = arrayBufferToBase64(arrayBuffer);

        // Call Claude Vision
        const claudeRes = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: {
            'Content-Type':    'application/json',
            'x-api-key':       ANTHROPIC_KEY,
            'anthropic-version': '2023-06-01',
          },
          body: JSON.stringify({
            model:      'claude-opus-4-5',
            max_tokens: 512,
            messages: [{
              role: 'user',
              content: [
                {
                  type: 'image',
                  source: { type: 'base64', media_type: mediaType, data: base64 }
                },
                {
                  type: 'text',
                  text: `This is a photo of an HVAC equipment data tag or nameplate. Extract all visible information and return ONLY a JSON object with exactly these keys (use null for anything not clearly visible):

{
  "brand": "manufacturer name",
  "model": "model number",
  "serial": "serial number",
  "equipmentType": "one of: WSHP, Split System, Package Unit, Mini Split, RTU, Air Handler, Condenser, Furnace, Heat Pump, Boiler, Chiller, Evaporative Cooler, Other",
  "refrigerantType": "e.g. R-410A, R-22, R-32, R-454B, R-407C — or null",
  "capacityTons": <number or null — from BTU/h ÷ 12000 or direct ton rating, round to nearest 0.5>,
  "voltage": "e.g. 208/230V or null",
  "manufactureYear": <4-digit year or null — check serial number date codes if no explicit year>
}

Return ONLY the raw JSON object. No markdown, no explanation.`
                }
              ]
            }]
          })
        });

        if (!claudeRes.ok) {
          const errText = await claudeRes.text();
          throw new Error('Claude API error: ' + errText.slice(0, 200));
        }

        const claudeData = await claudeRes.json();
        const rawText    = claudeData.content?.[0]?.text || '{}';
        let extracted    = {};
        try {
          const cleaned = rawText.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '').trim();
          extracted = JSON.parse(cleaned);
        } catch {
          extracted = { _parseError: rawText };
        }

        return new Response(JSON.stringify({ photoUrl, extracted }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Stripe Invoice — fetch existing draft ────────────────────────────
    if (path.startsWith('/api/invoice/') && request.method === 'GET') {
      const stripeInvoiceId = path.split('/api/invoice/')[1];
      const STRIPE_KEY = env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) {
        return new Response(JSON.stringify({ error: 'STRIPE_SECRET_KEY not configured' }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
      try {
        const [inv, lines] = await Promise.all([
          stripeGet(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}`),
          stripeGet(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}/lines?limit=100`)
        ]);
        return new Response(JSON.stringify({
          id:          inv.id,
          status:      inv.status,
          description: inv.description || '',
          hostedUrl:   inv.hosted_invoice_url || null,
          lines: (lines.data || []).map(l => ({
            description: l.description || '',
            quantity:    l.quantity    || 1,
            unitPrice:   (l.price?.unit_amount != null
              ? l.price.unit_amount
              : Math.round((l.amount || 0) / (l.quantity || 1))) / 100
          }))
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Stripe Invoice — create / update ─────────────────────────────────
    if (path === '/api/invoice' && request.method === 'POST') {
      try {
        const { workOrderId, customerId, lineItems, notes, sendNow } = await request.json();

        if (!customerId) throw new Error('customerId is required');
        if (!lineItems?.length) throw new Error('At least one line item is required');

        const STRIPE_KEY = env.STRIPE_SECRET_KEY;
        if (!STRIPE_KEY) throw new Error('STRIPE_SECRET_KEY not configured in Worker environment');

        // 1. Get customer from Airtable
        const custRec   = await airtableGetById('Customers', customerId);
        const custEmail = (custRec.fields['Email']         || '').trim();
        const custName  = (custRec.fields['Customer Name'] || '').trim();
        if (!custEmail) throw new Error(`Customer "${custName}" has no email — required for Stripe invoicing`);

        const billingAddr  = (custRec.fields['Billing Address'] || '').trim();
        const billingCity  = (custRec.fields['Billing City']    || '').trim();
        const billingState = (custRec.fields['Billing State']   || '').trim();
        const billingZip   = (custRec.fields['Billing Zip']     || '').trim();
        const addrParams   = {};
        if (billingAddr)  addrParams['address[line1]']       = billingAddr;
        if (billingCity)  addrParams['address[city]']        = billingCity;
        if (billingState) addrParams['address[state]']       = billingState;
        if (billingZip)   addrParams['address[postal_code]'] = billingZip;
        if (billingAddr)  addrParams['address[country]']     = 'US';

        // 2. Get Work Order record (needed for existing-draft check and WO name)
        let woRecord = null;
        if (workOrderId) {
          woRecord = await airtableGetById('Work Orders', workOrderId);
        }

        // 3. Find or create Stripe customer
        let stripeCustId;
        const srchRes  = await fetch(
          `https://api.stripe.com/v1/customers?email=${encodeURIComponent(custEmail)}&limit=1`,
          { headers: { Authorization: `Bearer ${STRIPE_KEY}` } }
        );
        const srchData = await srchRes.json();
        if (srchData.data?.length > 0) {
          stripeCustId = srchData.data[0].id;
          if (Object.keys(addrParams).length) {
            await stripePost(STRIPE_KEY, `/v1/customers/${stripeCustId}`, addrParams);
          }
        } else {
          const cc = await stripePost(STRIPE_KEY, '/v1/customers', { email: custEmail, name: custName, ...addrParams });
          stripeCustId = cc.id;
        }

        // 4. Delete any existing Stripe draft for this Work Order
        //    Items attached to a deleted draft become pending — cleaned up in step 5
        const woNotes = woRecord?.fields?.['Internal Notes'] || '';
        const existingIdMatch = woNotes.match(/Stripe Invoice ID: (in_[^\s\n]+)/);
        if (existingIdMatch) {
          try {
            const existingInv = await stripeGet(STRIPE_KEY, `/v1/invoices/${existingIdMatch[1]}`);
            if (existingInv.status === 'draft') {
              await stripeDelete(STRIPE_KEY, `/v1/invoices/${existingIdMatch[1]}`);
            }
          } catch (e) { /* invoice already gone — continue */ }
        }

        // 5. Delete ALL floating pending items for this customer
        //    Catches old test items + any items released from the deleted draft above
        try {
          const pendRes  = await fetch(
            `https://api.stripe.com/v1/invoiceitems?customer=${stripeCustId}&limit=100`,
            { headers: { Authorization: `Bearer ${STRIPE_KEY}` } }
          );
          const pendData = await pendRes.json();
          for (const pitem of (pendData.data || [])) {
            if (!pitem.invoice) {
              await stripeDelete(STRIPE_KEY, `/v1/invoiceitems/${pitem.id}`);
            }
          }
        } catch (e) { /* best effort */ }

        // 6. Create fresh pending items — Stripe requires items to exist before invoice creation
        const woName = woRecord?.fields?.['Work Order Name'] || '';
        for (const item of lineItems) {
          await stripePost(STRIPE_KEY, '/v1/invoiceitems', {
            customer:    stripeCustId,
            unit_amount: String(Math.round((item.unitPrice || 0) * 100)),
            quantity:    String(Math.max(1, Math.round(item.quantity || 1))),
            currency:    'usd',
            description: item.productName || 'Service'
          });
        }

        // 7. Create invoice — auto-collects all pending items created above
        const woUnwrap = key => {
          const v = woRecord?.fields?.[key];
          return Array.isArray(v) ? (v[0] || '') : (v || '');
        };
        const svcAddr = [woUnwrap('Service Address'), woUnwrap('City'), woUnwrap('State'), woUnwrap('Zip')]
          .filter(Boolean).join(', ');

        const descParts = [];
        if (svcAddr) descParts.push(`Service Address: ${svcAddr}`);
        if (notes)   descParts.push(notes);
        const fullDesc = descParts.join('\n\n').slice(0, 500);

        const isCommercial = (custRec.fields['Type'] || '').toLowerCase() === 'commercial';
        const invParams = {
          customer:          stripeCustId,
          auto_advance:      'false',
          collection_method: 'send_invoice',
          days_until_due:    isCommercial ? '30' : '0'
        };
        if (fullDesc) invParams.description              = fullDesc;
        if (woName)   invParams['metadata[work_order]'] = woName;
        const inv = await stripePost(STRIPE_KEY, '/v1/invoices', invParams);

        // 8. Finalize + send only if sendNow — otherwise leave as Stripe draft
        let finalInv = inv;
        if (sendNow) {
          await stripePost(STRIPE_KEY, `/v1/invoices/${inv.id}/finalize`, {});
          finalInv = await stripePost(STRIPE_KEY, `/v1/invoices/${inv.id}/send`, {});
        }

        // 9. Create Wave invoice (only on sendNow — drafts don't go to Wave yet)
        let waveInvoiceId = null;
        if (sendNow && env.WAVE_API_KEY) {
          try {
            const WAVE_KEY   = env.WAVE_API_KEY;
            const waveCustId = await waveEnsureCustomer(WAVE_KEY, custName, custEmail, customerId);
            const waveProdId = await waveEnsureServiceProduct(WAVE_KEY);
            const today2     = new Date().toISOString().split('T')[0];
            const due2       = new Date(Date.now() + 30 * 86400000).toISOString().split('T')[0];
            const waveInv    = await waveQuery(WAVE_KEY, `
              mutation($input: InvoiceCreateInput!) {
                invoiceCreate(input: $input) {
                  invoice { id invoiceNumber viewUrl }
                  didSucceed
                  inputErrors { message path code }
                }
              }`, {
              input: {
                businessId:  WAVE_BUSINESS_ID,
                customerId:  waveCustId,
                invoiceDate: today2,
                dueDate:     due2,
                memo:        notes || '',
                items: lineItems.map(li => ({
                  productId:   waveProdId,
                  description: li.productName || 'Service',
                  quantity:    String(Math.max(1, Math.round(li.quantity || 1))),
                  unitPrice:   String((li.unitPrice || 0).toFixed(2))
                }))
              }
            });
            if (!waveInv.invoiceCreate.didSucceed) {
              throw new Error(waveInv.invoiceCreate.inputErrors?.[0]?.message || 'Wave invoiceCreate failed');
            }
            waveInvoiceId = waveInv.invoiceCreate.invoice.id;
            // Approve the invoice so it shows as Accounts Receivable in Wave
            await waveQuery(WAVE_KEY, `
              mutation($input: InvoiceApproveInput!) {
                invoiceApprove(input: $input) { didSucceed inputErrors { message } }
              }`, { input: { invoiceId: waveInvoiceId } });
            // Store Wave Customer ID back to Airtable so future invoices skip the search
            await airtablePatch('Customers', customerId, { 'Wave Customer ID': waveCustId });
          } catch (waveErr) {
            // Wave failure doesn't block the Stripe invoice — log and continue
            console.error('Wave sync failed:', waveErr.message);
          }
        }

        // 10. Create or update Airtable Invoice record
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
          'Internal Notes': `Stripe Invoice ID: ${finalInv.id}${waveInvoiceId ? '\nWave Invoice ID: ' + waveInvoiceId : ''}${finalInv.hosted_invoice_url ? '\n' + finalInv.hosted_invoice_url : ''}`
        };
        if (workOrderId) atFields['Work Orders'] = [workOrderId];
        if (notes)       atFields['Notes']        = notes;

        const existingAtInvoiceId = woNotes.match(/Airtable Invoice ID: (rec[^\s\n]+)/)?.[1];
        let atInvId;
        if (existingAtInvoiceId) {
          await airtablePatch('Invoices', existingAtInvoiceId, atFields);
          atInvId = existingAtInvoiceId;
        } else {
          const atInv = await airtablePost('Invoices', atFields);
          atInvId = atInv.id;
        }

        // 11. Store Stripe ID + Airtable Invoice ID in Work Order Internal Notes
        if (workOrderId) {
          const woUpdate = {
            'Internal Notes': `Stripe Invoice ID: ${finalInv.id}\nAirtable Invoice ID: ${atInvId}${finalInv.hosted_invoice_url ? '\n' + finalInv.hosted_invoice_url : ''}`
          };
          if (sendNow) woUpdate['Status'] = 'Invoiced';
          await airtablePatch('Work Orders', workOrderId, woUpdate);
        }

        return new Response(JSON.stringify({
          ok:              true,
          stripeInvoiceId: finalInv.id,
          hostedUrl:       finalInv.hosted_invoice_url || null,
          waveInvoiceId:   waveInvoiceId,
          isDraft:         !sendNow,
          airtableId:      atInvId
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

// ── Stripe helpers ────────────────────────────────────────────────────────
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

async function stripeGet(apiKey, path) {
  const res = await fetch(`https://api.stripe.com${path}`, {
    headers: { Authorization: `Bearer ${apiKey}` }
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Stripe GET ${path}: ${data.error?.message || JSON.stringify(data)}`);
  return data;
}

async function stripeDelete(apiKey, path) {
  const res = await fetch(`https://api.stripe.com${path}`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${apiKey}` }
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Stripe DELETE ${path}: ${data.error?.message || JSON.stringify(data)}`);
  return data;
}

// ── Wave helpers ──────────────────────────────────────────────────────────
async function waveQuery(apiKey, query, variables = {}) {
  const res = await fetch('https://gql.waveapps.com/graphql/public', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query, variables })
  });
  const data = await res.json();
  if (data.errors?.length) throw new Error(`Wave: ${data.errors[0].message}`);
  return data.data;
}

async function waveEnsureCustomer(apiKey, name, email, airtableCustomerId) {
  // Check if Airtable already has the Wave Customer ID cached
  const custRec = await airtableGetById('Customers', airtableCustomerId);
  const cachedId = custRec.fields?.['Wave Customer ID'];
  if (cachedId) return cachedId;

  // Search Wave by email (up to 200 customers)
  if (email) {
    const data = await waveQuery(apiKey, `
      query($bizId: ID!) {
        business(id: $bizId) {
          customers(page: 1, pageSize: 200) {
            edges { node { id name email } }
          }
        }
      }`, { bizId: WAVE_BUSINESS_ID });
    const match = (data.business.customers.edges || [])
      .find(e => (e.node.email || '').toLowerCase() === email.toLowerCase());
    if (match) return match.node.id;
  }

  // Create new Wave customer
  const created = await waveQuery(apiKey, `
    mutation($input: CustomerCreateInput!) {
      customerCreate(input: $input) {
        customer { id }
        didSucceed
        inputErrors { message }
      }
    }`, { input: { businessId: WAVE_BUSINESS_ID, name, email } });
  if (!created.customerCreate.didSucceed) {
    throw new Error('Wave customer create: ' + created.customerCreate.inputErrors?.[0]?.message);
  }
  return created.customerCreate.customer.id;
}

async function waveEnsureServiceProduct(apiKey) {
  if (_waveServiceProductId) return _waveServiceProductId;

  // Search for existing generic product
  const data = await waveQuery(apiKey, `
    query($bizId: ID!) {
      business(id: $bizId) {
        products(page: 1, pageSize: 200) {
          edges { node { id name isArchived } }
        }
      }
    }`, { bizId: WAVE_BUSINESS_ID });
  const existing = (data.business.products.edges || [])
    .find(e => e.node.name === 'CJB Comfort Services' && !e.node.isArchived);
  if (existing) {
    _waveServiceProductId = existing.node.id;
    return _waveServiceProductId;
  }

  // Create it
  const created = await waveQuery(apiKey, `
    mutation($input: ProductCreateInput!) {
      productCreate(input: $input) {
        product { id }
        didSucceed
        inputErrors { message }
      }
    }`, {
    input: {
      businessId:       WAVE_BUSINESS_ID,
      name:             'CJB Comfort Services',
      incomeAccountId:  WAVE_INCOME_ACCOUNT_ID
    }
  });
  if (!created.productCreate.didSucceed) {
    throw new Error('Wave product create: ' + created.productCreate.inputErrors?.[0]?.message);
  }
  _waveServiceProductId = created.productCreate.product.id;
  return _waveServiceProductId;
}

// ── Utility: ArrayBuffer → base64 (chunked to avoid stack overflow) ──────────
function arrayBufferToBase64(buffer) {
  const bytes     = new Uint8Array(buffer);
  const chunkSize = 8192;
  let binary      = '';
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
}
