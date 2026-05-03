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

// These are loaded from Cloudflare Worker secrets on each request (see fetch handler).
// Declared here so helper functions defined outside fetch() can access them.
let AIRTABLE_BASE_ID = '';
let AIRTABLE_API_KEY = '';
let CALENDLY_TOKEN   = '';

const ALLOWED_TABLES = [
  'Customers','Contacts','Properties','Equipment','Jobs',
  'Work Orders','Technicians','Product List',
  'Maintenance Contracts','Invoices','Companies','Quotes','Follow-ups'
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
    // Load secrets from Cloudflare environment on every request
    AIRTABLE_BASE_ID = env.AIRTABLE_BASE_ID || AIRTABLE_BASE_ID;
    AIRTABLE_API_KEY = env.AIRTABLE_API_KEY || AIRTABLE_API_KEY;
    CALENDLY_TOKEN   = env.CALENDLY_TOKEN   || CALENDLY_TOKEN;

    const url  = new URL(request.url);
    const path = url.pathname;

    const corsHeaders = {
      'Access-Control-Allow-Origin':  '*',
      'Access-Control-Allow-Methods': 'GET,POST,PATCH,OPTIONS',
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
          let scheduledEnd  = '';
          let eventTypeName = '';
          let calendlyFetchError = '';
          let address = ''; // declared early so Calendly location field can populate it

          if (eventUuid) {
            try {
              const evRes = await fetch(`https://api.calendly.com/scheduled_events/${eventUuid}`, {
                headers: { Authorization: `Bearer ${CALENDLY_TOKEN}` }
              });
              const evData = await evRes.json();
              if (evRes.ok) {
                scheduledDate = evData.resource?.start_time || '';
                scheduledEnd  = evData.resource?.end_time   || '';
                eventTypeName = (evData.resource?.name || '').toLowerCase();
                // Calendly location field (invitee-provided location or custom location type)
                // Q&A answer for "address" will override this below if present
                const evLocation = evData.resource?.location?.location || '';
                if (evLocation) address = evLocation;
              } else {
                calendlyFetchError = `Calendly API ${evRes.status}: ${JSON.stringify(evData)}`;
              }
            } catch (fetchErr) {
              calendlyFetchError = `Calendly fetch error: ${fetchErr.message}`;
            }
          }

          let workOrderType = 'Inspection';
          for (const [key, val] of Object.entries(EVENT_TYPE_MAP)) {
            if (eventTypeName.includes(key)) { workOrderType = val; break; }
          }

          let phone = inviteePhone, unitCount = '', problemDesc = '';
          for (const qa of (payload.questions_and_answers || [])) {
            const q = (qa.question || '').toLowerCase();
            const a = (qa.answer   || '').trim();
            if (!a) continue;
            // Match most-specific patterns first to avoid cross-contamination.
            // "known issues" question contains "unit" so must be checked before "how many"
            if (q.includes('known issues') || q.includes('issues you are having')) {
              problemDesc = a;
            } else if (q.includes('how many')) {
              unitCount = a;
            } else if (q.includes('phone')) {
              phone = a;
            } else if (q.includes('service address') || q.includes('address')) {
              address = a; // form only asks for street address, no city/state/zip splitting needed
            }
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
              if (inviteeEmail) propFields['Customer Email'] = inviteeEmail;
              if (customerId)   propFields['Customer']       = [customerId];
              const newProp = await airtablePost('Properties', propFields);
              propertyId = newProp.id;
            }
            // Multiple properties → leave unlinked, Bridgett links manually
          }

          const noteParts = [];
          if (phone)              noteParts.push('Phone: ' + phone);
          if (unitCount)          noteParts.push('Units: ' + unitCount);
          if (calendlyFetchError) noteParts.push('⚠️ ' + calendlyFetchError);

          // ── Reschedule detection ──────────────────────────────────────────
          // When a customer reschedules via Calendly, a new invitee.created fires
          // with payload.old_invitee pointing back to the original booking.
          // Instead of creating a new WO, find the old one and update it.
          const oldInviteeUri = payload.old_invitee || '';
          if (oldInviteeUri) {
            const oldEventMatch = oldInviteeUri.match(/scheduled_events\/([^/]+)/);
            const oldEventUuid  = oldEventMatch ? oldEventMatch[1] : '';
            const oldEventUri   = oldEventUuid
              ? `https://api.calendly.com/scheduled_events/${oldEventUuid}`
              : '';
            if (oldEventUri) {
              const oldWoSearch = await airtableGet('Work Orders', `{Calendly ID}="${oldEventUri}"`);
              if (oldWoSearch.records?.length > 0) {
                const rescheduleFields = { 'Status': 'Scheduled' };
                if (scheduledDate) rescheduleFields['Scheduled Date'] = scheduledDate;
                if (scheduledEnd)  rescheduleFields['Scheduled End']  = scheduledEnd;
                if (eventUri)      rescheduleFields['Calendly ID']    = eventUri;
                await airtablePatch('Work Orders', oldWoSearch.records[0].id, rescheduleFields);
                return new Response(JSON.stringify({ ok: true, rescheduled: true }), {
                  headers: { ...corsHeaders, 'Content-Type': 'application/json' }
                });
              }
            }
          }

          // ── New booking (not a reschedule) — create Work Order ────────────
          const woFields = {
            'Work Order Name': `${inviteeName} — ${workOrderType}`,
            'Status':          'Scheduled',
            'Work Order Type': workOrderType,
            'Notes':           noteParts.join(' | '),
            'Active':          true
          };
          if (scheduledDate) woFields['Scheduled Date'] = scheduledDate;
          if (scheduledEnd)  woFields['Scheduled End']  = scheduledEnd;
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
          // If this is a reschedule (not a true cancellation), do nothing —
          // the new invitee.created event will find and update the existing WO.
          const isRescheduled = payload.rescheduled === true || !!payload.new_invitee;
          if (!isRescheduled) {
            // True cancellation — use payload.event (the event URI we stored on the WO)
            const calendlyId = payload.event || payload.uri || '';
            if (calendlyId) {
              const search = await airtableGet('Work Orders', `{Calendly ID}="${calendlyId}"`);
              if (search.records?.length > 0) {
                await airtablePatch('Work Orders', search.records[0].id, { 'Status': 'Cancelled' });
              }
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

    // ── Airtable schema meta (select field options) ───────────────────────
    if (path === '/api/meta/tables' && request.method === 'GET') {
      const res = await fetch(
        `https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`,
        { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } }
      );
      const body = await res.text();
      return new Response(body, {
        status: res.status,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
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

    // ── Stripe Webhook — invoice.paid ────────────────────────────────────
    if (path === '/api/stripe-webhook' && request.method === 'POST') {
      try {
        const rawBody      = await request.text();
        const sigHeader    = request.headers.get('Stripe-Signature') || '';
        const webhookSecret = env.STRIPE_WEBHOOK_SECRET;

        // Verify signature if secret is configured
        if (webhookSecret && sigHeader) {
          const parts = Object.fromEntries(sigHeader.split(',').map(p => { const [k,...v] = p.split('='); return [k, v.join('=')]; }));
          const t = parts.t; const v1 = parts.v1;
          if (!t || !v1) throw new Error('Invalid Stripe-Signature header');
          const key     = await crypto.subtle.importKey('raw', new TextEncoder().encode(webhookSecret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
          const signed  = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(`${t}.${rawBody}`));
          const computed = Array.from(new Uint8Array(signed)).map(b => b.toString(16).padStart(2, '0')).join('');
          if (computed !== v1) throw new Error('Stripe signature verification failed');
        }

        const event = JSON.parse(rawBody);

        // ── quote.accepted: update Airtable, flip WO status, create draft invoice ──
        if (event.type === 'quote.accepted') {
          const quote        = event.data.object;
          const stripeQuoteId = quote.id;
          const STRIPE_KEY   = env.STRIPE_SECRET_KEY;
          const today        = new Date().toISOString().split('T')[0];

          const quoteSearch = await airtableGet('Quotes', `{Stripe Quote ID}="${stripeQuoteId}"`);
          const atQuote     = quoteSearch.records?.[0];

          if (atQuote) {
            await airtablePatch('Quotes', atQuote.id, { 'Status': 'Accepted', 'Accepted Date': today });

            const woId   = (atQuote.fields['Work Order'] || [])[0];
            const custId = (atQuote.fields['Customer']   || [])[0];
            if (woId) await airtablePatch('Work Orders', woId, { 'Status': 'Estimate Approved' });

            // Fetch Stripe line items from the accepted quote
            const liData   = await stripeGet(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}/line_items?limit=50`);
            const lineItems = liData.data || [];
            const stripeCustId = typeof quote.customer === 'string' ? quote.customer : quote.customer?.id;

            if (stripeCustId && lineItems.length) {
              // Clean up floating pending items
              try {
                const pendRes  = await fetch(
                  `https://api.stripe.com/v1/invoiceitems?customer=${stripeCustId}&limit=100`,
                  { headers: { Authorization: `Bearer ${STRIPE_KEY}` } }
                );
                const pendData = await pendRes.json();
                for (const p of (pendData.data || [])) {
                  if (!p.invoice) await stripeDelete(STRIPE_KEY, `/v1/invoiceitems/${p.id}`);
                }
              } catch (e) {}

              // Create invoice items from quote line items
              for (const li of lineItems) {
                const unitAmt = li.price?.unit_amount
                  ?? Math.round((li.amount_subtotal || 0) / (li.quantity || 1));
                await stripePost(STRIPE_KEY, '/v1/invoiceitems', {
                  customer:    stripeCustId,
                  unit_amount: String(unitAmt),
                  quantity:    String(li.quantity || 1),
                  currency:    'usd',
                  description: li.description || 'Service'
                });
              }

              // Determine due days from customer type
              let daysUntilDue = '0';
              if (custId) {
                try {
                  const cr = await airtableGetById('Customers', custId);
                  if ((cr.fields['Type'] || '').toLowerCase() === 'commercial') daysUntilDue = '30';
                } catch (e) {}
              }

              // Create draft Stripe invoice
              const invParams = {
                customer:          stripeCustId,
                auto_advance:      'false',
                collection_method: 'send_invoice',
                days_until_due:    daysUntilDue,
                'metadata[quote_id]': stripeQuoteId
              };
              if (woId) invParams['metadata[work_order_airtable_id]'] = woId;
              const inv = await stripePost(STRIPE_KEY, '/v1/invoices', invParams);

              // Update WO with Stripe Invoice ID
              if (woId) await airtablePatch('Work Orders', woId, { 'Stripe Invoice ID': inv.id });

              // Create or update Airtable Invoice record
              const subtotal = lineItems.reduce((s, li) => {
                return s + ((li.price?.unit_amount || 0) * (li.quantity || 1)) / 100;
              }, 0);
              const custName = atQuote.fields['Quote Title']?.split(' — ')[0] || '';
              const atInvFields = {
                'Invoice Name': `${custName} — ${today}`,
                'Status':       'Draft',
                'Invoice Type': 'Standard',
                'Invoice Date': today,
                'Subtotal':     subtotal,
                'Total':        subtotal,
                'Internal Notes': `From Quote: ${stripeQuoteId}\nStripe Invoice ID: ${inv.id}`
              };
              if (custId) atInvFields['Customers']     = [custId];
              if (woId)   atInvFields['Work Orders']   = [woId];

              if (woId) {
                const woRec = await airtableGetById('Work Orders', woId);
                const existingAtInvId = (woRec.fields['Invoice'] || [])[0];
                if (existingAtInvId) {
                  await airtablePatch('Invoices', existingAtInvId, atInvFields);
                } else {
                  await airtablePost('Invoices', atInvFields);
                }
              } else {
                await airtablePost('Invoices', atInvFields);
              }

              // Mark quote as converted
              await airtablePatch('Quotes', atQuote.id, { 'Converted to Invoice': true });
            }
          }
        }

        // ── quote.will_expire: update Airtable statuses ──────────────────
        // Stripe has no quote.expired event — quote.will_expire fires before
        // expiry (X days prior, set in Stripe Automations). We treat this as
        // effectively expired for operational purposes.
        if (event.type === 'quote.will_expire') {
          const quote = event.data.object;
          const quoteSearch = await airtableGet('Quotes', `{Stripe Quote ID}="${quote.id}"`);
          const atQuote     = quoteSearch.records?.[0];
          if (atQuote) {
            await airtablePatch('Quotes', atQuote.id, { 'Status': 'Expired' });
            const woId = (atQuote.fields['Work Order'] || [])[0];
            if (woId) await airtablePatch('Work Orders', woId, { 'Status': 'Estimate Declined' });
          }
        }

        if (event.type === 'invoice.paid') {
          const inv            = event.data.object;
          const stripeInvId    = inv.id;
          const amountPaid     = (inv.amount_paid || 0) / 100;
          const paidDate       = new Date().toISOString().split('T')[0];

          // Find Work Order by Stripe Invoice ID field
          const woData = await airtableGet('Work Orders', `{Stripe Invoice ID}="${stripeInvId}"`);
          const wo     = (woData.records || [])[0];
          if (wo) {
            await airtablePatch('Work Orders', wo.id, { 'Status': 'Paid' });
            // Mirror Paid to linked Jobs
            const jobIds = wo.fields['Jobs'] || [];
            if (jobIds.length) {
              await Promise.all(jobIds.map(jid => airtablePatch('Jobs', jid, { 'Status': 'Paid' })));
            }
            // Get linked Airtable Invoice record from Work Order
            const atInvId = (wo.fields['Invoice'] || [])[0];
            if (atInvId) {
              await airtablePatch('Invoices', atInvId, {
                'Status':      'Paid in Full',
                'Paid Date':   paidDate,
                'Amount Paid': amountPaid
              });
            }
          }
        }

        return new Response('ok', { status: 200 });
      } catch (err) {
        console.error('Stripe webhook error:', err.message);
        return new Response(err.message, { status: 400 });
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
        const existingStripeId = (woRecord?.fields?.['Stripe Invoice ID'] || '').trim();
        if (existingStripeId) {
          try {
            const existingInv = await stripeGet(STRIPE_KEY, `/v1/invoices/${existingStripeId}`);
            if (existingInv.status === 'draft') {
              await stripeDelete(STRIPE_KEY, `/v1/invoices/${existingStripeId}`);
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
        // Stripe requires integer quantity — fold fractional qty into unit_amount and note in description
        const woName = woRecord?.fields?.['Work Order Name'] || '';
        for (const item of lineItems) {
          const qty     = Number(item.quantity) || 1;
          const isWhole = Number.isInteger(qty);
          const stripeAmt  = Math.round((item.unitPrice || 0) * (isWhole ? 1 : qty) * 100);
          const stripeDesc = isWhole
            ? (item.productName || 'Service')
            : `${item.productName || 'Service'} (${qty}x)`;
          await stripePost(STRIPE_KEY, '/v1/invoiceitems', {
            customer:    stripeCustId,
            unit_amount: String(stripeAmt),
            quantity:    isWhole ? String(Math.max(1, qty)) : '1',
            currency:    'usd',
            description: stripeDesc
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
            const due2       = new Date(Date.now() + (isCommercial ? 30 : 0) * 86400000).toISOString().split('T')[0];
            const waveInv    = await waveQuery(WAVE_KEY, `
              mutation($input: InvoiceCreateInput!) {
                invoiceCreate(input: $input) {
                  invoice { id invoiceNumber viewUrl }
                  didSucceed
                  inputErrors { message path code }
                }
              }`, {
              input: {
                businessId:    WAVE_BUSINESS_ID,
                customerId:    waveCustId,
                invoiceDate:   today2,
                dueDate:       due2,
                memo:          notes || '',
                ...(finalInv.number ? { invoiceNumber: finalInv.number } : {}),
                items: lineItems.map(li => ({
                  productId:   waveProdId,
                  description: li.productName || 'Service',
                  quantity:    String(Number(li.quantity) || 1),
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

        const existingAtInvoiceId = (woRecord?.fields?.['Invoice'] || [])[0] || null;
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
            'Stripe Invoice ID': finalInv.id,
            'Total Amount':      subtotal
          };
          if (finalInv.hosted_invoice_url) woUpdate['Internal Notes'] = finalInv.hosted_invoice_url;
          if (sendNow) {
            woUpdate['Status'] = 'Invoiced';
            // Mirror status to linked Jobs
            const jobIds = woRecord?.fields?.['Jobs'] || [];
            if (jobIds.length) {
              await Promise.all(jobIds.map(jid => airtablePatch('Jobs', jid, { 'Status': 'Invoiced' })));
            }
          }
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

    // ── Stripe Quote — fetch ──────────────────────────────────────────────
    if (path.startsWith('/api/quote/') && request.method === 'GET') {
      const stripeQuoteId = path.replace('/api/quote/', '');
      const STRIPE_KEY = env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return new Response(JSON.stringify({ error: 'STRIPE_SECRET_KEY not configured' }), {
        status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
      try {
        const [quote, lineItems] = await Promise.all([
          stripeGet(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}`),
          stripeGet(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}/line_items?limit=50`)
        ]);
        return new Response(JSON.stringify({
          id:          quote.id,
          status:      quote.status,
          hostedUrl:   quote.hosted_quote_url || null,
          pdfUrl:      quote.pdf             || null,
          quoteNumber: quote.number          || null,
          expiresAt:   quote.expires_at,
          amountTotal: (quote.amount_total   || 0) / 100,
          lines: (lineItems.data || []).map(l => ({
            description: l.description || '',
            quantity:    l.quantity    || 1,
            unitPrice:   (l.price?.unit_amount || 0) / 100
          }))
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Stripe Quote — send (finalize) ────────────────────────────────────
    if (path === '/api/quote/send' && request.method === 'POST') {
      try {
        const { stripeQuoteId, airtableQuoteId, workOrderId } = await request.json();
        const STRIPE_KEY = env.STRIPE_SECRET_KEY;
        if (!STRIPE_KEY) throw new Error('STRIPE_SECRET_KEY not configured');

        const finalizedQuote = await stripePost(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}/finalize`, {});
        const hostedUrl  = finalizedQuote.hosted_quote_url || null;
        const quoteNumber = finalizedQuote.number          || null;
        const expiresAt  = finalizedQuote.expires_at
          ? new Date(finalizedQuote.expires_at * 1000).toISOString().split('T')[0]
          : null;

        if (airtableQuoteId) {
          const atUpdate = { 'Status': 'Open' };
          if (hostedUrl)   atUpdate['Stripe Quote URL']  = hostedUrl;
          if (quoteNumber) atUpdate['Quote Number']      = quoteNumber;
          if (expiresAt)   atUpdate['Expiration Date']   = expiresAt;
          await airtablePatch('Quotes', airtableQuoteId, atUpdate);
        }
        if (workOrderId) {
          await airtablePatch('Work Orders', workOrderId, { 'Status': 'Estimate Sent' });
        }

        return new Response(JSON.stringify({ ok: true, hostedUrl, quoteNumber }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Stripe Quote — decline ────────────────────────────────────────────
    if (path === '/api/quote/decline' && request.method === 'POST') {
      try {
        const { stripeQuoteId, airtableQuoteId, workOrderId } = await request.json();
        const STRIPE_KEY = env.STRIPE_SECRET_KEY;
        if (!STRIPE_KEY) throw new Error('STRIPE_SECRET_KEY not configured');
        const today = new Date().toISOString().split('T')[0];

        if (stripeQuoteId) {
          await stripePost(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}/cancel`, {});
        }
        if (airtableQuoteId) {
          await airtablePatch('Quotes', airtableQuoteId, { 'Status': 'Declined', 'Declined Date': today });
        }
        if (workOrderId) {
          await airtablePatch('Work Orders', workOrderId, { 'Status': 'Estimate Declined' });
        }

        return new Response(JSON.stringify({ ok: true }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Stripe Quote — create ─────────────────────────────────────────────
    if (path === '/api/quote' && request.method === 'POST') {
      try {
        const { workOrderId, customerId, lineItems, notes } = await request.json();
        if (!customerId)        throw new Error('customerId is required');
        if (!lineItems?.length) throw new Error('At least one line item is required');

        const STRIPE_KEY = env.STRIPE_SECRET_KEY;
        if (!STRIPE_KEY) throw new Error('STRIPE_SECRET_KEY not configured');

        // 1. Get customer
        const custRec   = await airtableGetById('Customers', customerId);
        const custEmail = (custRec.fields['Email']         || '').trim();
        const custName  = (custRec.fields['Customer Name'] || '').trim();
        if (!custEmail) throw new Error(`Customer "${custName}" has no email — required for Stripe`);

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

        // 3. Build and create Stripe draft quote
        const expiresAt = Math.floor(Date.now() / 1000) + 30 * 24 * 60 * 60;
        const quoteParamsObj = {
          customer:    stripeCustId,
          expires_at:  String(expiresAt),
          line_items:  lineItems.map(item => {
            const qty     = Number(item.quantity) || 1;
            const isWhole = Number.isInteger(qty);
            const cents   = Math.round((item.unitPrice || 0) * (isWhole ? 1 : qty) * 100);
            return {
              price_data: {
                currency:     'usd',
                product_data: { name: item.productName || 'Service' },
                unit_amount:  String(cents)
              },
              quantity: String(isWhole ? Math.max(1, qty) : 1)
            };
          })
        };
        if (notes)        quoteParamsObj.description                      = notes;
        if (workOrderId)  quoteParamsObj['metadata[work_order_airtable_id]'] = workOrderId;

        const stripeQuote = await stripePostNested(STRIPE_KEY, '/v1/quotes', quoteParamsObj);

        // 4. Airtable Quote record
        const subtotal   = lineItems.reduce((s, li) => s + ((li.unitPrice || 0) * (Number(li.quantity) || 1)), 0);
        const expiresDate = new Date(expiresAt * 1000).toISOString().split('T')[0];
        const today      = new Date().toISOString().split('T')[0];

        let woName = '';
        if (workOrderId) {
          try { const wo = await airtableGetById('Work Orders', workOrderId); woName = wo.fields?.['Work Order Name'] || ''; } catch (e) {}
        }

        const atQuoteFields = {
          'Quote Title':     `${custName} — ${woName || 'Estimate'} — ${today}`,
          'Status':          'Draft',
          'Stripe Quote ID': stripeQuote.id,
          'Expiration Date': expiresDate,
          'Total Amount':    subtotal,
          'Customer':        [customerId]
        };
        if (workOrderId) atQuoteFields['Work Order'] = [workOrderId];
        if (notes)       atQuoteFields['Notes']      = notes;

        const atQuote = await airtablePost('Quotes', atQuoteFields);

        // Write Stripe Quote ID back to Work Order for fast lookup in admin UI
        if (workOrderId) {
          await airtablePatch('Work Orders', workOrderId, { 'Stripe Quote ID': stripeQuote.id });
        }

        return new Response(JSON.stringify({
          ok: true, stripeQuoteId: stripeQuote.id, airtableQuoteId: atQuote.id, status: 'draft'
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Stripe Quote — update draft ───────────────────────────────────────
    // Stripe doesn't support editing line items on an existing quote,
    // so we cancel the old draft and create a fresh one with the same Airtable record.
    if (path === '/api/quote' && request.method === 'PATCH') {
      try {
        const { stripeQuoteId, airtableQuoteId, workOrderId, customerId, lineItems, notes } = await request.json();
        if (!stripeQuoteId) throw new Error('stripeQuoteId is required');

        const STRIPE_KEY = env.STRIPE_SECRET_KEY;
        if (!STRIPE_KEY) throw new Error('STRIPE_SECRET_KEY not configured');

        // Get Stripe customer ID from Airtable customer
        let stripeCustId;
        if (customerId) {
          const custRec   = await airtableGetById('Customers', customerId);
          const custEmail = (custRec.fields['Email'] || '').trim();
          if (custEmail) {
            const srchRes  = await fetch(
              `https://api.stripe.com/v1/customers?email=${encodeURIComponent(custEmail)}&limit=1`,
              { headers: { Authorization: `Bearer ${STRIPE_KEY}` } }
            );
            const srchData = await srchRes.json();
            stripeCustId   = srchData.data?.[0]?.id;
            if (!stripeCustId) {
              const custRec2 = await airtableGetById('Customers', customerId);
              const cc = await stripePost(STRIPE_KEY, '/v1/customers', {
                email: custEmail, name: custRec2.fields?.['Customer Name'] || ''
              });
              stripeCustId = cc.id;
            }
          }
        }

        // Cancel old draft/open quote
        try { await stripePost(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}/cancel`, {}); } catch (e) {}

        // Create replacement draft
        const expiresAt = Math.floor(Date.now() / 1000) + 30 * 24 * 60 * 60;
        const quoteParamsObj = {
          customer:   stripeCustId,
          expires_at: String(expiresAt),
          line_items: lineItems.map(item => {
            const qty     = Number(item.quantity) || 1;
            const isWhole = Number.isInteger(qty);
            const cents   = Math.round((item.unitPrice || 0) * (isWhole ? 1 : qty) * 100);
            return {
              price_data: {
                currency:     'usd',
                product_data: { name: item.productName || 'Service' },
                unit_amount:  String(cents)
              },
              quantity: String(isWhole ? Math.max(1, qty) : 1)
            };
          })
        };
        if (notes)       quoteParamsObj.description                         = notes;
        if (workOrderId) quoteParamsObj['metadata[work_order_airtable_id]'] = workOrderId;

        const newQuote = await stripePostNested(STRIPE_KEY, '/v1/quotes', quoteParamsObj);

        const subtotal    = lineItems.reduce((s, li) => s + ((li.unitPrice || 0) * (Number(li.quantity) || 1)), 0);
        const expiresDate = new Date(expiresAt * 1000).toISOString().split('T')[0];

        if (airtableQuoteId) {
          const upd = {
            'Status':          'Draft',
            'Stripe Quote ID': newQuote.id,
            'Expiration Date': expiresDate,
            'Total Amount':    subtotal,
            'Stripe Quote URL': ''
          };
          if (notes !== undefined) upd['Notes'] = notes;
          await airtablePatch('Quotes', airtableQuoteId, upd);
        }

        return new Response(JSON.stringify({
          ok: true, stripeQuoteId: newQuote.id, airtableQuoteId, status: 'draft'
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

      // Hard-block DELETE — app uses Active:false soft-deletes only.
      // Prevents anyone with the worker URL from wiping Airtable records.
      if (request.method === 'DELETE') {
        return new Response(JSON.stringify({ error: 'DELETE not permitted' }), {
          status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
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

// Handles nested objects/arrays → Stripe bracket notation (e.g. line_items[0][price_data][currency])
async function stripePostNested(apiKey, path, paramsObj) {
  const body = new URLSearchParams();
  function flatten(val, prefix) {
    if (val === null || val === undefined) return;
    if (Array.isArray(val)) {
      val.forEach((item, i) => flatten(item, `${prefix}[${i}]`));
    } else if (typeof val === 'object') {
      Object.entries(val).forEach(([k, v]) => flatten(v, `${prefix}[${k}]`));
    } else {
      body.append(prefix, String(val));
    }
  }
  Object.entries(paramsObj).forEach(([k, v]) => {
    // Pass through keys that already use bracket notation (e.g. metadata[key])
    if (typeof v !== 'object' || v === null) { body.append(k, String(v)); }
    else { flatten(v, k); }
  });
  const res = await fetch(`https://api.stripe.com${path}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/x-www-form-urlencoded' },
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
      incomeAccountId:  WAVE_INCOME_ACCOUNT_ID,
      unitPrice:        '0'
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
