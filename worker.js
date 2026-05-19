// ═══════════════════════════════════════════════════════════════════════════
//  CJB Comfort — Cloudflare Worker
//  Deploy: cjb-comfort-proxy.bridgettrhart.workers.dev
//  Portal: portal.cjbcomfort.com
//
//  Environment secrets (Cloudflare dashboard → Settings → Variables):
//    AIRTABLE_API_KEY, AIRTABLE_BASE_ID, ANTHROPIC_API_KEY,
//    CALENDLY_TOKEN, RESEND_API_KEY, STRIPE_SECRET_KEY,
//    STRIPE_PUBLISHABLE_KEY, STRIPE_WEBHOOK_SECRET,
//    Telnyx_API, WAVE_API_KEY
// ═══════════════════════════════════════════════════════════════════════════

const WAVE_BUSINESS_ID       = 'QnVzaW5lc3M6ODQyOTljZjItODAyNy00NzFiLWE1NGUtOWVmYzZlZjRlNDY1';
const WAVE_INCOME_ACCOUNT_ID = 'QWNjb3VudDo2Mzg1NDYxMDc5MTUzNTU5MTg7QnVzaW5lc3M6ODQyOTljZjItODAyNy00NzFiLWE1NGUtOWVmYzZlZjRlNDY1';
let _waveServiceProductId = null; // cached per Worker instance

const R2_PUBLIC_URL    = 'https://pub-53ca3c753a32459a8ecc3f361afc4ab2.r2.dev';
const APPROVE_BASE_URL = 'https://app.cjbcomfort.com/approve.html';

const RESEND_FROM      = 'CJB Comfort <office@mail.cjbcomfort.com>';
const PORTAL_URL       = 'https://portal.cjbcomfort.com';
const OFFICE_PHONE     = '(623) 400-7761';
const OFFICE_PHONE_URL = 'tel:+16234007761';
const TELNYX_FROM      = '+14808639119';
const GOOGLE_REVIEW_URL = 'https://g.page/r/CUypICY_Qj1PEBM/review';

// White-label: swap these for a different company's branding
const BRAND_NAME    = 'CJB Comfort';
const BRAND_TAGLINE = 'HVAC you can trust';
// Upload a PNG logo to R2 and paste the public URL here.
// Email clients don't render SVG — use PNG or JPG only.
// Leave empty to show BRAND_NAME as text instead.
const BRAND_LOGO_URL = 'https://pub-53ca3c753a32459a8ecc3f361afc4ab2.r2.dev/CJBComfort_2026_logo.png';

// These are loaded from Cloudflare Worker secrets on each request (see fetch handler).
// Declared here so helper functions defined outside fetch() can access them.
let AIRTABLE_BASE_ID = '';
let AIRTABLE_API_KEY = '';
let CALENDLY_TOKEN   = '';

const ALLOWED_TABLES = [
  'Customers','Contacts','Properties','Equipment','Jobs',
  'Work Orders','Technicians','Product List',
  'Maintenance Contracts','Invoices','Companies','Quotes','Follow-Ups'
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
          const cancelUrl     = payload.cancel_url     || '';
          const rescheduleUrl = payload.reschedule_url || '';
          // First name for emails: use first word of invitee name
          const firstName = inviteeName.trim().split(' ')[0] || '';

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
                if (scheduledDate) rescheduleFields['Scheduled Date']          = scheduledDate;
                if (scheduledEnd)  rescheduleFields['Scheduled End']           = scheduledEnd;
                if (eventUri)      rescheduleFields['Calendly ID']             = eventUri;
                if (cancelUrl)     rescheduleFields['Calendly Cancel URL']     = cancelUrl;
                if (rescheduleUrl) rescheduleFields['Calendly Reschedule URL'] = rescheduleUrl;
                await airtablePatch('Work Orders', oldWoSearch.records[0].id, rescheduleFields);

                // Send rescheduled confirmation email
                if (inviteeEmail && scheduledDate) {
                  const { dateStr, timeStr, endTimeStr } = formatAZDateTime(scheduledDate);
                  const oldWo  = oldWoSearch.records[0].fields;
                  const html   = emailBookingConfirmedHtml({
                    firstName, dateStr, timeStr, endTimeStr,
                    address:            address || (Array.isArray(oldWo['Service Address']) ? oldWo['Service Address'][0] : oldWo['Service Address']) || '',
                    woType:             workOrderType,
                    problemDescription: problemDesc,
                    cancelUrl, rescheduleUrl,
                    isReschedule: true,
                  });
                  await sendEmail(env.RESEND_API_KEY, {
                    to:      inviteeEmail,
                    subject: `Your CJB Comfort appointment has been rescheduled — ${dateStr}`,
                    html,
                  });
                }

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
          if (scheduledDate) woFields['Scheduled Date']          = scheduledDate;
          if (scheduledEnd)  woFields['Scheduled End']           = scheduledEnd;
          if (problemDesc)   woFields['Problem Description']     = problemDesc;
          if (customerId)    woFields['Customer']                = [customerId];
          if (propertyId)    woFields['Property']                = [propertyId];
          if (eventUri)      woFields['Calendly ID']             = eventUri;
          if (cancelUrl)     woFields['Calendly Cancel URL']     = cancelUrl;
          if (rescheduleUrl) woFields['Calendly Reschedule URL'] = rescheduleUrl;

          await airtablePost('Work Orders', woFields);

          // Send booking confirmation email
          if (inviteeEmail && scheduledDate) {
            const { dateStr, timeStr, endTimeStr } = formatAZDateTime(scheduledDate);
            const html = emailBookingConfirmedHtml({
              firstName, dateStr, timeStr, endTimeStr,
              address:            address || '',
              woType:             workOrderType,
              problemDescription: problemDesc,
              cancelUrl, rescheduleUrl,
            });
            await sendEmail(env.RESEND_API_KEY, {
              to:      inviteeEmail,
              subject: `Your CJB Comfort appointment is confirmed — ${dateStr}`,
              html,
            });
          }

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

    // ── Booking confirmation email (admin-scheduled appointments) ────────────
    // Called by CJB_Admin.html spBook() after creating a Work Order manually.
    if (path === '/api/email/booking-confirmed' && request.method === 'POST') {
      try {
        const { email, firstName, scheduledDate, address, woType,
                problemDescription, cancelUrl, rescheduleUrl, techName } = await request.json();
        if (!email || !scheduledDate) {
          return new Response(JSON.stringify({ error: 'email and scheduledDate required' }),
            { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        const { dateStr, timeStr, endTimeStr } = formatAZDateTime(scheduledDate);
        const html = emailBookingConfirmedHtml({
          firstName, dateStr, timeStr, endTimeStr,
          address: address || '', woType: woType || 'Service Visit',
          problemDescription: problemDescription || '',
          cancelUrl: cancelUrl || '', rescheduleUrl: rescheduleUrl || '',
          techName: techName || '',
        });
        await sendEmail(env.RESEND_API_KEY, {
          to: email,
          subject: `Your CJB Comfort appointment is confirmed — ${dateStr}`,
          html,
        });
        return new Response(JSON.stringify({ ok: true }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ── "On My Way" SMS — Cornell triggers from field app ─────────────────
    if (path === '/api/sms/on-my-way' && request.method === 'POST') {
      try {
        const { phone, firstName, address } = await request.json();
        if (!phone) {
          return new Response(JSON.stringify({ error: 'phone required' }),
            { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        const name = firstName ? `Hi ${firstName}` : 'Hi there';
        const loc  = address ? ` to ${address}` : '';
        const text = `${name} — Cornell is on his way${loc} and will arrive within your scheduled window. Questions? Call or text us at ${OFFICE_PHONE}. – CJB Comfort`;
        await sendSms(env.Telnyx_API, phone, text);
        return new Response(JSON.stringify({ ok: true }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
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

    // ── Customer estimate approval page — fetch details ──────────────────
    if (path === '/api/approve' && request.method === 'GET') {
      try {
        const reqUrl = new URL(request.url);
        const woId   = reqUrl.searchParams.get('wo');
        if (!woId) return new Response(JSON.stringify({ error: 'Missing wo parameter' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

        const wo       = await airtableGetById('Work Orders', woId);
        const woFields = wo.fields;
        const stripeQuoteId = woFields['Stripe Quote ID'] || '';
        const STRIPE_KEY    = env.STRIPE_SECRET_KEY;

        let lineItems   = [];
        let description = '';
        let photoUrls   = [];

        if (stripeQuoteId && STRIPE_KEY) {
          try {
            const quote = await stripeGet(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}`);
            description = quote.description || '';
            const liRes = await stripeGet(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}/line_items`);
            lineItems = (liRes.data || []).map(li => ({
              name:      li.description || 'Service',
              qty:       li.quantity    || 1,
              unitPrice: (li.price?.unit_amount || 0) / 100,
              amount:    (li.amount_total || li.amount_subtotal || 0) / 100
            }));
          } catch (stripeErr) {
            // non-fatal — page still loads
          }
        }

        // Pull photo URLs from Airtable Quote record (stored in Notes with ---PHOTOS--- delimiter)
        if (stripeQuoteId) {
          try {
            const qSearch = await airtableGet('Quotes', `{Stripe Quote ID}="${stripeQuoteId}"`);
            const atQ = qSearch.records?.[0];
            if (atQ) {
              const qNotes = atQ.fields['Notes'] || '';
              const photosIdx = qNotes.indexOf('---PHOTOS---');
              if (photosIdx !== -1) {
                photoUrls = qNotes.slice(photosIdx + 12).trim().split('\n').filter(Boolean);
              }
            }
          } catch (e) { /* non-fatal */ }
        }

        const total = lineItems.reduce((s, li) => s + li.amount, 0);
        return new Response(JSON.stringify({
          ok:           true,
          customerName: woFields['Customer Name']  || 'Customer',
          woNumber:     woFields['Work Order ID']  || '',
          status:       woFields['Status']         || '',
          description,
          lineItems,
          total,
          photoUrls,
          stripeQuoteId
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ── Customer estimate approval page — record decision ─────────────────
    if (path === '/api/approve' && request.method === 'POST') {
      try {
        const { woId, decision } = await request.json();
        if (!woId || !decision) return new Response(JSON.stringify({ error: 'Missing woId or decision' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

        const newStatus = decision === 'approved' ? 'Estimate Approved' : 'Estimate Declined';
        await airtablePatch('Work Orders', woId, { 'Status': newStatus });

        // Send confirmation email to customer (non-fatal)
        if (env.RESEND_API_KEY) {
          try {
            const wo = await airtableGetById('Work Orders', woId);
            const custIds = wo.fields['Customer'] || [];
            if (custIds.length) {
              const cust     = await airtableGetById('Customers', custIds[0]);
              const custEmail = (cust.fields['Email'] || '').trim();
              const custName  = cust.fields['Customer Name'] || 'Customer';
              if (custEmail) {
                await sendEmail(env.RESEND_API_KEY, {
                  to:      custEmail,
                  subject: decision === 'approved'
                    ? 'Estimate Approved — CJB Comfort Will Proceed'
                    : 'Your Response Has Been Recorded — CJB Comfort',
                  html: emailApprovalHtml({ customerName: custName, approved: decision === 'approved' })
                });
              }
            }
          } catch (emailErr) {
            console.error('Approval email failed (non-fatal):', emailErr.message);
          }
        }

        // Update Airtable Quote record status if linked
        try {
          const wo = await airtableGetById('Work Orders', woId);
          const quoteLinks = wo.fields['Quote'] || [];
          if (quoteLinks.length) {
            const atStatus = decision === 'approved' ? 'Accepted' : 'Declined';
            await airtablePatch('Quotes', quoteLinks[0], { 'Status': atStatus });
          }
        } catch (e) { /* non-fatal */ }

        // Accept/cancel Stripe quote to keep records clean (non-fatal)
        if (env.STRIPE_SECRET_KEY) {
          try {
            const wo = await airtableGetById('Work Orders', woId);
            const stripeQuoteId = wo.fields['Stripe Quote ID'] || '';
            if (stripeQuoteId) {
              const endpoint = decision === 'approved' ? 'accept' : 'cancel';
              await stripePost(env.STRIPE_SECRET_KEY, `/v1/quotes/${stripeQuoteId}/${endpoint}`, {});
            }
          } catch (e) { /* non-fatal */ }
        }

        return new Response(JSON.stringify({ ok: true, status: newStatus }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ── Stripe quote PDF proxy ────────────────────────────────────────────
    if (path === '/api/quote-pdf' && request.method === 'GET') {
      try {
        const reqUrl = new URL(request.url);
        const woId   = reqUrl.searchParams.get('wo');
        if (!woId) return new Response('Missing wo parameter', { status: 400 });

        const wo          = await airtableGetById('Work Orders', woId);
        const quoteId     = wo.fields['Stripe Quote ID'];
        if (!quoteId) return new Response('No quote found for this work order', { status: 404 });

        const STRIPE_KEY  = env.STRIPE_SECRET_KEY;
        const pdfRes      = await fetch(`https://files.stripe.com/v1/quotes/${quoteId}/pdf`, {
          headers: { 'Authorization': `Bearer ${STRIPE_KEY}` }
        });
        if (!pdfRes.ok) return new Response('Could not retrieve PDF from Stripe', { status: 502 });

        return new Response(pdfRes.body, {
          headers: {
            'Content-Type':        'application/pdf',
            'Content-Disposition': `inline; filename="CJB-Comfort-Estimate.pdf"`,
            'Access-Control-Allow-Origin': '*'
          }
        });
      } catch (err) {
        return new Response(err.message, { status: 500 });
      }
    }

    // ── Job photo upload → R2 ─────────────────────────────────────────────
    if (path === '/api/upload-photo' && request.method === 'POST') {
      try {
        if (!env.PHOTOS_BUCKET) throw new Error('PHOTOS_BUCKET not configured');
        const form  = await request.formData();
        const files = form.getAll('image');
        if (!files.length) throw new Error('No images provided');
        const urls = [];
        for (const file of files) {
          const buf       = await file.arrayBuffer();
          const mediaType = file.type || 'image/jpeg';
          const ext       = mediaType.includes('png') ? 'png' : mediaType.includes('webp') ? 'webp' : 'jpg';
          const key       = `jobs/${Date.now()}-${Math.random().toString(36).slice(2)}.${ext}`;
          await env.PHOTOS_BUCKET.put(key, buf, { httpMetadata: { contentType: mediaType } });
          urls.push(`${R2_PUBLIC_URL}/${key}`);
        }
        return new Response(JSON.stringify({ ok: true, urls }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
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
                  customer:              stripeCustId,
                  unit_amount_decimal:   String(unitAmt),
                  quantity:              String(li.quantity || 1),
                  currency:              'usd',
                  description:           li.description || 'Service'
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
            // Mirror Paid to linked Jobs (skips Cancelled / skipped-unit jobs)
            await patchBillableJobs(wo.fields['Jobs'] || [], 'Paid');
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

          // Maintenance contract proposal payment → activate contract
          if (inv.metadata?.invoice_type === 'maintenance_contract' && inv.metadata?.contract_airtable_id) {
            const contractId = inv.metadata.contract_airtable_id;
            const startDate  = new Date();
            const endDate    = new Date(startDate);
            endDate.setFullYear(endDate.getFullYear() + 1);
            endDate.setDate(endDate.getDate() - 1);
            await airtablePatch('Maintenance Contracts', contractId, {
              'Status':                 'Active',
              'Start Date':             startDate.toISOString().split('T')[0],
              'End Date':               endDate.toISOString().split('T')[0],
              'Visits Used This Year':  0,
            });
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
          lines: (lines.data || []).map(l => {
            // unit_amount_excluding_tax is Stripe's most reliable per-unit field on invoice lines
            // (always present, always a string in cents). Fall back through price object then amount÷qty.
            let unitCents;
            if (l.unit_amount_excluding_tax != null) {
              unitCents = parseFloat(l.unit_amount_excluding_tax);
            } else if (l.price?.unit_amount_decimal != null) {
              unitCents = parseFloat(l.price.unit_amount_decimal);
            } else if (l.price?.unit_amount != null && l.price.unit_amount !== 0) {
              unitCents = l.price.unit_amount;
            } else {
              unitCents = Math.round((l.amount || 0) / (l.quantity || 1));
            }
            return {
              description: l.description || '',
              quantity:    l.quantity    || 1,
              unitPrice:   unitCents / 100
            };
          })
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Stripe publishable key (safe to expose — for client-side Elements) ──
    // ── Maintenance contract visit completion ─────────────────────────────────
    if (path === '/api/contract/visit-complete' && request.method === 'POST') {
      try {
        const { contractId } = await request.json();
        if (!contractId) return new Response(JSON.stringify({ error: 'Missing contractId' }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

        const contract = await airtableGetById('Maintenance Contracts', contractId);
        const cf = contract.fields;

        const visitsUsed    = (cf['Visits Used This Year'] || 0) + 1;
        const visitsPerYear = cf['Visits Per Year'] || 2;

        // Calculate next service due from today based on visit frequency
        const freqMonths = {
          'Monthly': 1, 'Bi-Monthly': 2, 'Quarterly': 3,
          'Semi-Annual': 6, 'Annual': 12, 'As Needed': 6,
        }[cf['Visit Frequency'] || 'Semi-Annual'] || 6;

        const nextDue = new Date();
        nextDue.setMonth(nextDue.getMonth() + freqMonths);
        const nextDueStr = nextDue.toISOString().split('T')[0];

        const contractUpdate = {
          'Visits Used This Year': visitsUsed,
          'Next Service Due': nextDueStr,
        };

        // If all visits for the year are used, flag for renewal
        const renewalNeeded = visitsUsed >= visitsPerYear;

        await airtablePatch('Maintenance Contracts', contractId, contractUpdate);

        // Auto-create next WO only if visits remain and contract runs through next due date
        let nextWorkOrderId = null;
        if (!renewalNeeded) {
          const endDate = cf['End Date'] ? new Date(cf['End Date']) : null;
          const nextDueDate = new Date(nextDueStr);
          if (!endDate || nextDueDate <= endDate) {
            const woFields = {
              'Work Order Name': (cf['Plan Name'] || 'Maintenance') + ' — ' + nextDueStr,
              'Work Order Type': 'Maintenance',
              'Status': 'Scheduled',
              'Scheduled Date': new Date(nextDueStr + 'T08:00:00').toISOString(),
              'Maintenance Contract': [contractId],
            };
            if (cf['Customer']?.length)        woFields['Customer']        = cf['Customer'];
            if (cf['Property']?.length)         woFields['Property']        = cf['Property'];
            if (cf['Primary Contact']?.length)  woFields['Primary Contact'] = cf['Primary Contact'];
            const nextWo = await airtablePost('Work Orders', woFields);
            nextWorkOrderId = nextWo.id;
          }
        }

        return new Response(JSON.stringify({
          ok: true, visitsUsed, nextServiceDue: nextDueStr,
          renewalNeeded, nextWorkOrderId,
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch(err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ── Contract proposal page (customer-facing) ──────────────────────────────
    if (path === '/api/contract/proposal' && request.method === 'GET') {
      const contractId = url.searchParams.get('id') || '';
      if (!contractId) return new Response('Missing contract id', { status: 400 });
      try {
        const contract = await airtableGetById('Maintenance Contracts', contractId);
        const cf = contract.fields;
        const custName   = Array.isArray(cf['Customer Name'])  ? cf['Customer Name'][0]  : (cf['Customer Name']  || '');
        const propName   = Array.isArray(cf['Property Name'])  ? cf['Property Name'][0]  : (cf['Property Name']  || '');
        const propAddr   = Array.isArray(cf['Service Address']) ? cf['Service Address'][0] : (cf['Service Address'] || '');
        const planName   = cf['Plan Name']  || 'Annual Maintenance Agreement';
        const annual     = cf['Annual Value'] != null ? '$' + Number(cf['Annual Value']).toLocaleString('en-US', {minimumFractionDigits:2}) : '';
        const startDate  = cf['Start Date']  ? new Date(cf['Start Date']  + 'T12:00:00').toLocaleDateString('en-US', {month:'long', day:'numeric', year:'numeric'}) : '';
        const endDate    = cf['End Date']    ? new Date(cf['End Date']    + 'T12:00:00').toLocaleDateString('en-US', {month:'long', day:'numeric', year:'numeric'}) : '';
        const services   = cf['Included Services'] || '';
        const discount   = cf['Repair Discount %'] ? cf['Repair Discount %'] + '% discount on repairs' : '';
        const payUrl     = cf['Stripe Invoice URL'] || '';
        const visitFreq  = cf['Visit Frequency'] || '';
        const visitsPerYear = cf['Visits Per Year'] || '';

        const servicesHtml = services
          ? services.split('\n').filter(Boolean).map(s => `<li>${s}</li>`).join('')
          : '';

        const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Maintenance Agreement Proposal — CJB Comfort</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f9fafb; color: #111827; min-height: 100vh; }
  .wrap { max-width: 600px; margin: 0 auto; padding: 24px 16px 48px; }
  .header { background: #111827; color: white; border-radius: 12px; padding: 24px; margin-bottom: 24px; text-align: center; }
  .header-logo { font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: #f0b429; margin-bottom: 8px; }
  .header-title { font-size: 22px; font-weight: 800; }
  .card { background: white; border-radius: 12px; padding: 20px; margin-bottom: 16px; box-shadow: 0 1px 4px rgba(0,0,0,0.06); }
  .card-label { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; color: #9ca3af; margin-bottom: 12px; }
  .row { display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #f3f4f6; font-size: 14px; }
  .row:last-child { border-bottom: none; }
  .row-label { color: #6b7280; }
  .row-value { font-weight: 600; text-align: right; }
  .amount { font-size: 36px; font-weight: 800; color: #111827; text-align: center; margin: 8px 0 4px; }
  .amount-label { font-size: 13px; color: #6b7280; text-align: center; }
  ul { padding-left: 18px; }
  ul li { font-size: 14px; line-height: 2; color: #374151; }
  .discount { background: #f0fdf4; border: 1.5px solid #86efac; border-radius: 8px; padding: 10px 14px; font-size: 13px; color: #166534; font-weight: 600; }
  .accept-btn { display: block; width: 100%; background: #059669; color: white; border: none; border-radius: 12px; padding: 18px; font-size: 18px; font-weight: 800; cursor: pointer; text-align: center; text-decoration: none; margin-top: 8px; letter-spacing: 0.3px; }
  .accept-btn:hover { background: #047857; }
  .footer { text-align: center; font-size: 12px; color: #9ca3af; margin-top: 24px; line-height: 1.6; }
  .check { color: #059669; margin-right: 6px; }
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <div class="header-logo">CJB Comfort</div>
    <div class="header-title">Maintenance Agreement Proposal</div>
  </div>

  <div class="card">
    <div class="card-label">Prepared For</div>
    <div class="row"><span class="row-label">Customer</span><span class="row-value">${custName}</span></div>
    ${propName ? `<div class="row"><span class="row-label">Property</span><span class="row-value">${propName}</span></div>` : ''}
    ${propAddr ? `<div class="row"><span class="row-label">Service Address</span><span class="row-value">${propAddr}</span></div>` : ''}
  </div>

  <div class="card">
    <div class="card-label">Plan Details — ${planName}</div>
    ${annual ? `<div class="amount">${annual}</div><div class="amount-label">per year</div><br>` : ''}
    ${visitFreq ? `<div class="row"><span class="row-label">Visit Frequency</span><span class="row-value">${visitFreq}</span></div>` : ''}
    ${visitsPerYear ? `<div class="row"><span class="row-label">Visits Per Year</span><span class="row-value">${visitsPerYear}</span></div>` : ''}
    ${startDate ? `<div class="row"><span class="row-label">Start Date</span><span class="row-value">${startDate}</span></div>` : ''}
    ${endDate   ? `<div class="row"><span class="row-label">End Date</span><span class="row-value">${endDate}</span></div>` : ''}
  </div>

  ${servicesHtml ? `<div class="card">
    <div class="card-label">What's Included</div>
    <ul>${servicesHtml}</ul>
  </div>` : ''}

  ${discount ? `<div class="discount"><span class="check">✓</span>${discount} for all maintenance agreement customers</div><br>` : ''}

  <div class="card">
    <div class="card-label">Ready to Accept?</div>
    <p style="font-size:14px;color:#6b7280;margin-bottom:16px;line-height:1.6;">By clicking the button below you agree to the terms of this maintenance agreement and will be taken to a secure payment page to complete your first year's payment.</p>
    ${payUrl
      ? `<a class="accept-btn" href="${payUrl}" target="_blank" rel="noopener">✓ Accept &amp; Pay Now</a>`
      : `<div style="background:#f9fafb;border-radius:8px;padding:14px;text-align:center;font-size:13px;color:#9ca3af;">Payment link will appear here once the invoice is ready.</div>`}
  </div>

  <div class="footer">
    Questions? Call or text us — we're happy to walk you through it.<br>
    CJB Comfort · Licensed &amp; Insured · Arizona
  </div>
</div>
</body>
</html>`;

        return new Response(html, { headers: { 'Content-Type': 'text/html;charset=UTF-8' } });
      } catch(err) {
        return new Response('Could not load proposal: ' + err.message, { status: 500 });
      }
    }

    // ── Send contract proposal invoice ────────────────────────────────────────
    if (path === '/api/contract/send-proposal' && request.method === 'POST') {
      const STRIPE_KEY = env.STRIPE_SECRET_KEY;
      try {
        const { contractId } = await request.json();
        if (!contractId) return new Response(JSON.stringify({ error: 'Missing contractId' }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

        const contract = await airtableGetById('Maintenance Contracts', contractId);
        const cf = contract.fields;

        // Get customer email
        const custId   = (cf['Customer'] || [])[0];
        const custRec  = custId ? await airtableGetById('Customers', custId) : null;
        const custEmail = custRec?.fields?.['Email'] || '';
        const custName  = cf['Customer Name']
          ? (Array.isArray(cf['Customer Name']) ? cf['Customer Name'][0] : cf['Customer Name'])
          : (custRec?.fields?.['Customer Name'] || '');
        if (!custEmail) return new Response(JSON.stringify({ error: 'Customer has no email address on file.' }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

        const annualValue = cf['Annual Value'] || 0;
        const planName    = cf['Plan Name'] || 'Annual Maintenance Agreement';

        // Find or create Stripe customer
        const srchRes  = await fetch(
          `https://api.stripe.com/v1/customers?email=${encodeURIComponent(custEmail)}&limit=1`,
          { headers: { Authorization: `Bearer ${STRIPE_KEY}` } });
        const srchData = await srchRes.json();
        let stripeCustId;
        if (srchData.data?.length > 0) {
          stripeCustId = srchData.data[0].id;
        } else {
          const cc = await stripePost(STRIPE_KEY, '/v1/customers', { email: custEmail, name: custName });
          stripeCustId = cc.id;
        }

        // Create invoice item
        await stripePost(STRIPE_KEY, '/v1/invoiceitems', {
          customer:    stripeCustId,
          amount:      Math.round(annualValue * 100),
          currency:    'usd',
          description: planName,
        });

        // Create invoice with contract metadata
        const workerUrl = new URL(request.url).origin;
        const proposalUrl = `${workerUrl}/api/contract/proposal?id=${contractId}`;
        const inv = await stripePost(STRIPE_KEY, '/v1/invoices', {
          customer:             stripeCustId,
          description:          `${planName} — Review your agreement: ${proposalUrl}`,
          'metadata[invoice_type]':             'maintenance_contract',
          'metadata[contract_airtable_id]':     contractId,
          'collection_method':  'send_invoice',
          'days_until_due':     '30',
        });

        // Finalize and send
        await stripePost(STRIPE_KEY, `/v1/invoices/${inv.id}/finalize`, {});
        const sent = await stripePost(STRIPE_KEY, `/v1/invoices/${inv.id}/send`, {});

        // Write Stripe Invoice ID + URL back to contract
        await airtablePatch('Maintenance Contracts', contractId, {
          'Stripe Invoice ID':  sent.id,
          'Stripe Invoice URL': sent.hosted_invoice_url || '',
        });

        return new Response(JSON.stringify({
          ok: true,
          stripeInvoiceId: sent.id,
          hostedUrl: sent.hosted_invoice_url || '',
          proposalUrl,
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch(err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    if (path === '/api/config/stripe-pk' && request.method === 'GET') {
      const pk = env.STRIPE_PUBLISHABLE_KEY || '';
      return new Response(JSON.stringify({ publishableKey: pk }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // ── Charge a card via Stripe Elements PaymentMethod ───────────────────
    if (path === '/api/invoice/charge-card' && request.method === 'POST') {
      try {
        const { workOrderId, paymentMethodId } = await request.json();
        if (!workOrderId) throw new Error('workOrderId required');
        if (!paymentMethodId) throw new Error('paymentMethodId required');
        const STRIPE_KEY = env.STRIPE_SECRET_KEY;

        const wo = await airtableGetById('Work Orders', workOrderId);
        const stripeInvoiceId = (wo.fields['Stripe Invoice ID'] || '').trim();
        if (!stripeInvoiceId) {
          return new Response(JSON.stringify({ error: 'No Stripe invoice on file for this work order. Create an invoice first.' }),
            { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        // Attach metadata before charging
        await stripePost(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}`, {
          'metadata[payment_method]': 'Card — key-in',
          'metadata[collected_by]':   'Admin — manual entry'
        });

        // Pay the open invoice with the supplied PaymentMethod
        const paid = await stripePost(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}/pay`, {
          payment_method: paymentMethodId,
          off_session:    'true'
        });

        if (paid.status === 'paid') {
          // Patch Airtable immediately — don't wait for the webhook (avoids race on UI refresh)
          await airtablePatch('Work Orders', workOrderId, { 'Status': 'Paid' });
          return new Response(JSON.stringify({ ok: true }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        } else {
          return new Response(JSON.stringify({ error: `Stripe returned status: ${paid.status}` }),
            { status: 402, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ── Mark invoice paid out-of-band (cash / check) ─────────────────────
    if (path === '/api/invoice/pay-offline' && request.method === 'POST') {
      try {
        const { workOrderId, method, checkNumber } = await request.json();
        if (!workOrderId) throw new Error('workOrderId required');
        const STRIPE_KEY = env.STRIPE_SECRET_KEY;

        const wo = await airtableGetById('Work Orders', workOrderId);
        const stripeInvoiceId = (wo.fields['Stripe Invoice ID'] || '').trim();
        if (!stripeInvoiceId) {
          // No Stripe invoice on this WO — nothing to do, not an error
          return new Response(JSON.stringify({ ok: true, skipped: true }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        // Build a readable payment note for Stripe metadata
        const paymentNote = method === 'check'
          ? `Check${checkNumber ? ' #' + checkNumber : ''}`
          : 'Cash';

        // Check current Stripe invoice status — $0 warranty invoices are auto-paid
        // by Stripe immediately on send, so calling /pay again would throw an error.
        const stripeInv = await stripeGet(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}`);
        const paidDate  = new Date().toISOString().split('T')[0];

        if (stripeInv.status === 'paid') {
          // Already paid (auto-paid $0 or previously collected) — just sync Airtable
          await airtablePatch('Work Orders', wo.id, { 'Status': 'Paid' });
          // Mirror Paid to linked Jobs (skips Cancelled / skipped-unit jobs)
          await patchBillableJobs(wo.fields['Jobs'] || [], 'Paid');
          const atInvId = (wo.fields['Invoice'] || [])[0] || (wo.fields['Invoices'] || [])[0];
          if (atInvId) {
            await airtablePatch('Invoices', atInvId, {
              'Status':         'Paid in Full',
              'Paid Date':      paidDate,
              'Amount Paid':    (stripeInv.amount_paid || 0) / 100,
              'Payment Method': paymentNote,
            });
          }
          return new Response(JSON.stringify({ ok: true }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        // Update invoice metadata before marking paid
        await stripePost(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}`, {
          'metadata[payment_method]': paymentNote,
          'metadata[collected_by]':   'Technician — on site'
        });

        // Mark invoice as paid out of band — triggers invoice.paid webhook
        await stripePost(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}/pay`, {
          paid_out_of_band: 'true'
        });

        return new Response(JSON.stringify({ ok: true }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
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

        // Build shared values used in both update-in-place and new-invoice paths
        const woUnwrap = key => {
          const v = woRecord?.fields?.[key];
          return Array.isArray(v) ? (v[0] || '') : (v || '');
        };
        const woName      = woRecord?.fields?.['Work Order Name'] || '';
        const svcAddr     = [woUnwrap('Service Address'), woUnwrap('City'), woUnwrap('State'), woUnwrap('Zip')]
          .filter(Boolean).join(', ');
        const descParts   = [];
        if (svcAddr) descParts.push(`Service Address: ${svcAddr}`);
        if (notes)   descParts.push(notes);
        const fullDesc    = descParts.join('\n\n').slice(0, 500);
        const isCommercial = (custRec.fields['Type'] || '').toLowerCase() === 'commercial';

        // Helper: attach line items directly to a known Stripe invoice ID.
        // Using the `invoice` param on invoiceitems guarantees attachment — no "floating
        // pending items" that rely on Stripe auto-collection (which proved unreliable here).
        const attachLineItems = async (invoiceId) => {
          for (const item of lineItems) {
            const qty      = Number(item.quantity) || 1;
            const isWhole  = Number.isInteger(qty);
            const stripeAmt  = Math.round((item.unitPrice || 0) * (isWhole ? 1 : qty) * 100);
            const stripeDesc = isWhole
              ? (item.productName || 'Service')
              : `${item.productName || 'Service'} (${qty}x)`;
            await stripePost(STRIPE_KEY, '/v1/invoiceitems', {
              customer:            stripeCustId,
              invoice:             invoiceId,
              unit_amount_decimal: String(stripeAmt),
              quantity:            isWhole ? String(Math.max(1, qty)) : '1',
              currency:            'usd',
              description:         stripeDesc
            });
          }
        };

        // 4. Update existing draft in place, or create a new invoice.
        //    Updating in place keeps the Stripe Invoice ID stable (no stale-ID issues).
        const existingStripeId = (woRecord?.fields?.['Stripe Invoice ID'] || '').trim();
        let inv = null;

        if (existingStripeId) {
          try {
            const existingInv = await stripeGet(STRIPE_KEY, `/v1/invoices/${existingStripeId}`);
            if (existingInv.status === 'draft') {
              // Remove current line items from the existing draft
              const existingLines = await stripeGet(STRIPE_KEY, `/v1/invoices/${existingStripeId}/lines?limit=100`);
              await Promise.all(
                (existingLines.data || [])
                  .filter(l => l.invoice_item)
                  .map(l => stripeDelete(STRIPE_KEY, `/v1/invoiceitems/${l.invoice_item}`).catch(() => {}))
              );
              // Attach new items directly to the existing draft
              await attachLineItems(existingStripeId);
              // Update description on the invoice
              await stripePost(STRIPE_KEY, `/v1/invoices/${existingStripeId}`, { description: fullDesc });
              inv = await stripeGet(STRIPE_KEY, `/v1/invoices/${existingStripeId}`);
            }
          } catch (e) { /* not a draft, gone, or update failed — fall through to create */ }
        }

        if (!inv) {
          // 5. Guard: delete any floating pending items that might interfere
          try {
            const pendRes  = await fetch(
              `https://api.stripe.com/v1/invoiceitems?customer=${stripeCustId}&limit=100`,
              { headers: { Authorization: `Bearer ${STRIPE_KEY}` } }
            );
            const pendData = await pendRes.json();
            for (const pitem of (pendData.data || [])) {
              if (!pitem.invoice) await stripeDelete(STRIPE_KEY, `/v1/invoiceitems/${pitem.id}`);
            }
          } catch (e) { /* best effort */ }

          // 6. Create a new empty draft invoice
          const invParams = {
            customer:          stripeCustId,
            auto_advance:      'false',
            collection_method: 'send_invoice',
            days_until_due:    isCommercial ? '30' : '0'
          };
          if (fullDesc) invParams.description              = fullDesc;
          if (woName)   invParams['metadata[work_order]'] = woName;
          inv = await stripePost(STRIPE_KEY, '/v1/invoices', invParams);

          // 7. Attach line items directly to the new invoice
          await attachLineItems(inv.id);
        }

        // 8. Before finalizing: purge any floating pending invoice items for this customer.
        //    When Stripe finalizes an invoice it auto-collects ALL unattached pending items
        //    for the customer onto it — doubling items if any strays exist from a prior
        //    failed save or the old "floating items" approach. Run this on every path
        //    (update-in-place AND new invoice) right before finalize.
        if (sendNow) {
          try {
            const pendRes  = await fetch(
              `https://api.stripe.com/v1/invoiceitems?customer=${stripeCustId}&pending=true&limit=100`,
              { headers: { Authorization: `Bearer ${STRIPE_KEY}` } }
            );
            const pendData = await pendRes.json();
            for (const pitem of (pendData.data || [])) {
              if (!pitem.invoice) {
                await stripeDelete(STRIPE_KEY, `/v1/invoiceitems/${pitem.id}`).catch(() => {});
              }
            }
          } catch (e) { /* best effort — don't block send */ }
        }

        // 9. Finalize + send only if sendNow — otherwise leave as Stripe draft
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
        const dueDate = new Date(Date.now() + (isCommercial ? 30 : 0) * 86400000).toISOString().split('T')[0];
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

        const existingAtInvoiceId =
          (woRecord?.fields?.['Invoice']  || [])[0] ||
          (woRecord?.fields?.['Invoices'] || [])[0] || null;
        let atInvId;
        if (existingAtInvoiceId) {
          await airtablePatch('Invoices', existingAtInvoiceId, atFields);
          atInvId = existingAtInvoiceId;
        } else {
          const atInv = await airtablePost('Invoices', atFields);
          atInvId = atInv.id;
        }

        // 11. Store Stripe ID + Airtable Invoice ID in Work Order Internal Notes
        // For $0 invoices Stripe auto-pays immediately on send (status='paid').
        // The invoice.paid webhook fires before Airtable has been updated with the
        // Stripe Invoice ID, so it finds nothing. Detect this here and mark Paid inline.
        const zeroDollarAutoPaid = sendNow && finalInv.status === 'paid';
        if (workOrderId) {
          const woUpdate = {
            'Stripe Invoice ID': finalInv.id,
            'Total Amount':      subtotal
          };
          if (finalInv.hosted_invoice_url) woUpdate['Internal Notes'] = finalInv.hosted_invoice_url;
          if (sendNow) {
            woUpdate['Status'] = zeroDollarAutoPaid ? 'Paid' : 'Invoiced';
            // Mirror status to linked Jobs (skips Cancelled / skipped-unit jobs)
            const jobStatus = zeroDollarAutoPaid ? 'Paid' : 'Invoiced';
            await patchBillableJobs(woRecord?.fields?.['Jobs'] || [], jobStatus);
          }
          await airtablePatch('Work Orders', workOrderId, woUpdate);
        }

        // For $0 auto-paid: also flip Airtable Invoice record to Paid in Full now
        if (zeroDollarAutoPaid && atInvId) {
          const paidDate = new Date().toISOString().split('T')[0];
          await airtablePatch('Invoices', atInvId, {
            'Status':      'Paid in Full',
            'Paid Date':   paidDate,
            'Amount Paid': (finalInv.amount_paid || 0) / 100,
          });
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
        const body = await request.json();
        // Accept both field-app keys (woId, name, unitAmount[cents], qty)
        // and admin keys (workOrderId, productName, unitPrice[dollars], quantity)
        const workOrderId = body.workOrderId || body.woId || null;
        const customerId  = body.customerId;
        const notes       = body.notes || '';
        const description = body.description || '';
        const lineItems   = body.lineItems || [];
        const finalize    = body.finalize === true; // if true: create draft + finalize in one step
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

        // 3. Build Stripe line items
        // Quotes API requires price_data[product] (an existing Stripe product ID).
        // product_data inline creation is not supported on quotes — create each product first.
        const stripeLineItems = [];
        for (const item of lineItems) {
          // Normalize field names: accept both admin format and field-app format
          const name      = (item.productName || item.name || 'Service').trim();
          const qty       = Number(item.quantity || item.qty) || 1;
          // unitPrice is dollars (admin); unitAmount is cents (field app)
          const unitPrice = item.unitPrice !== undefined
            ? Number(item.unitPrice)
            : (Number(item.unitAmount) || 0) / 100;
          const isWhole   = Number.isInteger(qty);
          const cents     = Math.round(unitPrice * (isWhole ? 1 : qty) * 100);

          // Create a one-off Stripe product for the display name on the quote
          const prod = await stripePost(STRIPE_KEY, '/v1/products', { name, type: 'service' });

          stripeLineItems.push({
            price_data: {
              currency:    'usd',
              product:     prod.id,
              unit_amount: String(cents),
            },
            quantity: String(isWhole ? Math.max(1, qty) : 1),
          });
        }

        // 4. Create the Stripe draft quote
        const expiresAt = Math.floor(Date.now() / 1000) + 30 * 24 * 60 * 60;
        const quoteParamsObj = {
          customer:   stripeCustId,
          expires_at: String(expiresAt),
          line_items: stripeLineItems,
        };
        // Stripe quote description is capped at 500 chars; full text is stored in Airtable Notes
        if (description || notes) quoteParamsObj.description = (description || notes).slice(0, 500);
        if (workOrderId) quoteParamsObj['metadata[work_order_airtable_id]'] = workOrderId;

        const stripeQuote = await stripePostNested(STRIPE_KEY, '/v1/quotes', quoteParamsObj);

        // 5. Airtable Quote record
        const subtotal = lineItems.reduce((s, li) => {
          const p = li.unitPrice !== undefined ? Number(li.unitPrice) : (Number(li.unitAmount) || 0) / 100;
          const q = Number(li.quantity || li.qty) || 1;
          return s + p * q;
        }, 0);
        const expiresDate = new Date(expiresAt * 1000).toISOString().split('T')[0];
        const today      = new Date().toISOString().split('T')[0];

        let woName = '';
        if (workOrderId) {
          try { const wo = await airtableGetById('Work Orders', workOrderId); woName = wo.fields?.['Work Order Name'] || ''; } catch (e) {}
        }

        const photoUrls = Array.isArray(body.photoUrls) ? body.photoUrls : [];

        const atQuoteFields = {
          'Quote Title':     `${custName} — ${woName || 'Estimate'} — ${today}`,
          'Status':          'Draft',
          'Stripe Quote ID': stripeQuote.id,
          'Expiration Date': expiresDate,
          'Total Amount':    subtotal,
          'Customer':        [customerId]
        };
        if (workOrderId)        atQuoteFields['Work Order'] = [workOrderId];
        if (notes)              atQuoteFields['Notes']      = notes;
        if (description)        atQuoteFields['Notes']      = description + (notes ? '\n' + notes : '');
        // Append photo URLs (delimited so GET /api/approve can parse them back)
        if (photoUrls.length) {
          const existing = atQuoteFields['Notes'] || '';
          atQuoteFields['Notes'] = (existing ? existing + '\n' : '') + '---PHOTOS---\n' + photoUrls.join('\n');
        }

        const atQuote = await airtablePost('Quotes', atQuoteFields);

        // Our own approval page URL — always available immediately from the WO ID
        const approveUrl = workOrderId ? `${APPROVE_BASE_URL}?wo=${workOrderId}` : null;

        // Write Stripe Quote ID + approve URL back to Work Order
        if (workOrderId) {
          const woUpdate = { 'Stripe Quote ID': stripeQuote.id };
          if (approveUrl) woUpdate['Stripe Quote URL'] = approveUrl;
          await airtablePatch('Work Orders', workOrderId, woUpdate);
        }

        // Send estimate email to customer only when finalizing (not on draft save)
        if (finalize && approveUrl && custEmail && env.RESEND_API_KEY) {
          try {
            const emailTotal = lineItems.reduce((s, li) => {
              const p = li.unitPrice !== undefined ? Number(li.unitPrice) : (Number(li.unitAmount) || 0) / 100;
              const q = Number(li.quantity || li.qty) || 1;
              return s + p * q;
            }, 0);
            const emailLineItems = lineItems.map(li => ({
              name:   (li.productName || li.name || 'Service').trim(),
              amount: li.unitPrice !== undefined
                ? Number(li.unitPrice) * (Number(li.quantity || li.qty) || 1)
                : (Number(li.unitAmount) || 0) / 100 * (Number(li.qty) || 1)
            }));
            await sendEmail(env.RESEND_API_KEY, {
              to:      custEmail,
              subject: 'Your CJB Comfort Estimate is Ready to Review',
              html:    emailEstimateHtml({
                customerName: custName,
                approveUrl,
                description,
                lineItems:    emailLineItems,
                total:        emailTotal
              })
            });
          } catch (emailErr) {
            console.error('Estimate email failed (non-fatal):', emailErr.message);
          }
        }

        // Finalize the Stripe quote (makes it Open in Stripe for clean records — non-fatal)
        let quoteStatus = 'draft';
        if (finalize) {
          try {
            await stripePost(STRIPE_KEY, `/v1/quotes/${stripeQuote.id}/finalize`, {});
            quoteStatus = 'open';
            const atFin = { 'Status': 'Open' };
            await airtablePatch('Quotes', atQuote.id, atFin);
            if (workOrderId) {
              await airtablePatch('Work Orders', workOrderId, { 'Status': 'Estimate Sent' });
            }
          } catch (finErr) {
            console.error('Quote finalize failed (non-fatal):', finErr.message);
            // Non-fatal — quote stays as draft in Stripe; approval page still works
            if (workOrderId) {
              try { await airtablePatch('Work Orders', workOrderId, { 'Status': 'Estimate Sent' }); } catch(e) {}
            }
          }
        }

        return new Response(JSON.stringify({
          ok: true, stripeQuoteId: stripeQuote.id, airtableQuoteId: atQuote.id,
          status: quoteStatus, approveUrl
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
        const body2 = await request.json();
        const { stripeQuoteId, airtableQuoteId, workOrderId, customerId, lineItems, notes } = body2;
        const finalizePatch = body2.finalize === true;
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

        // Create replacement draft FIRST — cancel old quote only after new one succeeds.
        // (Old order was cancel-then-create; if create failed the quote was lost permanently.)
        const expiresAt = Math.floor(Date.now() / 1000) + 30 * 24 * 60 * 60;
        const patchLineItems = [];
        for (const item of lineItems) {
          const qty     = Number(item.quantity) || 1;
          const isWhole = Number.isInteger(qty);
          const cents   = Math.round((item.unitPrice || 0) * (isWhole ? 1 : qty) * 100);
          const prod    = await stripePost(STRIPE_KEY, '/v1/products', {
            name: (item.productName || 'Service').trim(), type: 'service'
          });
          patchLineItems.push({
            price_data: { currency: 'usd', product: prod.id, unit_amount: String(cents) },
            quantity:   String(isWhole ? Math.max(1, qty) : 1)
          });
        }
        const quoteParamsObj = {
          customer:   stripeCustId,
          expires_at: String(expiresAt),
          line_items: patchLineItems
        };
        if (notes)       quoteParamsObj.description                         = notes.slice(0, 500);
        if (workOrderId) quoteParamsObj['metadata[work_order_airtable_id]'] = workOrderId;

        const newQuote = await stripePostNested(STRIPE_KEY, '/v1/quotes', quoteParamsObj);

        // New quote created successfully — now safe to cancel the old one
        try { await stripePost(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}/cancel`, {}); } catch (e) {}

        const subtotal    = lineItems.reduce((s, li) => s + ((li.unitPrice || 0) * (Number(li.quantity) || 1)), 0);
        const expiresDate = new Date(expiresAt * 1000).toISOString().split('T')[0];

        const patchApproveUrl = workOrderId ? `${APPROVE_BASE_URL}?wo=${workOrderId}` : null;

        if (airtableQuoteId) {
          const upd = {
            'Status':          'Draft',
            'Stripe Quote ID': newQuote.id,
            'Expiration Date': expiresDate,
            'Total Amount':    subtotal,
            'Stripe Quote URL': patchApproveUrl || ''
          };
          if (notes !== undefined) upd['Notes'] = notes;
          await airtablePatch('Quotes', airtableQuoteId, upd);
        }

        if (workOrderId && patchApproveUrl) {
          await airtablePatch('Work Orders', workOrderId, { 'Stripe Quote ID': newQuote.id, 'Stripe Quote URL': patchApproveUrl });
        }

        // Finalize + email if requested (non-fatal)
        let patchStatus = 'draft';
        if (finalizePatch) {
          try {
            await stripePost(STRIPE_KEY, `/v1/quotes/${newQuote.id}/finalize`, {});
            patchStatus = 'open';
            if (airtableQuoteId) await airtablePatch('Quotes', airtableQuoteId, { 'Status': 'Open' });
            if (workOrderId) await airtablePatch('Work Orders', workOrderId, { 'Status': 'Estimate Sent' });
          } catch (finErr) {
            if (workOrderId) { try { await airtablePatch('Work Orders', workOrderId, { 'Status': 'Estimate Sent' }); } catch(e) {} }
          }
          // Send estimate email
          if (patchApproveUrl && customerId && env.RESEND_API_KEY) {
            try {
              const cRec = await airtableGetById('Customers', customerId);
              const cEmail = (cRec.fields['Email'] || '').trim();
              const cName  = cRec.fields['Customer Name'] || 'Customer';
              if (cEmail) {
                const emailTotal = lineItems.reduce((s, li) => s + ((li.unitPrice||0) * (Number(li.quantity)||1)), 0);
                const emailLineItems = lineItems.map(li => ({ name: li.productName||'Service', amount: (li.unitPrice||0)*(Number(li.quantity)||1) }));
                await sendEmail(env.RESEND_API_KEY, {
                  to: cEmail, subject: 'Your CJB Comfort Estimate is Ready to Review',
                  html: emailEstimateHtml({ customerName: cName, approveUrl: patchApproveUrl, description: notes||'', lineItems: emailLineItems, total: emailTotal })
                });
              }
            } catch(e) { /* non-fatal */ }
          }
        }

        return new Response(JSON.stringify({
          ok: true, stripeQuoteId: newQuote.id, airtableQuoteId, status: patchStatus, approveUrl: patchApproveUrl
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // ── Equipment POST — serial number deduplication ─────────────────────
    // Prevents duplicate records when a tech retries a failed save or
    // double-scans the same data tag. If a real serial number is provided,
    // check whether that unit already exists at this property before creating.
    if (path === '/api/Equipment' && request.method === 'POST') {
      const body = await request.json();
      const fields = body.fields || {};
      const serial  = (fields['Serial Number'] || '').trim();
      const propId  = (fields['Property'] || [])[0] || '';

      if (serial && serial !== 'SN-PENDING' && propId) {
        try {
          const formula = `AND({Serial Number}="${serial.replace(/"/g, '\\"')}",SEARCH("${propId}",ARRAYJOIN({Property},";")))`;
          const dupRes  = await airtableGet('Equipment', formula);
          if (dupRes.records && dupRes.records.length > 0) {
            // Already exists — return it as if we just created it (idempotent)
            return new Response(JSON.stringify(dupRes.records[0]), {
              status: 200,
              headers: { ...corsHeaders, 'Content-Type': 'application/json' }
            });
          }
        } catch(e) { /* dedup check failed — fall through and create normally */ }
      }

      // No duplicate found — create via normal Airtable proxy
      const atBase = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Equipment`;
      const atRes  = await fetch(atBase, {
        method: 'POST',
        headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const atBody = await atRes.text();
      return new Response(atBody, {
        status: atRes.status,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
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
// Patch a set of Job records to a new status, skipping any that are Cancelled.
// Fetches current status in one batch query before patching.
async function patchBillableJobs(jobIds, status) {
  if (!jobIds || !jobIds.length) return;
  const formula = jobIds.length === 1
    ? `RECORD_ID()="${jobIds[0]}"`
    : `OR(${jobIds.map(id => `RECORD_ID()="${id}"`).join(',')})`;
  const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent('Jobs')}` +
    `?filterByFormula=${encodeURIComponent(formula)}&pageSize=100`;
  try {
    const res  = await fetch(url, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
    const data = await res.json();
    const billable = (data.records || [])
      .filter(j => j.fields['Status'] !== 'Cancelled')
      .map(j => j.id);
    if (billable.length) {
      await Promise.all(billable.map(jid => airtablePatch('Jobs', jid, { 'Status': status })));
    }
  } catch(e) {
    console.error('patchBillableJobs failed (non-fatal):', e.message);
  }
}

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
// Pin to a recent API version so hosted_quote_url and other newer fields are always returned.
const STRIPE_VERSION = '2026-04-22.dahlia';

async function stripePost(apiKey, path, params) {
  const body = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) body.append(k, String(v));
  }
  const res = await fetch(`https://api.stripe.com${path}`, {
    method: 'POST',
    headers: {
      Authorization:    `Bearer ${apiKey}`,
      'Content-Type':   'application/x-www-form-urlencoded',
      'Stripe-Version': STRIPE_VERSION
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
    headers: {
      Authorization:    `Bearer ${apiKey}`,
      'Content-Type':   'application/x-www-form-urlencoded',
      'Stripe-Version': STRIPE_VERSION
    },
    body: body.toString()
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Stripe ${path}: ${data.error?.message || JSON.stringify(data)}`);
  return data;
}

async function stripeGet(apiKey, path) {
  const res = await fetch(`https://api.stripe.com${path}`, {
    headers: { Authorization: `Bearer ${apiKey}`, 'Stripe-Version': STRIPE_VERSION }
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

// ── Resend email helper ───────────────────────────────────────────────────────
// ── SMS via Telnyx ────────────────────────────────────────────────────────
function normalizePhone(raw) {
  if (!raw) return null;
  const digits = String(raw).replace(/\D/g, '');
  if (digits.length === 10) return '+1' + digits;
  if (digits.length === 11 && digits.startsWith('1')) return '+' + digits;
  return null; // can't normalize — don't send
}

async function sendSms(apiKey, toRaw, text) {
  if (!apiKey || !toRaw || !text) return;
  const to = normalizePhone(toRaw);
  if (!to) throw new Error(`sendSms: could not normalize phone: ${toRaw}`);
  const res = await fetch('https://api.telnyx.com/v2/messages', {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ from: TELNYX_FROM, to, text })
  });
  if (!res.ok) {
    const errBody = await res.text();
    throw new Error(`Telnyx ${res.status}: ${errBody}`);
  }
}

// ── AZ date/time formatter (always UTC-7, no DST) ─────────────────────────
function formatAZDateTime(isoUtc) {
  // Returns { dateStr, timeStr, endTimeStr (+2 hrs), dayStr } for use in emails.
  if (!isoUtc) return { dateStr: '', timeStr: '', endTimeStr: '', dayStr: '' };
  const d  = new Date(isoUtc);
  const az = new Date(d.getTime() - 7 * 3600000); // shift UTC → AZ
  const DAYS   = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  const MONTHS = ['January','February','March','April','May','June',
                  'July','August','September','October','November','December'];
  const fmt12 = azDate => {
    let h = azDate.getUTCHours(), m = azDate.getUTCMinutes();
    const ap = h >= 12 ? 'PM' : 'AM';
    h = h % 12 || 12;
    return `${h}${m > 0 ? ':' + String(m).padStart(2,'0') : ''} ${ap}`;
  };
  const end = new Date(az.getTime() + 2 * 3600000);
  return {
    dayStr:     DAYS[az.getUTCDay()],
    dateStr:    `${DAYS[az.getUTCDay()]}, ${MONTHS[az.getUTCMonth()]} ${az.getUTCDate()}, ${az.getUTCFullYear()}`,
    timeStr:    fmt12(az),
    endTimeStr: fmt12(end),
  };
}

// ── Base email wrapper (all customer-facing emails use this) ──────────────
function emailBase({ preheader = '', body, marketingFooter = false }) {
  const unsubFooter = marketingFooter
    ? `<p style="margin:8px 0 0;font-size:11px;color:#9ca3af;">You're receiving this because you're a CJB Comfort customer.
         <a href="{{unsubscribe_url}}" style="color:#9ca3af;">Unsubscribe</a></p>`
    : '';
  // preheader text is hidden but shown in email client inbox preview
  const preheaderHtml = preheader
    ? `<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;">${preheader}${'&nbsp;‌'.repeat(40)}</div>`
    : '';
  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CJB Comfort</title></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,Helvetica,sans-serif;">
${preheaderHtml}
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f3f4f6;">
<tr><td align="center" style="padding:28px 16px;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:560px;">
  <!-- Header -->
  <tr><td style="background:#c81f25;border-radius:12px 12px 0 0;padding:24px 28px;text-align:center;">
    ${BRAND_LOGO_URL
      ? `<img src="${BRAND_LOGO_URL}" alt="${BRAND_NAME}" style="height:48px;width:auto;display:block;margin:0 auto;" border="0">`
      : `<div style="font-size:24px;font-weight:800;color:#ffffff;letter-spacing:2px;font-family:Arial,sans-serif;line-height:1;">${BRAND_NAME.toUpperCase()}</div>`
    }
    ${BRAND_TAGLINE ? `<div style="font-size:11px;color:rgba(255,255,255,0.80);letter-spacing:2px;text-transform:uppercase;margin-top:6px;">${BRAND_TAGLINE}</div>` : ''}
  </td></tr>
  <!-- Body -->
  <tr><td style="background:#ffffff;padding:32px 28px;border-radius:0 0 12px 12px;box-shadow:0 1px 4px rgba(0,0,0,0.08);">
    ${body}
  </td></tr>
  <!-- Footer -->
  <tr><td style="padding:20px 0 4px;text-align:center;">
    <p style="margin:0 0 4px;font-size:13px;color:#6b7280;">Questions? Call or text us: <a href="${OFFICE_PHONE_URL}" style="color:#c81f25;text-decoration:none;font-weight:600;">${OFFICE_PHONE}</a></p>
    <p style="margin:0 0 4px;font-size:12px;color:#9ca3af;">CJB Comfort &middot; Chandler, AZ &middot; <a href="https://cjbcomfort.com" style="color:#9ca3af;text-decoration:none;">cjbcomfort.com</a></p>
    ${unsubFooter}
  </td></tr>
</table>
</td></tr>
</table>
</body></html>`;
}

// ── Booking confirmation email ─────────────────────────────────────────────
function emailBookingConfirmedHtml({ firstName, dateStr, timeStr, endTimeStr, address, woType, problemDescription, cancelUrl, rescheduleUrl, techName = '', isReschedule = false }) {
  const greeting   = isReschedule ? 'Your appointment has been rescheduled.' : 'Your appointment is confirmed.';
  const preheader  = isReschedule
    ? `Rescheduled: your CJB Comfort visit is now ${dateStr}`
    : `Confirmed: CJB Comfort is coming ${dateStr} between ${timeStr} and ${endTimeStr}`;
  const typeLabel  = woType || 'Service Visit';
  const name       = firstName || 'there';

  const problemBlock = problemDescription ? `
    <div style="background:#f9fafb;border-radius:8px;padding:14px 18px;margin:20px 0 0;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:#9ca3af;margin-bottom:6px;">Your Request</div>
      <div style="font-size:14px;color:#374151;line-height:1.55;">${problemDescription}</div>
    </div>` : '';

  // Show reschedule/cancel links only if we have them (Calendly bookings).
  // Admin-scheduled bookings use the phone number instead.
  const changeBlock = (cancelUrl || rescheduleUrl)
    ? `<div style="border-top:1px solid #f3f4f6;margin-top:28px;padding-top:20px;">
        <p style="font-size:13px;color:#6b7280;margin:0 0 14px;line-height:1.5;">Need to make changes? You can reschedule or cancel up to 24&nbsp;hours before your appointment using the links below. After that, give us a call.</p>
        <div>
          ${rescheduleUrl ? `<a href="${rescheduleUrl}" style="display:inline-block;background:#f3f4f6;color:#374151;font-size:13px;font-weight:600;padding:10px 20px;border-radius:8px;text-decoration:none;margin-right:8px;">↩ Reschedule</a>` : ''}
          ${cancelUrl     ? `<a href="${cancelUrl}"     style="display:inline-block;background:#f3f4f6;color:#374151;font-size:13px;font-weight:600;padding:10px 20px;border-radius:8px;text-decoration:none;">✕ Cancel</a>` : ''}
        </div>
      </div>`
    : `<div style="border-top:1px solid #f3f4f6;margin-top:28px;padding-top:20px;">
        <p style="font-size:13px;color:#6b7280;margin:0;line-height:1.5;">Need to make changes? Call or text us at <a href="${OFFICE_PHONE_URL}" style="color:#c81f25;font-weight:600;">${OFFICE_PHONE}</a> at least 24&nbsp;hours before your appointment.</p>
      </div>`;

  const body = `
    <p style="font-size:18px;font-weight:700;color:#111827;margin:0 0 4px;">Hi ${name},</p>
    <p style="font-size:15px;color:#6b7280;margin:0 0 24px;">${greeting}</p>

    <div style="background:#fef2f2;border-left:4px solid #c81f25;border-radius:0 10px 10px 0;padding:20px 22px;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#c81f25;margin-bottom:10px;">${typeLabel}</div>
      <div style="font-size:20px;font-weight:800;color:#111827;margin-bottom:6px;">${dateStr}</div>
      <div style="font-size:15px;font-weight:600;color:#374151;">Arrival window: ${timeStr}&nbsp;&ndash;&nbsp;${endTimeStr}</div>
      ${address ? `<div style="font-size:13px;color:#6b7280;margin-top:8px;">&#128205; ${address}</div>` : ''}
    </div>

    ${problemBlock}

    <p style="font-size:15px;color:#374151;line-height:1.65;margin:24px 0 8px;">${techName ? techName : 'Your technician'} will send you a text when they&rsquo;re on the way&nbsp;&mdash; no need to wait by the door.</p>
    <p style="font-size:15px;color:#374151;line-height:1.65;margin:0;">They&rsquo;ll arrive ready to walk you through exactly what they find and answer any questions you have.</p>

    ${changeBlock}`;

  return emailBase({ preheader, body });
}

async function sendEmail(apiKey, { to, subject, html }) {
  if (!apiKey || !to) return; // non-fatal if key not configured or no email on file
  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ from: RESEND_FROM, to: [to], subject, html })
  });
  if (!res.ok) {
    const err = await res.text();
    console.error('Resend error:', err);
  }
}

function emailEstimateHtml({ customerName, approveUrl, description, lineItems, total }) {
  const itemRows = lineItems.length ? lineItems.map(li =>
    `<tr>
      <td style="padding:10px 0;font-size:15px;border-bottom:1px solid #f3f4f6;">${li.name}</td>
      <td style="padding:10px 0;font-size:15px;border-bottom:1px solid #f3f4f6;text-align:right;font-weight:600;">$${(li.amount||0).toFixed(2)}</td>
    </tr>`
  ).join('') : '';

  const totalRow = total > 0 ? `
    <tr>
      <td style="padding:14px 0 0;font-size:16px;font-weight:700;">Estimate Total</td>
      <td style="padding:14px 0 0;font-size:22px;font-weight:800;text-align:right;">$${total.toFixed(2)}</td>
    </tr>` : '';

  const descBlock = description ? `
    <div style="background:#f9fafb;border-radius:8px;padding:16px;margin-bottom:20px;">
      <div style="font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#6b7280;margin-bottom:8px;">Findings &amp; Recommendation</div>
      <div style="font-size:15px;color:#374151;line-height:1.55;white-space:pre-line;">${description}</div>
    </div>` : '';

  const tableBlock = (itemRows || totalRow) ? `
    <table style="width:100%;border-collapse:collapse;margin-bottom:8px;">
      <thead><tr>
        <th style="font-size:11px;font-weight:700;letter-spacing:0.5px;text-transform:uppercase;color:#6b7280;text-align:left;padding:0 0 10px;border-bottom:1px solid #e5e7eb;">Item</th>
        <th style="font-size:11px;font-weight:700;letter-spacing:0.5px;text-transform:uppercase;color:#6b7280;text-align:right;padding:0 0 10px;border-bottom:1px solid #e5e7eb;">Price</th>
      </tr></thead>
      <tbody>${itemRows}</tbody>
      ${totalRow ? `<tfoot>${totalRow}</tfoot>` : ''}
    </table>` : '';

  return `<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;padding:24px 16px;">
    <div style="background:#0f1729;padding:16px 20px;border-radius:10px 10px 0 0;text-align:center;">
      <span style="color:white;font-size:20px;font-weight:800;letter-spacing:1px;">CJB COMFORT</span>
    </div>
    <div style="background:white;padding:28px 24px;border-radius:0 0 10px 10px;box-shadow:0 1px 3px rgba(0,0,0,0.08);">
      <p style="font-size:17px;margin:0 0 20px;">Hi ${customerName},</p>
      <p style="font-size:15px;color:#374151;margin:0 0 24px;line-height:1.5;">Your technician has prepared an estimate for your review. Please take a moment to approve or decline.</p>
      ${descBlock}
      ${tableBlock}
      <div style="text-align:center;margin:28px 0;">
        <a href="${approveUrl}" style="display:inline-block;background:#059669;color:white;font-size:17px;font-weight:700;padding:16px 32px;border-radius:10px;text-decoration:none;">Review &amp; Respond to Estimate →</a>
      </div>
      <p style="font-size:13px;color:#6b7280;text-align:center;margin:0;">Or copy this link into your browser:<br><span style="color:#1e40af;">${approveUrl}</span></p>
    </div>
    <p style="text-align:center;font-size:12px;color:#9ca3af;margin-top:16px;">CJB Comfort · Arizona HVAC Services</p>
  </div>
</body></html>`;
}

function emailApprovalHtml({ customerName, approved }) {
  const icon    = approved ? '✅' : '👍';
  const heading = approved ? 'Estimate Approved!' : 'Response Recorded';
  const body    = approved
    ? "Thank you for approving — CJB Comfort will proceed with your repair. We'll be in touch to confirm next steps."
    : "We've noted your decision. A CJB Comfort team member will reach out to discuss your options.";

  return `<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
  <div style="max-width:520px;margin:0 auto;padding:24px 16px;">
    <div style="background:#0f1729;padding:16px 20px;border-radius:10px 10px 0 0;text-align:center;">
      <span style="color:white;font-size:20px;font-weight:800;letter-spacing:1px;">CJB COMFORT</span>
    </div>
    <div style="background:white;padding:40px 24px;border-radius:0 0 10px 10px;box-shadow:0 1px 3px rgba(0,0,0,0.08);text-align:center;">
      <div style="font-size:56px;margin-bottom:12px;">${icon}</div>
      <h2 style="font-size:24px;font-weight:800;margin:0 0 12px;">${heading}</h2>
      <p style="font-size:15px;color:#374151;line-height:1.5;margin:0;">Hi ${customerName},<br><br>${body}</p>
    </div>
    <p style="text-align:center;font-size:12px;color:#9ca3af;margin-top:16px;">CJB Comfort · Arizona HVAC Services</p>
  </div>
</body></html>`;
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
