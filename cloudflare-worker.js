const AIRTABLE_BASE_ID = 'appPv1xZIck89RxL2';
const AIRTABLE_API_KEY = 'patwLtwCfaf4ctJu1.5e01386c6580cb6dae0a160c566563d287f4d54fa7489c73cf5a7a920fbcb17f';
const ALLOWED_ORIGIN = 'https://bridgettrhart.github.io';
const CALENDLY_TOKEN = 'eyJraWQiOiIxY2UxZTEzNjE3ZGNmNzY2YjNjZWJjY2Y4ZGM1YmFmYThhNjVlNjg0MDIzZjdjMzJiZTgzNDliMjM4MDEzNWI0IiwidHlwIjoiUEFUIiwiYWxnIjoiRVMyNTYifQ.eyJpc3MiOiJodHRwczovL2F1dGguY2FsZW5kbHkuY29tIiwiaWF0IjoxNzc3MTU1OTU5LCJqdGkiOiIyYzVjYzU1OC1jYWM5LTQ3NjMtYmU1Ny0xNDU1ZTg5YmQ3NWUiLCJ1c2VyX3V1aWQiOiJFQ0dDUlNWSTVONVJUQ1hNIiwic2NvcGUiOiJhdmFpbGFiaWxpdHk6cmVhZCBhdmFpbGFiaWxpdHk6d3JpdGUgZXZlbnRfdHlwZXM6cmVhZCBldmVudF90eXBlczp3cml0ZSBsb2NhdGlvbnM6cmVhZCByb3V0aW5nX2Zvcm1zOnJlYWQgc2hhcmVzOndyaXRlIHNjaGVkdWxlZF9ldmVudHM6cmVhZCBzY2hlZHVsZWRfZXZlbnRzOndyaXRlIHNjaGVkdWxpbmdfbGlua3M6d3JpdGUgZ3JvdXBzOnJlYWQgb3JnYW5pemF0aW9uczpyZWFkIG9yZ2FuaXphdGlvbnM6d3JpdGUgdXNlcnM6cmVhZCB3ZWJob29rczpyZWFkIHdlYmhvb2tzOndyaXRlIn0.oSQlX0qk5yH2Mt8a0OMWBjxz-Tf6iHdJvZKKjuPik2-YygsLKQhGCXQTaYIDqhMaBaYd5c5FwOI0ZbJBCsaZug';
const WEBHOOK_URL = 'https://cjb-comfort-proxy.bridgettrhart.workers.dev/api/calendly-webhook';

const EVENT_TYPE_MAP = {
  'inspection': 'Inspection',
  'maintenance': 'Maintenance',
  'repair': 'Repair',
  'installation': 'Installation',
  'estimate': 'Estimate Only',
  'warranty': 'Warranty',
};

function mapEventType(calendlyName) {
  const key = (calendlyName || '').toLowerCase().trim();
  for (const [pattern, value] of Object.entries(EVENT_TYPE_MAP)) {
    if (key.includes(pattern)) return value;
  }
  return 'Inspection';
}

const ALLOWED_TABLES = [
  'Customers', 'Contacts', 'Properties', 'Equipment',
  'Jobs', 'Work Orders', 'Technicians', 'Product List',
  'Maintenance Contracts', 'Invoices', 'Companies'
];

async function airtableGet(path, params = {}) {
  let url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(path)}`;
  const qs = new URLSearchParams(params).toString();
  if (qs) url += '?' + qs;
  const res = await fetch(url, { headers: { 'Authorization': `Bearer ${AIRTABLE_API_KEY}` } });
  return res.json();
}

async function airtablePost(table, fields) {
  const res = await fetch(
    `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(table)}`,
    {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ fields }),
    }
  );
  const data = await res.json();
  if (!res.ok) console.error('Airtable POST error:', JSON.stringify(data), 'Fields sent:', JSON.stringify(fields));
  return data;
}

async function airtablePatch(table, recordId, fields) {
  const res = await fetch(
    `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(table)}/${recordId}`,
    {
      method: 'PATCH',
      headers: { 'Authorization': `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ fields }),
    }
  );
  return res.json();
}

export default {
  async fetch(request, env) {
    const corsHeaders = {
      'Access-Control-Allow-Origin': ALLOWED_ORIGIN,
      'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);

    // ── One-time webhook registration ─────────────────────────────────────────
    if (url.pathname === '/api/setup-calendly-webhook') {
      try {
        const meRes = await fetch('https://api.calendly.com/users/me', {
          headers: { 'Authorization': `Bearer ${CALENDLY_TOKEN}` }
        });
        const meData = await meRes.json();
        if (!meData.resource) {
          return new Response('Token error: ' + JSON.stringify(meData), { status: 400 });
        }
        const userUri = meData.resource.uri;
        const orgUri = meData.resource.current_organization;
        const whRes = await fetch('https://api.calendly.com/webhook_subscriptions', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${CALENDLY_TOKEN}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            url: WEBHOOK_URL,
            events: ['invitee.created', 'invitee.canceled'],
            organization: orgUri,
            user: userUri,
            scope: 'user',
          }),
        });
        const whData = await whRes.json();
        if (whRes.status === 201) {
          return new Response('SUCCESS — Calendly webhook registered! You can close this tab.', { status: 200 });
        } else {
          return new Response('Calendly response: ' + JSON.stringify(whData), { status: 400 });
        }
      } catch (err) {
        return new Response('Error: ' + err.message, { status: 500 });
      }
    }

    // ── Calendly webhook receiver ─────────────────────────────────────────────
    if (url.pathname === '/api/calendly-webhook') {
      if (request.method !== 'POST') {
        return new Response('Method not allowed', { status: 405 });
      }

      let body;
      try { body = await request.json(); }
      catch { return new Response('Invalid JSON', { status: 400 }); }

      const eventType = body.event;
      const payload = body.payload;

      const getAnswer = (qna, keyword) => {
        const match = (qna || []).find(item =>
          item.question && item.question.toLowerCase().includes(keyword.toLowerCase())
        );
        return match ? (match.answer || '').trim() : '';
      };

      if (eventType === 'invitee.created') {
        const inviteeName = payload.name || 'Unknown';
        const inviteeEmail = (payload.email || '').toLowerCase().trim();
        const eventUri = payload.event || '';
        const calendlyEventUuid = eventUri.split('/').pop() || '';
        const qna = payload.questions_and_answers || [];

        let startTime = '';
        let calendlyEventTypeName = 'Inspection';
        if (calendlyEventUuid) {
          try {
            const evRes = await fetch(`https://api.calendly.com/scheduled_events/${calendlyEventUuid}`, {
              headers: { 'Authorization': `Bearer ${CALENDLY_TOKEN}` }
            });
            const evData = await evRes.json();
            startTime = evData.resource?.start_time || '';
            calendlyEventTypeName = evData.resource?.name || 'Inspection';
          } catch(e) {
            console.error('Could not fetch Calendly event:', e.message);
          }
        }

        const workOrderType = mapEventType(calendlyEventTypeName);
        const phone = getAnswer(qna, 'phone');
        const serviceAddress = getAnswer(qna, 'service address') || getAnswer(qna, 'address');
        const unitCountRaw = getAnswer(qna, 'how many');
        const problemDesc = getAnswer(qna, 'known issues') || getAnswer(qna, 'notes');

        let city = '';
        let state = 'AZ';
        let zip = '';
        if (serviceAddress) {
          const parts = serviceAddress.split(',');
          if (parts.length >= 2) city = parts[parts.length - 2].trim();
          // Try to parse state and zip from last segment: "AZ 85028"
          const last = (parts[parts.length - 1] || '').trim();
          const stateZip = last.match(/([A-Z]{2})\s+(\d{5})/);
          if (stateZip) { state = stateZip[1]; zip = stateZip[2]; }
        }

        let scheduledDate = '';
        if (startTime) {
          const arizonaMs = new Date(startTime).getTime() - (7 * 60 * 60 * 1000);
          scheduledDate = new Date(arizonaMs).toISOString().split('T')[0];
        }

        const notesLines = [
          phone ? `Phone: ${phone}` : '',
          inviteeEmail ? `Email: ${inviteeEmail}` : '',
          unitCountRaw ? `Units: ${unitCountRaw}` : '',
        ].filter(Boolean).join('\n');

        // ── Step 1: Find or create Customer ──────────────────────────────
        let customerId = null;
        if (inviteeEmail) {
          try {
            const custSearch = await airtableGet('Customers', {
              filterByFormula: `{Email}="${inviteeEmail}"`,
              maxRecords: '1',
            });
            if (custSearch.records && custSearch.records.length > 0) {
              customerId = custSearch.records[0].id;
              console.error('Found existing customer:', customerId);
            } else {
              const nameParts = inviteeName.trim().split(' ');
              const custFields = {
                'Customer Name': inviteeName,
                'First Name': nameParts[0] || '',
                'Last Name': nameParts.slice(1).join(' ') || '',
                'Email': inviteeEmail,
                'Active': true,
                'Notes': 'Created automatically from Calendly booking',
              };
              if (phone) custFields['Phone'] = phone;
              const newCust = await airtablePost('Customers', custFields);
              customerId = newCust.id;
              console.error('Created new customer:', customerId);
            }
          } catch(e) {
            console.error('Customer lookup/create failed:', e.message);
          }
        }

        // ── Step 2: Find or create Property ──────────────────────────────
        // Search by Customer Email field on Properties
        let propertyId = null;
        if (inviteeEmail) {
          try {
            const propSearch = await airtableGet('Properties', {
              filterByFormula: `{Customer Email}="${inviteeEmail}"`,
              maxRecords: '10',
            });
            if (propSearch.records && propSearch.records.length === 1) {
              // Exactly one property — use it
              propertyId = propSearch.records[0].id;
              console.error('Found existing property:', propertyId);
            } else if (propSearch.records && propSearch.records.length > 1) {
              // Multiple properties — can't auto-pick, leave unlinked, note it
              console.error('Multiple properties found for', inviteeEmail, '— leaving unlinked');
            } else if (serviceAddress) {
              // No property found — create one
              const propFields = {
                'Property Name': inviteeName,
                'Service Address': serviceAddress,
                'Customer Email': inviteeEmail,
                'Active': true,
                'Notes': 'Created automatically from Calendly booking',
              };
              if (city) propFields['City'] = city;
              if (state) propFields['State'] = state;
              if (zip) propFields['Zip'] = zip;
              if (customerId) propFields['Customer'] = [customerId];
              const newProp = await airtablePost('Properties', propFields);
              propertyId = newProp.id;
              console.error('Created new property:', propertyId);
            }
          } catch(e) {
            console.error('Property lookup/create failed:', e.message);
          }
        }

        // ── Step 3: Create Work Order ─────────────────────────────────────
        const woFields = {
          'Work Order Name': `${inviteeName} — ${workOrderType}`,
          'Status': 'Scheduled',
          'Work Order Type': workOrderType,
          'Calendly ID': calendlyEventUuid,
        };

        if (scheduledDate) woFields['Scheduled Date'] = scheduledDate;
        if (problemDesc) woFields['Problem Description'] = problemDesc;
        if (notesLines) woFields['Notes'] = notesLines;
        if (customerId) woFields['Customer'] = [customerId];
        if (propertyId) woFields['Property'] = [propertyId];

        await airtablePost('Work Orders', woFields);
        return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }

      if (eventType === 'invitee.canceled') {
        const eventUri = payload.event || '';
        const calendlyEventUuid = eventUri.split('/').pop() || '';
        if (!calendlyEventUuid) {
          return new Response(JSON.stringify({ ok: true }), { status: 200 });
        }
        const searchData = await airtableGet('Work Orders', {
          filterByFormula: `{Calendly ID}="${calendlyEventUuid}"`,
          maxRecords: '1',
        });
        if (searchData.records && searchData.records.length > 0) {
          await airtablePatch('Work Orders', searchData.records[0].id, { 'Status': 'Cancelled' });
        }
        return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }

      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    }

    // ── Airtable proxy ────────────────────────────────────────────────────────
    const pathWithoutApi = url.pathname.replace(/^\/api\//, '');
    const decoded = decodeURIComponent(pathWithoutApi);

    let matchedTable = null;
    let recordId = '';

    for (const table of ALLOWED_TABLES) {
      if (decoded === table) { matchedTable = table; recordId = ''; break; }
      if (decoded.startsWith(table + '/')) { matchedTable = table; recordId = decoded.slice(table.length + 1); break; }
    }

    if (!matchedTable) {
      return new Response('Table not allowed', { status: 403, headers: corsHeaders });
    }

    let airtableUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(matchedTable)}`;
    if (recordId) airtableUrl += `/${recordId}`;
    const queryParams = url.searchParams.toString();
    if (queryParams) airtableUrl += `?${queryParams}`;

    const airtableRequest = new Request(airtableUrl, {
      method: request.method,
      headers: { 'Authorization': `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' },
      body: ['POST', 'PATCH'].includes(request.method) ? request.body : undefined,
    });

    const response = await fetch(airtableRequest);
    const data = await response.json();

    return new Response(JSON.stringify(data), {
      status: response.status,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
};
