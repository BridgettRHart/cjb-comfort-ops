// ═══════════════════════════════════════════════════════════════════════════
//  CJB Comfort — Cloudflare Worker
//  Deploy: cjb-comfort-proxy.bridgettrhart.workers.dev
//  Portal: portal.cjbcomfort.com
//
//  Environment secrets (Cloudflare dashboard → Settings → Variables):
//    AIRTABLE_API_KEY, AIRTABLE_BASE_ID, ANTHROPIC_API_KEY,
//    CALENDLY_TOKEN, RESEND_API_KEY, STRIPE_SECRET_KEY,
//    STRIPE_PUBLISHABLE_KEY, STRIPE_WEBHOOK_SECRET,
//    QUO_API_KEY, WAVE_API_KEY
// ═══════════════════════════════════════════════════════════════════════════

const WAVE_BUSINESS_ID       = 'QnVzaW5lc3M6ODQyOTljZjItODAyNy00NzFiLWE1NGUtOWVmYzZlZjRlNDY1';
const WAVE_INCOME_ACCOUNT_ID = 'QWNjb3VudDo2Mzg1NDYxMDc5MTUzNTU5MTg7QnVzaW5lc3M6ODQyOTljZjItODAyNy00NzFiLWE1NGUtOWVmYzZlZjRlNDY1';
let _waveServiceProductId = null; // cached per Worker instance

const R2_PUBLIC_URL    = 'https://pub-53ca3c753a32459a8ecc3f361afc4ab2.r2.dev';
const APPROVE_BASE_URL = 'https://app.cjbcomfort.com/approve.html';

const RESEND_FROM      = 'CJB Comfort <office@mail.cjbcomfort.com>';
const REPLY_TO_EMAIL   = 'service@cjbcomfort.com'; // customer replies land here
const PORTAL_URL       = 'https://portal.cjbcomfort.com';
const EA_REBATE_CUSTOMER_ID = 'recv1gOnHklRXsfoQ'; // "Efficiency Arizona — HEAR Program" — shared internal billing entity for all EA rebate receivables
const MANAGE_BASE_URL  = 'https://app.cjbcomfort.com/manage.html';
const ADMIN_EMAIL      = 'bridgett@cjbcomfort.com'; // admin notification destination
const OFFICE_PHONE     = '(480) 604-8622';
const OFFICE_PHONE_URL = 'tel:+14806048622';
const QUO_FROM         = '+14806048622'; // ported main number (formerly Telnyx +14808639119)
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
let _ctx = null; // Cloudflare execution context — set per request so logCommunication can use ctx.waitUntil()

const ALLOWED_TABLES = [
  'Customers','Contacts','Properties','Equipment','Jobs',
  'Work Orders','Technicians','Product List',
  'Maintenance Contracts','Invoices','Companies','Quotes','Follow-Ups',
  'EA Projects'
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
  async scheduled(event, env, ctx) {
    // Load secrets (same as fetch handler)
    AIRTABLE_BASE_ID = env.AIRTABLE_BASE_ID || AIRTABLE_BASE_ID;
    AIRTABLE_API_KEY = env.AIRTABLE_API_KEY || AIRTABLE_API_KEY;
    ctx.waitUntil(Promise.all([
      sendAppointmentReminders(env),
      checkContractRenewals(env),
      checkOverdueInvoices(env),
      checkResendDomain(env),
    ]));
  },

  async fetch(request, env, ctx) {
    // Load secrets from Cloudflare environment on every request
    AIRTABLE_BASE_ID = env.AIRTABLE_BASE_ID || AIRTABLE_BASE_ID;
    AIRTABLE_API_KEY = env.AIRTABLE_API_KEY || AIRTABLE_API_KEY;
    CALENDLY_TOKEN   = env.CALENDLY_TOKEN   || CALENDLY_TOKEN;
    _ctx = ctx; // make ctx available to logCommunication for ctx.waitUntil()

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
          let address = ''; // full address string (used for WO notes + property name)
          let addrStreet = '', addrCity = '', addrState = 'AZ', addrZip = '';

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
                // Calendly location field may carry a full address — parse all parts
                const evLocation = evData.resource?.location?.location || '';
                if (evLocation) {
                  address = evLocation;
                  ({ street: addrStreet, city: addrCity, state: addrState, zip: addrZip } = parseAddressString(evLocation));
                }
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

          let phone = inviteePhone, unitCount = '', problemDesc = '', leadSource = '', referredBy = '';
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
              // Q&A form collects street only — keep city/state/zip from evLocation if available
              address = a;
              addrStreet = a;
            } else if (q.includes('hear about')) {
              leadSource = a;
            } else if (q.includes('referred by') || q.includes('their name')) {
              referredBy = a;
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
              const newCustFields = {
                'Customer Name': inviteeName,
                'First Name':    nameParts[0] || '',
                'Last Name':     nameParts.slice(1).join(' ') || '',
                'Email':         inviteeEmail,
                'Phone':         phone,
                'Type':          'Residential',
                'Customer Tags': ['Residential'],
                'Lead Source':   leadSource ? leadSource.split(',').map(s => s.trim()).filter(Boolean) : ['Calendly'],
                'Active':        true
              };
              if (referredBy) newCustFields['Referred By'] = referredBy;
              const newCust = await airtablePost('Customers', newCustFields);
              customerId = newCust.id;
              // Auto-create primary contact record for new customer
              await airtablePost('Contacts', {
                'Contact Name':       inviteeName,
                'First Name':         nameParts[0] || '',
                'Last Name':          nameParts.slice(1).join(' ') || '',
                'Email':              inviteeEmail,
                'Phone':              phone,
                'Customers':          [newCust.id],
                'Is Primary Contact': true,
                'Active':             true
              });
            }
          }

          // Find or create Property
          let propertyId = null;
          {
            let existingProps = [];
            if (inviteeEmail) {
              const propSearch = await airtableGet('Properties', `{Customer Email}="${inviteeEmail}"`);
              existingProps = propSearch.records || [];
            }

            if (existingProps.length > 0 && (addrStreet || address)) {
              // Match by address so a customer with multiple locations gets separate properties
              const normalize = s => (s || '').toLowerCase().replace(/[,.\s]+/g, ' ').trim();
              const incoming  = normalize(addrStreet || address);
              const matched   = existingProps.find(p => {
                const pAddr = normalize(p.fields['Service Address']);
                return pAddr && (pAddr === incoming || pAddr.includes(incoming) || incoming.includes(pAddr));
              });
              if (matched) {
                propertyId = matched.id; // same address → reuse existing property
              }
              // No address match → fall through to create a new property below
            } else if (existingProps.length === 1 && !address) {
              // No address from booking and only one property on file → use it
              propertyId = existingProps[0].id;
            }
            // Multiple properties + no address → leave unlinked, Bridgett assigns manually

            if (!propertyId && (addrStreet || address)) {
              // Create new property (first booking, or address doesn't match any existing)
              const propFields = {
                'Property Name':   inviteeName + ' — ' + (addrStreet || address),
                'Service Address': addrStreet || address,
                'City':            addrCity,
                'State':           addrState || 'AZ',
                'Zip':             addrZip,
                'Active':          true
              };
              if (inviteeEmail) propFields['Customer Email'] = inviteeEmail;
              if (customerId)   propFields['Customer']       = [customerId];
              const newProp = await airtablePost('Properties', propFields);
              propertyId = newProp.id;
            }
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
                  const reschedSubject = `Your CJB Comfort appointment has been rescheduled — ${dateStr}`;
                  await sendEmail(env.RESEND_API_KEY, {
                    to:      inviteeEmail,
                    subject: reschedSubject,
                    html,
                  });
                  logCommunication(env, {
                    type:    'Email',
                    trigger: 'Booking Rescheduled',
                    sentTo:  inviteeEmail,
                    subject: reschedSubject,
                  }).catch(() => {});

                  // Reschedule confirmation SMS
                  if (inviteePhone && env.QUO_API_KEY) {
                    const { dateStr: rd, timeStr: rt, endTimeStr: ret } = formatAZDateTime(scheduledDate);
                    const rSmsText = `Hi ${firstName || 'there'} — your CJB Comfort appointment has been rescheduled to ${rd}, ${rt}–${ret}. Questions? Call or text ${OFFICE_PHONE}. – CJB Comfort`;
                    sendSms(env.QUO_API_KEY, inviteePhone, rSmsText)
                      .then(() => logCommunication(env, { type: 'SMS', trigger: 'Booking Rescheduled', sentTo: inviteePhone, subject: 'Reschedule confirmation SMS' }))
                      .catch(e => console.error('Reschedule SMS error:', e));
                  }
                }

                return new Response(JSON.stringify({ ok: true, rescheduled: true }), {
                  headers: { ...corsHeaders, 'Content-Type': 'application/json' }
                });
              }
            }
          }

          // ── New booking (not a reschedule) — create Work Order ────────────
          // Look up the Office technician so Calendly WOs appear in their dispatch column
          let officeTechId = null;
          try {
            const officeSearch = await airtableGet('Technicians', '{Dispatch Role}="Office"');
            if (officeSearch.records?.length > 0) officeTechId = officeSearch.records[0].id;
          } catch(e) {}

          const woFields = {
            'Work Order Name': `${inviteeName} — ${workOrderType}`,
            'Status':          'Scheduled',
            'Work Order Type': workOrderType,
            'Service Mode':    'All Units',
            'Notes':           noteParts.join(' | '),
            'Active':          true
          };
          if (scheduledDate)  woFields['Scheduled Date']          = scheduledDate;
          if (scheduledEnd)   woFields['Scheduled End']           = scheduledEnd;
          if (problemDesc)    woFields['Problem Description']     = problemDesc;
          if (customerId)     woFields['Customer']                = [customerId];
          if (propertyId)     woFields['Property']                = [propertyId];
          if (eventUri)       woFields['Calendly ID']             = eventUri;
          if (cancelUrl)      woFields['Calendly Cancel URL']     = cancelUrl;
          if (rescheduleUrl)  woFields['Calendly Reschedule URL'] = rescheduleUrl;
          if (officeTechId)   woFields['Technician']              = [officeTechId];

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
            const bookSubject = `Your CJB Comfort appointment is confirmed — ${dateStr}`;
            await sendEmail(env.RESEND_API_KEY, {
              to:      inviteeEmail,
              subject: bookSubject,
              html,
            });
            logCommunication(env, {
              type:    'Email',
              trigger: 'Booking Confirm',
              sentTo:  inviteeEmail,
              subject: bookSubject,
            }).catch(() => {});

            // Booking confirmation SMS
            if (inviteePhone && env.QUO_API_KEY) {
              const smsText = `Hi ${firstName || 'there'} — your CJB Comfort ${workOrderType || 'appointment'} is confirmed for ${dateStr}, ${timeStr}–${endTimeStr}. Questions? Call or text us at ${OFFICE_PHONE}. – CJB Comfort`;
              sendSms(env.QUO_API_KEY, inviteePhone, smsText)
                .then(() => logCommunication(env, { type: 'SMS', trigger: 'Booking Confirm', sentTo: inviteePhone, subject: 'Booking confirmation SMS' }))
                .catch(e => console.error('Booking SMS error:', e));
            }
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
        const { email, phone, firstName, scheduledDate, address, woType,
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
        const adminBookSubject = `Your CJB Comfort appointment is confirmed — ${dateStr}`;
        await sendEmail(env.RESEND_API_KEY, {
          to: email,
          subject: adminBookSubject,
          html,
        });
        logCommunication(env, {
          type:    'Email',
          trigger: 'Booking Confirm',
          sentTo:  email,
          subject: adminBookSubject,
        }).catch(() => {});

        // Booking confirmation SMS (admin-scheduled)
        if (phone && env.QUO_API_KEY) {
          const smsText = `Hi ${firstName || 'there'} — your CJB Comfort ${woType || 'appointment'} is confirmed for ${dateStr}, ${timeStr}–${endTimeStr}. Questions? Call or text us at ${OFFICE_PHONE}. – CJB Comfort`;
          sendSms(env.QUO_API_KEY, phone, smsText)
            .then(() => logCommunication(env, { type: 'SMS', trigger: 'Booking Confirm', sentTo: phone, subject: 'Booking confirmation SMS' }))
            .catch(e => console.error('Admin booking SMS error:', e));
        }

        return new Response(JSON.stringify({ ok: true }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ── "On My Way" SMS — technician triggers from field app ─────────────
    if (path === '/api/sms/on-my-way' && request.method === 'POST') {
      try {
        const { phone, firstName, address, techName } = await request.json();
        if (!phone) {
          return new Response(JSON.stringify({ error: 'phone required' }),
            { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        const custGreet = firstName ? `Hi ${firstName}` : 'Hi there';
        const loc       = address ? ` to ${address}` : '';
        const tech      = techName || 'Your technician';
        const text = `${custGreet} — ${tech} is on the way${loc} and will be there soon. Questions? Call or text us at ${OFFICE_PHONE}. – CJB Comfort`;
        await sendSms(env.QUO_API_KEY, phone, text);
        logCommunication(env, {
          type:    'SMS',
          trigger: 'On My Way',
          sentTo:  phone,
          subject: 'On my way SMS',
        });
        return new Response(JSON.stringify({ ok: true }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ── Appointment manage — customer self-service (cancel / reschedule) ──
    if (path === '/api/appointment/manage' && request.method === 'GET') {
      try {
        const woId = url.searchParams.get('wo');
        if (!woId) return new Response(JSON.stringify({ error: 'wo required' }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

        const wo = await airtableGetById('Work Orders', woId);
        const f  = wo.fields;
        const status        = f['Status'] || '';
        const scheduledDate = f['Scheduled Date'] || '';

        const isCancelled          = status === 'Cancelled';
        const isComplete           = ['Complete','Invoiced','Paid','In Progress'].includes(status);
        const rescheduleRequested  = !!(f['Reschedule Requested']);

        const apptMs     = scheduledDate ? new Date(scheduledDate).getTime() : 0;
        const cutoffMs   = apptMs - 24 * 60 * 60 * 1000;
        const isPastCutoff = apptMs > 0 && Date.now() > cutoffMs;
        const isPast       = apptMs > 0 && Date.now() > apptMs;

        const { dateStr, timeStr, endTimeStr } = scheduledDate
          ? formatAZDateTime(scheduledDate) : { dateStr:'', timeStr:'', endTimeStr:'' };

        return new Response(JSON.stringify({
          ok: true,
          firstName:           (Array.isArray(f['Customer Name']) ? (f['Customer Name'][0] || '') : (f['Customer Name'] || '')).split(' ')[0] || 'there',
          customerName:        (Array.isArray(f['Customer Name']) ? (f['Customer Name'][0] || '') : (f['Customer Name'] || '')),
          dateStr, timeStr, endTimeStr,
          address:             f['Service Address'] || '',
          woType:              f['Work Order Type'] || 'Service Visit',
          status, isCancelled, isComplete, isPastCutoff, isPast, rescheduleRequested,
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    if (path === '/api/appointment/cancel' && request.method === 'POST') {
      try {
        const { woId } = await request.json();
        if (!woId) return new Response(JSON.stringify({ error: 'woId required' }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

        const wo = await airtableGetById('Work Orders', woId);
        const f  = wo.fields;
        const scheduledDate = f['Scheduled Date'] || '';
        const apptMs   = scheduledDate ? new Date(scheduledDate).getTime() : 0;
        const cutoffMs = apptMs - 24 * 60 * 60 * 1000;
        if (apptMs && Date.now() > cutoffMs) {
          return new Response(JSON.stringify({ error: 'past_cutoff' }),
            { status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        await airtablePatch('Work Orders', woId, { 'Status': 'Cancelled' });

        const { dateStr, timeStr } = scheduledDate
          ? formatAZDateTime(scheduledDate) : { dateStr: '', timeStr: '' };
        const customerName = (Array.isArray(f['Customer Name']) ? (f['Customer Name'][0] || '') : (f['Customer Name'] || '')) || 'A customer';
        await sendEmail(env.RESEND_API_KEY, {
          to:      ADMIN_EMAIL,
          subject: `❌ Cancellation: ${customerName} — ${dateStr || 'upcoming appointment'}`,
          html:    emailAdminAlertHtml({ type: 'cancel', customerName,
                     dateStr, timeStr, address: f['Service Address'] || '',
                     woType: f['Work Order Type'] || 'appointment' }),
        });

        return new Response(JSON.stringify({ ok: true }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    if (path === '/api/appointment/reschedule-request' && request.method === 'POST') {
      try {
        const { woId } = await request.json();
        if (!woId) return new Response(JSON.stringify({ error: 'woId required' }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

        const wo = await airtableGetById('Work Orders', woId);
        const f  = wo.fields;
        const scheduledDate = f['Scheduled Date'] || '';
        const apptMs   = scheduledDate ? new Date(scheduledDate).getTime() : 0;
        const cutoffMs = apptMs - 24 * 60 * 60 * 1000;
        if (apptMs && Date.now() > cutoffMs) {
          return new Response(JSON.stringify({ error: 'past_cutoff' }),
            { status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        await airtablePatch('Work Orders', woId, { 'Reschedule Requested': true });

        const { dateStr, timeStr } = scheduledDate
          ? formatAZDateTime(scheduledDate) : { dateStr: '', timeStr: '' };
        const customerName = (Array.isArray(f['Customer Name']) ? (f['Customer Name'][0] || '') : (f['Customer Name'] || '')) || 'A customer';
        await sendEmail(env.RESEND_API_KEY, {
          to:      ADMIN_EMAIL,
          subject: `🔄 Reschedule Request: ${customerName} — ${dateStr || 'upcoming appointment'}`,
          html:    emailAdminAlertHtml({ type: 'reschedule', customerName,
                     dateStr, timeStr, address: f['Service Address'] || '',
                     woType: f['Work Order Type'] || 'appointment' }),
        });

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
            const liRes = await stripeGet(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}/line_items?limit=100`);
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
          customerName: (Array.isArray(woFields['Customer Name']) ? (woFields['Customer Name'][0] || '') : (woFields['Customer Name'] || '')) || 'Customer',
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

        // Don't overwrite Invoiced/Paid — the service call is already closed;
        // the new Repair WO created by the quote.accepted webhook is the follow-up.
        const wo0 = await airtableGetById('Work Orders', woId);
        const currentStatus = wo0.fields['Status'] || '';
        const newStatus = decision === 'approved' ? 'Estimate Approved' : 'Estimate Declined';
        if (!['Invoiced','Paid'].includes(currentStatus)) {
          await airtablePatch('Work Orders', woId, { 'Status': newStatus });
        }

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
          const quoteLinks = wo.fields['Quotes'] || [];
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

    // ── Convert an approved estimate into a real Job Work Order ────────────
    // Single source of truth for this business rule — any client (admin app,
    // future mobile/automations) gets the same correct linking every time,
    // instead of each client having to remember to set both links itself.
    if (path === '/api/convert-to-job' && request.method === 'POST') {
      try {
        const { woId, quoteId } = await request.json();
        if (!woId) return new Response(JSON.stringify({ error: 'Missing woId' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

        const estWO = await airtableGetById('Work Orders', woId);
        const f = estWO.fields;
        const custName = Array.isArray(f['Customer Name']) ? f['Customer Name'][0] : (f['Customer Name'] || 'Customer');
        const custId        = (f['Customer'] || [])[0] || null;
        const propId        = (f['Property'] || [])[0] || null;
        const stripeQuoteId = (f['Stripe Quote ID'] || '').trim();
        const today         = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

        const jobFields = {
          'Work Order Name': `${custName} — Job — ${today}`,
          'Work Order Type': 'Repair',
          'Status':          'New',
          'Active':          true,
          'Internal Notes':  `Converted from estimate: ${f['Work Order Name'] || woId}`,
          'Source Estimate': [woId],
        };
        if (custId)        jobFields['Customer']        = [custId];
        if (propId)        jobFields['Property']        = [propId];
        if (stripeQuoteId) jobFields['Stripe Quote ID'] = stripeQuoteId;

        const newJobWO = await airtablePost('Work Orders', jobFields);

        // Patch the originating Quote's direct Job link, so the Estimates tab can
        // resolve the live job status in one hop instead of inferring it. Resolve
        // the quote record from the request if given, else from the WO's own link.
        const quoteRecordId = quoteId || (f['Quotes'] || [])[0] || null;
        if (quoteRecordId) {
          try { await airtablePatch('Quotes', quoteRecordId, { 'Job': [newJobWO.id] }); }
          catch (e) { /* non-fatal — job is created either way */ }
        }

        return new Response(JSON.stringify({ ok: true, id: newJobWO.id }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
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

            // Determine WO type — estimate-type WOs flip status; service WOs get a new Repair WO
            let acceptedWoType = '', acceptedWoPropIds = [], acceptedWoTechIds = [];
            if (woId) {
              try {
                const awo = await airtableGetById('Work Orders', woId);
                acceptedWoType    = awo.fields?.['Work Order Type'] || '';
                acceptedWoPropIds = awo.fields?.['Property']        || [];
                acceptedWoTechIds = awo.fields?.['Technician']      || [];
              } catch(e) {}
            }
            const isAcceptedEstimateWO = ['Install Estimate', 'Estimate Only'].includes(acceptedWoType) || !acceptedWoType;

            if (isAcceptedEstimateWO) {
              if (woId) await airtablePatch('Work Orders', woId, { 'Status': 'Estimate Approved' });
            } else {
              // Service WO — create a new Repair WO and notify Bridgett
              const quoteNotes = atQuote.fields?.['Notes'] || '';
              const custRec  = custId ? await airtableGetById('Customers', custId).catch(() => null) : null;
              const custName = custRec?.fields?.['Customer Name'] || 'Customer';
              const todayLabel = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
              const newWoFields = {
                // Matches the manual /api/convert-to-job naming convention — an unnamed
                // WO doesn't show up meaningfully anywhere (Dispatch, Customer profile).
                'Work Order Name': `${custName} — Job — ${todayLabel}`,
                'Work Order Type': 'Repair',
                // 'New', not 'Scheduled' - this WO has no date/time yet, so marking it
                // Scheduled hid it from the Dispatch board (which expects a Scheduled WO
                // to actually have a scheduled date) without it ever being seen.
                'Status':          'New',
                'Problem Description': quoteNotes.split('---PHOTOS---')[0].trim() || 'Repair from approved estimate',
                'Internal Notes': `Auto-created from approved repair estimate — Quote: ${atQuote.fields?.['Quote Number'] || stripeQuoteId}`,
              };
              if (custId)                     newWoFields['Customer']   = [custId];
              if (acceptedWoPropIds.length)   newWoFields['Property']   = acceptedWoPropIds;
              if (acceptedWoTechIds.length)   newWoFields['Technician'] = acceptedWoTechIds;
              if (woId)                       newWoFields['Source Estimate'] = [woId];
              try {
                const newJobWO = await airtablePost('Work Orders', newWoFields);
                // Same direct link the manual conversion flow sets, so the Estimates tab
                // and dashboard widgets resolve this job in one hop instead of relying on
                // just the reverse Source Estimate link.
                await airtablePatch('Quotes', atQuote.id, { 'Job': [newJobWO.id] }).catch(() => {});
              } catch(e) { console.error('New Repair WO creation failed:', e.message); }

              // SMS Bridgett
              if (env.OWNER_PHONE && env.QUO_API_KEY) {
                sendSms(env.QUO_API_KEY, env.OWNER_PHONE,
                  `✅ Repair estimate approved — ${custName}. New Repair WO created in Airtable, ready to schedule.`
                ).catch(() => {});
              }
            }

            // NOTE: this used to also auto-create a "head start" draft Stripe invoice +
            // matching Airtable Invoice record on every accepted quote. Removed 2026-06-24 —
            // Bridgett never sends invoices directly from Stripe (only for overdue reminders),
            // always going through the dedicated Send Deposit / Send Final Balance / Create
            // Invoice flows instead, each of which creates its own invoice. The auto-draft was
            // never touched after creation, so it just accumulated as permanent dead weight in
            // the Stripe Drafts list for every quote ever accepted.
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
            if (woId) {
              let expWoType = '';
              try { const ewo = await airtableGetById('Work Orders', woId); expWoType = ewo.fields?.['Work Order Type'] || ''; } catch(e) {}
              if (['Install Estimate', 'Estimate Only'].includes(expWoType) || !expWoType) {
                await airtablePatch('Work Orders', woId, { 'Status': 'Estimate Declined' });
              }
            }
          }
        }

        if (event.type === 'invoice.paid') {
          const inv            = event.data.object;
          const stripeInvId    = inv.id;
          const amountPaid     = (inv.amount_paid || 0) / 100;
          const paidDate       = new Date().toISOString().split('T')[0];
          const stripeInvType  = inv.metadata?.invoice_type || 'standard'; // 'deposit' | 'final_balance' | 'standard'
          const custEmail      = inv.customer_email || '';
          const firstName      = (inv.customer_name || '').split(' ')[0] || 'there';

          // Find Work Order — prefer airtable_wo_id from metadata (direct, reliable),
          // fall back to formula search by Stripe Invoice ID
          let wo = null;
          const metaWoId = inv.metadata?.airtable_wo_id || inv.metadata?.work_order_airtable_id || '';
          if (metaWoId) {
            try { wo = await airtableGetById('Work Orders', metaWoId); } catch(e) { wo = null; }
          }
          if (!wo) {
            const woData = await airtableGet('Work Orders', `{Stripe Invoice ID}="${stripeInvId}"`);
            wo = (woData.records || [])[0] || null;
          }

          if (wo) {
            const atCustId = (wo.fields['Customer'] || [])[0] || null;

            if (stripeInvType === 'deposit') {
              // ── Deposit paid ─────────────────────────────────────────────
              // Mark deposit paid on WO; don't flip WO status to Paid yet
              await airtablePatch('Work Orders', wo.id, { 'Deposit Paid': true });

              // Update the Airtable Invoice record for this deposit.
              // Prefer an exact Stripe Invoice ID match — a WO can have more than one
              // Deposit-type Invoice record linked (e.g. a corrected replacement after
              // a voided one), and matching on type alone grabs whichever is linked
              // first, which can resurrect a stale/voided record instead of the real one.
              const linkedInvIds = wo.fields['Invoice'] || [];
              let matchedInvId = null;
              for (const invId of linkedInvIds) {
                try {
                  const atInv = await airtableGetById('Invoices', invId);
                  if (atInv.fields['Stripe Invoice ID'] === stripeInvId) { matchedInvId = invId; break; }
                } catch(e) { /* skip */ }
              }
              if (!matchedInvId) {
                // Fallback for legacy records with no Stripe Invoice ID stored — never
                // fall back onto a Void record.
                for (const invId of linkedInvIds) {
                  try {
                    const atInv = await airtableGetById('Invoices', invId);
                    if ((atInv.fields['Invoice Type'] || '') === 'Deposit' && (atInv.fields['Status'] || '') !== 'Void') {
                      matchedInvId = invId;
                      break;
                    }
                  } catch(e) { /* skip */ }
                }
              }
              if (matchedInvId) {
                await airtablePatch('Invoices', matchedInvId, {
                  'Status':            'Paid in Full',
                  'Paid Date':         paidDate,
                  'Amount Paid':       amountPaid,
                  'Deposit Paid':      true,
                  'Deposit Paid Date': paidDate,
                });
              }

              // Send deposit received confirmation email
              if (custEmail && env.RESEND_API_KEY) {
                const depSubject = 'Deposit received — CJB Comfort';
                await sendEmail(env.RESEND_API_KEY, {
                  to:      custEmail,
                  subject: depSubject,
                  html:    emailDepositReceivedHtml({
                    customerName: firstName,
                    amountPaid,
                    invoiceNumber: inv.number || null,
                  }),
                }).catch(e => console.error('Deposit email error:', e));
                logCommunication(env, {
                  type:        'Email',
                  trigger:     'Deposit Received',
                  sentTo:      custEmail,
                  subject:     depSubject,
                  customerId:  atCustId,
                  workOrderId: wo.id,
                }).catch(() => {});
              }

            } else if (stripeInvType === 'ea_rebate') {
              // ── EA rebate reimbursement received ──────────────────────────
              // This is CJB's own receivable from the EA program, not a customer payment -
              // never touch the WO's Status, never touch linked Jobs, never email the
              // Stripe customer on file (that's the shared internal EA billing contact, not
              // the actual job's customer - emailing them a "thank you for your payment"
              // would be a real, visible mistake).
              const linkedEAInvIds = wo.fields['Invoice'] || [];
              let matchedEAInvId = null;
              for (const invId of linkedEAInvIds) {
                try {
                  const atInv = await airtableGetById('Invoices', invId);
                  if (atInv.fields['Stripe Invoice ID'] === stripeInvId) { matchedEAInvId = invId; break; }
                } catch(e) { /* skip */ }
              }
              if (matchedEAInvId) {
                await airtablePatch('Invoices', matchedEAInvId, {
                  'Status':      'Paid in Full',
                  'Paid Date':   paidDate,
                  'Amount Paid': amountPaid,
                });
              }
            } else {
              // ── Standard or final balance paid ───────────────────────────
              await airtablePatch('Work Orders', wo.id, { 'Status': 'Paid' });
              await patchBillableJobs(wo.fields['Jobs'] || [], 'Paid');

              // Update linked Invoice record — match by exact Stripe Invoice ID rather
              // than just taking Invoice[0], so a stale/voided duplicate linked to the
              // same WO never gets overwritten instead of the real one.
              const linkedStdInvIds = wo.fields['Invoice'] || [];
              let atInvId = null;
              for (const invId of linkedStdInvIds) {
                try {
                  const atInv = await airtableGetById('Invoices', invId);
                  if (atInv.fields['Stripe Invoice ID'] === stripeInvId) { atInvId = invId; break; }
                } catch(e) { /* skip */ }
              }
              if (!atInvId) atInvId = linkedStdInvIds[0] || null; // legacy fallback
              if (atInvId) {
                await airtablePatch('Invoices', atInvId, {
                  'Status':      'Paid in Full',
                  'Paid Date':   paidDate,
                  'Amount Paid': amountPaid
                });
              }

              // Send thank you + Google Review ask
              if (custEmail && env.RESEND_API_KEY) {
                const tySubject = 'Thank you for your payment — CJB Comfort';
                await sendEmail(env.RESEND_API_KEY, {
                  to:      custEmail,
                  subject: tySubject,
                  html:    emailPaymentThankYouHtml({
                    customerName:    firstName,
                    amountPaid,
                    invoiceNumber:   inv.number || null,
                    googleReviewUrl: GOOGLE_REVIEW_URL,
                  }),
                }).catch(e => console.error('Thank-you email error:', e));
                logCommunication(env, {
                  type:        'Email',
                  trigger:     'Invoice Paid',
                  sentTo:      custEmail,
                  subject:     tySubject,
                  customerId:  atCustId,
                  workOrderId: wo.id,
                }).catch(() => {});
              }
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
            // Schedule first visit WO + follow-up
            scheduleFirstContractVisit(env, contractId).catch(e => console.error('scheduleFirstContractVisit error:', e.message));
          }

          // Maintenance contract renewal payment → roll contract forward one year
          if (inv.metadata?.invoice_type === 'maintenance_renewal' && inv.metadata?.contract_airtable_id) {
            const contractId = inv.metadata.contract_airtable_id;
            try {
              const contract = await airtableGetById('Maintenance Contracts', contractId);
              const cf = contract.fields;

              // Roll dates: new start = old end + 1 day, new end = new start + 1 year - 1 day
              const oldEnd      = cf['End Date'] ? new Date(cf['End Date'] + 'T12:00:00') : new Date();
              const newStart    = new Date(oldEnd);
              newStart.setDate(newStart.getDate() + 1);
              const newEnd      = new Date(newStart);
              newEnd.setFullYear(newEnd.getFullYear() + 1);
              newEnd.setDate(newEnd.getDate() - 1);

              await airtablePatch('Maintenance Contracts', contractId, {
                'Status':                'Active',
                'Start Date':            newStart.toISOString().split('T')[0],
                'End Date':              newEnd.toISOString().split('T')[0],
                'Visits Used This Year': 0,
                'Renewal Invoice Sent':  null, // clear so next year's renewal can fire
              });

              // Send renewal confirmed email
              const custId    = (cf['Customer'] || [])[0] || null;
              const custRec   = custId ? await airtableGetById('Customers', custId) : null;
              const custEmail = custRec?.fields?.['Email'] || inv.customer_email || '';
              const custFirst = (custRec?.fields?.['First Name'] || inv.customer_name || '').split(' ')[0] || 'there';
              const planName  = cf['Plan Name'] || 'Annual Maintenance Agreement';
              const amtPaid   = (inv.amount_paid || 0) / 100;

              if (custEmail && env.RESEND_API_KEY) {
                const renewSubj = `Your CJB Comfort maintenance agreement has been renewed`;
                await sendEmail(env.RESEND_API_KEY, {
                  to:      custEmail,
                  subject: renewSubj,
                  html:    emailRenewalConfirmedHtml({
                    customerName: custFirst,
                    planName,
                    amountPaid:   amtPaid,
                    newStartDate: newStart.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' }),
                    newEndDate:   newEnd.toLocaleDateString('en-US',   { month: 'long', day: 'numeric', year: 'numeric' }),
                  }),
                }).catch(e => console.error('Renewal confirmed email error:', e));
                logCommunication(env, {
                  type:       'Email',
                  trigger:    'Contract Renewed',
                  sentTo:     custEmail,
                  subject:    renewSubj,
                  customerId: custId,
                }).catch(() => {});
              }
              // Schedule first visit WO + follow-up for the new year
              scheduleFirstContractVisit(env, contractId).catch(e => console.error('scheduleFirstContractVisit (renewal) error:', e.message));
            } catch(e) {
              console.error('Contract renewal roll-forward error:', e.message);
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

        // Fetch customer and property names directly (more portable than lookup fields)
        const custId  = (cf['Customer'] || [])[0] || null;
        const propId  = (cf['Property'] || [])[0] || null;
        const [custRec, propRec] = await Promise.all([
          custId ? airtableGetById('Customers',  custId) : Promise.resolve(null),
          propId ? airtableGetById('Properties', propId) : Promise.resolve(null),
        ]);
        const custName = custRec?.fields?.['Customer Name'] || '';
        const propName = propRec?.fields?.['Property Name'] || '';
        const propAddr = propRec?.fields?.['Service Address'] || '';
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
        const { workOrderId, method, checkNumber, amount, slot = 'standard' } = await request.json();
        if (!workOrderId) throw new Error('workOrderId required');
        const STRIPE_KEY = env.STRIPE_SECRET_KEY;

        const wo = await airtableGetById('Work Orders', workOrderId);
        // A WO can have up to three invoices linked simultaneously (customer deposit, customer
        // final balance/standard, and the internal EA rebate receivable) — slot picks which one.
        const stripeInvoiceIdField = slot === 'deposit'   ? 'Deposit Invoice Stripe ID'
                                    : slot === 'ea_rebate' ? 'EA Rebate Invoice Stripe ID'
                                    : 'Stripe Invoice ID';
        const stripeInvoiceId = (wo.fields[stripeInvoiceIdField] || '').trim();
        if (!stripeInvoiceId) {
          // No Stripe invoice on this WO — nothing to do, not an error
          return new Response(JSON.stringify({ ok: true, skipped: true }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        // Airtable Payment Method is a select field — must match exact option values
        const paymentMethodSelect = method === 'check'  ? 'Check'
          : (method === 'keyin' || method === 'card')   ? 'Credit Card'
          : 'Cash';
        // Human-readable note includes check number detail
        const paymentNote = method === 'check'
          ? `Check${checkNumber ? ' #' + checkNumber : ''}`
          : paymentMethodSelect;

        // Check current Stripe invoice status — $0 warranty invoices are auto-paid
        // by Stripe immediately on send, so calling /pay again would throw an error.
        const stripeInv  = await stripeGet(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}`);
        const paidDate   = new Date().toISOString().split('T')[0];
        // A WO can have up to three Invoice records linked at once (deposit, final/standard,
        // EA rebate) — match the one whose own Stripe Invoice ID matches the invoice we're
        // actually acting on here, rather than blindly taking the first link.
        const linkedInvIdsForSlot = wo.fields['Invoice'] || [];
        let atInvId = null;
        for (const invId of linkedInvIdsForSlot) {
          try {
            const candidate = await airtableGetById('Invoices', invId);
            if (candidate.fields['Stripe Invoice ID'] === stripeInvoiceId) { atInvId = invId; break; }
          } catch(e) { /* skip */ }
        }
        if (!atInvId) atInvId = linkedInvIdsForSlot[0] || null; // legacy fallback

        if (stripeInv.status === 'paid') {
          // Already paid (auto-paid $0 or previously collected) — just sync Airtable.
          // Never flip the WO/Jobs to Paid for the EA rebate slot — that's CJB's own
          // receivable, not the customer's job-completion state.
          if (slot !== 'ea_rebate') {
            await airtablePatch('Work Orders', wo.id, { 'Status': 'Paid' });
            await patchBillableJobs(wo.fields['Jobs'] || [], 'Paid');
          }
          if (atInvId) {
            await airtablePatch('Invoices', atInvId, {
              'Status':         'Paid in Full',
              'Paid Date':      paidDate,
              'Amount Paid':    (stripeInv.amount_paid || 0) / 100,
            });
          }
          return new Response(JSON.stringify({ ok: true }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        // Determine invoice total and whether this is a partial payment.
        // amount_due = full balance Stripe expects; amount_remaining = after any prior Stripe payments.
        const invoiceTotal = (stripeInv.amount_due || 0) / 100;
        const payAmount    = (amount && Number(amount) > 0) ? Number(amount) : invoiceTotal;

        // Read prior partial payments already recorded in Airtable Invoice
        let priorPaid        = 0;
        let priorPayNotes    = '';
        if (atInvId) {
          try {
            const atInv   = await airtableGetById('Invoices', atInvId);
            priorPaid      = Number(atInv.fields['Amount Paid'] || 0);
            priorPayNotes  = atInv.fields['Payment Notes'] || '';
          } catch(e) { /* non-fatal */ }
        }

        const totalPaid  = priorPaid + payAmount;
        const balanceDue = Math.max(0, invoiceTotal - totalPaid);
        // Treat as full if the remaining balance is ≤ 1¢ (rounding tolerance)
        const isFullPayment = balanceDue <= 0.01;

        // Append timestamped payment note
        const dateStr    = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
        const newNote    = `${dateStr}: $${payAmount.toFixed(2)} — ${paymentNote}`;
        const allNotes   = priorPayNotes ? `${priorPayNotes}\n${newNote}` : newNote;

        if (!isFullPayment) {
          // ── Partial / deposit payment ────────────────────────────────────
          // Use deposit fields; leave Stripe invoice open.
          // Payment Method is a multiple-select field — must pass an array.
          // Balance Due is a formula field — don't write it.
          if (atInvId) {
            await airtablePatch('Invoices', atInvId, {
              'Status':            'Deposit Paid',
              'Amount Paid':       totalPaid,
              'Deposit Amount':    payAmount,
              'Deposit Paid':      true,
              'Deposit Paid Date': paidDate,
              'Payment Method':    [paymentMethodSelect],
              'Payment Notes':     allNotes,
            });
          }
          // Update Stripe invoice footer — visible to customer on hosted invoice + PDF.
          // Appends to any existing footer text rather than replacing it.
          try {
            const depositLine = `Deposit received: $${payAmount.toFixed(2)} (${paymentNote}) on ${dateStr} · Balance due: $${balanceDue.toFixed(2)}`;
            const existingFooter = (stripeInv.footer || '').trim();
            const newFooter = existingFooter ? `${existingFooter}\n${depositLine}` : depositLine;
            await stripePost(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}`, {
              footer: newFooter,
              'metadata[deposit_received]': `$${payAmount.toFixed(2)} — ${paymentNote} — ${dateStr}`,
              'metadata[balance_due]':      `$${balanceDue.toFixed(2)}`,
            });
          } catch(e) { /* non-fatal — Airtable already has the record */ }

          // Append a note to the WO Internal Notes for visibility
          const existingNotes = (wo.fields['Internal Notes'] || '');
          const partialNote   = `\n[Deposit: $${payAmount.toFixed(2)} — ${paymentNote} — ${dateStr}]`;
          await airtablePatch('Work Orders', workOrderId, {
            'Internal Notes': existingNotes + partialNote
          });
          return new Response(JSON.stringify({ ok: true, partial: true, amountPaid: payAmount, totalPaid, balanceDue }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        // ── Full payment ─────────────────────────────────────────────────
        // Update Stripe invoice metadata, then mark paid out of band.
        await stripePost(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}`, {
          'metadata[payment_method]': paymentNote,
          'metadata[collected_by]':   'Technician — on site'
        });
        // Triggers invoice.paid webhook → worker updates WO status to Paid
        await stripePost(STRIPE_KEY, `/v1/invoices/${stripeInvoiceId}/pay`, {
          paid_out_of_band: 'true'
        });

        // Also sync Airtable Invoice so it shows correct totals even before webhook fires
        // Balance Due is a formula field — don't write it.
        // Payment Method is multiple-select — must be an array.
        if (atInvId) {
          await airtablePatch('Invoices', atInvId, {
            'Status':          'Paid in Full',
            'Paid Date':       paidDate,
            'Amount Paid':     totalPaid,
            'Payment Method':  [paymentMethodSelect],
            'Payment Notes':   allNotes,
          });
        }

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
        const body = await request.json();
        const { workOrderId, notes } = body;
        const invoiceType    = body.invoiceType   || 'standard'; // 'standard' | 'deposit' | 'final_balance' | 'ea_rebate'
        // EA rebate invoices are an internal receivable (EA owes CJB, not the customer) - always
        // billed to the shared EA customer record regardless of what's passed in, and never sent
        // by email (sendNow forced false) since there's no one to email it to.
        const customerId = invoiceType === 'ea_rebate' ? EA_REBATE_CUSTOMER_ID : body.customerId;
        const sendNow     = invoiceType === 'ea_rebate' ? false : body.sendNow;
        const depositAmount  = body.depositAmount || 0;          // dollar amount for deposit invoices
        const discountType   = body.discountType  || 'pct';      // 'pct' | 'dollar'
        const discountValue  = Number(body.discountValue) || 0;  // percent (0-100) or dollar amount
        const discountReason = body.discountReason || '';
        const ccEmails       = (body.ccEmails || []).filter(e => e && typeof e === 'string' && e.includes('@'));

        if (!customerId) throw new Error('customerId is required');

        // For deposit invoices, override line items with a single deposit line
        let lineItems = body.lineItems || [];
        if (invoiceType === 'deposit') {
          if (!depositAmount || depositAmount <= 0) throw new Error('depositAmount is required for deposit invoices');
          lineItems = [{ productName: 'Deposit', quantity: 1, unitPrice: depositAmount }];
        }
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
            // Stripe rejects negative unit_amount_decimal — use 'amount' for credits/discounts
            const itemParams = { customer: stripeCustId, invoice: invoiceId, currency: 'usd', description: stripeDesc };
            if (stripeAmt < 0) {
              itemParams.amount = String(stripeAmt);
            } else {
              itemParams.unit_amount_decimal = String(stripeAmt);
              itemParams.quantity = isWhole ? String(Math.max(1, qty)) : '1';
            }
            await stripePost(STRIPE_KEY, '/v1/invoiceitems', itemParams);
          }
        };

        // 4. If a draft invoice already exists, delete it entirely and start fresh.
        //    Attempting to surgically delete individual line items proved unreliable —
        //    Stripe silently keeps items in some cases, causing doubles on every save.
        //    Deleting the whole draft and recreating is simpler and always correct.
        //    The new ID gets written back to Airtable immediately after creation.
        const existingStripeId = invoiceType === 'ea_rebate'
          ? (woRecord?.fields?.['EA Rebate Invoice Stripe ID'] || '').trim()
          : (woRecord?.fields?.['Stripe Invoice ID'] || '').trim();
        let inv = null;

        if (existingStripeId) {
          try {
            const existingInv = await stripeGet(STRIPE_KEY, `/v1/invoices/${existingStripeId}`);
            if (existingInv.status === 'draft') {
              await stripeDelete(STRIPE_KEY, `/v1/invoices/${existingStripeId}`);
            }
          } catch (e) { /* invoice gone or inaccessible — proceed to create fresh */ }
        }

        // 5. Guard: delete any floating pending invoice items for this customer
        try {
          const pendRes  = await fetch(
            `https://api.stripe.com/v1/invoiceitems?customer=${stripeCustId}&limit=100`,
            { headers: { Authorization: `Bearer ${STRIPE_KEY}` } }
          );
          const pendData = await pendRes.json();
          for (const pitem of (pendData.data || [])) {
            if (!pitem.invoice) await stripeDelete(STRIPE_KEY, `/v1/invoiceitems/${pitem.id}`).catch(() => {});
          }
        } catch (e) { /* best effort */ }

        // 6. Create a fresh draft invoice
        const invParams = {
          customer:          stripeCustId,
          auto_advance:      'false',
          collection_method: 'send_invoice',
          days_until_due:    isCommercial ? '30' : '0'
        };
        if (fullDesc) invParams.description              = fullDesc;
        if (woName)   invParams['metadata[work_order]'] = woName;
        // Tag invoice type so the paid webhook knows what to do
        invParams['metadata[invoice_type]'] = invoiceType;
        if (workOrderId) invParams['metadata[airtable_wo_id]'] = workOrderId;
        inv = await stripePost(STRIPE_KEY, '/v1/invoices', invParams);

        // Compute subtotal and discount BEFORE attaching line items to Stripe
        const subtotal    = lineItems.reduce((s, li) => s + ((li.unitPrice || 0) * (li.quantity || 1)), 0);
        const discountAmt = discountValue > 0
          ? (discountType === 'pct' ? Math.round(subtotal * (discountValue / 100) * 100) / 100 : discountValue)
          : 0;
        if (discountAmt > 0) {
          const discLabel = discountReason
            ? `Discount — ${discountReason}`
            : discountType === 'pct' ? `Discount (${discountValue}%)` : 'Discount';
          lineItems = [...lineItems, { productName: discLabel, quantity: 1, unitPrice: -discountAmt }];
        }

        // 7. Attach line items directly to the new invoice (includes discount if any)
        await attachLineItems(inv.id);

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
        const today   = new Date().toISOString().split('T')[0];
        const dueDate = new Date(Date.now() + (isCommercial ? 30 : 0) * 86400000).toISOString().split('T')[0];

        // 9. Finalize + send only if sendNow — otherwise leave as Stripe draft
        let finalInv = inv;
        if (sendNow) {
          await stripePost(STRIPE_KEY, `/v1/invoices/${inv.id}/finalize`, {});
          finalInv = await stripePost(STRIPE_KEY, `/v1/invoices/${inv.id}/send`, {});

          // Send our own branded invoice email via Resend (in addition to Stripe's)
          if (custEmail && env.RESEND_API_KEY) {
            const firstName      = custName.split(' ')[0] || custName;
            const isDeposit      = invoiceType === 'deposit';
            const isFinal        = invoiceType === 'final_balance';
            const invoiceSubject = isDeposit
              ? `Deposit invoice from CJB Comfort${finalInv.number ? ` — ${finalInv.number}` : ''}`
              : isFinal
              ? `Final balance due — CJB Comfort${finalInv.number ? ` — ${finalInv.number}` : ''}`
              : `Your invoice from CJB Comfort${finalInv.number ? ` — ${finalInv.number}` : ''}`;
            await sendEmail(env.RESEND_API_KEY, {
              to:      custEmail,
              subject: invoiceSubject,
              cc:      ccEmails,
              html:    emailInvoiceHtml({
                customerName:  firstName,
                invoiceNumber: finalInv.number || null,
                total:         subtotal,
                hostedUrl:     finalInv.hosted_invoice_url || '',
                dueDate,
                isDeposit,
                isFinal,
              }),
            }).catch(e => console.error('Invoice email error:', e));
            logCommunication(env, {
              type:        'Email',
              trigger:     'Invoice Sent',
              sentTo:      custEmail,
              subject:     invoiceSubject,
              customerId,
              workOrderId: workOrderId || null,
            }).catch(() => {});
          }
        }

        // 10. Create Wave invoice (only on sendNow — drafts don't go to Wave yet)
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

        // Map invoiceType to Airtable Invoice Type select value.
        const atInvoiceType = invoiceType === 'deposit'        ? 'Deposit'
                            : invoiceType === 'final_balance'  ? 'Final — Balance Due'
                            : invoiceType === 'ea_rebate'       ? 'EA Rebate'
                            : 'Standard';

        const atFields = {
          'Invoice Name':   `${custName} — ${today}${invoiceType === 'deposit' ? ' (Deposit)' : invoiceType === 'final_balance' ? ' (Final)' : invoiceType === 'ea_rebate' ? ' (EA Rebate)' : ''}`,
          'Customers':      [customerId],
          'Status':         sendNow ? 'Sent' : 'Draft',
          'Invoice Type':   atInvoiceType,
          'Invoice Date':   today,
          'Due Date':       dueDate,
          'Subtotal':       subtotal,
          'Stripe Invoice ID': finalInv.id,
          'Internal Notes': `Stripe Invoice ID: ${finalInv.id}${waveInvoiceId ? '\nWave Invoice ID: ' + waveInvoiceId : ''}${finalInv.hosted_invoice_url ? '\n' + finalInv.hosted_invoice_url : ''}`,
          ...(sendNow ? { 'Sent Date': today } : {}),
          ...(waveInvoiceId ? { 'Wave Exported': true, 'Wave Invoice ID': waveInvoiceId } : {}),
          // Deposit-specific fields
          ...(invoiceType === 'deposit' ? {
            'Deposit Required': true,
            'Deposit Amount':   depositAmount,
          } : {}),
          // Discount fields
          ...(discountAmt > 0 ? {
            'Discount Amount': discountAmt,
            'Discount Reason': discountReason || (discountType === 'pct' ? `${discountValue}%` : `$${discountAmt.toFixed(2)}`),
          } : {}),
        };
        if (workOrderId) atFields['Work Orders'] = [workOrderId];
        if (notes)       atFields['Notes']        = notes;

        // For deposit invoices: always create a NEW Airtable record (don't overwrite any existing invoice)
        // For final_balance: also create a new record (second invoice on the same WO)
        // For standard: check for existing draft invoice to update in place
        let atInvId;
        const isNewInvoiceType = invoiceType === 'deposit' || invoiceType === 'final_balance' || invoiceType === 'ea_rebate';
        const existingAtInvoiceId = isNewInvoiceType ? null :
          (woRecord?.fields?.['Invoice'] || [])[0] || null;

        if (existingAtInvoiceId) {
          await airtablePatch('Invoices', existingAtInvoiceId, atFields);
          atInvId = existingAtInvoiceId;
        } else {
          const atInv = await airtablePost('Invoices', atFields);
          atInvId = atInv.id;
        }

        // 11. Update Work Order with Stripe Invoice ID and status
        // For $0 invoices Stripe auto-pays immediately on send (status='paid').
        const zeroDollarAutoPaid = sendNow && finalInv.status === 'paid';
        if (workOrderId) {
          // Deposit and EA rebate invoices each write to their own dedicated field so the
          // main Stripe Invoice ID stays clean for the standard / final balance invoice —
          // a WO can have all three open at once (customer deposit, customer final balance,
          // and the internal EA rebate receivable) without them colliding.
          const stripeIdField = invoiceType === 'deposit'   ? 'Deposit Invoice Stripe ID'
                              : invoiceType === 'ea_rebate'  ? 'EA Rebate Invoice Stripe ID'
                              : 'Stripe Invoice ID';
          const woUpdate = { [stripeIdField]: finalInv.id };
          // Only set Total Amount on standard / final invoices billed to the actual customer —
          // not deposit (partial) or EA rebate (a different payer entirely, not part of what
          // the customer owes).
          if (invoiceType !== 'deposit' && invoiceType !== 'ea_rebate') woUpdate['Total Amount'] = subtotal;
          if (invoiceType !== 'ea_rebate' && finalInv.hosted_invoice_url) woUpdate['Internal Notes'] = finalInv.hosted_invoice_url;
          if (sendNow) {
            if (invoiceType === 'deposit') {
              // Deposit sent — don't flip WO to Invoiced; keep current status
              // (WO remains Scheduled/In Progress until deposit is paid)
            } else {
              woUpdate['Status'] = zeroDollarAutoPaid ? 'Paid' : 'Invoiced';
              const jobStatus = zeroDollarAutoPaid ? 'Paid' : 'Invoiced';
              await patchBillableJobs(woRecord?.fields?.['Jobs'] || [], jobStatus);
            }
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
          let declineWoType = '';
          try { const dwo = await airtableGetById('Work Orders', workOrderId); declineWoType = dwo.fields?.['Work Order Type'] || ''; } catch(e) {}
          if (['Install Estimate', 'Estimate Only'].includes(declineWoType) || !declineWoType) {
            await airtablePatch('Work Orders', workOrderId, { 'Status': 'Estimate Declined' });
          }
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
        const workOrderId    = body.workOrderId || body.woId || null;
        const customerId     = body.customerId;
        const notes          = body.notes || '';
        const description    = body.description || '';
        const ccEmails       = (body.ccEmails || []).filter(e => e && typeof e === 'string' && e.includes('@'));
        const lineItems      = body.lineItems || [];
        const discountType   = body.discountType  || '';   // 'pct' | 'fixed'
        const discountValue  = Number(body.discountValue) || 0;
        const finalize     = body.finalize === true;     // finalize + email customer
        const finalizeOnly = body.finalizeOnly === true; // finalize for PDF only — no email, no WO status change
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
        // description (short WO context blurb, e.g. site notes) and notes (whatever was
        // typed into the customer-visible Notes box - which can run long, e.g. an EA
        // disclosure block) go to two different places on the rendered PDF. Cramming both
        // into description's 500-char header area was truncating long Notes content
        // mid-sentence; footer has real room (5000 chars) and renders at the bottom of the
        // document, where there's actually space.
        if (description) quoteParamsObj.description = description.slice(0, 500);
        if (notes)        quoteParamsObj.footer      = notes.slice(0, 5000);
        if (workOrderId) quoteParamsObj['metadata[work_order_airtable_id]'] = workOrderId;

        // Apply discount via Stripe coupon if provided
        if (discountValue > 0 && (discountType === 'pct' || discountType === 'fixed')) {
          const couponParams = { duration: 'once' };
          if (discountType === 'pct') {
            couponParams.percent_off = String(Math.min(100, Math.max(0, Math.round(discountValue))));
          } else {
            couponParams.amount_off = String(Math.round(discountValue * 100));
            couponParams.currency   = 'usd';
          }
          const coupon = await stripePost(STRIPE_KEY, '/v1/coupons', couponParams);
          quoteParamsObj['discounts[0][coupon]'] = coupon.id;
        }

        const stripeQuote = await stripePostNested(STRIPE_KEY, '/v1/quotes', quoteParamsObj);

        // 5. Airtable Quote record
        const subtotal = lineItems.reduce((s, li) => {
          const p = li.unitPrice !== undefined ? Number(li.unitPrice) : (Number(li.unitAmount) || 0) / 100;
          const q = Number(li.quantity || li.qty) || 1;
          return s + p * q;
        }, 0);
        const discountedTotal = discountValue > 0
          ? (discountType === 'pct'
              ? subtotal * (1 - Math.min(100, discountValue) / 100)
              : Math.max(0, subtotal - discountValue))
          : subtotal;
        const expiresDate = new Date(expiresAt * 1000).toISOString().split('T')[0];
        const today      = new Date().toISOString().split('T')[0];

        let woName = '', woType = '', woCustIds = [], woPropIds = [], woTechIds = [];
        if (workOrderId) {
          try {
            const wo = await airtableGetById('Work Orders', workOrderId);
            woName    = wo.fields?.['Work Order Name'] || '';
            woType    = wo.fields?.['Work Order Type'] || '';
            woCustIds = wo.fields?.['Customer']        || [];
            woPropIds = wo.fields?.['Property']        || [];
            woTechIds = wo.fields?.['Technician']      || [];
          } catch (e) {}
        }
        // Only Install Estimate / Estimate Only WOs track estimate status on the WO itself.
        // Service/repair/maintenance WOs keep their status independent of the estimate.
        const isEstimateTypeWO = ['Install Estimate', 'Estimate Only'].includes(woType) || !woType;

        const photoUrls = Array.isArray(body.photoUrls) ? body.photoUrls : [];

        const atQuoteFields = {
          'Quote Title':     `${custName} — ${woName || 'Estimate'} — ${today}`,
          'Status':          'Draft',
          'Stripe Quote ID': stripeQuote.id,
          'Expiration Date': expiresDate,
          'Total Amount':    discountedTotal,
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
            const estSubject = 'Your CJB Comfort Estimate is Ready to Review';
            await sendEmail(env.RESEND_API_KEY, {
              to:      custEmail,
              cc:      ccEmails,
              subject: estSubject,
              html:    emailEstimateHtml({
                customerName: custName,
                approveUrl,
                description,
                lineItems:    emailLineItems,
                total:        emailTotal
              })
            });
            logCommunication(env, {
              type:        'Email',
              trigger:     'Estimate Sent',
              sentTo:      custEmail,
              subject:     estSubject,
              customerId,
              workOrderId: workOrderId || null,
            }).catch(() => {});
          } catch (emailErr) {
            console.error('Estimate email failed (non-fatal):', emailErr.message);
          }
        }

        // Finalize the Stripe quote (makes it Open in Stripe, enables PDF generation — non-fatal).
        // finalizeOnly finalizes for PDF purposes without notifying the customer or touching WO status —
        // e.g. attaching a PDF to an EA program SOW submission before the customer has seen the estimate.
        let quoteStatus = 'draft';
        if (finalize || finalizeOnly) {
          try {
            await stripePost(STRIPE_KEY, `/v1/quotes/${stripeQuote.id}/finalize`, {});
            quoteStatus = 'open';
            await airtablePatch('Quotes', atQuote.id, { 'Status': 'Open' });
            if (finalize && workOrderId && isEstimateTypeWO) {
              await airtablePatch('Work Orders', workOrderId, { 'Status': 'Estimate Sent' });
            }
          } catch (finErr) {
            console.error('Quote finalize failed (non-fatal):', finErr.message);
            if (finalize && workOrderId && isEstimateTypeWO) {
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
        const { stripeQuoteId, airtableQuoteId, workOrderId, customerId, lineItems, notes, description } = body2;
        const discountTypePatch  = body2.discountType  || '';
        const discountValuePatch = Number(body2.discountValue) || 0;
        const finalizePatch     = body2.finalize === true;
        const finalizeOnlyPatch = body2.finalizeOnly === true; // finalize for PDF only — no email, no WO status change
        const ccEmailsPatch = (body2.ccEmails || []).filter(e => e && typeof e === 'string' && e.includes('@'));
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
        // Same split as the POST handler: description is the short WO context blurb,
        // notes (which can be a long EA disclosure block) goes to footer instead, where
        // there's real room rather than a 500-char header area that truncates mid-sentence.
        if (description)  quoteParamsObj.description                         = description.slice(0, 500);
        if (notes)         quoteParamsObj.footer                              = notes.slice(0, 5000);
        if (workOrderId)  quoteParamsObj['metadata[work_order_airtable_id]'] = workOrderId;

        if (discountValuePatch > 0 && (discountTypePatch === 'pct' || discountTypePatch === 'fixed')) {
          const couponParamsPatch = { duration: 'once' };
          if (discountTypePatch === 'pct') {
            couponParamsPatch.percent_off = String(Math.min(100, Math.max(0, Math.round(discountValuePatch))));
          } else {
            couponParamsPatch.amount_off = String(Math.round(discountValuePatch * 100));
            couponParamsPatch.currency   = 'usd';
          }
          const couponPatch = await stripePost(STRIPE_KEY, '/v1/coupons', couponParamsPatch);
          quoteParamsObj['discounts[0][coupon]'] = couponPatch.id;
        }

        const newQuote = await stripePostNested(STRIPE_KEY, '/v1/quotes', quoteParamsObj);

        // New quote created successfully — now safe to cancel the old one
        try { await stripePost(STRIPE_KEY, `/v1/quotes/${stripeQuoteId}/cancel`, {}); } catch (e) {}

        const subtotal    = lineItems.reduce((s, li) => s + ((li.unitPrice || 0) * (Number(li.quantity) || 1)), 0);
        const discountedTotalPatch = discountValuePatch > 0
          ? (discountTypePatch === 'pct'
              ? subtotal * (1 - Math.min(100, discountValuePatch) / 100)
              : Math.max(0, subtotal - discountValuePatch))
          : subtotal;
        const expiresDate = new Date(expiresAt * 1000).toISOString().split('T')[0];

        const patchApproveUrl = workOrderId ? `${APPROVE_BASE_URL}?wo=${workOrderId}` : null;

        if (airtableQuoteId) {
          const upd = {
            'Status':          'Draft',
            'Stripe Quote ID': newQuote.id,
            'Expiration Date': expiresDate,
            'Total Amount':    discountedTotalPatch,
            'Stripe Quote URL': patchApproveUrl || ''
          };
          if (notes)       upd['Notes'] = notes;
          if (description) upd['Notes'] = description + (notes ? '\n' + notes : '');
          await airtablePatch('Quotes', airtableQuoteId, upd);
        }

        if (workOrderId && patchApproveUrl) {
          await airtablePatch('Work Orders', workOrderId, { 'Stripe Quote ID': newQuote.id, 'Stripe Quote URL': patchApproveUrl });
        }

        // Finalize + email if requested (non-fatal). finalizeOnlyPatch finalizes for PDF
        // purposes only — no email, no WO status change (see POST handler for why).
        let patchStatus = 'draft';
        if (finalizePatch || finalizeOnlyPatch) {
          try {
            await stripePost(STRIPE_KEY, `/v1/quotes/${newQuote.id}/finalize`, {});
            patchStatus = 'open';
            if (airtableQuoteId) await airtablePatch('Quotes', airtableQuoteId, { 'Status': 'Open' });
            if (finalizePatch && workOrderId) await airtablePatch('Work Orders', workOrderId, { 'Status': 'Estimate Sent' });
          } catch (finErr) {
            if (finalizePatch && workOrderId) { try { await airtablePatch('Work Orders', workOrderId, { 'Status': 'Estimate Sent' }); } catch(e) {} }
          }
          // Send estimate email (full finalize only)
          if (finalizePatch && patchApproveUrl && customerId && env.RESEND_API_KEY) {
            try {
              const cRec = await airtableGetById('Customers', customerId);
              const cEmail = (cRec.fields['Email'] || '').trim();
              const cName  = cRec.fields['Customer Name'] || 'Customer';
              if (cEmail) {
                const emailTotal = lineItems.reduce((s, li) => s + ((li.unitPrice||0) * (Number(li.quantity)||1)), 0);
                const emailLineItems = lineItems.map(li => ({ name: li.productName||'Service', amount: (li.unitPrice||0)*(Number(li.quantity)||1) }));
                const patchEstSubject = 'Your CJB Comfort Estimate is Ready to Review';
                await sendEmail(env.RESEND_API_KEY, {
                  to: cEmail, subject: patchEstSubject, cc: ccEmailsPatch,
                  html: emailEstimateHtml({ customerName: cName, approveUrl: patchApproveUrl, description: notes||'', lineItems: emailLineItems, total: emailTotal })
                });
                logCommunication(env, {
                  type:        'Email',
                  trigger:     'Estimate Sent',
                  sentTo:      cEmail,
                  subject:     patchEstSubject,
                  customerId,
                  workOrderId: workOrderId || null,
                }).catch(() => {});
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
    // ── Geocode all properties missing lat/lng ────────────────────────────
    if (path === '/api/geocode-properties' && request.method === 'POST') {
      try {
        const MAPS_KEY = env.GOOGLE_MAPS_KEY;
        if (!MAPS_KEY) throw new Error('GOOGLE_MAPS_KEY not configured in Worker secrets');

        const atHeaders = { Authorization: `Bearer ${AIRTABLE_API_KEY}` };
        const atBase    = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

        // Fetch one batch of 20 properties missing lat/lng per invocation
        // (Cloudflare subrequest limit: ~50 free / 1000 paid — caller loops until total=0)
        const listUrl = `${atBase}/Properties?filterByFormula=${encodeURIComponent('OR({Latitude}=BLANK(),{Longitude}=BLANK())')}` +
          `&fields[]=Service Address&fields[]=City&fields[]=State&fields[]=Zip&pageSize=20`;
        const listRes  = await fetch(listUrl, { headers: atHeaders });
        const listData = await listRes.json();
        const props    = listData.records || [];

        let geocoded = 0, failed = 0, firstError = '';
        const updates = []; // collect successful geocodes for bulk Airtable PATCH

        for (const prop of props) {
          const f    = prop.fields;
          const addr = [f['Service Address'], f['City'], f['State'], f['Zip']].filter(Boolean).join(', ');
          if (!addr) { failed++; continue; }
          try {
            const gRes  = await fetch(`https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(addr)}&key=${MAPS_KEY}`);
            const gData = await gRes.json();
            if (gData.status !== 'OK' || !gData.results?.[0]) {
              if (!firstError) firstError = `Google status: ${gData.status} — ${gData.error_message || 'no results'}`;
              failed++; continue;
            }
            const loc = gData.results[0].geometry.location;
            updates.push({ id: prop.id, fields: { Latitude: loc.lat, Longitude: loc.lng } });
            geocoded++;
          } catch(e) {
            if (!firstError) firstError = e.message;
            failed++;
          }
        }

        // Bulk PATCH to Airtable in groups of 10 (API max per request)
        for (let i = 0; i < updates.length; i += 10) {
          const batch = updates.slice(i, i + 10);
          await fetch(`${atBase}/Properties`, {
            method: 'PATCH',
            headers: { ...atHeaders, 'Content-Type': 'application/json' },
            body: JSON.stringify({ records: batch })
          });
        }

        return new Response(JSON.stringify({ ok: true, geocoded, failed, total: props.length, firstError }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch(err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ── Dispatch — send booking confirmation from WO ID ───────────────────
    if (path === '/api/dispatch/confirm' && request.method === 'POST') {
      try {
        const { workOrderId, techId } = await request.json();
        if (!workOrderId) throw new Error('workOrderId required');

        const atH    = { Authorization: `Bearer ${AIRTABLE_API_KEY}` };
        const atBase = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

        const woRes = await fetch(
          `${atBase}/Work%20Orders/${workOrderId}?fields[]=Customer&fields[]=Customer Name&fields[]=Service Address&fields[]=Work Order Type&fields[]=Scheduled Date&fields[]=Problem Description`,
          { headers: atH }
        );
        const wo = await woRes.json();
        const wf = wo.fields || {};

        const custId = (wf['Customer'] || [])[0];
        if (!custId || !wf['Scheduled Date']) {
          return new Response(JSON.stringify({ ok: true, skipped: 'no customer or no date' }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        const custRes = await fetch(`${atBase}/Customers/${custId}?fields[]=Email&fields[]=Phone&fields[]=First Name`, { headers: atH });
        const cf      = (await custRes.json()).fields || {};
        const email   = (cf['Email'] || '').trim();
        if (!email) {
          return new Response(JSON.stringify({ ok: true, skipped: 'no email on customer' }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        let techName = '';
        if (techId) {
          try {
            const tRes = await fetch(`${atBase}/Technicians/${techId}?fields[]=Technician Name`, { headers: atH });
            techName = (await tRes.json()).fields?.['Technician Name'] || '';
          } catch(e) {}
        }

        const firstName  = (cf['First Name'] || '').trim()
          || (Array.isArray(wf['Customer Name']) ? wf['Customer Name'][0] : wf['Customer Name'] || '').split(' ')[0]
          || 'there';
        const phone      = (cf['Phone'] || '').trim();
        const addr       = Array.isArray(wf['Service Address']) ? wf['Service Address'][0] : (wf['Service Address'] || '');
        const woType     = wf['Work Order Type'] || 'Service Visit';
        const cancelUrl  = `https://app.cjbcomfort.com/manage.html?wo=${workOrderId}&action=cancel`;
        const reschedUrl = `https://app.cjbcomfort.com/manage.html?wo=${workOrderId}&action=reschedule`;
        const { dateStr, timeStr, endTimeStr } = formatAZDateTime(wf['Scheduled Date']);

        const html    = emailBookingConfirmedHtml({ firstName, dateStr, timeStr, endTimeStr, address: addr, woType, problemDescription: wf['Problem Description'] || '', cancelUrl, rescheduleUrl: reschedUrl, techName });
        const subject = `Your CJB Comfort appointment is confirmed — ${dateStr}`;

        await sendEmail(env.RESEND_API_KEY, { to: email, subject, html });
        logCommunication(env, { type: 'Email', trigger: 'Booking Confirm (Dispatch)', sentTo: email, subject }).catch(() => {});

        if (phone && env.QUO_API_KEY) {
          const sms = `Hi ${firstName} — your CJB Comfort ${woType} is confirmed for ${dateStr}, ${timeStr}–${endTimeStr}. Questions? Call or text us at ${OFFICE_PHONE}. – CJB Comfort`;
          sendSms(env.QUO_API_KEY, phone, sms)
            .then(() => logCommunication(env, { type: 'SMS', trigger: 'Booking Confirm (Dispatch)', sentTo: phone, subject: 'Booking confirmation SMS' }))
            .catch(e => console.error('Dispatch SMS error:', e));
        }

        return new Response(JSON.stringify({ ok: true }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch(err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ── Dispatch — send reschedule notification from admin drag ─────────
    if (path === '/api/dispatch/reschedule-notify' && request.method === 'POST') {
      try {
        const { workOrderId } = await request.json();
        if (!workOrderId) throw new Error('workOrderId required');

        const atH    = { Authorization: `Bearer ${AIRTABLE_API_KEY}` };
        const atBase = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

        const woRes = await fetch(
          `${atBase}/Work%20Orders/${workOrderId}?fields[]=Customer&fields[]=Customer%20Name&fields[]=Service%20Address&fields[]=Work%20Order%20Type&fields[]=Scheduled%20Date&fields[]=Scheduled%20End&fields[]=Problem%20Description&fields[]=Calendly%20Cancel%20URL&fields[]=Calendly%20Reschedule%20URL`,
          { headers: atH }
        );
        const wo = await woRes.json();
        const wf = wo.fields || {};

        const custId = (wf['Customer'] || [])[0];
        if (!custId || !wf['Scheduled Date']) {
          return new Response(JSON.stringify({ ok: true, skipped: 'no customer or no date' }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        const custRes = await fetch(`${atBase}/Customers/${custId}?fields[]=Email&fields[]=Phone&fields[]=First%20Name`, { headers: atH });
        const cf      = (await custRes.json()).fields || {};
        const email   = (cf['Email'] || '').trim();
        if (!email) {
          return new Response(JSON.stringify({ ok: true, skipped: 'no email on customer' }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        const firstName = (cf['First Name'] || '').trim()
          || (Array.isArray(wf['Customer Name']) ? wf['Customer Name'][0] : wf['Customer Name'] || '').split(' ')[0]
          || 'there';
        const phone   = (cf['Phone'] || '').trim();
        const addr    = Array.isArray(wf['Service Address']) ? wf['Service Address'][0] : (wf['Service Address'] || '');
        const woType  = wf['Work Order Type'] || 'Service Visit';
        const cancelUrl  = wf['Calendly Cancel URL']     || `https://app.cjbcomfort.com/manage.html?wo=${workOrderId}&action=cancel`;
        const reschedUrl = wf['Calendly Reschedule URL'] || `https://app.cjbcomfort.com/manage.html?wo=${workOrderId}&action=reschedule`;
        const { dateStr, timeStr, endTimeStr } = formatAZDateTime(wf['Scheduled Date']);

        const html    = emailBookingConfirmedHtml({ firstName, dateStr, timeStr, endTimeStr, address: addr, woType, problemDescription: wf['Problem Description'] || '', cancelUrl, rescheduleUrl: reschedUrl, isReschedule: true });
        const subject = `Your CJB Comfort appointment has been rescheduled — ${dateStr}`;

        await sendEmail(env.RESEND_API_KEY, { to: email, subject, html });
        logCommunication(env, { type: 'Email', trigger: 'Reschedule (Dispatch)', sentTo: email, subject }).catch(() => {});

        if (phone && env.QUO_API_KEY) {
          const sms = `Hi ${firstName} — your CJB Comfort ${woType} has been rescheduled to ${dateStr}, ${timeStr}–${endTimeStr}. Questions? Call or text us at ${OFFICE_PHONE}. – CJB Comfort`;
          sendSms(env.QUO_API_KEY, phone, sms)
            .then(() => logCommunication(env, { type: 'SMS', trigger: 'Reschedule (Dispatch)', sentTo: phone, subject: 'Reschedule notification SMS' }))
            .catch(e => console.error('Reschedule SMS error:', e));
        }

        return new Response(JSON.stringify({ ok: true }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch(err) {
        return new Response(JSON.stringify({ error: err.message }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ── Properties POST — intercept to auto-geocode new address ──────────
    if (path === '/api/Properties' && request.method === 'POST') {
      const bodyText = await request.text();
      const atBase   = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Properties`;
      const atH      = { Authorization: `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' };
      const createRes = await fetch(atBase, { method: 'POST', headers: atH, body: bodyText });
      const created   = await createRes.json();

      if (createRes.ok && env.GOOGLE_MAPS_KEY) {
        const f    = (JSON.parse(bodyText).fields || {});
        const addr = [f['Service Address'], f['City'], f['State'], f['Zip']].filter(Boolean).join(', ');
        if (addr && created.id) {
          _ctx?.waitUntil((async () => {
            try {
              const gRes  = await fetch(`https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(addr)}&key=${env.GOOGLE_MAPS_KEY}`);
              const gData = await gRes.json();
              if (gData.status === 'OK' && gData.results?.[0]) {
                const loc = gData.results[0].geometry.location;
                await fetch(`${atBase}/${created.id}`, {
                  method: 'PATCH', headers: atH,
                  body: JSON.stringify({ fields: { Latitude: loc.lat, Longitude: loc.lng } })
                });
              }
            } catch(e) { console.error('Auto-geocode error:', e); }
          })());
        }
      }

      return new Response(JSON.stringify(created), {
        status: createRes.status,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

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

// Retries on network errors and 5xx (transient Cloudflare<->Airtable connectivity issues —
// see the 522/525 incident + the silently-dropped Calendly webhook on 2026-06-23).
// 4xx is a real client error (bad formula, bad field, etc.) and is never retried.
async function airtableFetchWithRetry(url, options) {
  let lastErr = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(url, options);
      if (res.ok || res.status < 500) return res;
      lastErr = new Error(`Airtable ${res.status}: ${await res.text().catch(() => '')}`);
    } catch (e) {
      lastErr = e;
    }
    if (attempt < 2) await new Promise(r => setTimeout(r, 800 * (attempt + 1)));
  }
  throw lastErr;
}

async function airtableGet(table, formula) {
  const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(table)}` +
              `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=5`;
  const res = await airtableFetchWithRetry(url, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
  if (!res.ok) throw new Error(`Airtable GET ${table}: ${res.status}`);
  return res.json();
}

async function airtableGetById(table, recordId) {
  const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(table)}/${recordId}`;
  const res = await airtableFetchWithRetry(url, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
  if (!res.ok) throw new Error(`Airtable GET ${table}/${recordId}: ${res.status}`);
  return res.json();
}

async function airtablePost(table, fields) {
  const res = await airtableFetchWithRetry(
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
  const res = await airtableFetchWithRetry(
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
// ── SMS via OpenPhone (Quo) ───────────────────────────────────────────────
// Parse a full address string (e.g. "123 Main St, Mesa, AZ 85201") into parts.
// If no commas (Q&A form returns street only), street = full string, rest empty.
function parseAddressString(addr) {
  if (!addr) return { street: '', city: '', state: 'AZ', zip: '' };
  const parts = addr.split(',').map(s => s.trim());
  const street = parts[0] || '';
  const city   = parts[1] || '';
  const stateZip = (parts[2] || '').trim().split(/\s+/);
  const state  = stateZip[0] || 'AZ';
  const zip    = stateZip[1] || '';
  return { street, city, state, zip };
}

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
  const res = await fetch('https://api.openphone.com/v1/messages', {
    method: 'POST',
    headers: { Authorization: apiKey, 'Content-Type': 'application/json' }, // no "Bearer" for OpenPhone
    body: JSON.stringify({ from: QUO_FROM, to: [to], content: text }) // "content" not "text", "to" is array
  });
  if (!res.ok) {
    const errBody = await res.text();
    throw new Error(`OpenPhone ${res.status}: ${errBody}`);
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

async function sendEmail(apiKey, { to, subject, html, replyTo = REPLY_TO_EMAIL, cc = [] }) {
  if (!apiKey || !to) return; // non-fatal if key not configured or no email on file
  const payload = { from: RESEND_FROM, to: [to], subject, html };
  if (replyTo) payload.reply_to = replyTo;
  if (cc && cc.length) payload.cc = cc; // Resend accepts cc as an array
  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!res.ok) {
    const err = await res.text();
    console.error('Resend error:', err);
  }
}

// ── Communication log — fire-and-forget, never blocks main flow ───────────────
function logCommunication(env, { type, trigger, sentTo, subject, customerId, workOrderId, status = 'Sent' }) {
  const promise = (async () => {
    try {
      const now     = new Date().toISOString();
      const logName = `${type} · ${trigger} · ${sentTo || ''} · ${now.substring(0, 10)}`;
      const fields  = {
        'Log Name': logName,
        'Type':     type,
        'Trigger':  trigger,
        'Sent To':  Array.isArray(sentTo) ? (sentTo[0] || '') : (sentTo || ''),
        'Subject':  subject || '',
        'Status':   status,
        'Sent At':  now,
      };
      if (customerId)  fields['Customer']   = [customerId];
      if (workOrderId) fields['Work Order'] = [workOrderId];
      await airtablePost('Communication Log', fields);
    } catch (e) {
      console.error('logCommunication failed:', e.message);
    }
  })();
  // Register with ctx.waitUntil() so Cloudflare keeps the worker alive
  // long enough to complete the Airtable write, even after the response is sent.
  if (_ctx) _ctx.waitUntil(promise);
  return promise;
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

// ── Appointment reminder cron ─────────────────────────────────────────────────
async function sendAppointmentReminders(env) {
  const now = Date.now();
  const windows = [
    { hours: 48, field: '48hr Reminder Sent' },
    { hours: 24, field: '24hr Reminder Sent'  },
  ];

  for (const { hours, field } of windows) {
    const windowStart = new Date(now + (hours - 1) * 3600000).toISOString();
    const windowEnd   = new Date(now + (hours + 1) * 3600000).toISOString();

    const formula = `AND({Status}="Scheduled",NOT({${field}}),IS_AFTER({Scheduled Date},"${windowStart}"),IS_BEFORE({Scheduled Date},"${windowEnd}"))`;
    const res = await fetch(
      `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Work%20Orders?filterByFormula=${encodeURIComponent(formula)}`,
      { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } }
    );
    if (!res.ok) continue;
    const data = await res.json();

    for (const wo of (data.records || [])) {
      const f     = wo.fields;
      const emailRaw = f['Email (from Customer)'];
      const email = Array.isArray(emailRaw) ? (emailRaw[0] || '') : (emailRaw || '');
      if (!email) continue;

      const { dateStr, timeStr, endTimeStr } = formatAZDateTime(f['Scheduled Date']);
      const firstName = (Array.isArray(f['Customer Name']) ? (f['Customer Name'][0] || '') : (f['Customer Name'] || '')).split(' ')[0] || 'there';
      const isDay = hours === 24;

      const cancelUrl     = `${MANAGE_BASE_URL}?wo=${wo.id}&action=cancel`;
      const rescheduleUrl = `${MANAGE_BASE_URL}?wo=${wo.id}&action=reschedule`;

      const subject = isDay
        ? `Reminder: your CJB Comfort appointment is tomorrow — ${dateStr}`
        : `Reminder: your CJB Comfort appointment is in 2 days — ${dateStr}`;

      await sendEmail(env.RESEND_API_KEY, {
        to: email, subject,
        html: emailReminderHtml({
          firstName, dateStr, timeStr, endTimeStr, isDay,
          address: f['Service Address'] || '',
          woType:  f['Work Order Type'] || 'Service Visit',
          cancelUrl, rescheduleUrl,
        }),
      });
      logCommunication(env, {
        type:        'Email',
        trigger:     isDay ? 'Appt Reminder 24hr' : 'Appt Reminder 48hr',
        sentTo:      email,
        subject,
        workOrderId: wo.id,
      }).catch(() => {});

      // SMS reminder — 24hr only (day-before is most useful; 48hr is email-only)
      if (isDay && env.QUO_API_KEY) {
        const custPhone = Array.isArray(f['Customer Phone (lookup)']) ? f['Customer Phone (lookup)'][0] : (f['Customer Phone (lookup)'] || '');
        if (custPhone) {
          const smsReminder = `Reminder: your CJB Comfort ${f['Work Order Type'] || 'appointment'} is TOMORROW, ${dateStr} between ${timeStr}–${endTimeStr}. Questions? Call or text ${OFFICE_PHONE}. – CJB Comfort`;
          sendSms(env.QUO_API_KEY, custPhone, smsReminder)
            .then(() => logCommunication(env, { type: 'SMS', trigger: 'Appt Reminder 24hr', sentTo: custPhone, subject: '24hr reminder SMS', workOrderId: wo.id }))
            .catch(e => console.error('Reminder SMS error:', e));
        }
      }

      // Mark sent so cron doesn't re-send
      await airtablePatch('Work Orders', wo.id, { [field]: true });
    }
  }
}

// ── Overdue invoice follow-up (7 days) + late fee (30 days) ──────────────────
async function checkOverdueInvoices(env) {
  if (!env.RESEND_API_KEY) return;
  try {
    const today   = new Date();
    const todayStr = today.toISOString().split('T')[0];
    const day7    = new Date(today.getTime() -  7 * 86400000).toISOString().split('T')[0];
    const day30   = new Date(today.getTime() - 30 * 86400000).toISOString().split('T')[0];

    // Process 30-day late fees FIRST so those invoices get Overdue Notice Sent set,
    // preventing a duplicate 7-day notice from firing in the same run.
    const f30 = `AND({Status}="Sent",NOT({Late Fee Applied}),IS_BEFORE({Due Date},"${day30}"),{Active})`;
    const r30 = await fetch(
      `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Invoices?filterByFormula=${encodeURIComponent(f30)}`,
      { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } }
    );
    if (r30.ok) {
      const d30 = await r30.json();
      for (const inv of (d30.records || [])) {
        await applyLateFee(env, inv, todayStr);
      }
    }

    // 7-day overdue notices (invoices that haven't had any notice yet)
    const f7 = `AND({Status}="Sent",NOT({Overdue Notice Sent}),IS_BEFORE({Due Date},"${day7}"),{Active})`;
    const r7 = await fetch(
      `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Invoices?filterByFormula=${encodeURIComponent(f7)}`,
      { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } }
    );
    if (r7.ok) {
      const d7 = await r7.json();
      for (const inv of (d7.records || [])) {
        await sendOverdueNotice(env, inv, todayStr);
      }
    }
  } catch(e) {
    console.error('checkOverdueInvoices error:', e.message);
  }
}

async function sendOverdueNotice(env, atInv, todayStr) {
  try {
    const f       = atInv.fields;
    const custIds = f['Customers'] || [];
    const custId  = custIds[0] || null;
    if (!custId) return;

    const custRec   = await airtableGetById('Customers', custId);
    const custEmail = custRec?.fields?.['Email'] || '';
    const custPhone = custRec?.fields?.['Phone'] || '';
    const custName  = custRec?.fields?.['Customer Name'] || '';
    const custFirst = custRec?.fields?.['First Name'] || custName.split(' ')[0] || 'there';
    if (!custEmail) return;

    // Fetch Stripe invoice for hosted URL and live amount
    const stripeInvId = f['Stripe Invoice ID'] || '';
    let hostedUrl = '', amountDue = 0, invNumber = '';
    if (stripeInvId && env.STRIPE_SECRET_KEY) {
      const sr = await fetch(`https://api.stripe.com/v1/invoices/${stripeInvId}`,
        { headers: { Authorization: `Bearer ${env.STRIPE_SECRET_KEY}` } });
      if (sr.ok) {
        const sd = await sr.json();
        hostedUrl  = sd.hosted_invoice_url || '';
        amountDue  = (sd.amount_due || 0) / 100;
        invNumber  = sd.number || '';
      }
    }

    const dueDate    = f['Due Date'] || '';
    const daysOverdue = dueDate
      ? Math.floor((Date.now() - new Date(dueDate + 'T12:00:00').getTime()) / 86400000)
      : 7;

    // Email customer
    const subject = `Friendly reminder: your CJB Comfort invoice is past due`;
    await sendEmail(env.RESEND_API_KEY, { to: custEmail, subject,
      html: emailOverdueHtml({ customerName: custFirst, invoiceNumber: invNumber, amountDue, hostedUrl, dueDate, daysOverdue }),
    }).catch(e => console.error('Overdue email error:', e));
    logCommunication(env, { type: 'Email', trigger: 'Overdue Notice', sentTo: custEmail, subject, customerId: custId }).catch(() => {});

    // SMS customer
    if (custPhone && env.QUO_API_KEY) {
      const amtStr = amountDue > 0 ? ` ($${amountDue.toFixed(2)})` : '';
      const payStr = hostedUrl ? ` Pay here: ${hostedUrl}` : '';
      sendSms(env.QUO_API_KEY, custPhone,
        `Hi ${custFirst} — a friendly reminder that your CJB Comfort invoice${amtStr} is past due.${payStr} Questions? Call or text ${OFFICE_PHONE}. – CJB Comfort`
      ).then(() => logCommunication(env, { type: 'SMS', trigger: 'Overdue Notice', sentTo: custPhone, subject: 'Overdue SMS', customerId: custId }))
       .catch(e => console.error('Overdue SMS error:', e));
    }

    // Follow-Up for Bridgett
    const woIds = f['Work Orders'] || [];
    const fuFields = {
      'Title':    `⚠️ Overdue Invoice — ${custName} (${daysOverdue} days)`,
      'Type':     'Follow-Up',
      'Status':   'Open',
      'Due Date': new Date().toISOString(),
      'Notes':    `Invoice${invNumber ? ` #${invNumber}` : ''}${amountDue ? ` for $${amountDue.toFixed(2)}` : ''} is ${daysOverdue} days past due. Customer notified via email${custPhone ? ' and SMS' : ''}.`,
    };
    if (custId)    fuFields['Customer']   = [custId];
    if (woIds[0])  fuFields['Work Order'] = [woIds[0]];
    await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Follow-Ups`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ fields: fuFields }),
    }).catch(e => console.error('Overdue Follow-Up error:', e.message));

    // Notify Bridgett
    const amtLabel = amountDue > 0 ? ` — $${amountDue.toFixed(2)}` : '';
    const dueFmt   = dueDate ? new Date(dueDate + 'T12:00:00').toLocaleDateString('en-US', { month:'long', day:'numeric', year:'numeric' }) : '';
    await sendEmail(env.RESEND_API_KEY, {
      to: ADMIN_EMAIL, replyTo: ADMIN_EMAIL,
      subject: `⚠️ Overdue invoice: ${custName}${amtLabel} (${daysOverdue} days)`,
      html: emailBase({ preheader: `Invoice ${daysOverdue} days overdue — customer notified.`, body: `
        <p style="font-size:17px;font-weight:700;color:#111827;margin:0 0 16px;">Overdue Invoice — Customer Notified</p>
        <div style="background:#fef3c7;border-left:4px solid #d97706;border-radius:0 8px 8px 0;padding:16px 20px;margin-bottom:20px;">
          <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#d97706;margin-bottom:8px;">${daysOverdue} Days Past Due</div>
          <div style="font-size:16px;font-weight:700;color:#111827;">${custName}</div>
          ${invNumber  ? `<div style="font-size:13px;color:#374151;margin-top:4px;">Invoice ${invNumber}</div>` : ''}
          ${amountDue  ? `<div style="font-size:13px;color:#374151;">Amount due: $${amountDue.toFixed(2)}</div>` : ''}
          ${dueFmt     ? `<div style="font-size:13px;color:#6b7280;">Due date: ${dueFmt}</div>` : ''}
        </div>
        <p style="font-size:14px;color:#374151;line-height:1.6;margin:0 0 20px;">Reminder email${custPhone ? ' and SMS' : ''} sent. A Follow-Up task has been created.</p>
        ${hostedUrl ? `<div style="text-align:center;"><a href="${hostedUrl}" style="display:inline-block;background:#c81f25;color:white;font-size:15px;font-weight:700;padding:13px 28px;border-radius:8px;text-decoration:none;">View Invoice ↗</a></div>` : ''}`,
      }),
    }).catch(e => console.error('Admin overdue email error:', e));

    await airtablePatch('Invoices', atInv.id, { 'Overdue Notice Sent': todayStr });
    console.log(`Overdue notice sent: ${atInv.id} → ${custEmail}`);
  } catch(e) {
    console.error('sendOverdueNotice error:', e.message);
  }
}

async function applyLateFee(env, atInv, todayStr) {
  try {
    const f       = atInv.fields;
    const custIds = f['Customers'] || [];
    const custId  = custIds[0] || null;
    if (!custId || !env.STRIPE_SECRET_KEY) return;

    const custRec   = await airtableGetById('Customers', custId);
    const custEmail = custRec?.fields?.['Email'] || '';
    const custPhone = custRec?.fields?.['Phone'] || '';
    const custName  = custRec?.fields?.['Customer Name'] || '';
    const custFirst = custRec?.fields?.['First Name'] || custName.split(' ')[0] || 'there';
    if (!custEmail) return;

    // Fetch Stripe invoice for live amount and hosted URL
    const stripeInvId = f['Stripe Invoice ID'] || '';
    let hostedUrl = '', amountDue = 0, invNumber = '';
    if (stripeInvId) {
      const sr = await fetch(`https://api.stripe.com/v1/invoices/${stripeInvId}`,
        { headers: { Authorization: `Bearer ${env.STRIPE_SECRET_KEY}` } });
      if (sr.ok) {
        const sd = await sr.json();
        hostedUrl = sd.hosted_invoice_url || '';
        amountDue = (sd.amount_due || 0) / 100;
        invNumber = sd.number || '';
      }
    }

    // 1.5% of amount due, minimum $5
    const lateFee = Math.max(Math.round(amountDue * 0.015 * 100) / 100, 5);

    // Find or create Stripe customer
    const srchRes  = await fetch(
      `https://api.stripe.com/v1/customers?email=${encodeURIComponent(custEmail)}&limit=1`,
      { headers: { Authorization: `Bearer ${env.STRIPE_SECRET_KEY}` } }
    );
    const srchData = await srchRes.json();
    const stripeCustId = srchData.data?.length > 0
      ? srchData.data[0].id
      : (await stripePost(env.STRIPE_SECRET_KEY, '/v1/customers', { email: custEmail, name: custName })).id;

    // Create and send a separate late fee invoice
    await stripePost(env.STRIPE_SECRET_KEY, '/v1/invoiceitems', {
      customer:    stripeCustId,
      amount:      Math.round(lateFee * 100),
      currency:    'usd',
      description: `Late fee (1.5%)${invNumber ? ` — Invoice ${invNumber}` : ''}`,
    });
    const lfInv  = await stripePost(env.STRIPE_SECRET_KEY, '/v1/invoices', {
      customer:                   stripeCustId,
      description:                `Late fee — Invoice${invNumber ? ` ${invNumber}` : ''} (1.5% of $${amountDue.toFixed(2)})`,
      'metadata[invoice_type]':   'late_fee',
      'collection_method':        'send_invoice',
      'days_until_due':           '15',
    });
    await stripePost(env.STRIPE_SECRET_KEY, `/v1/invoices/${lfInv.id}/finalize`, {});
    const lfSent    = await stripePost(env.STRIPE_SECRET_KEY, `/v1/invoices/${lfInv.id}/send`, {});
    const lateFeeUrl = lfSent.hosted_invoice_url || '';

    // Email customer
    const subject = `Late fee added to your CJB Comfort account`;
    await sendEmail(env.RESEND_API_KEY, { to: custEmail, subject,
      html: emailLateFeeHtml({ customerName: custFirst, invoiceNumber: invNumber, originalAmount: amountDue, lateFeeAmount: lateFee, hostedUrl, lateFeeUrl }),
    }).catch(e => console.error('Late fee email error:', e));
    logCommunication(env, { type: 'Email', trigger: 'Late Fee', sentTo: custEmail, subject, customerId: custId }).catch(() => {});

    // SMS customer
    if (custPhone && env.QUO_API_KEY) {
      sendSms(env.QUO_API_KEY, custPhone,
        `Hi ${custFirst} — a 1.5% late fee ($${lateFee.toFixed(2)}) has been added to your CJB Comfort account.${hostedUrl ? ` Original invoice: ${hostedUrl}` : ''}${lateFeeUrl ? ` Late fee: ${lateFeeUrl}` : ''} Questions? Call or text ${OFFICE_PHONE}. – CJB Comfort`
      ).then(() => logCommunication(env, { type: 'SMS', trigger: 'Late Fee', sentTo: custPhone, subject: 'Late fee SMS', customerId: custId }))
       .catch(e => console.error('Late fee SMS error:', e));
    }

    // Notify Bridgett
    await sendEmail(env.RESEND_API_KEY, {
      to: ADMIN_EMAIL, replyTo: ADMIN_EMAIL,
      subject: `💰 Late fee applied: ${custName} — $${lateFee.toFixed(2)}`,
      html: emailBase({ preheader: `$${lateFee.toFixed(2)} late fee invoiced to ${custName}.`, body: `
        <p style="font-size:17px;font-weight:700;color:#111827;margin:0 0 16px;">Late Fee Applied &amp; Invoiced</p>
        <div style="background:#fef2f2;border-left:4px solid #c81f25;border-radius:0 8px 8px 0;padding:16px 20px;margin-bottom:20px;">
          <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#c81f25;margin-bottom:8px;">30 Days Past Due</div>
          <div style="font-size:16px;font-weight:700;color:#111827;">${custName}</div>
          ${invNumber  ? `<div style="font-size:13px;color:#374151;margin-top:4px;">Original invoice: ${invNumber} — $${amountDue.toFixed(2)}</div>` : ''}
          <div style="font-size:13px;color:#374151;">Late fee sent: $${lateFee.toFixed(2)} (1.5%)</div>
        </div>
        <p style="font-size:14px;color:#374151;line-height:1.6;margin:0 0 20px;">A separate late fee invoice was created in Stripe and sent to the customer via email${custPhone ? ' and SMS' : ''}.</p>
        <div style="text-align:center;">
          ${hostedUrl  ? `<a href="${hostedUrl}"  style="display:inline-block;background:#f3f4f6;color:#374151;font-size:14px;font-weight:600;padding:11px 22px;border-radius:8px;text-decoration:none;margin-right:8px;">Original Invoice ↗</a>` : ''}
          ${lateFeeUrl ? `<a href="${lateFeeUrl}" style="display:inline-block;background:#c81f25;color:white;font-size:14px;font-weight:600;padding:11px 22px;border-radius:8px;text-decoration:none;">Late Fee Invoice ↗</a>` : ''}
        </div>`,
      }),
    }).catch(e => console.error('Admin late fee email error:', e));

    // Update Airtable — also set Overdue Notice Sent to block 7-day notice
    await airtablePatch('Invoices', atInv.id, {
      'Late Fee Applied':    true,
      'Late Fee Amount':     lateFee,
      'Late Fee Date':       todayStr,
      'Overdue Notice Sent': todayStr,
    });
    console.log(`Late fee applied: ${atInv.id} → ${custEmail}, $${lateFee}`);
  } catch(e) {
    console.error('applyLateFee error:', e.message);
  }
}

// ── Create first-visit Work Order + Follow-Up when a contract activates/renews ─
async function scheduleFirstContractVisit(env, contractId) {
  const contract  = await airtableGetById('Maintenance Contracts', contractId);
  const cf        = contract.fields;

  const custIds   = cf['Customer']  || [];
  const propIds   = cf['Property']  || [];
  const planName  = cf['Plan Name'] || 'Maintenance Agreement';
  const custId    = custIds[0] || null;
  const propId    = propIds[0] || null;

  // Customer name for WO/Follow-Up title
  const custRec   = custId ? await airtableGetById('Customers', custId).catch(() => null) : null;
  const custName  = custRec?.fields?.['Customer Name'] || 'Customer';

  // Due date = 2 weeks from now — prompt Bridgett to reach out and book soon
  const dueDate   = new Date(Date.now() + 14 * 86400000).toISOString();
  const woName    = `${custName} — ${planName} (Visit 1)`;

  // Create placeholder Work Order (On Hold — no scheduled date yet)
  const woFields = {
    'Work Order Name': woName,
    'Work Order Type': 'Maintenance',
    'Status':          'On Hold',
    'Active':          true,
    'Notes':           `Maintenance contract visit — contact customer to schedule. Contract: ${planName}.`,
  };
  if (custId) woFields['Customer']             = [custId];
  if (propId) woFields['Property']             = [propId];
  woFields['Maintenance Contract']             = [contractId];

  const woRes  = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Work%20Orders`, {
    method:  'POST',
    headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' },
    body:    JSON.stringify({ fields: woFields }),
  });
  const woData = await woRes.json();
  const woId   = woData.id || null;

  // Create Follow-Up task for Bridgett to schedule the visit
  const fuFields = {
    'Title':    `Schedule Visit 1 — ${custName} · ${planName}`,
    'Type':     'Follow-Up',
    'Status':   'Open',
    'Due Date': dueDate,
    'Notes':    `Contract activated/renewed. Reach out to schedule the first maintenance visit. Placeholder work order created: "${woName}".`,
  };
  if (custId) fuFields['Customer']   = [custId];
  if (woId)   fuFields['Work Order'] = [woId];

  await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Follow-Ups`, {
    method:  'POST',
    headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' },
    body:    JSON.stringify({ fields: fuFields }),
  });

  console.log(`scheduleFirstContractVisit: WO + Follow-Up created for contract ${contractId}`);
}

// ── Maintenance contract renewal check (runs every cron tick, idempotent) ────
async function checkContractRenewals(env) {
  try {
    // Target window: End Date is 28–32 days from today (catches any missed ticks)
    const now        = new Date();
    const dayMs      = 86400000;
    const windowLow  = new Date(now.getTime() + 28 * dayMs).toISOString().split('T')[0];
    const windowHigh = new Date(now.getTime() + 32 * dayMs).toISOString().split('T')[0];
    const today      = now.toISOString().split('T')[0];

    // Find Active contracts expiring in the window that haven't had a renewal notice sent yet
    const formula = `AND({Status}="Active",NOT({Renewal Invoice Sent}),IS_AFTER({End Date},"${windowLow}"),IS_BEFORE({End Date},"${windowHigh}"))`;
    const res = await fetch(
      `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Maintenance%20Contracts?filterByFormula=${encodeURIComponent(formula)}`,
      { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } }
    );
    if (!res.ok) { console.error('Contract renewal query failed:', await res.text()); return; }
    const data = await res.json();
    const contracts = data.records || [];

    for (const contract of contracts) {
      const cf         = contract.fields;
      const contractId = contract.id;
      const planName   = cf['Plan Name'] || 'Annual Maintenance Agreement';
      const autoRenew  = !!(cf['Auto Renew']);
      const annualVal  = cf['Annual Value'] || 0;
      const endDate    = cf['End Date'] || '';

      // Fetch customer
      const custId    = (cf['Customer'] || [])[0] || null;
      const custRec   = custId ? await airtableGetById('Customers', custId).catch(() => null) : null;
      const custEmail = custRec?.fields?.['Email'] || '';
      const custName  = custRec?.fields?.['Customer Name'] || '';
      const custFirst = custRec?.fields?.['First Name'] || custName.split(' ')[0] || 'there';

      if (autoRenew) {
        // ── Auto-renew: create and send renewal invoice ───────────────────
        if (!custEmail || !env.STRIPE_SECRET_KEY || !env.RESEND_API_KEY) {
          console.error(`Contract ${contractId}: missing email or API keys, skipping auto-renew`);
          continue;
        }

        // Find or create Stripe customer
        const srchRes  = await fetch(
          `https://api.stripe.com/v1/customers?email=${encodeURIComponent(custEmail)}&limit=1`,
          { headers: { Authorization: `Bearer ${env.STRIPE_SECRET_KEY}` } }
        );
        const srchData = await srchRes.json();
        let stripeCustId;
        if (srchData.data?.length > 0) {
          stripeCustId = srchData.data[0].id;
        } else {
          const cc = await stripePost(env.STRIPE_SECRET_KEY, '/v1/customers', { email: custEmail, name: custName });
          stripeCustId = cc.id;
        }

        // Invoice item + invoice
        await stripePost(env.STRIPE_SECRET_KEY, '/v1/invoiceitems', {
          customer:    stripeCustId,
          amount:      Math.round(annualVal * 100),
          currency:    'usd',
          description: `${planName} — Annual Renewal`,
        });

        const inv = await stripePost(env.STRIPE_SECRET_KEY, '/v1/invoices', {
          customer:                          stripeCustId,
          description:                       `${planName} — Annual Renewal`,
          'metadata[invoice_type]':          'maintenance_renewal',
          'metadata[contract_airtable_id]':  contractId,
          'collection_method':               'send_invoice',
          'days_until_due':                  '30',
        });

        await stripePost(env.STRIPE_SECRET_KEY, `/v1/invoices/${inv.id}/finalize`, {});
        const sent = await stripePost(env.STRIPE_SECRET_KEY, `/v1/invoices/${inv.id}/send`, {});

        // Email customer
        const endDateFmt = endDate
          ? new Date(endDate + 'T12:00:00').toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })
          : '';
        const renewSubj = `Your CJB Comfort maintenance agreement is renewing — action required`;
        await sendEmail(env.RESEND_API_KEY, {
          to:      custEmail,
          subject: renewSubj,
          html:    emailRenewalInvoiceHtml({
            customerName: custFirst,
            planName,
            annualValue:  annualVal,
            hostedUrl:    sent.hosted_invoice_url || '',
            expiresDate:  endDateFmt,
          }),
        }).catch(e => console.error('Renewal invoice email error:', e));
        logCommunication(env, {
          type:       'Email',
          trigger:    'Contract Renewal Invoice',
          sentTo:     custEmail,
          subject:    renewSubj,
          customerId: custId,
        }).catch(() => {});

        // Write Stripe Invoice ID back to contract + mark notice sent
        await airtablePatch('Maintenance Contracts', contractId, {
          'Stripe Invoice ID':    sent.id,
          'Stripe Invoice URL':   sent.hosted_invoice_url || '',
          'Renewal Invoice Sent': today,
        });

        console.log(`Contract renewal invoice sent: ${contractId} → ${custEmail}`);

      } else {
        // ── Not auto-renew: flag for Bridgett, no customer email ─────────
        const endDateFmt = endDate
          ? new Date(endDate + 'T12:00:00').toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })
          : endDate;

        // Create Follow-Up record
        await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/Follow-Ups`, {
          method:  'POST',
          headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ fields: {
            'Title':      `Contract Renewal — ${custName || 'Customer'} · ${planName}`,
            'Type':       'Follow-Up',
            'Status':     'Open',
            'Due Date':   new Date(now.getTime() + 7 * dayMs).toISOString(), // follow up within a week
            'Customer':   custId ? [custId] : undefined,
            'Notes':      `Maintenance agreement "${planName}" expires ${endDateFmt}. Auto-renew is OFF — manual renewal needed.`,
          }}),
        }).catch(e => console.error('Follow-Up create error:', e.message));

        // Notify Bridgett
        if (env.RESEND_API_KEY) {
          await sendEmail(env.RESEND_API_KEY, {
            to:      ADMIN_EMAIL,
            subject: `⚠️ Contract expiring (no auto-renew): ${custName} — ${planName}`,
            replyTo: ADMIN_EMAIL, // internal email, reply to self
            html:    emailBase({
              preheader: `${custName}'s maintenance agreement expires ${endDateFmt} — no auto-renew set.`,
              body: `
                <p style="font-size:17px;font-weight:700;color:#111827;margin:0 0 16px;">Maintenance Agreement Expiring</p>
                <div style="background:#fef3c7;border-left:4px solid #d97706;border-radius:0 8px 8px 0;padding:16px 20px;margin-bottom:20px;">
                  <div style="font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#d97706;margin-bottom:8px;">No Auto-Renew</div>
                  <div style="font-size:15px;font-weight:700;color:#111827;">${custName}</div>
                  <div style="font-size:14px;color:#374151;margin-top:4px;">${planName}</div>
                  <div style="font-size:13px;color:#6b7280;margin-top:4px;">Expires: ${endDateFmt}</div>
                  <div style="font-size:13px;color:#6b7280;">Annual value: $${annualVal.toFixed(2)}</div>
                </div>
                <p style="font-size:14px;color:#374151;line-height:1.6;margin:0 0 20px;">This customer's contract does not have auto-renew enabled. A follow-up has been created in Airtable. You'll need to reach out manually to offer renewal.</p>
                <div style="text-align:center;">
                  <a href="https://app.cjbcomfort.com/CJB_Admin.html" style="display:inline-block;background:#c81f25;color:white;font-size:15px;font-weight:700;padding:13px 28px;border-radius:8px;text-decoration:none;">Open Admin App →</a>
                </div>`,
            }),
          }).catch(e => console.error('Admin renewal notice error:', e));
        }

        // Mark notice sent so we don't create duplicate follow-ups
        await airtablePatch('Maintenance Contracts', contractId, {
          'Renewal Invoice Sent': today,
        });

        console.log(`Contract renewal notice sent to admin: ${contractId} (no auto-renew)`);
      }
    }
  } catch(e) {
    console.error('checkContractRenewals error:', e.message);
  }
}

function emailReminderHtml({ firstName, dateStr, timeStr, endTimeStr, address, woType, cancelUrl, rescheduleUrl, isDay }) {
  const timeLabel = isDay ? 'tomorrow' : 'in 2 days';
  const preheader = `Reminder: your CJB Comfort ${woType} is ${timeLabel} — ${dateStr}, ${timeStr}–${endTimeStr}`;
  const typeLabel = woType || 'Service Visit';

  const body = `
    <p style="font-size:18px;font-weight:700;color:#111827;margin:0 0 4px;">Hi ${firstName},</p>
    <p style="font-size:15px;color:#6b7280;margin:0 0 24px;">Just a reminder — your appointment is coming up ${timeLabel}.</p>

    <div style="background:#fef2f2;border-left:4px solid #c81f25;border-radius:0 10px 10px 0;padding:20px 22px;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#c81f25;margin-bottom:10px;">${typeLabel}</div>
      <div style="font-size:20px;font-weight:800;color:#111827;margin-bottom:6px;">${dateStr}</div>
      <div style="font-size:15px;font-weight:600;color:#374151;">Arrival window: ${timeStr}&nbsp;&ndash;&nbsp;${endTimeStr}</div>
      ${address ? `<div style="font-size:13px;color:#6b7280;margin-top:8px;">&#128205; ${address}</div>` : ''}
    </div>

    <p style="font-size:15px;color:#374151;line-height:1.65;margin:24px 0 0;">Your technician will send you a text when they&rsquo;re on the way &mdash; no need to wait by the door.</p>

    <div style="border-top:1px solid #f3f4f6;margin-top:28px;padding-top:20px;">
      <p style="font-size:13px;color:#6b7280;margin:0 0 14px;line-height:1.5;">Need to make changes? You can reschedule or cancel up to 24&nbsp;hours before your appointment.</p>
      <div>
        <a href="${rescheduleUrl}" style="display:inline-block;background:#f3f4f6;color:#374151;font-size:13px;font-weight:600;padding:10px 20px;border-radius:8px;text-decoration:none;margin-right:8px;">↩ Reschedule</a>
        <a href="${cancelUrl}"     style="display:inline-block;background:#f3f4f6;color:#374151;font-size:13px;font-weight:600;padding:10px 20px;border-radius:8px;text-decoration:none;">✕ Cancel</a>
      </div>
    </div>`;

  return emailBase({ preheader, body });
}

function emailAdminAlertHtml({ type, customerName, dateStr, timeStr, address, woType }) {
  const isCancel = type === 'cancel';
  const icon     = isCancel ? '❌' : '🔄';
  const headline = isCancel ? 'Appointment Cancelled' : 'Reschedule Requested';
  const detail   = isCancel
    ? `<strong>${customerName}</strong> has cancelled their ${woType} appointment${dateStr ? ` on <strong>${dateStr}</strong>${timeStr ? ` at ${timeStr}` : ''}` : ''}${address ? ` at ${address}` : ''}.`
    : `<strong>${customerName}</strong> has requested to reschedule their ${woType} appointment${dateStr ? ` on <strong>${dateStr}</strong>${timeStr ? ` at ${timeStr}` : ''}` : ''}${address ? ` at ${address}` : ''}. Please reach out to confirm a new time.`;

  const preheader = `${headline}: ${customerName}${dateStr ? ' — ' + dateStr : ''}`;
  const body = `
    <div style="text-align:center;font-size:48px;margin-bottom:16px;">${icon}</div>
    <p style="font-size:20px;font-weight:800;color:#111827;margin:0 0 16px;text-align:center;">${headline}</p>
    <p style="font-size:15px;color:#374151;line-height:1.65;margin:0;">${detail}</p>
    ${!isCancel ? `<p style="font-size:14px;color:#6b7280;margin:16px 0 0;">Log into the <a href="https://app.cjbcomfort.com/CJB_Admin.html" style="color:#c81f25;font-weight:600;">admin app</a> to view and reschedule this work order.</p>` : ''}`;

  return emailBase({ preheader, body });
}

// ── Invoice sent email ────────────────────────────────────────────────────────
function emailInvoiceHtml({ customerName, invoiceNumber, total, hostedUrl, dueDate }) {
  const invoiceLabel  = invoiceNumber ? `Invoice ${invoiceNumber}` : 'Your Invoice';
  const formattedTotal = typeof total === 'number' && total > 0 ? `$${total.toFixed(2)}` : '';
  const dueLine        = dueDate ? `<div style="font-size:13px;color:#6b7280;margin-top:6px;">Due: ${dueDate}</div>` : '';
  const preheader      = `Your invoice from CJB Comfort is ready${formattedTotal ? ` — ${formattedTotal}` : ''}. Tap to view and pay.`;

  const body = `
    <p style="font-size:18px;font-weight:700;color:#111827;margin:0 0 4px;">Hi ${customerName},</p>
    <p style="font-size:15px;color:#6b7280;margin:0 0 24px;">Your invoice is ready. You can view and pay securely online using the button below.</p>

    <div style="background:#fef2f2;border-left:4px solid #c81f25;border-radius:0 10px 10px 0;padding:20px 22px;margin-bottom:24px;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#c81f25;margin-bottom:10px;">${invoiceLabel}</div>
      ${formattedTotal ? `<div style="font-size:28px;font-weight:800;color:#111827;margin-bottom:4px;">${formattedTotal}</div>` : ''}
      ${dueLine}
    </div>

    <div style="text-align:center;margin:0 0 28px;">
      <a href="${hostedUrl}" style="display:inline-block;background:#c81f25;color:white;font-size:17px;font-weight:700;padding:16px 36px;border-radius:10px;text-decoration:none;">View &amp; Pay Invoice &rarr;</a>
    </div>

    <p style="font-size:13px;color:#6b7280;text-align:center;margin:0 0 6px;">We accept all major credit and debit cards.</p>
    <p style="font-size:13px;color:#6b7280;text-align:center;margin:0;">Prefer to pay by check or have questions? Call or text us at <a href="${OFFICE_PHONE_URL}" style="color:#c81f25;font-weight:600;">${OFFICE_PHONE}</a>.</p>`;

  return emailBase({ preheader, body });
}

// ── Invoice paid — thank you + Google Review ask ──────────────────────────────
function emailPaymentThankYouHtml({ customerName, amountPaid, invoiceNumber, googleReviewUrl }) {
  const amountStr  = typeof amountPaid === 'number' && amountPaid > 0 ? `$${amountPaid.toFixed(2)}` : '';
  const invoiceRef = invoiceNumber ? ` (${invoiceNumber})` : '';
  const preheader  = `Payment received${amountStr ? ` — ${amountStr}` : ''}. Thank you for choosing CJB Comfort!`;

  const body = `
    <p style="font-size:18px;font-weight:700;color:#111827;margin:0 0 4px;">Hi ${customerName},</p>
    <p style="font-size:15px;color:#6b7280;margin:0 0 24px;">We&rsquo;ve received your payment${invoiceRef}. Thank you for choosing CJB Comfort &mdash; it means a lot to us.</p>

    ${amountStr ? `
    <div style="background:#f0fdf4;border-left:4px solid #16a34a;border-radius:0 10px 10px 0;padding:20px 22px;margin-bottom:24px;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#16a34a;margin-bottom:10px;">Payment Received</div>
      <div style="font-size:28px;font-weight:800;color:#111827;">${amountStr}</div>
    </div>` : ''}

    <p style="font-size:15px;color:#374151;line-height:1.65;margin:0 0 24px;">If you have any questions about your service or notice anything we can help with, don&rsquo;t hesitate to reach out &mdash; we&rsquo;re always a call or text away.</p>

    <div style="background:#f9fafb;border-radius:10px;padding:24px;text-align:center;">
      <p style="font-size:15px;font-weight:700;color:#111827;margin:0 0 8px;">Happy with your service?</p>
      <p style="font-size:14px;color:#6b7280;margin:0 0 18px;line-height:1.5;">A quick Google review helps our small business more than you know. It takes less than a minute and means the world to us.</p>
      <a href="${googleReviewUrl}" style="display:inline-block;background:#111827;color:white;font-size:15px;font-weight:700;padding:13px 28px;border-radius:8px;text-decoration:none;">&#11088; Leave a Google Review</a>
    </div>`;

  return emailBase({ preheader, body });
}

// ── Deposit received confirmation ─────────────────────────────────────────────
function emailDepositReceivedHtml({ customerName, amountPaid, invoiceNumber }) {
  const amountStr  = typeof amountPaid === 'number' && amountPaid > 0 ? `$${amountPaid.toFixed(2)}` : '';
  const invoiceRef = invoiceNumber ? ` (${invoiceNumber})` : '';
  const preheader  = `Deposit received${amountStr ? ` — ${amountStr}` : ''}. You're all set with CJB Comfort!`;

  const body = `
    <p style="font-size:18px;font-weight:700;color:#111827;margin:0 0 4px;">Hi ${customerName},</p>
    <p style="font-size:15px;color:#6b7280;margin:0 0 24px;">We&rsquo;ve received your deposit${invoiceRef} &mdash; thank you! Your appointment is confirmed and we look forward to seeing you.</p>

    ${amountStr ? `
    <div style="background:#eff6ff;border-left:4px solid #2563eb;border-radius:0 10px 10px 0;padding:20px 22px;margin-bottom:24px;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#2563eb;margin-bottom:10px;">Deposit Received</div>
      <div style="font-size:28px;font-weight:800;color:#111827;">${amountStr}</div>
    </div>` : ''}

    <div style="background:#f9fafb;border-radius:10px;padding:20px 22px;margin-bottom:24px;">
      <p style="font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#6b7280;margin:0 0 10px;">What happens next</p>
      <p style="font-size:14px;color:#374151;line-height:1.65;margin:0 0 8px;">&#10003;&nbsp; Your deposit is applied to the total cost of your service.</p>
      <p style="font-size:14px;color:#374151;line-height:1.65;margin:0 0 8px;">&#10003;&nbsp; After the work is complete, we&rsquo;ll send a final balance invoice for the remaining amount.</p>
      <p style="font-size:14px;color:#374151;line-height:1.65;margin:0;">&#10003;&nbsp; You&rsquo;ll get a reminder before your appointment.</p>
    </div>

    <p style="font-size:13px;color:#6b7280;text-align:center;margin:0;">Questions? Call or text us anytime at <a href="${OFFICE_PHONE_URL}" style="color:#c81f25;font-weight:600;">${OFFICE_PHONE}</a>.</p>`;

  return emailBase({ preheader, body });
}

// ── Overdue invoice reminder (7 days past due) ────────────────────────────────
function emailOverdueHtml({ customerName, invoiceNumber, amountDue, hostedUrl, dueDate, daysOverdue }) {
  const amountStr  = amountDue > 0 ? `$${amountDue.toFixed(2)}` : '';
  const invoiceRef = invoiceNumber ? `Invoice ${invoiceNumber}` : 'Your Invoice';
  const dueFmt     = dueDate ? new Date(dueDate + 'T12:00:00').toLocaleDateString('en-US', { month:'long', day:'numeric', year:'numeric' }) : '';
  const preheader  = `Your CJB Comfort invoice${amountStr ? ` for ${amountStr}` : ''} is past due. A quick payment keeps everything on track.`;

  const body = `
    <p style="font-size:18px;font-weight:700;color:#111827;margin:0 0 4px;">Hi ${customerName},</p>
    <p style="font-size:15px;color:#6b7280;margin:0 0 24px;">Just a friendly heads-up &mdash; we have an invoice that&rsquo;s past due on your account. If you&rsquo;ve already taken care of it, please disregard this message!</p>

    <div style="background:#fef3c7;border-left:4px solid #d97706;border-radius:0 10px 10px 0;padding:20px 22px;margin-bottom:24px;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#d97706;margin-bottom:10px;">${invoiceRef} &mdash; Past Due</div>
      ${amountStr ? `<div style="font-size:28px;font-weight:800;color:#111827;margin-bottom:4px;">${amountStr}</div>` : ''}
      ${dueFmt    ? `<div style="font-size:13px;color:#6b7280;">Was due: ${dueFmt} (${daysOverdue} day${daysOverdue === 1 ? '' : 's'} ago)</div>` : ''}
    </div>

    ${hostedUrl ? `
    <div style="text-align:center;margin:0 0 28px;">
      <a href="${hostedUrl}" style="display:inline-block;background:#c81f25;color:white;font-size:17px;font-weight:700;padding:16px 36px;border-radius:10px;text-decoration:none;">Pay Now &rarr;</a>
    </div>` : ''}

    <p style="font-size:14px;color:#374151;line-height:1.65;margin:0 0 12px;">If you have any questions about this invoice or need to make other arrangements, don&rsquo;t hesitate to reach out &mdash; we&rsquo;re happy to help.</p>
    <p style="font-size:13px;color:#6b7280;text-align:center;margin:0;">Call or text us at <a href="${OFFICE_PHONE_URL}" style="color:#c81f25;font-weight:600;">${OFFICE_PHONE}</a>.</p>`;

  return emailBase({ preheader, body });
}

// ── Late fee notification (30 days past due) ──────────────────────────────────
function emailLateFeeHtml({ customerName, invoiceNumber, originalAmount, lateFeeAmount, hostedUrl, lateFeeUrl }) {
  const origStr    = originalAmount  > 0 ? `$${originalAmount.toFixed(2)}`  : '';
  const feeStr     = lateFeeAmount   > 0 ? `$${lateFeeAmount.toFixed(2)}`   : '';
  const invoiceRef = invoiceNumber ? `Invoice ${invoiceNumber}` : 'your invoice';
  const preheader  = `A 1.5% late fee${feeStr ? ` of ${feeStr}` : ''} has been added to your CJB Comfort account.`;

  const body = `
    <p style="font-size:18px;font-weight:700;color:#111827;margin:0 0 4px;">Hi ${customerName},</p>
    <p style="font-size:15px;color:#6b7280;margin:0 0 24px;">We haven&rsquo;t received payment for ${invoiceRef}${origStr ? ` ($${originalAmount.toFixed(2)})` : ''}, which is now 30 days past due. Per our billing policy, a 1.5% late fee has been added to your account.</p>

    <div style="background:#fef2f2;border-left:4px solid #c81f25;border-radius:0 10px 10px 0;padding:20px 22px;margin-bottom:24px;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#c81f25;margin-bottom:10px;">Late Fee Added</div>
      ${origStr ? `<div style="font-size:14px;color:#6b7280;margin-bottom:4px;">Original invoice: ${origStr}</div>` : ''}
      ${feeStr  ? `<div style="font-size:24px;font-weight:800;color:#111827;">+ ${feeStr} late fee</div>` : ''}
    </div>

    <p style="font-size:14px;color:#374151;line-height:1.65;margin:0 0 20px;">Both the original invoice and the late fee have separate pay links below. To avoid additional fees, please pay as soon as possible.</p>

    <div style="text-align:center;margin:0 0 24px;">
      ${hostedUrl    ? `<a href="${hostedUrl}"    style="display:inline-block;background:#f3f4f6;color:#374151;font-size:15px;font-weight:600;padding:13px 24px;border-radius:8px;text-decoration:none;margin:0 6px 8px;">Pay Original Invoice &rarr;</a>` : ''}
      ${lateFeeUrl   ? `<a href="${lateFeeUrl}"   style="display:inline-block;background:#c81f25;color:white;font-size:15px;font-weight:600;padding:13px 24px;border-radius:8px;text-decoration:none;margin:0 6px 8px;">Pay Late Fee &rarr;</a>` : ''}
    </div>

    <p style="font-size:14px;color:#374151;line-height:1.65;margin:0 0 12px;">If there&rsquo;s been an error or you&rsquo;d like to discuss your account, please reach out right away and we&rsquo;ll get it sorted out.</p>
    <p style="font-size:13px;color:#6b7280;text-align:center;margin:0;">Call or text us at <a href="${OFFICE_PHONE_URL}" style="color:#c81f25;font-weight:600;">${OFFICE_PHONE}</a>.</p>`;

  return emailBase({ preheader, body });
}

// ── Renewal invoice email (sent 30 days before expiry) ───────────────────────
function emailRenewalInvoiceHtml({ customerName, planName, annualValue, hostedUrl, expiresDate }) {
  const amountStr = typeof annualValue === 'number' && annualValue > 0 ? `$${annualValue.toFixed(2)}` : '';
  const preheader = `Your CJB Comfort maintenance agreement is coming up for renewal${amountStr ? ` — ${amountStr}` : ''}. Pay to keep your coverage active.`;

  const body = `
    <p style="font-size:18px;font-weight:700;color:#111827;margin:0 0 4px;">Hi ${customerName},</p>
    <p style="font-size:15px;color:#6b7280;margin:0 0 24px;">Your maintenance agreement with CJB Comfort is coming up for renewal. Pay your renewal invoice below to keep your coverage active without any interruption.</p>

    <div style="background:#fef2f2;border-left:4px solid #c81f25;border-radius:0 10px 10px 0;padding:20px 22px;margin-bottom:24px;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#c81f25;margin-bottom:10px;">Renewal Invoice — ${planName}</div>
      ${amountStr ? `<div style="font-size:28px;font-weight:800;color:#111827;margin-bottom:4px;">${amountStr} / year</div>` : ''}
      ${expiresDate ? `<div style="font-size:13px;color:#6b7280;margin-top:4px;">Current coverage expires: ${expiresDate}</div>` : ''}
    </div>

    <div style="text-align:center;margin:0 0 28px;">
      <a href="${hostedUrl}" style="display:inline-block;background:#c81f25;color:white;font-size:17px;font-weight:700;padding:16px 36px;border-radius:10px;text-decoration:none;">Pay Renewal Invoice &rarr;</a>
    </div>

    <div style="background:#f9fafb;border-radius:10px;padding:20px 22px;margin-bottom:24px;">
      <p style="font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#6b7280;margin:0 0 10px;">What your agreement includes</p>
      <p style="font-size:14px;color:#374151;line-height:1.65;margin:0 0 6px;">&#10003;&nbsp; Scheduled maintenance visits to keep your system running efficiently</p>
      <p style="font-size:14px;color:#374151;line-height:1.65;margin:0 0 6px;">&#10003;&nbsp; Priority scheduling as a maintenance agreement customer</p>
      <p style="font-size:14px;color:#374151;line-height:1.65;margin:0;">&#10003;&nbsp; Discounted rates on any repairs needed throughout the year</p>
    </div>

    <p style="font-size:13px;color:#6b7280;text-align:center;margin:0;">Questions? Call or text us at <a href="${OFFICE_PHONE_URL}" style="color:#c81f25;font-weight:600;">${OFFICE_PHONE}</a>.</p>`;

  return emailBase({ preheader, body });
}

// ── Renewal confirmed email (sent when renewal invoice is paid) ───────────────
function emailRenewalConfirmedHtml({ customerName, planName, amountPaid, newStartDate, newEndDate }) {
  const amountStr = typeof amountPaid === 'number' && amountPaid > 0 ? `$${amountPaid.toFixed(2)}` : '';
  const preheader = `Your CJB Comfort maintenance agreement has been renewed through ${newEndDate}. Thank you!`;

  const body = `
    <p style="font-size:18px;font-weight:700;color:#111827;margin:0 0 4px;">Hi ${customerName},</p>
    <p style="font-size:15px;color:#6b7280;margin:0 0 24px;">Your maintenance agreement has been renewed &mdash; thank you! You&rsquo;re all set for another year of worry-free comfort.</p>

    <div style="background:#f0fdf4;border-left:4px solid #16a34a;border-radius:0 10px 10px 0;padding:20px 22px;margin-bottom:24px;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#16a34a;margin-bottom:10px;">Agreement Renewed — ${planName}</div>
      ${amountStr ? `<div style="font-size:28px;font-weight:800;color:#111827;margin-bottom:8px;">${amountStr}</div>` : ''}
      <div style="font-size:14px;color:#374151;">Coverage period: <strong>${newStartDate}</strong> &ndash; <strong>${newEndDate}</strong></div>
    </div>

    <p style="font-size:15px;color:#374151;line-height:1.65;margin:0 0 24px;">We&rsquo;ll be in touch to schedule your first maintenance visit of the new agreement year. As always, if you notice anything with your system before then, just give us a call or text &mdash; you&rsquo;re a priority customer.</p>

    <p style="font-size:13px;color:#6b7280;text-align:center;margin:0;">Questions? Call or text us at <a href="${OFFICE_PHONE_URL}" style="color:#c81f25;font-weight:600;">${OFFICE_PHONE}</a>.</p>`;

  return emailBase({ preheader, body });
}

// ── Resend domain health check — runs hourly, alerts at 8 AM AZ if failed ────
async function checkResendDomain(env) {
  if (!env.RESEND_API_KEY || !env.QUO_API_KEY || !env.OWNER_PHONE) return;
  try {
    // Only alert once per day — at 8 AM Arizona time
    const nowAZ = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Phoenix' }));
    if (nowAZ.getHours() !== 8) return;

    const res = await fetch('https://api.resend.com/domains', {
      headers: { 'Authorization': 'Bearer ' + env.RESEND_API_KEY }
    });
    if (!res.ok) return;
    const data = await res.json();
    const failed = (data.data || []).filter(d => d.status !== 'verified');
    if (failed.length === 0) return;

    const names = failed.map(d => d.name).join(', ');
    await sendSms(env.QUO_API_KEY, env.OWNER_PHONE,
      `CJB Comfort alert: Resend email domain FAILED for ${names}. Emails are NOT sending. Log in to resend.com/domains to fix.`
    );
  } catch(e) { /* non-fatal */ }
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
