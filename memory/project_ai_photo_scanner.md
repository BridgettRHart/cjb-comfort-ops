---
name: AI Equipment Photo Scanner — Priority Feature
description: AI-powered data tag photo reader for auto-populating equipment records. Day-one priority, not a later phase item.
type: project
---

AI photo scanner for equipment data tags is a high-priority feature, not a later phase item.

**Why:** Cornell needs to update/sync equipment records from day one of using the app. He already has photos of most equipment. For future franchise/product customers, bulk onboarding of equipment data is a major friction point — solving it is a competitive differentiator.

**Two build paths planned:**

1. **Bulk batch processor** (desktop tool, first priority) — Upload many photos at once, AI reads each tag, GPS EXIF matches to property, creates/updates Airtable Equipment records in bulk. Fits the existing pattern of separate importer tools (CJB_Equipment_Importer.html already exists). Cornell can run his existing photo library through this immediately.

2. **In-app single photo scan** (mobile, second) — "Scan Tag" button on the Add/Edit Equipment form. Cornell takes a photo on-site, AI fills the form fields, he reviews and saves.

**Technical requirements:**
- Anthropic API key (separate from Claude Code, pay-per-use, low cost)
- Cloudflare Worker update to add a /api/vision endpoint that calls Claude's vision API
- EXIF GPS reading in browser (exifr.js or similar)
- Reverse geocoding API to convert GPS coords to address for property matching

**Why: Cornell has existing photos now and needs to enter a LOT of equipment records upfront. For new franchise customers, this removes the biggest onboarding friction entirely.**
