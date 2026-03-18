import 'dotenv/config';
import { chromium, Browser, BrowserContext, Page } from 'playwright';
import axios, { AxiosInstance } from 'axios';
import * as fs from 'fs';

const API_BASE_URL = 'https://core.uniteus.io';
const PROGRESS_FILE = 'export_progress.json';
const OUTPUT_FILE = 'clients_full_export.csv';
const BATCH_SIZE = 100; // Clients per batch before saving progress
const CONCURRENCY = 30;
const PAGE_SIZE = 100;

interface AuthInfo {
  token: string;
  employeeId: string;
  providerId: string;
  expiresAt: number;
}

interface Progress {
  completedIds: string[];
  lastPage: number;
  totalClients: number;
  startedAt: string;
  lastUpdated: string;
}

interface ApiData {
  id: string;
  type: string;
  attributes: Record<string, unknown>;
  relationships?: Record<string, unknown>;
}

let shuttingDown = false;

// Handle Ctrl+C gracefully
process.on('SIGINT', () => {
  console.log('\n\nReceived SIGINT. Finishing current batch and saving progress...');
  shuttingDown = true;
});

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function authenticate(email: string, password: string): Promise<{ authInfo: AuthInfo; close: () => Promise<void> }> {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  let authToken = '';
  let employeeId = '';
  let providerId = '';

  page.on('response', async (response) => {
    if (response.url().includes('oauth2/token')) {
      try {
        const data = await response.json();
        if (data.access_token) authToken = data.access_token;
      } catch {}
    }
  });

  page.on('request', (request) => {
    const empId = request.headers()['x-employee-id'];
    if (empId) employeeId = empId;
    const url = request.url();
    const providerMatch = url.match(/filter%5Bclient_relationships\.provider%5D=([a-f0-9-]+)/);
    if (providerMatch) providerId = providerMatch[1];
  });

  console.log('Authenticating...');
  await page.goto('https://app.auth.uniteus.io/');

  // Step 1: Enter email on Unite Us
  await page.waitForSelector('input[type="email"], input[name="user[email]"]', { timeout: 15000 });
  await page.fill('input[type="email"], input[name="user[email]"]', email);
  await page.click('button[type="submit"], input[type="submit"]');

  // Step 2: Fill credentials on NYC.ID SAML login
  await page.waitForURL('**nyc.gov**', { timeout: 15000 });
  await page.waitForSelector('#gigya-loginID', { timeout: 15000 });
  await page.fill('#gigya-loginID', email);
  await page.fill('#gigya-password', password);
  await page.click('input[type="submit"]');

  // Step 3: Select PHS group
  await page.waitForSelector('text=Stand Out Care Corp - SCN - PHS', { timeout: 30000 });
  await page.click('text=Stand Out Care Corp - SCN - PHS');

  await page.waitForURL('**/dashboard/**', { timeout: 60000 });
  await page.waitForTimeout(2000);
  await page.goto('https://app.uniteus.io/dashboard/clients/all', { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(5000);

  if (!authToken) throw new Error('Failed to capture auth token');
  if (!providerId) providerId = 'e15af87f-3d21-4cbb-b5b0-c3c637a5dd8e';

  // Token expires in 15 minutes, set expiry 12 minutes from now to be safe
  const expiresAt = Date.now() + 12 * 60 * 1000;

  console.log('Authentication successful!');

  return {
    authInfo: { token: authToken, employeeId, providerId, expiresAt },
    close: () => browser.close(),
  };
}

function createApiClient(authInfo: AuthInfo): AxiosInstance {
  return axios.create({
    baseURL: API_BASE_URL,
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'Authorization': `Bearer ${authInfo.token}`,
      'x-employee-id': authInfo.employeeId,
      'x-application-source': 'web',
    },
  });
}

function isRetryableError(error: unknown): boolean {
  if (axios.isAxiosError(error)) {
    const status = error.response?.status;
    if (status && [429, 500, 502, 503, 504].includes(status)) return true;
    // Network errors
    const code = error.code;
    if (code && ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN'].includes(code)) return true;
  }
  // Generic network errors
  if (error instanceof Error) {
    const msg = error.message.toLowerCase();
    if (msg.includes('timeout') || msg.includes('network') || msg.includes('socket')) return true;
  }
  return false;
}

async function withRetry<T>(fn: () => Promise<T>, maxRetries = 6, baseDelayMs = 2000): Promise<T> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (isRetryableError(error) && attempt < maxRetries) {
        const delay = baseDelayMs * Math.pow(2, attempt - 1); // Exponential backoff: 2s, 4s, 8s, 16s, 32s
        const status = axios.isAxiosError(error) ? error.response?.status || error.code : 'error';
        console.log(`  Retry ${attempt}/${maxRetries} after ${status}, waiting ${delay / 1000}s...`);
        await sleep(delay);
        continue;
      }
      throw error;
    }
  }
  throw new Error('Max retries exceeded');
}

function loadProgress(): Progress | null {
  if (fs.existsSync(PROGRESS_FILE)) {
    const data = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf-8'));
    return data as Progress;
  }
  return null;
}

function saveProgress(progress: Progress): void {
  progress.lastUpdated = new Date().toISOString();
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

function flattenClient(
  person: ApiData,
  addresses: ApiData[],
  insurances: ApiData[],
  languages: ApiData[]
): Record<string, string> {
  const flat: Record<string, string> = { id: person.id };
  const attrs = person.attributes || {};

  for (const [key, value] of Object.entries(attrs)) {
    if (value === null || value === undefined) {
      flat[key] = '';
    } else if (key === 'phone_numbers' && Array.isArray(value) && value.length > 0) {
      const primary = value.find((p: any) => p.is_primary) || value[0];
      flat['phone'] = primary?.phone_number || '';
      flat['phone_type'] = primary?.phone_type || '';
    } else if (key === 'email_addresses' && Array.isArray(value) && value.length > 0) {
      flat['email'] = (value[0] as any)?.email || '';
    } else if (Array.isArray(value)) {
      flat[key] = value.length > 0 ? JSON.stringify(value) : '';
    } else if (typeof value === 'object') {
      flat[key] = JSON.stringify(value);
    } else {
      flat[key] = String(value);
    }
  }

  const primaryAddr = addresses.find((a: any) => a.attributes?.is_primary) || addresses[0];
  if (primaryAddr) {
    const addr = primaryAddr.attributes || {};
    flat['address_line_1'] = String(addr.line_1 || '');
    flat['address_line_2'] = String(addr.line_2 || '');
    flat['address_city'] = String(addr.city || '');
    flat['address_state'] = String(addr.state || '');
    flat['address_postal_code'] = String(addr.postal_code || '');
    flat['address_county'] = String(addr.county || '');
    flat['address_type'] = String(addr.address_type || '');
  }

  const activeInsurance = insurances.find((i: any) => i.attributes?.state === 'active') || insurances[0];
  if (activeInsurance) {
    const ins = activeInsurance.attributes || {};
    flat['insurance_member_id'] = String(ins.external_member_id || '');
    flat['insurance_group_id'] = String(ins.external_group_id || '');
    flat['insurance_status'] = String(ins.state || '');
    flat['insurance_enrolled_at'] = String(ins.enrolled_at || '');
    flat['insurance_expired_at'] = String(ins.expired_at || '');
  }

  if (languages.length > 0) {
    const spokenLangs = languages
      .filter((l: any) => l.attributes?.record_language_type === 'spoken')
      .map((l: any) => l.attributes?.language_name || l.attributes?.language_code || '')
      .filter(Boolean);
    flat['spoken_languages'] = spokenLangs.join('; ');

    const writtenLangs = languages
      .filter((l: any) => l.attributes?.record_language_type === 'written')
      .map((l: any) => l.attributes?.language_name || l.attributes?.language_code || '')
      .filter(Boolean);
    flat['written_languages'] = writtenLangs.join('; ');
  } else {
    flat['spoken_languages'] = '';
    flat['written_languages'] = '';
  }

  return flat;
}

function getHeaders(): string[] {
  return [
    'id', 'first_name', 'last_name', 'middle_name', 'date_of_birth',
    'gender', 'race', 'ethnicity', 'spoken_languages', 'written_languages',
    'phone', 'phone_type', 'email',
    'address_line_1', 'address_line_2', 'address_city', 'address_state',
    'address_postal_code', 'address_county', 'address_type',
    'insurance_member_id', 'insurance_group_id', 'insurance_status',
    'insurance_enrolled_at', 'insurance_expired_at',
  ];
}

function rowToCsv(row: Record<string, string>, headers: string[]): string {
  return headers.map(h => {
    const val = row[h] || '';
    if (val.includes(',') || val.includes('"') || val.includes('\n')) {
      return `"${val.replace(/"/g, '""')}"`;
    }
    return val;
  }).join(',');
}

function initCsvFile(): void {
  if (!fs.existsSync(OUTPUT_FILE)) {
    fs.writeFileSync(OUTPUT_FILE, getHeaders().join(',') + '\n');
  }
}

function appendToCsv(rows: Record<string, string>[]): void {
  const headers = getHeaders();
  const lines = rows.map(row => rowToCsv(row, headers)).join('\n');
  fs.appendFileSync(OUTPUT_FILE, lines + '\n');
}

async function main() {
  const email = process.env.UNITEUS_EMAIL || '';
  const password = process.env.UNITEUS_PASSWORD || '';

  if (!email || !password) {
    console.error('Set UNITEUS_EMAIL and UNITEUS_PASSWORD in .env');
    process.exit(1);
  }

  // Load or initialize progress
  let progress = loadProgress();
  const completedSet = new Set(progress?.completedIds || []);

  if (progress) {
    console.log(`\nResuming export from ${progress.completedIds.length} completed clients...`);
    console.log(`Started: ${progress.startedAt}`);
    console.log(`Last updated: ${progress.lastUpdated}\n`);
  } else {
    progress = {
      completedIds: [],
      lastPage: 0,
      totalClients: 0,
      startedAt: new Date().toISOString(),
      lastUpdated: new Date().toISOString(),
    };
    console.log('\nStarting fresh export...\n');
  }

  initCsvFile();

  // Authenticate
  let { authInfo, close } = await authenticate(email, password);
  let apiClient = createApiClient(authInfo);

  // Get total count
  const countRes = await apiClient.get('/v1/people', {
    params: {
      'filter[client_relationships.provider]': authInfo.providerId,
      'page[number]': 1,
      'page[size]': 1,
    },
  });
  const totalClients = countRes.data.meta?.page?.total_count || 0;
  progress.totalClients = totalClients;
  console.log(`Total clients to export: ${totalClients.toLocaleString()}\n`);

  const totalPages = Math.ceil(totalClients / PAGE_SIZE);
  const startPage = progress.lastPage + 1;

  const startTime = Date.now();
  let processedThisSession = 0;

  for (let page = startPage; page <= totalPages && !shuttingDown; page++) {
    // Check if we need to re-authenticate
    if (Date.now() > authInfo.expiresAt) {
      console.log('\nToken expiring, re-authenticating...');
      await close();
      const newAuth = await authenticate(email, password);
      authInfo = newAuth.authInfo;
      close = newAuth.close;
      apiClient = createApiClient(authInfo);
    }

    // Fetch page of client IDs with extended retry for page-level failures
    let clientIds: string[];
    let pageRetries = 0;
    const maxPageRetries = 3;
    while (true) {
      try {
        const listRes = await withRetry(() =>
          apiClient.get('/v1/people', {
            params: {
              'filter[client_relationships.provider]': authInfo.providerId,
              'sort': 'last_name,first_name',
              'page[number]': page,
              'page[size]': PAGE_SIZE,
            },
          })
        );
        const clientList = Array.isArray(listRes.data.data) ? listRes.data.data : [listRes.data.data];
        clientIds = clientList.map((c: ApiData) => c.id).filter((id: string) => !completedSet.has(id));
        break; // Success, exit retry loop
      } catch (error) {
        pageRetries++;
        if (pageRetries >= maxPageRetries) {
          console.error(`Error fetching page ${page} after ${maxPageRetries} attempts, skipping...`);
          clientIds = [];
          break;
        }
        const waitTime = 30000 * pageRetries; // 30s, 60s, 90s
        console.log(`Page ${page} failed, waiting ${waitTime / 1000}s before retry ${pageRetries}/${maxPageRetries}...`);
        await sleep(waitTime);

        // Re-authenticate in case token expired
        if (Date.now() > authInfo.expiresAt - 60000) {
          console.log('Re-authenticating before retry...');
          await close();
          const newAuth = await authenticate(email, password);
          authInfo = newAuth.authInfo;
          close = newAuth.close;
          apiClient = createApiClient(authInfo);
        }
      }
    }

    if (clientIds.length === 0) {
      progress.lastPage = page;
      continue;
    }

    // Fetch client details in parallel batches
    const batchResults: Record<string, string>[] = [];

    for (let i = 0; i < clientIds.length && !shuttingDown; i += CONCURRENCY) {
      const batch = clientIds.slice(i, i + CONCURRENCY);
      const promises = batch.map(async (clientId) => {
        try {
          const [personRes, insuranceRes, langRes] = await Promise.all([
            withRetry(() => apiClient.get(`/v1/people/${clientId}`, { params: { include: 'addresses' } })),
            withRetry(() => apiClient.get('/v1/insurances', { params: { 'filter[person]': clientId } })),
            withRetry(() => apiClient.get('/v1/record_languages', {
              params: { 'filter[record_id]': clientId, 'filter[record_type]': 'Person' },
            })),
          ]);

          const person = personRes.data.data;
          const addresses = personRes.data.included || [];
          const insurances = Array.isArray(insuranceRes.data.data) ? insuranceRes.data.data : [];
          const languages = Array.isArray(langRes.data.data) ? langRes.data.data : [];

          return { clientId, data: flattenClient(person, addresses, insurances, languages) };
        } catch (error) {
          if (axios.isAxiosError(error) && error.response?.status === 401) {
            throw error; // Propagate auth errors
          }
          return null;
        }
      });

      const results = await Promise.all(promises);
      for (const result of results) {
        if (result) {
          batchResults.push(result.data);
          completedSet.add(result.clientId);
          progress.completedIds.push(result.clientId);
        }
      }
      // No delay - only sleep on rate limit (handled by withRetry)
    }

    // Save batch to CSV
    if (batchResults.length > 0) {
      appendToCsv(batchResults);
      processedThisSession += batchResults.length;
    }

    progress.lastPage = page;
    saveProgress(progress);

    // Progress report
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = processedThisSession / elapsed;
    const remaining = totalClients - progress.completedIds.length;
    const etaHours = remaining / rate / 3600;

    console.log(
      `Page ${page}/${totalPages} | ` +
      `${progress.completedIds.length.toLocaleString()}/${totalClients.toLocaleString()} clients | ` +
      `${rate.toFixed(1)}/sec | ` +
      `ETA: ${etaHours.toFixed(1)}h`
    );
  }

  await close();

  if (shuttingDown) {
    console.log('\n=== PAUSED ===');
    console.log(`Progress saved. Run again to resume.`);
    console.log(`Completed: ${progress.completedIds.length.toLocaleString()} / ${totalClients.toLocaleString()}`);
  } else {
    console.log('\n=== EXPORT COMPLETE ===');
    console.log(`Total clients: ${progress.completedIds.length.toLocaleString()}`);
    console.log(`Output file: ${OUTPUT_FILE}`);
    // Clean up progress file on completion
    if (fs.existsSync(PROGRESS_FILE)) {
      fs.unlinkSync(PROGRESS_FILE);
    }
  }
}

async function runWithAutoRestart() {
  const maxRestarts = 10;
  let restarts = 0;

  while (restarts < maxRestarts && !shuttingDown) {
    try {
      await main();
      break; // Completed successfully or user stopped
    } catch (error) {
      restarts++;
      console.error(`\n=== PROCESS CRASHED (attempt ${restarts}/${maxRestarts}) ===`);
      console.error(error);

      if (restarts < maxRestarts && !shuttingDown) {
        const waitTime = 60000 * restarts; // 1min, 2min, 3min...
        console.log(`Restarting in ${waitTime / 1000} seconds...\n`);
        await sleep(waitTime);
      }
    }
  }

  if (restarts >= maxRestarts) {
    console.error(`\nMax restarts (${maxRestarts}) exceeded. Please check the error and restart manually.`);
    process.exit(1);
  }
}

runWithAutoRestart();
