import 'dotenv/config';
import { chromium, Browser, BrowserContext, Page } from 'playwright';
import axios, { AxiosInstance } from 'axios';
import * as fs from 'fs';

const API_BASE_URL = 'https://core.uniteus.io';

interface AuthInfo {
  token: string;
  employeeId: string;
  providerId: string;
}

interface ApiData {
  id: string;
  type: string;
  attributes: Record<string, unknown>;
  relationships?: Record<string, unknown>;
}

interface ApiResponse {
  data: ApiData | ApiData[];
  included?: ApiData[];
  meta?: {
    page?: {
      number?: number;
      size?: number;
      total_pages?: number;
      total_count?: number;
    };
  };
}

export class UniteUsScraper {
  private browser: Browser | null = null;
  private context: BrowserContext | null = null;
  private page: Page | null = null;
  private apiClient: AxiosInstance | null = null;
  private authInfo: AuthInfo | null = null;

  private email: string;
  private password: string;

  constructor(email: string, password: string) {
    this.email = email;
    this.password = password;
  }

  async init(): Promise<void> {
    this.browser = await chromium.launch({ headless: true });
    this.context = await this.browser.newContext();
    this.page = await this.context.newPage();
  }

  async login(): Promise<AuthInfo> {
    if (!this.page) throw new Error('Browser not initialized');

    let authToken = '';
    let employeeId = '';
    let providerId = '';

    this.page.on('response', async (response) => {
      if (response.url().includes('oauth2/token')) {
        try {
          const data = await response.json();
          if (data.access_token) authToken = data.access_token;
        } catch {}
      }
    });

    this.page.on('request', (request) => {
      const empId = request.headers()['x-employee-id'];
      if (empId) employeeId = empId;
      const url = request.url();
      const providerMatch = url.match(/filter%5Bclient_relationships\.provider%5D=([a-f0-9-]+)/);
      if (providerMatch) providerId = providerMatch[1];
    });

    console.log('Logging in...');
    await this.page.goto('https://app.auth.uniteus.io/');

    // Step 1: Enter email on Unite Us
    await this.page.waitForSelector('input[type="email"], input[name="user[email]"]', { timeout: 15000 });
    await this.page.fill('input[type="email"], input[name="user[email]"]', this.email);
    await this.page.click('button[type="submit"], input[type="submit"]');

    // Step 2: Fill credentials on NYC.ID SAML login
    await this.page.waitForURL('**nyc.gov**', { timeout: 15000 });
    await this.page.waitForSelector('#gigya-loginID', { timeout: 15000 });
    await this.page.fill('#gigya-loginID', this.email);
    await this.page.fill('#gigya-password', this.password);
    await this.page.click('input[type="submit"]');

    // Step 3: Select PHS group
    await this.page.waitForSelector('text=Stand Out Care Corp - SCN - PHS', { timeout: 30000 });
    await this.page.click('text=Stand Out Care Corp - SCN - PHS');

    await this.page.waitForURL('**/dashboard/**', { timeout: 60000 });
    await this.page.waitForTimeout(2000);

    await this.page.goto('https://app.uniteus.io/dashboard/clients/all', { waitUntil: 'domcontentloaded' });
    await this.page.waitForTimeout(5000);

    if (!authToken) throw new Error('Failed to capture auth token');
    if (!providerId) providerId = 'e15af87f-3d21-4cbb-b5b0-c3c637a5dd8e';

    this.authInfo = { token: authToken, employeeId, providerId };
    console.log('Login successful!');

    this.apiClient = axios.create({
      baseURL: API_BASE_URL,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': `Bearer ${authToken}`,
        'x-employee-id': employeeId,
        'x-application-source': 'web',
      },
    });

    return this.authInfo;
  }

  async listClients(pageNum = 1, pageSize = 50): Promise<ApiResponse> {
    if (!this.apiClient || !this.authInfo) throw new Error('Not authenticated');
    const response = await this.apiClient.get('/v1/people', {
      params: {
        'filter[client_relationships.provider]': this.authInfo.providerId,
        'sort': 'last_name,first_name',
        'page[number]': pageNum,
        'page[size]': pageSize,
        'include': 'addresses',
      },
    });
    return response.data;
  }

  async getPersonFull(id: string): Promise<{ person: ApiData; addresses: ApiData[]; insurances: ApiData[]; languages: ApiData[] }> {
    if (!this.apiClient) throw new Error('Not authenticated');

    // Fetch all 3 endpoints in parallel
    const [personRes, insuranceRes, langRes] = await Promise.all([
      this.apiClient.get(`/v1/people/${id}`, { params: { include: 'addresses' } }),
      this.apiClient.get('/v1/insurances', { params: { 'filter[person]': id } }),
      this.apiClient.get('/v1/record_languages', {
        params: { 'filter[record_id]': id, 'filter[record_type]': 'Person' },
      }),
    ]);

    const person = personRes.data.data;
    const addresses = personRes.data.included || [];
    const insurances = Array.isArray(insuranceRes.data.data) ? insuranceRes.data.data : [];
    const languages = Array.isArray(langRes.data.data) ? langRes.data.data : [];

    return { person, addresses, insurances, languages };
  }

  async close(): Promise<void> {
    if (this.browser) await this.browser.close();
  }
}

function flattenClient(
  person: ApiData,
  addresses: ApiData[],
  insurances: ApiData[],
  languages: ApiData[]
): Record<string, string> {
  const flat: Record<string, string> = { id: person.id };
  const attrs = person.attributes || {};

  // Basic person info
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

  // Primary address
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

  // Primary insurance
  const activeInsurance = insurances.find((i: any) => i.attributes?.state === 'active') || insurances[0];
  if (activeInsurance) {
    const ins = activeInsurance.attributes || {};
    flat['insurance_member_id'] = String(ins.external_member_id || '');
    flat['insurance_group_id'] = String(ins.external_group_id || '');
    flat['insurance_status'] = String(ins.state || '');
    flat['insurance_enrolled_at'] = String(ins.enrolled_at || '');
    flat['insurance_expired_at'] = String(ins.expired_at || '');
  }

  // Languages (spoken/written)
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

function writeCsv(filename: string, data: Record<string, string>[]): void {
  if (data.length === 0) return;

  // Ordered columns for better readability
  const priorityKeys = [
    'id', 'first_name', 'last_name', 'middle_name', 'date_of_birth',
    'gender', 'race', 'ethnicity', 'spoken_languages', 'written_languages',
    'phone', 'phone_type', 'email',
    'address_line_1', 'address_line_2', 'address_city', 'address_state',
    'address_postal_code', 'address_county', 'address_type',
    'insurance_member_id', 'insurance_group_id', 'insurance_status',
    'insurance_enrolled_at', 'insurance_expired_at',
  ];

  const allKeys = [...new Set(data.flatMap(d => Object.keys(d)))];
  const keys = [
    ...priorityKeys.filter(k => allKeys.includes(k)),
    ...allKeys.filter(k => !priorityKeys.includes(k)),
  ];

  const header = keys.join(',');
  const rows = data.map(row =>
    keys.map(k => {
      const val = row[k] || '';
      if (val.includes(',') || val.includes('"') || val.includes('\n')) {
        return `"${val.replace(/"/g, '""')}"`;
      }
      return val;
    }).join(',')
  );

  fs.writeFileSync(filename, [header, ...rows].join('\n'));
  console.log(`Wrote ${data.length} rows to ${filename}`);
}

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function withRetry<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  delayMs: number = 1000
): Promise<T> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const status = error.response?.status;
        if (status && [502, 503, 504, 429].includes(status) && attempt < maxRetries) {
          console.log(`  Retry ${attempt}/${maxRetries} after ${status}...`);
          await sleep(delayMs * attempt);
          continue;
        }
      }
      throw error;
    }
  }
  throw new Error('Max retries exceeded');
}

async function fetchClientBatch(
  scraper: UniteUsScraper,
  clientIds: string[],
  concurrency: number = 10
): Promise<Record<string, string>[]> {
  const results: Record<string, string>[] = [];
  let completed = 0;
  const total = clientIds.length;

  // Process in batches
  for (let i = 0; i < clientIds.length; i += concurrency) {
    const batch = clientIds.slice(i, i + concurrency);
    const promises = batch.map(async (clientId) => {
      try {
        const data = await withRetry(() => scraper.getPersonFull(clientId));
        return flattenClient(data.person, data.addresses, data.insurances, data.languages);
      } catch (error) {
        if (axios.isAxiosError(error)) {
          console.error(`Error fetching ${clientId}: ${error.response?.status}`);
        }
        return null;
      }
    });

    const batchResults = await Promise.all(promises);
    for (const result of batchResults) {
      if (result) results.push(result);
      completed++;
    }
    console.log(`Progress: ${completed}/${total} (${Math.round(completed/total*100)}%)`);

    // No delay - only sleep on rate limit (handled by withRetry)
  }

  return results;
}

async function main() {
  const email = process.env.UNITEUS_EMAIL || '';
  const password = process.env.UNITEUS_PASSWORD || '';

  if (!email || !password) {
    console.error('Set UNITEUS_EMAIL and UNITEUS_PASSWORD in .env');
    process.exit(1);
  }

  const scraper = new UniteUsScraper(email, password);
  const targetCount = 1000;
  const concurrency = 30; // Parallel requests
  const totalClients = 2317038; // Total on site

  try {
    await scraper.init();
    await scraper.login();

    console.log(`\nFetching ${targetCount} clients with full details (concurrency: ${concurrency})...\n`);

    // Fetch client IDs in pages (larger page size, parallel fetching)
    const allClientIds: string[] = [];
    const pageSize = 100; // Larger pages = fewer requests
    const pagesNeeded = Math.ceil(targetCount / pageSize);
    const pageConcurrency = 5; // Fetch pages in parallel

    const listStartTime = Date.now();
    for (let i = 0; i < pagesNeeded; i += pageConcurrency) {
      const pageNums = Array.from(
        { length: Math.min(pageConcurrency, pagesNeeded - i) },
        (_, j) => i + j + 1
      );
      const pagePromises = pageNums.map(page =>
        withRetry(() => scraper.listClients(page, pageSize))
      );
      const results = await Promise.all(pagePromises);
      for (const listRes of results) {
        const clientList = Array.isArray(listRes.data) ? listRes.data : [listRes.data];
        allClientIds.push(...clientList.map(c => c.id));
      }
      console.log(`Fetched pages ${pageNums[0]}-${pageNums[pageNums.length - 1]} of ${pagesNeeded}...`);
    }
    const listTime = (Date.now() - listStartTime) / 1000;

    const clientIds = allClientIds.slice(0, targetCount);
    console.log(`\nFetching full details for ${clientIds.length} clients...\n`);

    const detailStartTime = Date.now();
    const allClients = await fetchClientBatch(scraper, clientIds, concurrency);
    const detailTime = (Date.now() - detailStartTime) / 1000;

    if (allClients.length > 0) {
      writeCsv('clients_1000.csv', allClients);
    }

    // Benchmark report
    const totalTime = listTime + detailTime;
    const clientsPerSecond = allClients.length / detailTime;
    const estimatedFullTime = totalClients / clientsPerSecond;
    const estimatedHours = estimatedFullTime / 3600;

    console.log('\n=== BENCHMARK REPORT ===');
    console.log(`Clients fetched: ${allClients.length}`);
    console.log(`List fetch time: ${listTime.toFixed(1)}s`);
    console.log(`Detail fetch time: ${detailTime.toFixed(1)}s`);
    console.log(`Total time: ${totalTime.toFixed(1)}s`);
    console.log(`Rate: ${clientsPerSecond.toFixed(2)} clients/sec`);
    console.log(`\nEstimate for ${totalClients.toLocaleString()} clients:`);
    console.log(`  ${estimatedHours.toFixed(1)} hours (${(estimatedHours/24).toFixed(1)} days)`);

  } finally {
    await scraper.close();
  }
}

main();
