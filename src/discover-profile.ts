import 'dotenv/config';
import { chromium } from 'playwright';
import axios from 'axios';

async function discoverProfile() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  let authToken = '';
  let employeeId = '';

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
  });

  const email = process.env.UNITEUS_EMAIL || '';
  const password = process.env.UNITEUS_PASSWORD || '';

  console.log('Logging in...');
  await page.goto('https://app.auth.uniteus.io/');
  await page.waitForSelector('input[type="email"], input[name="user[email]"]', { timeout: 15000 });
  await page.fill('input[type="email"], input[name="user[email]"]', email);
  await page.click('button[type="submit"], input[type="submit"]');
  await page.waitForSelector('input[type="password"]', { timeout: 15000 });
  await page.fill('input[type="password"]', password);
  await page.click('button[type="submit"], input[type="submit"]');
  await page.waitForURL('**/dashboard/**', { timeout: 60000 });
  await page.goto('https://app.uniteus.io/dashboard/clients/all');
  await page.waitForTimeout(3000);
  await browser.close();

  const client = axios.create({
    baseURL: 'https://core.uniteus.io',
    headers: {
      'Authorization': `Bearer ${authToken}`,
      'x-employee-id': employeeId,
      'x-application-source': 'web',
      'Accept': 'application/json',
    },
  });

  const clientId = 'fe5d8fc0-b48a-40ed-8ef1-a5e6e7f3496f';

  console.log('\n=== Record languages for client ===\n');
  const res = await client.get('/v1/record_languages', {
    params: {
      'filter[record_id]': clientId,
      'filter[record_type]': 'Person',
    },
  });
  console.log(JSON.stringify(res.data, null, 2));
}

discoverProfile().catch(console.error);
