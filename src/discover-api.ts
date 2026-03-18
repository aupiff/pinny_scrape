import 'dotenv/config';
import { chromium } from 'playwright';

async function discoverApi() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  // Log API requests to core.uniteus.io
  page.on('request', (request) => {
    const url = request.url();
    if (url.includes('core.uniteus.io') && !url.includes('.js') && !url.includes('.css')) {
      console.log(`\n[REQUEST] ${request.method()} ${url}`);
      const postData = request.postData();
      if (postData) {
        console.log('Body:', postData.slice(0, 1000));
      }
    }
  });

  page.on('response', async (response) => {
    const url = response.url();
    if (url.includes('core.uniteus.io/v1') && response.status() === 200) {
      console.log(`[RESPONSE] ${response.status()} ${url}`);
      try {
        const data = await response.json();
        if (data.meta) {
          console.log('Meta:', JSON.stringify(data.meta));
        }
        if (Array.isArray(data.data)) {
          console.log(`Data: Array of ${data.data.length} items`);
        }
      } catch {}
    }
  });

  const email = process.env.UNITEUS_EMAIL || '';
  const password = process.env.UNITEUS_PASSWORD || '';

  console.log('Logging in...');
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
  console.log('\nLogged in, navigating to clients list...\n');

  // Navigate to clients list and observe API calls
  await page.goto('https://app.uniteus.io/dashboard/clients/all');
  await page.waitForTimeout(10000);

  console.log('\nDone observing');
  await browser.close();
}

discoverApi().catch(console.error);
