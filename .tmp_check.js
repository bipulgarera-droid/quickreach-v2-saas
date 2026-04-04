const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  page.on('console', msg => console.log('BROWSER CONSOLE:', msg.text()));
  page.on('pageerror', err => console.log('BROWSER PAGE ERROR:', err.message));
  await page.route('**/*', route => route.continue());
  await page.goto("file://" + process.cwd() + "/public/dashboard.html");
  await new Promise(r => setTimeout(r, 2000));
  await browser.close();
})();
