const { chromium } = require('playwright');
const path = require('path');

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const input = process.argv[3] || 'summary.html';
  const file = 'file://' + path.resolve(__dirname, input);
  await page.goto(file, { waitUntil: 'networkidle' });
  await page.emulateMedia({ media: 'print' });
  const out = process.argv[2];
  await page.pdf({
    path: out,
    format: 'A4',
    printBackground: true,
    margin: { top: '0', bottom: '0', left: '0', right: '0' },
  });
  console.log('PDF written to ' + out);
  await browser.close();
})();
