// Render .work/report.html -> <repo-root>/REDIS_IO_WORKERS.pdf
// Requires Playwright Chromium: npx playwright install chromium
// (resolve playwright from NODE_PATH if not installed locally)
const path = require('path');
const { execSync } = require('child_process');

let chromium;
try {
  ({ chromium } = require('playwright'));
} catch {
  console.error('Playwright not found. Install with:  npm i playwright && npx playwright install chromium');
  process.exit(1);
}

(async () => {
  const here = __dirname;
  const work = process.env.WORK_DIR || path.join(here, '.work');
  const root = process.env.PROJECT_ROOT ||
    execSync('git -C "' + here + '" rev-parse --show-toplevel').toString().trim();
  const out = process.argv[2] || path.join(root, 'REDIS_IO_WORKERS.pdf');

  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto('file://' + path.join(work, 'report.html'), { waitUntil: 'networkidle' });
  await page.emulateMedia({ media: 'print' });
  await page.pdf({ path: out, format: 'A4', printBackground: true,
                   margin: { top: '0', bottom: '0', left: '0', right: '0' } });
  await browser.close();
  console.log('wrote ' + out);
})();
