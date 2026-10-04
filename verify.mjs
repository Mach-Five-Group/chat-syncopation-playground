/**
 * The audit the jsdom suite cannot do.
 *
 * jsdom has no layout engine and no computed style, so it is structurally blind
 * to contrast, overflow, stacking and scroll position. Every visual defect in
 * this family of packages was found here, while the unit suite stayed green.
 *
 *   npm run build && npm run preview     # then, in another shell:
 *   npm run verify
 */
import { chromium } from 'playwright';

const URL_UNDER_TEST = process.env.PLAYGROUND_URL ?? 'http://localhost:4173/';
const AA = 4.5;        // WCAG AA, normal text
const AA_LARGE = 3.0;  // WCAG AA, large text and UI components

/* ------------------------------------------------- contrast, in the page */

/** Injected: WCAG relative luminance and contrast ratio. */
const CONTRAST_HELPERS = () => {
  const parse = (value) => {
    const m = value.match(/rgba?\(([^)]+)\)/);
    if (!m) return null;
    const parts = m[1].split(/[,/\s]+/).filter(Boolean).map(Number);
    return { r: parts[0], g: parts[1], b: parts[2], a: parts[3] ?? 1 };
  };

  const luminance = ({ r, g, b }) => {
    const channel = (c) => {
      const s = c / 255;
      return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
    };
    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
  };

  /** Walks up (through shadow hosts) for the first element that paints. */
  const backdrop = (element) => {
    let node = element;
    while (node) {
      const colour = parse(getComputedStyle(node).backgroundColor);
      if (colour && colour.a > 0) return colour;
      node = node.parentElement ?? node.getRootNode()?.host ?? null;
    }
    return { r: 255, g: 255, b: 255, a: 1 };
  };

  window.__contrast = (element) => {
    const fg = parse(getComputedStyle(element).color);
    const bg = backdrop(element);
    if (!fg || !bg) return null;
    const [a, b] = [luminance(fg), luminance(bg)].sort((x, y) => y - x);
    return (a + 0.05) / (b + 0.05);
  };

  /** Resolves a path like "machvive-...-canvas >> .bubble" across shadow roots. */
  window.__pierce = (path) => {
    const parts = path.split('>>').map((s) => s.trim());
    let root = document;
    let element = null;
    for (const part of parts) {
      element = root.querySelector(part);
      if (!element) return null;
      root = element.shadowRoot ?? element;
    }
    return element;
  };
};

/* --------------------------------------------------------- what to check */

const ROLES = '#roles-services machvive-chat-syncopation-canvas';
const INSTR = '#instrument-services';

/**
 * Paths are section-scoped on purpose. An unscoped selector finds the *first*
 * matching element, which here is the empty minimal surface — so the audit
 * reported "not present" for every bubble while claiming to check them.
 */
const TARGETS = [
  { label: 'user bubble',       path: `${ROLES} >> li[data-role="user"] .bubble`,           min: AA },
  { label: 'reply bubble',      path: `${ROLES} >> li[data-role="assistant"] .bubble`,      min: AA },
  { label: 'system bubble',     path: `${ROLES} >> li[data-role="system"] .bubble`,         min: AA },
  { label: 'tool bubble',       path: `${ROLES} >> li[data-role="tool"] .bubble`,           min: AA },
  { label: 'error bubble',      path: `${ROLES} >> li[data-status="error"] .bubble`,        min: AA },
  { label: 'empty-state note',  path: 'machvive-chat-syncopation-canvas >> .empty',         min: AA },
  { label: 'composer textarea', path: 'machvive-chat-syncopation-prompt >> textarea',       min: AA },
  { label: 'send button',       path: 'machvive-chat-syncopation-prompt >> button',         min: AA_LARGE },
  { label: 'nudge chip',        path: 'machvive-chat-syncopation-nudge >> button',          min: AA },
  { label: 'cli input',         path: 'machvive-chat-syncopation-cli >> input',             min: AA },
  { label: 'cli output',        path: 'machvive-chat-syncopation-cli >> .out',              min: AA },
  { label: 'voice button',      path: 'machvive-chat-syncopation-voice >> .mic',            min: AA_LARGE },
  { label: 'voice note',        path: 'machvive-chat-syncopation-voice >> .note',           min: AA },
  { label: 'inspector heading', path: `${INSTR} machvive-chat-syncopation-inspector >> h2`,          min: AA },
  { label: 'inspector topic',   path: `${INSTR} machvive-chat-syncopation-inspector >> .topic`,      min: AA },
  { label: 'inspector detail',  path: `${INSTR} machvive-chat-syncopation-inspector >> .detail`,     min: AA },
  { label: 'inspector time',    path: `${INSTR} machvive-chat-syncopation-inspector >> time`,        min: AA },
  { label: 'inspector button',  path: `${INSTR} machvive-chat-syncopation-inspector >> .pause`,      min: AA_LARGE },
  { label: 'history title',     path: `${INSTR} machvive-chat-syncopation-history >> .title`,        min: AA },
  { label: 'history count',     path: `${INSTR} machvive-chat-syncopation-history >> .count`,        min: AA },
  { label: 'history note',      path: `${INSTR} machvive-chat-syncopation-history >> .note`,         min: AA },
  { label: 'history delete',    path: `${INSTR} machvive-chat-syncopation-history >> button.danger`, min: AA_LARGE },
  { label: 'page lede',         path: '.lede',                                              min: AA },
  { label: 'section note',      path: '.note',                                              min: AA }
];

/** Produces the states the audit needs: bus traffic, a stored conversation. */
const SEED = async () => {
  const instruments = document.querySelector('#instrument-services');
  instruments.conversation.add({ role: 'user', text: 'a stored question' });
  instruments.conversation.add({ role: 'assistant', text: 'a stored answer' });
  await new Promise((r) => setTimeout(r, 150));
  await instruments.querySelector('machvive-chat-syncopation-history').refresh();
};

const COMBINATIONS = [];
for (const colorScheme of ['light', 'dark']) {
  for (const theme of [null, 'light', 'dark']) {
    COMBINATIONS.push({ colorScheme, theme });
  }
}

/* ------------------------------------------------------------------- run */

const browser = await chromium.launch({ channel: 'chrome' });
let failures = 0;
let checks = 0;

for (const { colorScheme, theme } of COMBINATIONS) {
  const context = await browser.newContext({ colorScheme, viewport: { width: 1280, height: 900 } });
  const page = await context.newPage();
  page.on('pageerror', (error) => {
    console.log(`  ✖ page error: ${error.message}`);
    failures++;
  });
  await page.goto(URL_UNDER_TEST, { waitUntil: 'networkidle' });
  await page.addInitScript(CONTRAST_HELPERS);
  await page.evaluate(CONTRAST_HELPERS);

  if (theme) {
    await page.selectOption('#theme', theme);
  }
  await page.evaluate(SEED);
  // Give the components a turn: they populate on connect and after a bus event.
  await page.waitForTimeout(300);

  const label = `OS ${colorScheme} / theme ${theme ?? 'auto'}`;
  console.log(`\n${label}`);

  const results = await page.evaluate((targets) => targets.map(({ label, path, min }) => {
    const element = window.__pierce(path);
    if (!element) return { label, missing: true };
    const ratio = window.__contrast(element);
    const style = getComputedStyle(element);
    return {
      label,
      min,
      ratio,
      color: style.color,
      background: style.backgroundColor,
      // Caught a real bug in the sibling package: a long name overflowing a
      // fixed column, invisible to any unit test.
      overflowX: element.scrollWidth - element.clientWidth,
      invisible: element.getClientRects().length === 0
    };
  }), TARGETS);

  for (const result of results) {
    if (result.missing) {
      failures++;
      console.log(`  ✖ ${result.label}: not present — the audit measured nothing`);
      continue;
    }
    checks++;
    const ok = result.ratio !== null && result.ratio >= result.min;
    const ratio = result.ratio === null ? 'n/a' : result.ratio.toFixed(2);
    if (!ok) {
      failures++;
      console.log(`  ✖ ${result.label}: ${ratio}:1 (needs ${result.min}) — ${result.color} on ${result.background}`);
    } else if (result.overflowX > 1) {
      failures++;
      console.log(`  ✖ ${result.label}: ${ratio}:1 but overflows by ${result.overflowX}px`);
    } else {
      console.log(`  ✔ ${result.label}: ${ratio}:1`);
    }
  }

  // Scroll-position behaviour, which jsdom cannot observe at all.
  const autoscroll = await page.evaluate(async () => {
    const canvas = document.querySelector('#stream-services machvive-chat-syncopation-canvas');
    const services = document.querySelector('#stream-services');
    // Poll for a settled scrollTop rather than sleeping a guessed interval: a
    // fixed wait here is what made this check pass against a hidden element.
    const settle = async () => {
      let last = -1;
      for (let i = 0; i < 40; i++) {
        await new Promise((r) => requestAnimationFrame(r));
        if (canvas.scrollTop === last) return canvas.scrollTop;
        last = canvas.scrollTop;
      }
      return canvas.scrollTop;
    };

    for (let i = 0; i < 30; i++) {
      services.conversation.add({ role: 'assistant', text: `filler turn ${i}` });
    }
    await settle();
    const atBottom = canvas.scrollHeight - canvas.scrollTop - canvas.clientHeight < 4;

    canvas.scrollTop = 0;
    await settle();
    services.conversation.add({ role: 'assistant', text: 'arrived while reading earlier context' });
    await settle();
    const stayedPut = canvas.scrollTop < 40;

    return { atBottom, stayedPut };
  });

  if (!autoscroll.atBottom) { failures++; console.log('  ✖ autoscroll: did not follow the bottom'); }
  else console.log('  ✔ autoscroll follows the bottom');
  if (!autoscroll.stayedPut) { failures++; console.log('  ✖ autoscroll: yanked the reader back down'); }
  else console.log('  ✔ autoscroll respects a reader who scrolled up');
  checks += 2;

  await page.screenshot({ path: `shot-${colorScheme}-${theme ?? 'auto'}.png`, fullPage: true });
  await context.close();
}

await browser.close();

console.log(`\n${checks} checks, ${failures} failure${failures === 1 ? '' : 's'}`);
process.exit(failures ? 1 : 0);
