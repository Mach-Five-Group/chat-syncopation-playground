import '@machfivetechchicago/machvive-chat-syncopation-ai';
import { createRecord, META } from '@machfivetechchicago/machvive-chat-syncopation-ai';

const $ = (selector) => document.querySelector(selector);
const surfaces = () => [...document.querySelectorAll('machvive-chat-syncopation')];

/* ---------------------------------------------------------------- theme */

$('#theme').addEventListener('change', (event) => {
  const value = event.target.value;
  for (const surface of surfaces()) {
    // Surface B declares its own theme as a demonstration; leave it alone.
    if (surface.dataset.lockTheme) continue;
    if (value) surface.setAttribute('theme', value);
    else surface.removeAttribute('theme');
  }
});
// Mark the deliberately-dark one so the picker does not override the point it makes.
document.querySelectorAll('machvive-chat-syncopation[theme]').forEach((el) => {
  el.dataset.lockTheme = 'true';
});

/* --------------------------------------------------------------- tokens */

const applyToken = (name, value) => document.documentElement.style.setProperty(name, value);

$('#accent').addEventListener('input', (e) => applyToken('--mcs-accent', e.target.value));
$('#radius').addEventListener('input', (e) => applyToken('--mcs-radius', `${e.target.value}px`));
$('#reset').addEventListener('click', () => {
  document.documentElement.removeAttribute('style');
  $('#accent').value = '#1565c0';
  $('#radius').value = '12';
  $('#theme').value = '';
  $('#theme').dispatchEvent(new Event('change'));
});

/* ------------------------------------------------------ a real transport */

import { loremIpsum, loremSentence } from '@machfivetechchicago/machvive-webmcp-ai';

/**
 * A mock model.
 *
 * Replies vary in shape the way real ones do — a one-line acknowledgement, a
 * paragraph, occasionally three. That matters more than it sounds: a transcript
 * of uniformly-sized bubbles makes autoscroll, wrapping and the pending caret
 * all look fine, and none of them are being tested.
 *
 * English business-speak rather than Latin, because that is the register real
 * product copy is written in and its word lengths are what break a column.
 */
function mockReply() {
  const roll = Math.random();
  if (roll < 0.2) return loremSentence({ lang: 'english', minWords: 4, maxWords: 9 });
  if (roll < 0.75) return loremIpsum({ lang: 'english', sentences: -1 });
  return loremIpsum({ lang: 'english', sentences: 3, paragraphs: 2 + Math.floor(Math.random() * 2) });
}

/**
 * A transport, registered the supported way.
 *
 * Earlier this page intercepted prompt-submit and added records itself, which
 * produced two replies per message — the mock's and the daemon's — because the
 * composer sends regardless of who listens. Registering is both correct and
 * shorter, and it keeps the busy guard, stop() and daemon:idle working, which is
 * what turns the Stop button back into Send.
 */
async function* mockTransport(prompt) {
  // Split on whitespace but keep it, so separators stream too and the text never
  // reflows as a word completes.
  for (const token of mockReply(prompt).split(/(\s+)/)) {
    // A longer beat after a sentence ends. Uniform timing reads as a progress
    // bar; varied timing reads as something composing an answer.
    await new Promise((r) => setTimeout(r, /[.!?]$/.test(token) ? 110 : 16 + Math.random() * 22));
    yield token;
  }
}

const streamServices = $('#stream-services');

streamServices.registerTransport('lorem', mockTransport);

$('#transport').addEventListener('change', (event) => {
  // Every surface on the page follows the picker; they each have their own
  // services element, so each needs the transport registered on it.
  for (const services of document.querySelectorAll('machvive-chat-syncopation-services')) {
    if (event.target.value === 'lorem') services.registerTransport('lorem', mockTransport);
    else services.config.transport = 'echo';
  }
});

// Seed a conversation so the transcript has something to scroll through. An
// empty box demonstrates the nudge; a full one demonstrates everything else.
for (const prompt of [
  'What does this collection actually give me?',
  'How would I wire it to a real model?',
  'And if I want it to run offline?'
]) {
  streamServices.conversation.add({ role: 'user', text: prompt });
  streamServices.conversation.add({
    role: 'assistant',
    // Seeded, so the opening transcript is identical on every load and a visual
    // diff of this page stays meaningful.
    text: loremIpsum({ lang: 'english', sentences: 3, seed: prompt.length }),
    meta: { [META.SOURCE]: 'mock' }
  });
}

streamServices.bus.on('daemon:idle', () => { streamServices.__stopped = true; });

/* ------------------------------------------------------------------ CLI */

const cliServices = $('#cli-services');
customElements.whenDefined('machvive-chat-syncopation-cli').then(() => {
  const cli = cliServices.querySelector('machvive-chat-syncopation-cli');
  // Registered from page code — domain verbs without forking the component.
  cli.register('order', {
    describe: 'look up an order by number',
    run: (args) => {
      const id = args.trim();
      if (!id) return 'usage: /order <number>';
      cliServices.conversation.add({
        role: 'tool',
        text: `order ${id}: shipped 2 Oct, arriving 7 Oct`
      });
      return `looked up order ${id}`;
    }
  });
  cli.register('inspect', {
    describe: 'emit a custom bus event the inspector will show',
    run: () => {
      cliServices.bus.emit('playground:custom', { from: 'the CLI' });
      return 'emitted playground:custom';
    }
  });
});

/* --------------------------------------------------- roles and statuses */

const rolesServices = $('#roles-services');
await customElements.whenDefined('machvive-chat-syncopation-canvas');

const SAMPLES = [
  { role: 'system', text: 'Session started. Cart contains 2 items.' },
  { role: 'user', text: 'Can you summarise what I have in the cart?' },
  { role: 'assistant', text: 'Two items: a 12-cup kettle and a pack of filters.\n\nThe kettle ships today; the filters are backordered until Friday.' },
  { role: 'tool', text: 'cart.read() → { items: 2, total: 48.20 }' },
  { role: 'assistant', text: 'Still typing, as it happens', status: 'pending' },
  { role: 'assistant', text: 'The model endpoint returned 503.', status: 'error', meta: { [META.ERROR]: 'service unavailable' } },
  { role: 'assistant', text: '<img src=x onerror=alert(1)>' }
];

for (const sample of SAMPLES) rolesServices.conversation.add(createRecord(sample));

/* --------------------------------------------- a custom bus subscriber */

const instruments = $('#instrument-services');
instruments.bus.on('record:added', (record) => {
  // Shows up in the inspector beside the built-in topics, which is the point:
  // the wildcard subscription sees topics it was never taught.
  instruments.bus.emit('playground:counted', { role: record.role, length: record.text.length });
});

/* ----------------------------------------------------- deep-link anchors */

for (const heading of document.querySelectorAll('main section h2')) {
  const id = heading.closest('section').id;
  heading.insertAdjacentHTML('afterbegin', `<a class="anchor" href="#${id}" aria-label="Link to this section">#</a>`);
}
