(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const r of document.querySelectorAll('link[rel="modulepreload"]'))s(r);new MutationObserver(r=>{for(const i of r)if(i.type==="childList")for(const n of i.addedNodes)n.tagName==="LINK"&&n.rel==="modulepreload"&&s(n)}).observe(document,{childList:!0,subtree:!0});function t(r){const i={};return r.integrity&&(i.integrity=r.integrity),r.referrerPolicy&&(i.referrerPolicy=r.referrerPolicy),r.crossOrigin==="use-credentials"?i.credentials="include":r.crossOrigin==="anonymous"?i.credentials="omit":i.credentials="same-origin",i}function s(r){if(r.ep)return;r.ep=!0;const i=t(r);fetch(r.href,i)}})();class q{#e=new Map;on(e,t){if(typeof t!="function")return()=>{};const s=this.#e.get(e)??new Set;return s.add(t),this.#e.set(e,s),()=>s.delete(t)}once(e,t){const s=this.on(e,r=>{s(),t(r)});return s}emit(e,t){for(const s of this.#e.get(e)??[])try{s(t)}catch(r){console.warn(`[syncopation] "${e}" subscriber failed:`,r)}for(const s of this.#e.get("*")??[])try{s({topic:e,payload:t})}catch{}}get topics(){return[...this.#e.keys()]}}let D=0;const H=()=>`m-${Date.now().toString(36)}-${(D++).toString(36)}`,S=Object.freeze(["user","assistant","system","tool"]);function M({role:o="user",text:e="",...t}={}){if(!S.includes(o))throw new TypeError(`record.role must be one of ${S.join(", ")}`);return{id:H(),role:o,text:e,at:new Date().toISOString(),status:"complete",meta:{},...t}}class P{#e=[];#s;#t;constructor({bus:e,maxTurns:t=200,id:s}={}){this.#s=e,this.#t=t,this.id=s??`c-${Date.now().toString(36)}`}get records(){return[...this.#e]}get length(){return this.#e.length}get last(){return this.#e.at(-1)??null}add(e){const t=e?.id?e:M(e);return this.#e.push(t),this.#e.length>this.#t&&this.#e.splice(0,this.#e.length-this.#t),this.#s?.emit("record:added",t),t}append(e,t){const s=this.#e.find(r=>r.id===e);return s?(s.text+=t,this.#s?.emit("record:appended",{id:e,chunk:t,record:s}),s):null}update(e,t){const s=this.#e.find(r=>r.id===e);return s?(Object.assign(s,t),this.#s?.emit("record:updated",s),s):null}clear(){this.#e=[],this.#s?.emit("conversation:cleared",{id:this.id})}toJSON(){return{id:this.id,records:this.#e}}}const d=Object.freeze({TOKENS:"mcs:tokens",LATENCY:"mcs:latencyMs",MODEL:"mcs:model",SOURCE:"mcs:source",ERROR:"mcs:error",CITATIONS:"mcs:citations",TOOL_CALLS:"mcs:toolCalls"}),x={async*echo(o){for(const e of`You said: ${o}`.split(" "))await new Promise(t=>setTimeout(t,40)),yield e+" "},async*local(o,{config:e}){yield`[local model "${e.model||"unset"}" not yet wired] ${o}`},async*remote(o,{config:e}){yield`[remote endpoint "${e.endpoint||"unset"}" not yet wired] ${o}`}};class j{#e;#s;#t;#o=null;constructor({bus:e,conversation:t,config:s}){this.#e=e,this.#s=t,this.#t=s}get busy(){return this.#o!==null}stop(){this.#o?.abort(),this.#o=null}async send(e){if(this.busy)return null;this.#s.add({role:"user",text:e});const t=this.#s.add({role:"assistant",text:"",status:"pending",meta:{[d.SOURCE]:this.#t.transport,[d.MODEL]:this.#t.model}}),s=x[this.#t.transport]??x.echo;this.#o=new AbortController;const r=Date.now();try{for await(const i of s(e,{config:this.#t})){if(this.#o.signal.aborted)break;this.#s.append(t.id,i)}this.#s.update(t.id,{status:"complete",meta:{...t.meta,[d.LATENCY]:Date.now()-r}})}catch(i){this.#s.update(t.id,{status:"error",meta:{...t.meta,[d.ERROR]:String(i?.message??i)}}),this.#e?.emit("daemon:error",{id:t.id,error:i})}finally{this.#o=null,this.#e?.emit("daemon:idle",{id:t.id})}return t}}const k=Object.keys(x),z="machvive-chat-syncopation",b="conversations",I=1;class _{#e=null;#s=new Map;get available(){return!!globalThis.indexedDB}#t(){return this.available?(this.#e??=new Promise(e=>{let t;try{t=globalThis.indexedDB.open(z,I)}catch{return e(null)}t.onupgradeneeded=()=>{const s=t.result;s.objectStoreNames.contains(b)||s.createObjectStore(b,{keyPath:"id"})},t.onsuccess=()=>e(t.result),t.onerror=t.onblocked=()=>e(null)}),this.#e):Promise.resolve(null)}async#o(e,t){const s=await this.#t();return s?new Promise(r=>{let i;try{i=s.transaction(b,e)}catch{return r(null)}const n=t(i.objectStore(b));i.oncomplete=()=>r(n?n.result:null),i.onerror=i.onabort=()=>r(null)}):null}async put(e){return this.#s.set(e.id,e),await this.#o("readwrite",t=>t.put(e)),e}async get(e){return await this.#o("readonly",s=>s.get(e))??this.#s.get(e)??null}async list(){const e=await this.#o("readonly",t=>t.getAll());return e?.length?e:[...this.#s.values()]}async remove(e){this.#s.delete(e),await this.#o("readwrite",t=>t.delete(e))}async clear(){this.#s.clear(),await this.#o("readwrite",e=>e.clear())}}const C=Object.freeze({transport:"echo",model:"",endpoint:"",persist:!1,maxTurns:200,streaming:!0,locale:void 0}),L={persist:o=>o!=="false",streaming:o=>o!=="false",maxTurns:o=>Number(o)};function U(o,e={}){const t={};for(const s of Object.keys(C)){const r=s.replace(/[A-Z]/g,i=>`-${i.toLowerCase()}`);if(o?.hasAttribute?.(r)){const i=o.getAttribute(r);t[s]=L[s]?L[s](i):i}}return{...C,...t,...e}}const R=`
    --mcs-fg: #1a1a1a;
    --mcs-muted: #646464;
    --mcs-bg: #ffffff;
    --mcs-surface: #f6f7f9;
    --mcs-user-bg: #e8f0fe;
    --mcs-assistant-bg: #f3f4f6;
    --mcs-border: #e2e4e8;
    --mcs-accent: #1565c0;
    --mcs-accent-fg: #ffffff;
    --mcs-danger: #a4161a;
    --mcs-radius: 12px;
    --mcs-font: system-ui, -apple-system, sans-serif;
    --mcs-mono: ui-monospace, SFMono-Regular, Menlo, monospace;
`,T=`
    --mcs-fg: #e8eaed;
    --mcs-muted: #a6acb3;
    --mcs-bg: #1f2125;
    --mcs-surface: #282b30;
    --mcs-user-bg: #1e3a5f;
    --mcs-assistant-bg: #2b2f36;
    --mcs-border: #3c4046;
    --mcs-accent: #5b9bf8;
    --mcs-accent-fg: #0b1220;
    --mcs-danger: #ff9d97;
`,l=`
  :host {
    color-scheme: light dark;
${R}  }
  @media (prefers-color-scheme: dark) {
    :host(:not([theme="light"])) {
${T}    }
  }
  :host([theme="dark"]) {
${T}  }
  :host([theme="light"]) {
    color-scheme: light;
${R}  }
`,u=`
  button, input, textarea, select {
    font: inherit;
    color: var(--mcs-fg);
    background: var(--mcs-bg);
    border: 1px solid var(--mcs-border);
    border-radius: 8px;
  }
`,y="machvive-chat-syncopation-services";function E(o){let e=o;for(;e;){if(e.localName===y)return e;for(const t of e.children??[])if(t.localName===y)return t;e=e.parentElement??e.getRootNode?.()?.host??null}return document.querySelector(y)}function h(o,{timeoutMs:e=2e3}={}){const t=E(o);return t?.bus?Promise.resolve(t):new Promise(s=>{const r=Date.now()+e,i=()=>{const n=E(o);if(n?.bus)return s(n);if(Date.now()>r)return s(null);setTimeout(i,16)};i()})}class B extends HTMLElement{#e=new q;#s=new _;#t=null;#o=null;#r=null;constructor(){super(),this.attachShadow({mode:"open"})}connectedCallback(){this.shadowRoot.innerHTML="<style>:host { display: contents; }</style><slot></slot>",this.#t=U(this),this.#o=new P({bus:this.#e,maxTurns:this.#t.maxTurns}),this.#r=new j({bus:this.#e,conversation:this.#o,config:this.#t}),this.#t.persist&&(this.#e.on("record:added",()=>this.#s.put(this.#o.toJSON())),this.#e.on("record:updated",()=>this.#s.put(this.#o.toJSON()))),this.#e.emit("services:ready",{config:this.#t}),this.dispatchEvent(new CustomEvent("services-ready",{bubbles:!0,composed:!0}))}get bus(){return this.#e}get conversation(){return this.#o}get daemon(){return this.#r}get cache(){return this.#s}get config(){return this.#t}send(e){return this.#r?.send(e)}}customElements.get("machvive-chat-syncopation-services")||customElements.define("machvive-chat-syncopation-services",B);const F=["transport","model","endpoint","persist","max-turns","streaming"];class J extends HTMLElement{#e=null;constructor(){super(),this.attachShadow({mode:"open"})}connectedCallback(){this.#e=E(this)??this.#s(),this.shadowRoot.innerHTML=`
      <style>
${l}
        :host {
          display: flex;
          flex-direction: column;
          /* A chat surface needs a bounded height or the transcript grows the
             page and the prompt walks off the bottom of the viewport. */
          height: var(--mcs-height, 32rem);
          max-height: 100%;
          font-family: var(--mcs-font);
          color: var(--mcs-fg);
          background: var(--mcs-bg);
          border: 1px solid var(--mcs-border);
          border-radius: var(--mcs-radius);
          overflow: hidden;
        }
        header, footer { flex: 0 0 auto; }
        .body { flex: 1 1 auto; min-height: 0; display: flex; flex-direction: column; }
        header::slotted(*) { display: block; }
      </style>
      <header><slot name="header"></slot></header>
      <div class="body"><slot></slot></div>
      <footer><slot name="footer"></slot></footer>
    `}#s(){const e=document.createElement(y);for(const t of F)this.hasAttribute(t)&&e.setAttribute(t,this.getAttribute(t));return this.prepend(e),e}get services(){return this.#e}get conversation(){return this.#e?.conversation??null}send(e){return this.#e?.send(e)}}customElements.get("machvive-chat-syncopation")||customElements.define("machvive-chat-syncopation",J);const K=48;class Y extends HTMLElement{#e=[];#s=new Map;#t=null;#o=!0;constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){this.shadowRoot.innerHTML=`
      <style>
${l}
        :host {
          display: block;
          flex: 1 1 auto;
          min-height: 0;
          overflow-y: auto;
          padding: 0.75rem;
          font-family: var(--mcs-font);
          color: var(--mcs-fg);
          background: var(--mcs-bg);
          /* Deliberately NOT scroll-behavior: smooth. Autoscroll here fires on
             every streamed chunk, and an animated scroll means the view
             perpetually lags the newest text — while scrollTop reads taken
             mid-animation are unreliable, which breaks the very pinned-to-bottom
             detection that keeps us from yanking a reader around. Measured in
             Chrome: setting scrollTop = 0 under smooth settled at 35, not 0. */
        }
        ol { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 0.5rem; }
        li { display: flex; }
        li[data-role="user"] { justify-content: flex-end; }
        .bubble {
          max-width: min(42rem, 85%);
          padding: 0.5rem 0.75rem;
          border-radius: var(--mcs-radius);
          background: var(--mcs-assistant-bg);
          /* Model output contains newlines and runs of spaces that carry
             meaning; collapsing them turns a list into a paragraph. */
          white-space: pre-wrap;
          overflow-wrap: anywhere;
          line-height: 1.45;
        }
        li[data-role="user"] .bubble { background: var(--mcs-user-bg); }
        li[data-role="system"] .bubble,
        li[data-role="tool"] .bubble {
          background: var(--mcs-surface);
          color: var(--mcs-muted);
          font-family: var(--mcs-mono);
          font-size: 0.875rem;
        }
        li[data-status="error"] .bubble { color: var(--mcs-danger); border: 1px solid var(--mcs-danger); }
        li[data-status="pending"] .bubble::after {
          content: '▋';
          color: var(--mcs-muted);
          animation: blink 1s steps(2, start) infinite;
        }
        @keyframes blink { to { visibility: hidden; } }
        @media (prefers-reduced-motion: reduce) {
          li[data-status="pending"] .bubble::after { animation: none; }
        }
        .empty { color: var(--mcs-muted); font-size: 0.9375rem; padding: 0.5rem; }
      </style>
      <ol role="log" aria-live="polite" aria-relevant="additions text"></ol>
      <p class="empty" hidden>No messages yet.</p>
    `,this.#t=this.shadowRoot.querySelector("ol"),this.addEventListener("scroll",this.#r,{passive:!0});const e=await h(this);if(!(!e||!this.isConnected)){for(const t of e.conversation.records)this.#i(t);this.#e=[e.bus.on("record:added",t=>this.#i(t)),e.bus.on("record:appended",({record:t})=>this.#i(t)),e.bus.on("record:updated",t=>this.#i(t)),e.bus.on("conversation:cleared",()=>this.#a())],this.#n()}}disconnectedCallback(){this.removeEventListener("scroll",this.#r);for(const e of this.#e)e();this.#e=[]}#r=()=>{const e=this.scrollHeight-this.scrollTop-this.clientHeight;this.#o=e<=K};#i(e){let t=this.#s.get(e.id);t||(t=document.createElement("li"),t.innerHTML='<div class="bubble"></div>',this.#s.set(e.id,t),this.#t.append(t)),t.dataset.role=e.role,t.dataset.status=e.status,t.querySelector(".bubble").textContent=e.text,this.#n(),this.#o&&(this.scrollTop=this.scrollHeight)}#a(){this.#s.clear(),this.#t.replaceChildren(),this.#n()}#n(){this.shadowRoot.querySelector(".empty").hidden=this.#s.size>0}}customElements.get("machvive-chat-syncopation-canvas")||customElements.define("machvive-chat-syncopation-canvas",Y);class G extends HTMLElement{#e=null;#s=[];#t=null;#o=null;constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){const e=this.getAttribute("placeholder")??"Message…",t=this.getAttribute("label")??"Message";this.shadowRoot.innerHTML=`
      <style>
${l}
${u}
        :host {
          display: block;
          flex: 0 0 auto;
          padding: 0.5rem;
          font-family: var(--mcs-font);
          color: var(--mcs-fg);
          background: var(--mcs-bg);
          border-top: 1px solid var(--mcs-border);
        }
        form { display: flex; gap: 0.5rem; align-items: flex-end; }
        textarea {
          flex: 1 1 auto;
          min-height: 2.5rem;
          max-height: 8rem;
          padding: 0.5rem;
          resize: none;
          background: var(--mcs-surface);
        }
        button {
          flex: 0 0 auto;
          padding: 0.5rem 1rem;
          min-height: 2.5rem;
          cursor: pointer;
          background: var(--mcs-accent);
          color: var(--mcs-accent-fg);
          border-color: transparent;
        }
        button[data-mode="stop"] { background: var(--mcs-surface); color: var(--mcs-fg); border-color: var(--mcs-border); }
        /* Focus has to be visible against both themes; the UA default ring
           disappears on a dark surface. */
        :focus-visible { outline: 2px solid var(--mcs-accent); outline-offset: 2px; }
        .sr { position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); }
      </style>
      <form>
        <label class="sr" for="field">${t}</label>
        <textarea id="field" rows="1" placeholder="${e}"></textarea>
        <button type="submit" data-mode="send">Send</button>
      </form>
    `,this.#t=this.shadowRoot.querySelector("textarea"),this.#o=this.shadowRoot.querySelector("button"),this.shadowRoot.querySelector("form").addEventListener("submit",this.#a),this.#t.addEventListener("keydown",this.#i),this.#t.addEventListener("input",this.#r),this.#e=await h(this),!(!this.#e||!this.isConnected)&&(this.#s=[this.#e.bus.on("daemon:idle",()=>this.#c("send")),this.#e.bus.on("prompt:fill",({text:s,send:r})=>this.fill(s,{send:r}))])}disconnectedCallback(){for(const e of this.#s)e();this.#s=[]}fill(e,{send:t=!1}={}){this.#t&&(this.#t.value=e,this.#r(),this.#t.focus(),t&&this.#n())}#r=()=>{this.#t&&(this.#t.style.height="auto",this.#t.style.height=`${this.#t.scrollHeight}px`)};#i=e=>{e.key==="Enter"&&!e.shiftKey&&!e.isComposing&&(e.preventDefault(),this.#n())};#a=e=>{e.preventDefault(),this.#n()};#n(){if(this.#e?.daemon?.busy){this.#e.daemon.stop(),this.#c("send");return}const e=this.#t.value.trim();e&&(this.#t.value="",this.#r(),this.#c("stop"),this.dispatchEvent(new CustomEvent("prompt-submit",{detail:{text:e},bubbles:!0,composed:!0})),this.#e?.send(e))}#c(e){this.#o&&(this.#o.dataset.mode=e,this.#o.textContent=e==="stop"?"Stop":"Send")}}customElements.get("machvive-chat-syncopation-prompt")||customElements.define("machvive-chat-syncopation-prompt",G);class V extends HTMLElement{#e=null;#s=[];#t=null;#o=!1;static get observedAttributes(){return["suggestions","idle-ms","idle-text"]}constructor(){super(),this.attachShadow({mode:"open"})}get suggestions(){const e=this.getAttribute("suggestions");return e?e.split("|").map(t=>t.trim()).filter(Boolean):[]}async connectedCallback(){this.shadowRoot.innerHTML=`
      <style>
${l}
${u}
        :host {
          display: block;
          flex: 0 0 auto;
          padding: 0.5rem;
          font-family: var(--mcs-font);
          color: var(--mcs-fg);
          background: var(--mcs-bg);
        }
        :host([hidden]) { display: none; }
        .chips { display: flex; flex-wrap: wrap; gap: 0.375rem; }
        button {
          padding: 0.375rem 0.75rem;
          font-size: 0.875rem;
          cursor: pointer;
          background: var(--mcs-surface);
          border-radius: 999px;
          text-align: left;
        }
        button:hover { border-color: var(--mcs-accent); }
        :focus-visible { outline: 2px solid var(--mcs-accent); outline-offset: 2px; }
        .idle {
          margin: 0.5rem 0 0;
          font-size: 0.875rem;
          color: var(--mcs-muted);
        }
        .idle[hidden] { display: none; }
      </style>
      <div class="chips" role="group" aria-label="Suggested messages"></div>
      <p class="idle" hidden></p>
    `,this.#r(),this.#e=await h(this),!(!this.#e||!this.isConnected)&&(this.#s=[this.#e.bus.on("record:added",e=>{e.role==="user"&&this.#a(),this.#n()})],this.#n())}disconnectedCallback(){clearTimeout(this.#t);for(const e of this.#s)e();this.#s=[]}attributeChangedCallback(){this.shadowRoot?.childElementCount&&this.#r()}#r(){const e=this.shadowRoot.querySelector(".chips");e&&e.replaceChildren(...this.suggestions.map(t=>{const s=document.createElement("button");return s.type="button",s.textContent=t,s.addEventListener("click",()=>this.#i(t)),s}))}#i(e){this.dispatchEvent(new CustomEvent("nudge-select",{detail:{text:e},bubbles:!0,composed:!0})),this.#e?.bus.emit("prompt:fill",{text:e,send:!1}),this.#a()}#a(){this.shadowRoot?.querySelector(".chips")?.replaceChildren()}#n(){clearTimeout(this.#t);const e=Number(this.getAttribute("idle-ms"));!e||this.#o||(this.#t=setTimeout(()=>{this.#o=!0;const t=this.shadowRoot.querySelector(".idle");t.textContent=this.getAttribute("idle-text")??"Still here if you need anything.",t.hidden=!1,this.dispatchEvent(new CustomEvent("nudge-idle",{bubbles:!0,composed:!0}))},e))}}customElements.get("machvive-chat-syncopation-nudge")||customElements.define("machvive-chat-syncopation-nudge",V);class W extends HTMLElement{#e=null;#s=null;#t=null;#o=[];#r=0;#i=new Map;constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){this.shadowRoot.innerHTML=`
      <style>
${l}
${u}
        :host {
          display: block;
          flex: 0 0 auto;
          padding: 0.5rem;
          font-family: var(--mcs-mono);
          font-size: 0.875rem;
          color: var(--mcs-fg);
          background: var(--mcs-surface);
          border-top: 1px solid var(--mcs-border);
        }
        .out { margin: 0 0 0.375rem; white-space: pre-wrap; color: var(--mcs-muted); }
        .out:empty { display: none; }
        .row { display: flex; align-items: center; gap: 0.5rem; }
        .sigil { color: var(--mcs-accent); user-select: none; }
        input {
          flex: 1 1 auto;
          padding: 0.375rem 0.5rem;
          font-family: inherit;
          background: var(--mcs-bg);
        }
        :focus-visible { outline: 2px solid var(--mcs-accent); outline-offset: 2px; }
        .sr { position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); }
      </style>
      <p class="out" role="status" aria-live="polite"></p>
      <div class="row">
        <span class="sigil" aria-hidden="true">&gt;</span>
        <label class="sr" for="cli">Command or message</label>
        <input id="cli" type="text" autocomplete="off" spellcheck="false"
               placeholder="message, or /help" />
      </div>
    `,this.#s=this.shadowRoot.querySelector("input"),this.#t=this.shadowRoot.querySelector(".out"),this.#s.addEventListener("keydown",this.#n),this.#a(),this.#e=await h(this)}register(e,{describe:t="",run:s}){this.#i.set(e.replace(/^\//,""),{describe:t,run:s})}#a(){this.register("help",{describe:"list commands",run:()=>[...this.#i.entries()].map(([e,t])=>`/${e} — ${t.describe}`).join(`
`)}),this.register("clear",{describe:"clear the conversation",run:()=>(this.#e?.conversation.clear(),"cleared")}),this.register("transport",{describe:`show or set transport (${k.join(", ")})`,run:e=>e?k.includes(e)?(this.#e.config.transport=e,`transport: ${e}`):`unknown transport "${e}"`:`transport: ${this.#e?.config.transport}`}),this.register("stop",{describe:"interrupt the current turn",run:()=>(this.#e?.daemon.stop(),"stopped")})}#n=e=>{if(e.key==="Enter"){e.preventDefault(),this.#c(this.#s.value.trim()),this.#s.value="";return}if(e.key==="ArrowUp"||e.key==="ArrowDown"){if(!this.#o.length)return;e.preventDefault(),this.#r=Math.max(0,Math.min(this.#o.length,this.#r+(e.key==="ArrowUp"?-1:1))),this.#s.value=this.#o[this.#r]??""}};#c(e){if(!e)return;if(this.#o.push(e),this.#r=this.#o.length,!e.startsWith("/")){this.#l(""),this.#e?.send(e);return}const[t,...s]=e.slice(1).split(/\s+/),r=this.#i.get(t);if(!r)return this.#l(`unknown command "/${t}" — try /help`);try{this.#l(String(r.run(s.join(" "))??""))}catch(i){this.#l(`/${t} failed: ${i?.message??i}`)}this.dispatchEvent(new CustomEvent("cli-command",{detail:{name:t,args:s},bubbles:!0,composed:!0}))}#l(e){this.#t&&(this.#t.textContent=e)}}customElements.get("machvive-chat-syncopation-cli")||customElements.define("machvive-chat-syncopation-cli",W);const g=typeof window<"u"?window.SpeechRecognition??window.webkitSpeechRecognition??null:null,A=()=>typeof window<"u"&&"speechSynthesis"in window;class X extends HTMLElement{#e=null;#s=[];#t=null;#o=!1;constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){this.shadowRoot.innerHTML=`
      <style>
${l}
${u}
        :host {
          display: block;
          flex: 0 0 auto;
          padding: 0.5rem;
          font-family: var(--mcs-font);
          color: var(--mcs-fg);
          background: var(--mcs-bg);
        }
        .row { display: flex; align-items: center; gap: 0.5rem; }
        button { padding: 0.375rem 0.75rem; cursor: pointer; background: var(--mcs-surface); }
        button[aria-pressed="true"] { background: var(--mcs-accent); color: var(--mcs-accent-fg); border-color: transparent; }
        button[disabled] { cursor: not-allowed; color: var(--mcs-muted); }
        .note { margin: 0; font-size: 0.8125rem; color: var(--mcs-muted); }
        :focus-visible { outline: 2px solid var(--mcs-accent); outline-offset: 2px; }
      </style>
      <div class="row">
        <button type="button" class="mic" aria-pressed="false">🎤 Hold to talk</button>
        <button type="button" class="speak" aria-pressed="false">🔊 Read replies</button>
      </div>
      <p class="note"></p>
    `;const e=this.shadowRoot.querySelector(".mic"),t=this.shadowRoot.querySelector(".speak"),s=this.shadowRoot.querySelector(".note"),r=[];g||(e.disabled=!0,r.push("dictation")),A()||(t.disabled=!0,r.push("read-aloud")),s.textContent=r.length?`${r.join(" and ")} unavailable in this browser`:"",g&&(e.addEventListener("pointerdown",()=>this.start()),e.addEventListener("pointerup",()=>this.stop()),e.addEventListener("pointerleave",()=>this.stop())),t.addEventListener("click",()=>this.#i(t)),this.#e=await h(this)}disconnectedCallback(){this.stop(),A()&&window.speechSynthesis.cancel();for(const e of this.#s)e();this.#s=[]}start(){if(!(!g||this.#o)){this.#t=new g,this.#t.lang=this.getAttribute("lang")||this.#e?.config.locale||navigator.language,this.#t.interimResults=!0,this.#t.onresult=e=>{const t=[...e.results].map(s=>s[0].transcript).join("");this.#e?.bus.emit("prompt:fill",{text:t,send:!1}),this.dispatchEvent(new CustomEvent("voice-transcript",{detail:{text:t},bubbles:!0,composed:!0}))},this.#t.onerror=e=>{this.shadowRoot.querySelector(".note").textContent=`microphone: ${e.error}`,this.#r(!1)},this.#t.onend=()=>this.#r(!1);try{this.#t.start(),this.#r(!0)}catch{this.#r(!1)}}}stop(){if(this.#o){try{this.#t?.stop()}catch{}this.#r(!1)}}#r(e){this.#o=e,this.shadowRoot?.querySelector(".mic")?.setAttribute("aria-pressed",String(e))}#i(e){const t=e.getAttribute("aria-pressed")!=="true";if(e.setAttribute("aria-pressed",String(t)),!t){window.speechSynthesis.cancel();for(const s of this.#s)s();this.#s=[];return}this.#e&&this.#s.push(this.#e.bus.on("record:updated",s=>{if(s.role!=="assistant"||s.status!=="complete"||!s.text)return;const r=new SpeechSynthesisUtterance(s.text);r.lang=this.#t?.lang??navigator.language,window.speechSynthesis.speak(r)}))}}customElements.get("machvive-chat-syncopation-voice")||customElements.define("machvive-chat-syncopation-voice",X);const Z=200;class Q extends HTMLElement{#e=null;#s=[];#t=[];#o=null;#r=!1;constructor(){super(),this.attachShadow({mode:"open"})}get limit(){return Number(this.getAttribute("limit"))||Z}get events(){return[...this.#t]}async connectedCallback(){if(this.shadowRoot.innerHTML=`
      <style>
${l}
${u}
        :host {
          display: block;
          font-family: var(--mcs-mono);
          font-size: 0.8125rem;
          color: var(--mcs-fg);
          /* Paints its own surface: a component that themes its text and
             inherits the page's background renders light-on-light. */
          background: var(--mcs-bg);
          border: 1px solid var(--mcs-border);
          border-radius: var(--mcs-radius);
          overflow: hidden;
        }
        header {
          display: flex;
          align-items: center;
          gap: 0.5rem;
          padding: 0.375rem 0.5rem;
          background: var(--mcs-surface);
          border-bottom: 1px solid var(--mcs-border);
        }
        h2 { margin: 0; font-size: 0.8125rem; font-weight: 600; flex: 1 1 auto; }
        button { padding: 0.25rem 0.5rem; cursor: pointer; font-size: 0.75rem; }
        button[aria-pressed="true"] { background: var(--mcs-accent); color: var(--mcs-accent-fg); border-color: transparent; }
        .log { margin: 0; padding: 0; list-style: none; max-height: var(--mcs-inspector-height, 14rem); overflow-y: auto; }
        .log li { display: flex; gap: 0.5rem; padding: 0.1875rem 0.5rem; border-bottom: 1px solid var(--mcs-border); }
        .log li:last-child { border-bottom: 0; }
        time { color: var(--mcs-muted); flex: 0 0 5.5rem; }
        .topic { flex: 0 0 11rem; color: var(--mcs-accent); overflow-wrap: anywhere; }
        .detail { flex: 1 1 auto; color: var(--mcs-muted); overflow-wrap: anywhere; }
        .empty { padding: 0.5rem; margin: 0; color: var(--mcs-muted); }
        :focus-visible { outline: 2px solid var(--mcs-accent); outline-offset: 2px; }
      </style>
      <header>
        <h2>Bus</h2>
        <button type="button" class="pause" aria-pressed="false">Pause</button>
        <button type="button" class="clear">Clear</button>
      </header>
      <ul class="log"></ul>
      <p class="empty">Waiting for events…</p>
    `,this.#o=this.shadowRoot.querySelector(".log"),this.shadowRoot.querySelector(".pause").addEventListener("click",e=>{this.#r=!this.#r,e.currentTarget.setAttribute("aria-pressed",String(this.#r)),e.currentTarget.textContent=this.#r?"Resume":"Pause"}),this.shadowRoot.querySelector(".clear").addEventListener("click",()=>{this.#t=[],this.#o.replaceChildren(),this.shadowRoot.querySelector(".empty").hidden=!1}),this.#e=await h(this),!this.#e||!this.isConnected){this.shadowRoot.querySelector(".empty").textContent="No <machvive-chat-syncopation-services> found — the inspector reads that element’s bus.";return}this.#s=[this.#e.bus.on("*",e=>this.#i(e))]}disconnectedCallback(){for(const e of this.#s)e();this.#s=[]}#i({topic:e,payload:t}){if(this.#r)return;const s={at:new Date,topic:e,detail:ee(t)};this.#t.push(s),this.#t.length>this.limit&&this.#t.splice(0,this.#t.length-this.limit);const r=document.createElement("li"),i=document.createElement("time");i.textContent=s.at.toISOString().slice(11,23);const n=document.createElement("span");n.className="topic",n.textContent=e;const c=document.createElement("span");for(c.className="detail",c.textContent=s.detail,r.append(i,n,c),this.#o.append(r);this.#o.childElementCount>this.limit;)this.#o.firstElementChild.remove();this.shadowRoot.querySelector(".empty").hidden=!0,this.#o.scrollTop=this.#o.scrollHeight}}function ee(o){if(o==null)return"";if(typeof o=="string")return v(o);if(o.chunk!==void 0)return v(o.chunk);if(o.role)return`${o.role}/${o.status} ${v(o.text??"")}`;try{return v(JSON.stringify(o))}catch{return String(o)}}const v=(o,e=120)=>o.length>e?`${o.slice(0,e)}…`:o;customElements.get("machvive-chat-syncopation-inspector")||customElements.define("machvive-chat-syncopation-inspector",Q);class te extends HTMLElement{#e=null;#s=[];#t=null;constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){if(this.shadowRoot.innerHTML=`
      <style>
${l}
${u}
        :host {
          display: block;
          font-family: var(--mcs-font);
          font-size: 0.875rem;
          color: var(--mcs-fg);
          background: var(--mcs-bg);
          border: 1px solid var(--mcs-border);
          border-radius: var(--mcs-radius);
          overflow: hidden;
        }
        header {
          display: flex;
          align-items: center;
          gap: 0.5rem;
          padding: 0.375rem 0.5rem;
          background: var(--mcs-surface);
          border-bottom: 1px solid var(--mcs-border);
        }
        h2 { margin: 0; font-size: 0.875rem; font-weight: 600; flex: 1 1 auto; }
        button { padding: 0.25rem 0.5rem; cursor: pointer; font-size: 0.75rem; }
        button.danger { color: var(--mcs-danger); }
        ul { margin: 0; padding: 0; list-style: none; max-height: var(--mcs-history-height, 14rem); overflow-y: auto; }
        li { display: flex; align-items: center; gap: 0.5rem; padding: 0.375rem 0.5rem; border-bottom: 1px solid var(--mcs-border); }
        li:last-child { border-bottom: 0; }
        .label { flex: 1 1 auto; min-width: 0; }
        .title { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .count { color: var(--mcs-muted); font-size: 0.75rem; }
        .note { margin: 0; padding: 0.5rem; color: var(--mcs-muted); }
        :focus-visible { outline: 2px solid var(--mcs-accent); outline-offset: 2px; }
      </style>
      <header>
        <h2>History</h2>
        <button type="button" class="export">Export all</button>
        <button type="button" class="danger forget">Delete all</button>
      </header>
      <ul></ul>
      <p class="note"></p>
    `,this.#t=this.shadowRoot.querySelector("ul"),this.shadowRoot.querySelector(".export").addEventListener("click",()=>this.export()),this.shadowRoot.querySelector(".forget").addEventListener("click",()=>this.forgetAll()),this.#e=await h(this),!this.#e||!this.isConnected){this.#i("No <machvive-chat-syncopation-services> found.");return}this.#s=[this.#e.bus.on("record:added",()=>this.refresh()),this.#e.bus.on("conversation:cleared",()=>this.refresh())],await this.refresh()}disconnectedCallback(){for(const e of this.#s)e();this.#s=[]}async refresh(){if(!this.#e)return;const e=await this.#e.cache.list();this.#t.replaceChildren(...e.map(t=>this.#o(t))),e.length?this.#i(""):this.#i(this.#e.config.persist?"Nothing stored yet.":"Persistence is off — set the persist attribute on the services element to keep conversations.")}async export(e){if(!this.#e)return null;const t=e?await this.#e.cache.get(e):{exportedAt:new Date().toISOString(),conversations:await this.#e.cache.list()},s=JSON.stringify(t,null,2);if(this.dispatchEvent(new CustomEvent("history-export",{detail:{json:s},bubbles:!0,composed:!0})),typeof URL?.createObjectURL!="function")return s;const r=URL.createObjectURL(new Blob([s],{type:"application/json"})),i=document.createElement("a");return i.href=r,i.download=`syncopation-${e??"all"}.json`,i.click(),URL.revokeObjectURL(r),s}async forget(e){await this.#e?.cache.remove(e),this.dispatchEvent(new CustomEvent("history-forget",{detail:{id:e},bubbles:!0,composed:!0})),await this.refresh()}async forgetAll(){await this.#e?.cache.clear(),this.dispatchEvent(new CustomEvent("history-forget",{detail:{id:null},bubbles:!0,composed:!0})),await this.refresh()}#o(e){const t=e.records?.find(N=>N.role==="user"),s=document.createElement("li"),r=document.createElement("div");r.className="label";const i=document.createElement("span");i.className="title",i.textContent=t?.text?.trim()||e.id;const n=document.createElement("span");n.className="count",n.textContent=`${e.records?.length??0} turns`,r.append(i,n);const c=document.createElement("button");c.type="button",c.textContent="Open",c.addEventListener("click",()=>this.#r(e));const f=document.createElement("button");f.type="button",f.textContent="Export",f.addEventListener("click",()=>this.export(e.id));const m=document.createElement("button");return m.type="button",m.className="danger",m.textContent="Delete",m.addEventListener("click",()=>this.forget(e.id)),s.append(r,c,f,m),s}#r(e){const t=this.#e?.conversation;if(t){t.clear();for(const s of e.records??[])t.add(s);this.dispatchEvent(new CustomEvent("history-open",{detail:{id:e.id},bubbles:!0,composed:!0}))}}#i(e){const t=this.shadowRoot.querySelector(".note");t.textContent=e,t.hidden=!e}}customElements.get("machvive-chat-syncopation-history")||customElements.define("machvive-chat-syncopation-history",te);const a=o=>document.querySelector(o),se=()=>[...document.querySelectorAll("machvive-chat-syncopation")];a("#theme").addEventListener("change",o=>{const e=o.target.value;for(const t of se())t.dataset.lockTheme||(e?t.setAttribute("theme",e):t.removeAttribute("theme"))});document.querySelectorAll("machvive-chat-syncopation[theme]").forEach(o=>{o.dataset.lockTheme="true"});const O=(o,e)=>document.documentElement.style.setProperty(o,e);a("#accent").addEventListener("input",o=>O("--mcs-accent",o.target.value));a("#radius").addEventListener("input",o=>O("--mcs-radius",`${o.target.value}px`));a("#reset").addEventListener("click",()=>{document.documentElement.removeAttribute("style"),a("#accent").value="#1565c0",a("#radius").value="12",a("#theme").value="",a("#theme").dispatchEvent(new Event("change"))});const oe=`A conversation is a reading surface before it is an input surface.
That sounds like a small distinction until you watch someone try to read the
middle of a transcript while new text arrives at the bottom.

Most chat interfaces re-render the whole list on every update. The scroll
position jumps, a text selection collapses mid-copy, and a screen reader
re-announces turns the reader already heard. None of that is a model problem,
and all of it is avoidable: append to one node, and follow the bottom only when
the reader is already there.

Try scrolling up while this is still arriving. The transcript will stay where
you left it.`;async function re(o,e){const{conversation:t}=o;t.add({role:"user",text:e});const s=t.add({role:"assistant",text:"",status:"pending",meta:{[d.SOURCE]:"scripted"}}),r=Date.now(),i=oe.split(/(\s+)/);for(const n of i){if(o.__stopped)break;await new Promise(c=>setTimeout(c,18)),t.append(s.id,n)}t.update(s.id,{status:"complete",meta:{...s.meta,[d.LATENCY]:Date.now()-r}})}const p=a("#stream-services");a("#transport").addEventListener("change",o=>{const e=o.target.value;for(const t of document.querySelectorAll("machvive-chat-syncopation-services"))t.dataset.mode=e});p.addEventListener("prompt-submit",async o=>{a("#transport").value==="scripted"&&(o.stopPropagation(),p.__stopped=!1,await re(p,o.detail.text))},!0);p.bus.on("daemon:idle",()=>{p.__stopped=!0});const w=a("#cli-services");customElements.whenDefined("machvive-chat-syncopation-cli").then(()=>{const o=w.querySelector("machvive-chat-syncopation-cli");o.register("order",{describe:"look up an order by number",run:e=>{const t=e.trim();return t?(w.conversation.add({role:"tool",text:`order ${t}: shipped 2 Oct, arriving 7 Oct`}),`looked up order ${t}`):"usage: /order <number>"}}),o.register("inspect",{describe:"emit a custom bus event the inspector will show",run:()=>(w.bus.emit("playground:custom",{from:"the CLI"}),"emitted playground:custom")})});const ie=a("#roles-services");await customElements.whenDefined("machvive-chat-syncopation-canvas");const ne=[{role:"system",text:"Session started. Cart contains 2 items."},{role:"user",text:"Can you summarise what I have in the cart?"},{role:"assistant",text:`Two items: a 12-cup kettle and a pack of filters.

The kettle ships today; the filters are backordered until Friday.`},{role:"tool",text:"cart.read() → { items: 2, total: 48.20 }"},{role:"assistant",text:"Still typing, as it happens",status:"pending"},{role:"assistant",text:"The model endpoint returned 503.",status:"error",meta:{[d.ERROR]:"service unavailable"}},{role:"assistant",text:"<img src=x onerror=alert(1)>"}];for(const o of ne)ie.conversation.add(M(o));const $=a("#instrument-services");$.bus.on("record:added",o=>{$.bus.emit("playground:counted",{role:o.role,length:o.text.length})});for(const o of document.querySelectorAll("main section h2")){const e=o.closest("section").id;o.insertAdjacentHTML("afterbegin",`<a class="anchor" href="#${e}" aria-label="Link to this section">#</a>`)}
