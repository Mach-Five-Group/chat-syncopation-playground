(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const r of document.querySelectorAll('link[rel="modulepreload"]'))s(r);new MutationObserver(r=>{for(const n of r)if(n.type==="childList")for(const a of n.addedNodes)a.tagName==="LINK"&&a.rel==="modulepreload"&&s(a)}).observe(document,{childList:!0,subtree:!0});function t(r){const n={};return r.integrity&&(n.integrity=r.integrity),r.referrerPolicy&&(n.referrerPolicy=r.referrerPolicy),r.crossOrigin==="use-credentials"?n.credentials="include":r.crossOrigin==="anonymous"?n.credentials="omit":n.credentials="same-origin",n}function s(r){if(r.ep)return;r.ep=!0;const n=t(r);fetch(r.href,n)}})();class ve{#e=new Map;on(e,t){if(typeof t!="function")return()=>{};const s=this.#e.get(e)??new Set;return s.add(t),this.#e.set(e,s),()=>s.delete(t)}once(e,t){const s=this.on(e,r=>{s(),t(r)});return s}emit(e,t){for(const s of this.#e.get(e)??[])try{s(t)}catch(r){console.warn(`[syncopation] "${e}" subscriber failed:`,r)}for(const s of this.#e.get("*")??[])try{s({topic:e,payload:t})}catch{}}get topics(){return[...this.#e.keys()]}}let ye=0;const we=()=>`m-${Date.now().toString(36)}-${(ye++).toString(36)}`,F=Object.freeze(["user","assistant","system","tool"]);function ce({role:o="user",text:e="",...t}={}){if(!F.includes(o))throw new TypeError(`record.role must be one of ${F.join(", ")}`);return{id:we(),role:o,text:e,at:new Date().toISOString(),status:"complete",meta:{},...t}}class xe{#e=[];#t;#s;constructor({bus:e,maxTurns:t=200,id:s}={}){this.#t=e,this.#s=t,this.id=s??`c-${Date.now().toString(36)}`}get records(){return[...this.#e]}get length(){return this.#e.length}get last(){return this.#e.at(-1)??null}add(e){const t=e?.id?e:ce(e);return this.#e.push(t),this.#e.length>this.#s&&this.#e.splice(0,this.#e.length-this.#s),this.#t?.emit("record:added",t),t}append(e,t){const s=this.#e.find(r=>r.id===e);return s?(s.text+=t,this.#t?.emit("record:appended",{id:e,chunk:t,record:s}),s):null}update(e,t){const s=this.#e.find(r=>r.id===e);return s?(Object.assign(s,t),this.#t?.emit("record:updated",s),s):null}clear(){this.#e=[],this.#t?.emit("conversation:cleared",{id:this.id})}toJSON(){return{id:this.id,records:this.#e}}}const k=Object.freeze({TOKENS:"mcs:tokens",LATENCY:"mcs:latencyMs",MODEL:"mcs:model",SOURCE:"mcs:source",ERROR:"mcs:error",CITATIONS:"mcs:citations",TOOL_CALLS:"mcs:toolCalls"}),le={async*echo(o){for(const e of`You said: ${o}`.split(" "))await new Promise(t=>setTimeout(t,40)),yield e+" "},async*local(o,{config:e}){yield`[local model "${e.model||"unset"}" not yet wired] ${o}`},async*remote(o,{config:e}){yield`[remote endpoint "${e.endpoint||"unset"}" not yet wired] ${o}`}};class ke{#e;#t;#s;#o=null;#r;constructor({bus:e,conversation:t,config:s,transports:r}={}){this.#e=e,this.#t=t,this.#s=s,this.#r={...le,...r}}register(e,t){if(typeof t!="function")throw new TypeError(`transport "${e}" must be an async generator function`);return this.#r[e]=t,this.#e?.emit("daemon:transport-registered",{name:e}),this}get transports(){return Object.keys(this.#r)}get busy(){return this.#o!==null}stop(){this.#o?.abort(),this.#o=null}async send(e){if(this.busy)return null;this.#t.add({role:"user",text:e});const t=this.#t.add({role:"assistant",text:"",status:"pending",meta:{[k.SOURCE]:this.#s.transport,[k.MODEL]:this.#s.model}}),s=this.#r[this.#s.transport]??this.#r.echo;this.#o=new AbortController;const r=Date.now();try{for await(const n of s(e,{config:this.#s})){if(this.#o.signal.aborted)break;this.#t.append(t.id,n)}this.#t.update(t.id,{status:"complete",meta:{...t.meta,[k.LATENCY]:Date.now()-r}})}catch(n){this.#t.update(t.id,{status:"error",meta:{...t.meta,[k.ERROR]:String(n?.message??n)}}),this.#e?.emit("daemon:error",{id:t.id,error:n})}finally{this.#o=null,this.#e?.emit("daemon:idle",{id:t.id})}return t}}const K=Object.keys(le),Ee="machvive-chat-syncopation",A="conversations",Se=1;class Ce{#e=null;#t=new Map;get available(){return!!globalThis.indexedDB}#s(){return this.available?(this.#e??=new Promise(e=>{let t;try{t=globalThis.indexedDB.open(Ee,Se)}catch{return e(null)}t.onupgradeneeded=()=>{const s=t.result;s.objectStoreNames.contains(A)||s.createObjectStore(A,{keyPath:"id"})},t.onsuccess=()=>e(t.result),t.onerror=t.onblocked=()=>e(null)}),this.#e):Promise.resolve(null)}async#o(e,t){const s=await this.#s();return s?new Promise(r=>{let n;try{n=s.transaction(A,e)}catch{return r(null)}const a=t(n.objectStore(A));n.oncomplete=()=>r(a?a.result:null),n.onerror=n.onabort=()=>r(null)}):null}async put(e){return this.#t.set(e.id,e),await this.#o("readwrite",t=>t.put(e)),e}async get(e){return await this.#o("readonly",s=>s.get(e))??this.#t.get(e)??null}async list(){const e=await this.#o("readonly",t=>t.getAll());return e?.length?e:[...this.#t.values()]}async remove(e){this.#t.delete(e),await this.#o("readwrite",t=>t.delete(e))}async clear(){this.#t.clear(),await this.#o("readwrite",e=>e.clear())}}const G=Object.freeze({transport:"echo",model:"",endpoint:"",persist:!1,maxTurns:200,streaming:!0,locale:void 0}),V={persist:o=>o!=="false",streaming:o=>o!=="false",maxTurns:o=>Number(o)};function $e(o,e={}){const t={};for(const s of Object.keys(G)){const r=s.replace(/[A-Z]/g,n=>`-${n.toLowerCase()}`);if(o?.hasAttribute?.(r)){const n=o.getAttribute(r);t[s]=V[s]?V[s](n):n}}return{...G,...t,...e}}const Y=`
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
`,X=`
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
`,b=`
  :host {
    color-scheme: light dark;
${Y}  }
  @media (prefers-color-scheme: dark) {
    :host(:not([theme="light"])) {
${X}    }
  }
  :host([theme="dark"]) {
${X}  }
  :host([theme="light"]) {
    color-scheme: light;
${Y}  }
`,E=`
  button, input, textarea, select {
    font: inherit;
    color: var(--mcs-fg);
    background: var(--mcs-bg);
    border: 1px solid var(--mcs-border);
    border-radius: 8px;
  }
`,O="machvive-chat-syncopation-services";function z(o){let e=o;for(;e;){if(e.localName===O)return e;for(const t of e.children??[])if(t.localName===O)return t;e=e.parentElement??e.getRootNode?.()?.host??null}return document.querySelector(O)}function w(o,{timeoutMs:e=2e3}={}){const t=z(o);return t?.bus?Promise.resolve(t):new Promise(s=>{const r=Date.now()+e,n=()=>{const a=z(o);if(a?.bus)return s(a);if(Date.now()>r)return s(null);setTimeout(n,16)};n()})}class Te extends HTMLElement{#e=new ve;#t=new Ce;#s=null;#o=null;#r=null;constructor(){super(),this.attachShadow({mode:"open"})}connectedCallback(){this.shadowRoot.innerHTML="<style>:host { display: contents; }</style><slot></slot>",this.#s=$e(this),this.#o=new xe({bus:this.#e,maxTurns:this.#s.maxTurns}),this.#r=new ke({bus:this.#e,conversation:this.#o,config:this.#s}),this.#s.persist&&(this.#e.on("record:added",()=>this.#t.put(this.#o.toJSON())),this.#e.on("record:updated",()=>this.#t.put(this.#o.toJSON()))),this.#e.emit("services:ready",{config:this.#s}),this.dispatchEvent(new CustomEvent("services-ready",{bubbles:!0,composed:!0}))}get bus(){return this.#e}get conversation(){return this.#o}get daemon(){return this.#r}get cache(){return this.#t}get config(){return this.#s}send(e){return this.#r?.send(e)}registerTransport(e,t,{select:s=!0}={}){return this.#r?.register(e,t),s&&this.#s&&(this.#s.transport=e),this}}customElements.get("machvive-chat-syncopation-services")||customElements.define("machvive-chat-syncopation-services",Te);const Ae=["transport","model","endpoint","persist","max-turns","streaming"];class Le extends HTMLElement{#e=null;constructor(){super(),this.attachShadow({mode:"open"})}connectedCallback(){this.#e=z(this)??this.#t(),this.shadowRoot.innerHTML=`
      <style>
${b}
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
    `}#t(){const e=document.createElement(O);for(const t of Ae)this.hasAttribute(t)&&e.setAttribute(t,this.getAttribute(t));return this.prepend(e),e}get services(){return this.#e}get conversation(){return this.#e?.conversation??null}send(e){return this.#e?.send(e)}}customElements.get("machvive-chat-syncopation")||customElements.define("machvive-chat-syncopation",Le);const qe=48;class Me extends HTMLElement{#e=[];#t=new Map;#s=null;#o=!0;constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){this.shadowRoot.innerHTML=`
      <style>
${b}
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
    `,this.#s=this.shadowRoot.querySelector("ol"),this.addEventListener("scroll",this.#r,{passive:!0});const e=await w(this);if(!(!e||!this.isConnected)){for(const t of e.conversation.records)this.#n(t);this.#e=[e.bus.on("record:added",t=>this.#n(t)),e.bus.on("record:appended",({record:t})=>this.#n(t)),e.bus.on("record:updated",t=>this.#n(t)),e.bus.on("conversation:cleared",()=>this.#i())],this.#a()}}disconnectedCallback(){this.removeEventListener("scroll",this.#r);for(const e of this.#e)e();this.#e=[]}#r=()=>{const e=this.scrollHeight-this.scrollTop-this.clientHeight;this.#o=e<=qe};#n(e){let t=this.#t.get(e.id);t||(t=document.createElement("li"),t.innerHTML='<div class="bubble"></div>',this.#t.set(e.id,t),this.#s.append(t)),t.dataset.role=e.role,t.dataset.status=e.status,t.querySelector(".bubble").textContent=e.text,this.#a(),this.#o&&(this.scrollTop=this.scrollHeight)}#i(){this.#t.clear(),this.#s.replaceChildren(),this.#a()}#a(){this.shadowRoot.querySelector(".empty").hidden=this.#t.size>0}}customElements.get("machvive-chat-syncopation-canvas")||customElements.define("machvive-chat-syncopation-canvas",Me);class Oe extends HTMLElement{#e=null;#t=[];#s=null;#o=null;constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){const e=this.getAttribute("placeholder")??"Message…",t=this.getAttribute("label")??"Message";this.shadowRoot.innerHTML=`
      <style>
${b}
${E}
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
    `,this.#s=this.shadowRoot.querySelector("textarea"),this.#o=this.shadowRoot.querySelector("button"),this.shadowRoot.querySelector("form").addEventListener("submit",this.#i),this.#s.addEventListener("keydown",this.#n),this.#s.addEventListener("input",this.#r),this.#e=await w(this),!(!this.#e||!this.isConnected)&&(this.#t=[this.#e.bus.on("daemon:idle",()=>this.#c("send")),this.#e.bus.on("prompt:fill",({text:s,send:r})=>this.fill(s,{send:r}))])}disconnectedCallback(){for(const e of this.#t)e();this.#t=[]}fill(e,{send:t=!1}={}){this.#s&&(this.#s.value=e,this.#r(),this.#s.focus(),t&&this.#a())}#r=()=>{this.#s&&(this.#s.style.height="auto",this.#s.style.height=`${this.#s.scrollHeight}px`)};#n=e=>{e.key==="Enter"&&!e.shiftKey&&!e.isComposing&&(e.preventDefault(),this.#a())};#i=e=>{e.preventDefault(),this.#a()};#a(){if(this.#e?.daemon?.busy){this.#e.daemon.stop(),this.#c("send");return}const e=this.#s.value.trim();e&&(this.#s.value="",this.#r(),this.#c("stop"),this.dispatchEvent(new CustomEvent("prompt-submit",{detail:{text:e},bubbles:!0,composed:!0})),this.#e?.send(e))}#c(e){this.#o&&(this.#o.dataset.mode=e,this.#o.textContent=e==="stop"?"Stop":"Send")}}customElements.get("machvive-chat-syncopation-prompt")||customElements.define("machvive-chat-syncopation-prompt",Oe);class _e extends HTMLElement{#e=null;#t=[];#s=null;#o=!1;static get observedAttributes(){return["suggestions","idle-ms","idle-text"]}constructor(){super(),this.attachShadow({mode:"open"})}get suggestions(){const e=this.getAttribute("suggestions");return e?e.split("|").map(t=>t.trim()).filter(Boolean):[]}async connectedCallback(){this.shadowRoot.innerHTML=`
      <style>
${b}
${E}
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
    `,this.#r(),this.#e=await w(this),!(!this.#e||!this.isConnected)&&(this.#t=[this.#e.bus.on("record:added",e=>{e.role==="user"&&this.#i(),this.#a()})],this.#a())}disconnectedCallback(){clearTimeout(this.#s);for(const e of this.#t)e();this.#t=[]}attributeChangedCallback(){this.shadowRoot?.childElementCount&&this.#r()}#r(){const e=this.shadowRoot.querySelector(".chips");e&&e.replaceChildren(...this.suggestions.map(t=>{const s=document.createElement("button");return s.type="button",s.textContent=t,s.addEventListener("click",()=>this.#n(t)),s}))}#n(e){this.dispatchEvent(new CustomEvent("nudge-select",{detail:{text:e},bubbles:!0,composed:!0})),this.#e?.bus.emit("prompt:fill",{text:e,send:!1}),this.#i()}#i(){this.shadowRoot?.querySelector(".chips")?.replaceChildren()}#a(){clearTimeout(this.#s);const e=Number(this.getAttribute("idle-ms"));!e||this.#o||(this.#s=setTimeout(()=>{this.#o=!0;const t=this.shadowRoot.querySelector(".idle");t.textContent=this.getAttribute("idle-text")??"Still here if you need anything.",t.hidden=!1,this.dispatchEvent(new CustomEvent("nudge-idle",{bubbles:!0,composed:!0}))},e))}}customElements.get("machvive-chat-syncopation-nudge")||customElements.define("machvive-chat-syncopation-nudge",_e);class Re extends HTMLElement{#e=null;#t=null;#s=null;#o=[];#r=0;#n=new Map;constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){this.shadowRoot.innerHTML=`
      <style>
${b}
${E}
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
    `,this.#t=this.shadowRoot.querySelector("input"),this.#s=this.shadowRoot.querySelector(".out"),this.#t.addEventListener("keydown",this.#a),this.#i(),this.#e=await w(this)}register(e,{describe:t="",run:s}){this.#n.set(e.replace(/^\//,""),{describe:t,run:s})}#i(){this.register("help",{describe:"list commands",run:()=>[...this.#n.entries()].map(([e,t])=>`/${e} — ${t.describe}`).join(`
`)}),this.register("clear",{describe:"clear the conversation",run:()=>(this.#e?.conversation.clear(),"cleared")}),this.register("transport",{describe:`show or set transport (${K.join(", ")})`,run:e=>e?K.includes(e)?(this.#e.config.transport=e,`transport: ${e}`):`unknown transport "${e}"`:`transport: ${this.#e?.config.transport}`}),this.register("stop",{describe:"interrupt the current turn",run:()=>(this.#e?.daemon.stop(),"stopped")})}#a=e=>{if(e.key==="Enter"){e.preventDefault(),this.#c(this.#t.value.trim()),this.#t.value="";return}if(e.key==="ArrowUp"||e.key==="ArrowDown"){if(!this.#o.length)return;e.preventDefault(),this.#r=Math.max(0,Math.min(this.#o.length,this.#r+(e.key==="ArrowUp"?-1:1))),this.#t.value=this.#o[this.#r]??""}};#c(e){if(!e)return;if(this.#o.push(e),this.#r=this.#o.length,!e.startsWith("/")){this.#l(""),this.#e?.send(e);return}const[t,...s]=e.slice(1).split(/\s+/),r=this.#n.get(t);if(!r)return this.#l(`unknown command "/${t}" — try /help`);try{this.#l(String(r.run(s.join(" "))??""))}catch(n){this.#l(`/${t} failed: ${n?.message??n}`)}this.dispatchEvent(new CustomEvent("cli-command",{detail:{name:t,args:s},bubbles:!0,composed:!0}))}#l(e){this.#s&&(this.#s.textContent=e)}}customElements.get("machvive-chat-syncopation-cli")||customElements.define("machvive-chat-syncopation-cli",Re);const L=typeof window<"u"?window.SpeechRecognition??window.webkitSpeechRecognition??null:null,Z=()=>typeof window<"u"&&"speechSynthesis"in window;class Ne extends HTMLElement{#e=null;#t=[];#s=null;#o=!1;constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){this.shadowRoot.innerHTML=`
      <style>
${b}
${E}
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
    `;const e=this.shadowRoot.querySelector(".mic"),t=this.shadowRoot.querySelector(".speak"),s=this.shadowRoot.querySelector(".note"),r=[];L||(e.disabled=!0,r.push("dictation")),Z()||(t.disabled=!0,r.push("read-aloud")),s.textContent=r.length?`${r.join(" and ")} unavailable in this browser`:"",L&&(e.addEventListener("pointerdown",()=>this.start()),e.addEventListener("pointerup",()=>this.stop()),e.addEventListener("pointerleave",()=>this.stop())),t.addEventListener("click",()=>this.#n(t)),this.#e=await w(this)}disconnectedCallback(){this.stop(),Z()&&window.speechSynthesis.cancel();for(const e of this.#t)e();this.#t=[]}start(){if(!(!L||this.#o)){this.#s=new L,this.#s.lang=this.getAttribute("lang")||this.#e?.config.locale||navigator.language,this.#s.interimResults=!0,this.#s.onresult=e=>{const t=[...e.results].map(s=>s[0].transcript).join("");this.#e?.bus.emit("prompt:fill",{text:t,send:!1}),this.dispatchEvent(new CustomEvent("voice-transcript",{detail:{text:t},bubbles:!0,composed:!0}))},this.#s.onerror=e=>{this.shadowRoot.querySelector(".note").textContent=`microphone: ${e.error}`,this.#r(!1)},this.#s.onend=()=>this.#r(!1);try{this.#s.start(),this.#r(!0)}catch{this.#r(!1)}}}stop(){if(this.#o){try{this.#s?.stop()}catch{}this.#r(!1)}}#r(e){this.#o=e,this.shadowRoot?.querySelector(".mic")?.setAttribute("aria-pressed",String(e))}#n(e){const t=e.getAttribute("aria-pressed")!=="true";if(e.setAttribute("aria-pressed",String(t)),!t){window.speechSynthesis.cancel();for(const s of this.#t)s();this.#t=[];return}this.#e&&this.#t.push(this.#e.bus.on("record:updated",s=>{if(s.role!=="assistant"||s.status!=="complete"||!s.text)return;const r=new SpeechSynthesisUtterance(s.text);r.lang=this.#s?.lang??navigator.language,window.speechSynthesis.speak(r)}))}}customElements.get("machvive-chat-syncopation-voice")||customElements.define("machvive-chat-syncopation-voice",Ne);const je=200;class Pe extends HTMLElement{#e=null;#t=[];#s=[];#o=null;#r=!1;constructor(){super(),this.attachShadow({mode:"open"})}get limit(){return Number(this.getAttribute("limit"))||je}get events(){return[...this.#s]}async connectedCallback(){if(this.shadowRoot.innerHTML=`
      <style>
${b}
${E}
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
    `,this.#o=this.shadowRoot.querySelector(".log"),this.shadowRoot.querySelector(".pause").addEventListener("click",e=>{this.#r=!this.#r,e.currentTarget.setAttribute("aria-pressed",String(this.#r)),e.currentTarget.textContent=this.#r?"Resume":"Pause"}),this.shadowRoot.querySelector(".clear").addEventListener("click",()=>{this.#s=[],this.#o.replaceChildren(),this.shadowRoot.querySelector(".empty").hidden=!1}),this.#e=await w(this),!this.#e||!this.isConnected){this.shadowRoot.querySelector(".empty").textContent="No <machvive-chat-syncopation-services> found — the inspector reads that element’s bus.";return}this.#t=[this.#e.bus.on("*",e=>this.#n(e))]}disconnectedCallback(){for(const e of this.#t)e();this.#t=[]}#n({topic:e,payload:t}){if(this.#r)return;const s={at:new Date,topic:e,detail:De(t)};this.#s.push(s),this.#s.length>this.limit&&this.#s.splice(0,this.#s.length-this.limit);const r=document.createElement("li"),n=document.createElement("time");n.textContent=s.at.toISOString().slice(11,23);const a=document.createElement("span");a.className="topic",a.textContent=e;const i=document.createElement("span");for(i.className="detail",i.textContent=s.detail,r.append(n,a,i),this.#o.append(r);this.#o.childElementCount>this.limit;)this.#o.firstElementChild.remove();this.shadowRoot.querySelector(".empty").hidden=!0,this.#o.scrollTop=this.#o.scrollHeight}}function De(o){if(o==null)return"";if(typeof o=="string")return q(o);if(o.chunk!==void 0)return q(o.chunk);if(o.role)return`${o.role}/${o.status} ${q(o.text??"")}`;try{return q(JSON.stringify(o))}catch{return String(o)}}const q=(o,e=120)=>o.length>e?`${o.slice(0,e)}…`:o;customElements.get("machvive-chat-syncopation-inspector")||customElements.define("machvive-chat-syncopation-inspector",Pe);class Ie extends HTMLElement{#e=null;#t=[];#s=null;constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){if(this.shadowRoot.innerHTML=`
      <style>
${b}
${E}
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
    `,this.#s=this.shadowRoot.querySelector("ul"),this.shadowRoot.querySelector(".export").addEventListener("click",()=>this.export()),this.shadowRoot.querySelector(".forget").addEventListener("click",()=>this.forgetAll()),this.#e=await w(this),!this.#e||!this.isConnected){this.#n("No <machvive-chat-syncopation-services> found.");return}this.#t=[this.#e.bus.on("record:added",()=>this.refresh()),this.#e.bus.on("conversation:cleared",()=>this.refresh())],await this.refresh()}disconnectedCallback(){for(const e of this.#t)e();this.#t=[]}async refresh(){if(!this.#e)return;const e=await this.#e.cache.list();this.#s.replaceChildren(...e.map(t=>this.#o(t))),e.length?this.#n(""):this.#n(this.#e.config.persist?"Nothing stored yet.":"Persistence is off — set the persist attribute on the services element to keep conversations.")}async export(e){if(!this.#e)return null;const t=e?await this.#e.cache.get(e):{exportedAt:new Date().toISOString(),conversations:await this.#e.cache.list()},s=JSON.stringify(t,null,2);if(this.dispatchEvent(new CustomEvent("history-export",{detail:{json:s},bubbles:!0,composed:!0})),typeof URL?.createObjectURL!="function")return s;const r=URL.createObjectURL(new Blob([s],{type:"application/json"})),n=document.createElement("a");return n.href=r,n.download=`syncopation-${e??"all"}.json`,n.click(),URL.revokeObjectURL(r),s}async forget(e){await this.#e?.cache.remove(e),this.dispatchEvent(new CustomEvent("history-forget",{detail:{id:e},bubbles:!0,composed:!0})),await this.refresh()}async forgetAll(){await this.#e?.cache.clear(),this.dispatchEvent(new CustomEvent("history-forget",{detail:{id:null},bubbles:!0,composed:!0})),await this.refresh()}#o(e){const t=e.records?.find(h=>h.role==="user"),s=document.createElement("li"),r=document.createElement("div");r.className="label";const n=document.createElement("span");n.className="title",n.textContent=t?.text?.trim()||e.id;const a=document.createElement("span");a.className="count",a.textContent=`${e.records?.length??0} turns`,r.append(n,a);const i=document.createElement("button");i.type="button",i.textContent="Open",i.addEventListener("click",()=>this.#r(e));const c=document.createElement("button");c.type="button",c.textContent="Export",c.addEventListener("click",()=>this.export(e.id));const l=document.createElement("button");return l.type="button",l.className="danger",l.textContent="Delete",l.addEventListener("click",()=>this.forget(e.id)),s.append(r,i,c,l),s}#r(e){const t=this.#e?.conversation;if(t){t.clear();for(const s of e.records??[])t.add(s);this.dispatchEvent(new CustomEvent("history-open",{detail:{id:e.id},bubbles:!0,composed:!0}))}}#n(e){const t=this.shadowRoot.querySelector(".note");t.textContent=e,t.hidden=!e}}customElements.get("machvive-chat-syncopation-history")||customElements.define("machvive-chat-syncopation-history",Ie);const Q=`
    --mv-fg: #1a1a1a;
    --mv-muted: #666;
    --mv-faint: #6e6e6e;
    --mv-bg: #fff;
    --mv-surface: #fafafa;
    --mv-input-bg: #fff;
    --mv-border: #e2e2e2;
    --mv-border-soft: #eee;
    --mv-control-border: #ccc;
    --mv-hover: #f2f2f2;
    --mv-selected: #e8f0fe;
    --mv-accent: #1565c0;
    --mv-accent-fg: #fff;
    --mv-ok-bg: #e6f4ea;
    --mv-ok-fg: #0f6b2e;
    --mv-err-bg: #fce8e6;
    --mv-err-fg: #b3261e;
    --mv-err-border: #f5c6c2;
    --mv-danger: #a4161a;
    --mv-danger-border: #d8a0a0;
    --mv-shadow: rgba(0, 0, 0, .18);
`,ee=`
    --mv-fg: #e8eaed;
    --mv-muted: #9aa0a6;
    --mv-faint: #8b9096;
    --mv-bg: #1f2125;
    --mv-surface: #282b30;
    --mv-input-bg: #16181b;
    --mv-border: #3c4046;
    --mv-border-soft: #33363b;
    --mv-control-border: #4a4f56;
    --mv-hover: #32363c;
    --mv-selected: #1e3a5f;
    --mv-accent: #5b9bf8;
    --mv-accent-fg: #0b1220;
    --mv-ok-bg: #16351f;
    --mv-ok-fg: #7ee2a0;
    --mv-err-bg: #3f1d1c;
    --mv-err-fg: #ff9d97;
    --mv-err-border: #5c2c2a;
    --mv-danger: #ff9d97;
    --mv-danger-border: #5c2c2a;
    --mv-shadow: rgba(0, 0, 0, .5);
`,U=`
  :host {
    color-scheme: light dark;
${Q}  }

  @media (prefers-color-scheme: dark) {
    :host(:not([theme="light"])) {
${ee}    }
  }

  /* Explicit choice beats the OS preference, in both directions. */
  :host([theme="dark"]) {
${ee}  }

  :host([theme="light"]) {
    color-scheme: light;
${Q}  }
`,He=`
  lorem ipsum a ab accusamus accusantium ad adipiscing alias aliquam aliquid amet animi
  aperiam architecto asperiores aspernatur assumenda at atque aut autem beatae blanditiis
  commodi consectetur consequatur consequuntur corporis corrupti culpa cum cumque cupiditate
  debitis delectus deleniti deserunt dicta dignissimos distinctio do dolor dolore dolorem
  doloremque dolores doloribus dolorum dquis ducimus ea eaque earum eius eligendi enim eos
  error ert esse est et eum eveniet ex excepturi exercitationem expedita explicabo facere
  facilis fuga fugiat fugit harum hic id illo illum impedit in incididunt inventore ipsa ipsam
  irure iste itaque iusto labore laboriosam laborum laudantium libero magnam magni maiores
  maxime minima minus modi molestiae molestias mollitia nam natus necessitatibus nemo neque
  nesciunt nihil nisi nobis non nostrumd nulla numquam obcaecati odio odit officia officiis
  omnis optio pariatur perferendis perspiciatis placeat porro possimus praesentium provident
  quae quaerat quam quas quasi qui quia quibusdam quidem quis quisquam quo quod quos ratione
  recusandae reiciendis rem repellat repellendaus reprehenderit repudiandae rerudum rerum
  saepe sapiente sed sequi similique sint sit soluta sunt suscipit tempora tempore temporibus
  tenetur totam ullam unde ut vel velit veniam veritatis vero vitae voluptas voluptate
  voluptatem voluptates voluptatibus voluptatum
`.trim().split(/\s+/),ze=`
  a about absolutely accident action actionology admirable advocacy alchemy all an and angel
  animals anime answer anywhere application apprehension are arrester artisanal as asked atoms
  avant-garde awareness back bamboozle bananas based beauty best beyond blink bold book boost
  boy boys brand brandformance brands breakfast build but butterscotch buyer buyers by cadence
  can candy cash cellaphane centricity chain chaos claironic clarity clever clevor
  clevorvoyant clock clockwork coming common compass compel consensus contagious content
  context convergence conversational conviction convinced countenance counter crackerjack
  craftwork create creating crucial culture customer cut cutting damn data day decisis
  delirious delta depth design destroyed digital directed discerning discoverability
  distinctly doctor dog’s don't done doppelganger double dubious dust easy eat efficiency
  elastic electrokinetic emerging enablement enemy engage enigmatic enliven enterprise entice
  etch even every evocative evoked experience express extremely eyes faculties fad fall fast
  feasible ferver fishstick flow fluent fluid fly food for foray form freak fresh fringe from
  fruition fulcrum fundamental fusion future-proof game garish generation give glide glory
  gobsmackingly god golden gotta grokked grow guile hand happenstance he's heart help here
  high hold honey how however hyper-persuasive i iconic ideas ideation if imagine
  impossibilities in indigo infinite insipid into intuitive ion irrepressibly is isn’t it it's
  journeys joy keystone knot know latticework laureate leading-edge leapyear lengths let life
  like linchpin longing luxe madness majestic man many marketing me meaningful microsecond
  minds monkeys moonlight motion move movie moving my needle nevermore new noble noise
  nonsense not numbers observable of on once orgin other our outcome-based own page paragon
  pay pepper peppercoin physically piece pimento poet portal posh post potential probability
  product productivity propulsion purple quantum questions quinque quintessence rancid reason
  reasonable refreshingly reliable replicating required resonance results revenue reversed
  ridiculous right rise rocket salesforce saw say scalable score sculptor secure see seize
  sell seller sense shakespeare shoes single small smart socks sorrow stainless stars
  steadfast story straight strawberry striking studio sub-committee sunset surgery syncopated
  syncopation syndication take tech technology tell that the thee their theory there they this
  those through time to trebuchet trillions typing uncommon unexpected unfathomably universe
  unreasonable urgency use vantagepoint veins versus vessel video visualization visualizer
  visuals want was watch way were what wherewithal wind wine wise with within won't words work
  world yet you you'd your zenith
`.trim().split(/\s+/),de=Object.freeze({latin:Object.freeze(He),english:Object.freeze(ze)}),he=Object.freeze(Object.keys(de)),Be=["lorem","ipsum","dolor","sit","amet"];function Ue(o){let e=o>>>0;return()=>{e=e+1831565813>>>0;let t=Math.imul(e^e>>>15,1|e);return t=t+Math.imul(t^t>>>7,61|t)^t,((t^t>>>14)>>>0)/4294967296}}const Je=o=>o.charAt(0).toUpperCase()+o.slice(1);function $({sentences:o=5,paragraphs:e=1,lang:t="latin",minWords:s=5,maxWords:r=14,seed:n,classicOpening:a}={}){const i=de[t];if(!i)throw new TypeError(`lang must be one of ${he.join(", ")}`);const c=n===void 0?Math.random:Ue(n),l=(g,S)=>g+Math.floor(c()*(S-g+1)),h=Math.max(1,Math.min(s,r)),d=Math.max(h,r),m=a??t==="latin",u=[];let f=!0;for(let g=0;g<Math.max(1,e);g++){const S=o===-1?l(1,5):Math.max(1,o),J=[];for(let W=0;W<S;W++){const N=l(h,d),j=new Set,x=[];if(f&&m)for(const C of Be.slice(0,N))x.push(C),j.add(C);for(let C=0;x.length<N&&C<N*8;C++){const P=i[Math.floor(c()*i.length)];j.has(P)||(j.add(P),x.push(P))}x[0]=Je(x[0]),J.push(`${x.join(" ")}.`),f=!1}u.push(J.join(" "))}return u.join(`

`)}const We=(o={})=>$({...o,sentences:1,paragraphs:1});class Fe extends HTMLElement{#e="";static get observedAttributes(){return["lang","sentences","paragraphs","seed","theme"]}constructor(){super(),this.attachShadow({mode:"open"})}get theme(){return this.getAttribute("theme")}set theme(e){e==null?this.removeAttribute("theme"):this.setAttribute("theme",e)}get lang(){const e=this.getAttribute("lang");return he.includes(e)?e:"latin"}set lang(e){this.setAttribute("lang",e)}get sentences(){const e=Number(this.getAttribute("sentences"));return Number.isFinite(e)&&e!==0?e:5}set sentences(e){this.setAttribute("sentences",String(e))}get paragraphs(){const e=Number(this.getAttribute("paragraphs"));return Number.isFinite(e)&&e>0?e:1}set paragraphs(e){this.setAttribute("paragraphs",String(e))}get seed(){const e=Number(this.getAttribute("seed"));return this.hasAttribute("seed")&&Number.isFinite(e)?e:void 0}set seed(e){e==null?this.removeAttribute("seed"):this.setAttribute("seed",String(e))}get text(){return this.#e}connectedCallback(){this.shadowRoot.innerHTML=`
      <style>
${U}
        :host {
          display: block;
          font-family: system-ui, -apple-system, sans-serif;
          line-height: 1.6;
          /* No background on purpose. Placeholder copy stands in for a page's own
             text, so it has to sit on whatever surface hosts it — a card, a table
             cell, a chat bubble. That makes it the same deliberate exception the
             floating inspector is, and why it is outside the strict theme tests. */
          color: var(--mv-fg);
        }
        p { margin: 0 0 0.75em; }
        p:last-child { margin-bottom: 0; }
      </style>
      <slot></slot>
    `,this.#t()}attributeChangedCallback(e){e==="theme"||!this.shadowRoot?.childElementCount||this.#t()}regenerate(){return this.#t(),this.#e}#t(){const e=this.shadowRoot.querySelector("slot");e&&(this.#e=$({sentences:this.sentences,paragraphs:this.paragraphs,lang:this.lang,seed:this.seed}),e.replaceChildren(...this.#e.split(`

`).map(t=>{const s=document.createElement("p");return s.textContent=t,s})))}}customElements.get("machvive-lorum-ipsum")||customElements.define("machvive-lorum-ipsum",Fe);const R="machvive-webmcp-change";function te(o){if(!o||typeof o!="object")throw new TypeError("WebMCP: tool descriptor must be an object");if(typeof o.name!="string"||o.name.length===0)throw new TypeError("WebMCP: tool.name must be a non-empty string");if(typeof o.execute!="function")throw new TypeError(`WebMCP: tool "${o.name}" must supply an execute() function`)}function Ke(o){return o&&Array.isArray(o.content)?o:typeof o=="string"?{content:[{type:"text",text:o}]}:o==null?{content:[]}:{content:[{type:"text",text:JSON.stringify(o)}]}}class Ge{#e=new Map;registerTool(e){te(e),this.#e.set(e.name,e),this.#s()}unregisterTool(e){const t=this.#e.delete(e);return t&&this.#s(),t}provideContext({tools:e=[]}={}){e.forEach(te),this.#e.clear();for(const t of e)this.#e.set(t.name,t);this.#s()}get tools(){return[...this.#e.values()].map(({name:e,description:t,inputSchema:s})=>({name:e,description:t,inputSchema:s}))}async callTool(e,t={}){const s=this.#e.get(e);if(!s)return{content:[{type:"text",text:`Unknown tool: ${e}`}],isError:!0};try{return Ke(await s.execute(t,this.#t()))}catch(r){return{content:[{type:"text",text:String(r?.message??r)}],isError:!0}}}#t(){return{requestUserInteraction:e=>Promise.resolve().then(e)}}#s(){window.dispatchEvent(new CustomEvent(R,{detail:{tools:this.tools}}))}}function ue({allowInsecureContext:o=!1}={}){return"modelContext"in navigator?!1:!window.isSecureContext&&!o?(console.warn(`WebMCP: ${globalThis.location?.origin??"this page"} is not a secure context, so navigator.modelContext was not installed. A secure origin, localhost or 127.0.0.1 qualifies; a LAN address or custom hostname does not. For an offline or intranet bundle, opt in with <machvive-webmcp-polyfill allow-insecure> or installWebmcpPolyfill({ allowInsecureContext: true }).`),!1):(Object.defineProperty(navigator,"modelContext",{value:new Ge,configurable:!0,enumerable:!1,writable:!1}),!0)}class Ve extends HTMLElement{constructor(){super(),this.attachShadow({mode:"open"})}connectedCallback(){ue({allowInsecureContext:this.hasAttribute("allow-insecure")}),this.shadowRoot.innerHTML="<style>:host { display: none; }</style>"}registerTool(e){return navigator.modelContext?.registerTool(e)}unregisterTool(e){return navigator.modelContext?.unregisterTool(e)}}ue();customElements.get("machvive-webmcp-polyfill")||customElements.define("machvive-webmcp-polyfill",Ve);function Ye(o,e={}){if(o.type==="checkbox")return o.checked;const t=o.value;if(t!==""){if(e.type==="number"||e.type==="integer"){const s=Number(t);if(Number.isNaN(s))throw new TypeError(`"${t}" is not a number`);return e.type==="integer"?Math.trunc(s):s}if(e.type==="object"||e.type==="array")try{return JSON.parse(t)}catch{throw new TypeError("must be valid JSON")}return t}}const Xe=`
  ${U}

  /* See analytics: a component that themes its own text must paint its own
     surface rather than assume the embedding page supplies a matching one. */
  :host { display: block; font: 13px/1.5 system-ui, sans-serif;
          color: var(--mv-fg); background: var(--mv-bg); }
  :host([hidden]) { display: none; }

  /* Floating mode docks the panel without disturbing page layout.
     The corner is configurable because bottom-right is crowded — chat widgets,
     cookie banners and support launchers all live there, and a fixed position
     means the inspector lands on top of one. */
  /* Floating mode is a detached panel: the .panel and .fab paint themselves, so
     the host must stay transparent or it draws a block over the page. */
  :host([floating]) { position: fixed; z-index: 2147483000; display: block; width: auto;
                      background: transparent;
                      inset-block-end: var(--mv-fab-offset-block, 16px);
                      inset-inline-end: var(--mv-fab-offset-inline, 16px); }
  :host([floating][position="bottom-left"])  { inset-inline-end: auto;
                      inset-inline-start: var(--mv-fab-offset-inline, 16px); }
  :host([floating][position="top-right"])    { inset-block-end: auto;
                      inset-block-start: var(--mv-fab-offset-block, 16px); }
  :host([floating][position="top-left"])     { inset-block-end: auto; inset-inline-end: auto;
                      inset-block-start: var(--mv-fab-offset-block, 16px);
                      inset-inline-start: var(--mv-fab-offset-inline, 16px); }
  /* hidden-fab keeps the panel reachable by hotkey with no launcher on screen. */
  :host([floating][hidden-fab]) .fab { display: none; }
  :host([floating]) .panel { display: none; width: min(420px, calc(100vw - 32px));
                             max-height: min(70vh, 560px); overflow: auto;
                             box-shadow: 0 8px 28px var(--mv-shadow); background: var(--mv-bg); }
  :host([floating][open]) .panel { display: block; }
  :host([floating]) .fab { display: inline-flex; }
  .fab { display: none; align-items: center; gap: 6px; margin-top: 8px; float: right;
         padding: 7px 13px; border-radius: 999px; border: 1px solid var(--mv-control-border);
         background: var(--mv-bg); color: var(--mv-fg); cursor: pointer; font: inherit;
         box-shadow: 0 2px 8px var(--mv-shadow); }

  .panel { border: 1px solid var(--mv-border); border-radius: 8px; overflow: hidden;
           background: var(--mv-bg); }
  header { display: flex; align-items: center; gap: 8px; padding: 7px 10px;
           background: var(--mv-surface); color: var(--mv-fg);
           border-bottom: 1px solid var(--mv-border-soft); font-weight: 600; }
  header .close { margin-left: auto; border: 0; background: none; cursor: pointer;
                  font-size: 16px; line-height: 1; color: var(--mv-muted); }
  :host(:not([floating])) header .close { display: none; }

  .body { display: flex; min-height: 150px; }
  @media (max-width: 520px) { .body { flex-direction: column; } }

  .tools { flex: 0 1 auto; min-width: 120px; max-width: 200px;
           border-right: 1px solid var(--mv-border-soft); overflow-y: auto; }
  @media (max-width: 520px) { .tools { flex: none; max-width: none; border-right: 0;
                                       border-bottom: 1px solid var(--mv-border-soft); } }
  /* Tool names are arbitrary identifiers; long ones must wrap inside the column
     rather than spill over the divider. */
  .tools button { display: block; width: 100%; text-align: left; padding: 6px 10px;
                  border: 0; background: none; color: var(--mv-fg); cursor: pointer;
                  font: inherit; font-family: ui-monospace, monospace; font-size: 12px;
                  overflow-wrap: anywhere; border-bottom: 1px solid var(--mv-border-soft); }
  .tools button:hover { background: var(--mv-hover); }
  .tools button[aria-current="true"] { background: var(--mv-selected); font-weight: 600; }

  .form { flex: 1; padding: 10px; min-width: 0; }
  .desc { color: var(--mv-muted); margin: 0 0 8px; }
  label { display: block; margin-bottom: 7px; }
  .name { font-family: ui-monospace, monospace; font-size: 12px; }
  .req { color: var(--mv-err-fg); }
  .hint { color: var(--mv-faint); font-size: 11px; }
  /* color/background are required, not decorative: form controls do not inherit
     them, so without these the UA picks per-theme defaults and text can render
     white on white. */
  input, select, textarea { width: 100%; box-sizing: border-box; font: inherit; font-size: 12px;
                            color: var(--mv-fg); background: var(--mv-input-bg); padding: 4px 6px;
                            border: 1px solid var(--mv-control-border); border-radius: 4px; }
  input[type="checkbox"] { width: auto; }
  textarea { font-family: ui-monospace, monospace; }
  .run { margin-top: 4px; padding: 5px 14px; border: 1px solid var(--mv-accent);
         border-radius: 4px; background: var(--mv-accent); color: var(--mv-accent-fg);
         cursor: pointer; font: inherit; }
  .run:disabled { opacity: .6; cursor: default; }
  pre { margin: 8px 0 0; padding: 7px; color: var(--mv-fg); background: var(--mv-surface);
        border: 1px solid var(--mv-border-soft); border-radius: 4px; font-size: 12px;
        white-space: pre-wrap; word-break: break-word; max-height: 180px; overflow: auto; }
  pre.error { background: var(--mv-err-bg); border-color: var(--mv-err-border); color: var(--mv-err-fg); }
  .field-error { color: var(--mv-err-fg); font-size: 11px; }
  .empty { padding: 20px; text-align: center; color: var(--mv-faint); }
`;class Ze extends HTMLElement{#e=null;#t=null;#s=()=>this.#i();static get observedAttributes(){return["floating","open","theme","position"]}constructor(){super(),this.attachShadow({mode:"open"})}connectedCallback(){this.shadowRoot.innerHTML=`<style>${Xe}</style><div id="root"></div>`,globalThis.window.addEventListener(R,this.#s),this.getAttribute("hotkey")&&globalThis.window.addEventListener("keydown",this.#o),this.#i()}disconnectedCallback(){globalThis.window.removeEventListener(R,this.#s),globalThis.window.removeEventListener("keydown",this.#o)}#o=e=>{const t=(this.getAttribute("hotkey")||"").toLowerCase().split("+").map(n=>n.trim()),s=t.at(-1);if(!s)return;const r=n=>t.includes(n);e.key.toLowerCase()!==s||e.ctrlKey!==r("ctrl")||e.shiftKey!==r("shift")||e.altKey!==r("alt")||e.metaKey!==(r("meta")||r("cmd"))||(e.preventDefault(),this.hasAttribute("open")?this.hide():this.show())};attributeChangedCallback(){this.shadowRoot?.getElementById("root")&&this.#i()}get theme(){return this.getAttribute("theme")}set theme(e){e==null?this.removeAttribute("theme"):this.setAttribute("theme",e)}show(){this.setAttribute("open","")}hide(){this.removeAttribute("open")}get#r(){return globalThis.navigator?.modelContext??null}get#n(){return this.#r?.tools??[]}#i(){const e=this.shadowRoot?.getElementById("root");if(!e)return;const t=this.#r,s=!!(t&&Array.isArray(t.tools)&&typeof t.callTool=="function"),r=s?this.#n:[];this.#e&&!r.some(a=>a.name===this.#e)&&(this.#e=null),this.#e??=r[0]?.name??null;const n=r.find(a=>a.name===this.#e)??null;e.innerHTML=`
      <div class="panel">
        <header>WebMCP Inspector<button class="close" title="Close">&times;</button></header>
        ${s?r.length===0?'<div class="empty">No tools registered.</div>':`<div class="body">
                   <div class="tools">${r.map(a=>`<button data-tool="${v(a.name)}" aria-current="${a.name===this.#e}">${y(a.name)}</button>`).join("")}</div>
                   <div class="form">${this.#a(n)}</div>
                 </div>`:`<div class="empty">navigator.modelContext is unavailable here.<br>
                 <span class="hint">Needs a secure context, and tool discovery requires the machvive polyfill.</span></div>`}
      </div>
      <button class="fab" title="WebMCP Inspector">&#128295; WebMCP</button>
    `,this.#l(e)}#a(e){if(!e)return"";const t=e.inputSchema??{},s=t.properties??{},r=new Set(t.required??[]),n=Object.keys(s),a=n.length?n.map(i=>this.#c(i,s[i],r.has(i))).join(""):'<p class="hint">This tool takes no parameters.</p>';return`
      ${e.description?`<p class="desc">${y(e.description)}</p>`:""}
      <form>
        ${a}
        <button type="submit" class="run">Execute</button>
      </form>
      ${this.#t?`<pre class="${this.#t.isError?"error":""}">${y(this.#t.text)}</pre>`:""}
    `}#c(e,t={},s){const r=`<span class="name">${y(e)}</span>${s?' <span class="req" title="required">*</span>':""}${t.description?` <span class="hint">— ${y(t.description)}</span>`:""}`;let n;return Array.isArray(t.enum)?n=`<select data-field="${v(e)}">${s?"":'<option value=""></option>'}${t.enum.map(a=>`<option value="${v(a)}">${y(a)}</option>`).join("")}</select>`:t.type==="boolean"?n=`<input type="checkbox" data-field="${v(e)}">`:t.type==="object"||t.type==="array"?n=`<textarea rows="3" data-field="${v(e)}" placeholder="JSON"></textarea>`:n=`<input type="${t.type==="number"||t.type==="integer"?"number":"text"}"${t.type==="integer"?' step="1"':""} data-field="${v(e)}">`,`<label>${r}${n}<span class="field-error" data-error="${v(e)}"></span></label>`}#l(e){e.querySelector(".fab")?.addEventListener("click",()=>this.hasAttribute("open")?this.hide():this.show()),e.querySelector("header .close")?.addEventListener("click",()=>this.hide()),e.querySelectorAll("[data-tool]").forEach(t=>t.addEventListener("click",()=>{this.#e=t.dataset.tool,this.#t=null,this.#i()})),e.querySelector("form")?.addEventListener("submit",t=>{t.preventDefault(),this.#d(e)})}async#d(e){const t=this.#n.find(c=>c.name===this.#e);if(!t)return;const s=t.inputSchema?.properties??{},r=new Set(t.inputSchema?.required??[]),n={};let a=!1;e.querySelectorAll("[data-error]").forEach(c=>c.textContent="");for(const[c,l]of Object.entries(s)){const h=e.querySelector(`[data-field="${CSS.escape(c)}"]`);if(!h)continue;const d=e.querySelector(`[data-error="${CSS.escape(c)}"]`);try{const m=Ye(h,l);if(m===void 0){r.has(c)&&(d&&(d.textContent="required"),a=!0);continue}n[c]=m}catch(m){d&&(d.textContent=String(m.message??m)),a=!0}}if(a)return;const i=e.querySelector(".run");i&&(i.disabled=!0);try{const c=await this.#r.callTool(t.name,n),l=(c?.content??[]).map(h=>h?.text??JSON.stringify(h)).join(`
`);this.#t={text:l||JSON.stringify(c,null,2),isError:!!c?.isError}}catch(c){this.#t={text:String(c?.message??c),isError:!0}}finally{i&&(i.disabled=!1)}this.#i()}}function y(o){return String(o).replace(/[&<>"]/g,e=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"})[e])}const v=y;customElements.get("machvive-webmcp-inspect")||customElements.define("machvive-webmcp-inspect",Ze);const me={text:"string",email:"string",phone:"string",single_select:"string",multi_select:"string"},_=o=>Object.hasOwn(me,o.step_type),Qe=o=>new Promise(e=>setTimeout(e,o));async function et(o=15e3){const e=Date.now()+o;for(;Date.now()<e;){const t=globalThis.window?.MachFiveMagnet,s=globalThis.window?.machfivemagnet;if(t&&Array.isArray(t.instances)&&t.instances.length&&s?.magnets?.length)return{api:t,config:s};await Qe(150)}return null}function se(o){const e=tt(o).filter(i=>i?.step_type==="scheduler"),t=(o.mag_actions??[]).find(i=>i?.type==="book");if(!e.length&&!t)return null;const s=t?.config?.source??null,r=/^https:\/\/(outlook\.office(365)?\.com|([a-z0-9-]+\.)?cal\.com)\//i,n=t?.value??"";let a=null;return e.some(i=>i.step_config?.remote==="ms_bookings")||s==="calendly"||s==="ms_bookings"||r.test(n)?a="calendar":t&&/^https?:\/\//i.test(n)?a="external":(e.length||t)&&(a="preferred_time"),{kind:a,url:a==="external"?n:null,source:s??(a==="calendar"?"a calendar":null),eventName:t?.config?.event_name??null}}function tt(o){const e=o.mag_routes??{};return[o.mag_macro_steps,e.chat,e.book,e.support].filter(Array.isArray).flat()}function pe(o){return(o.mag_macro_steps??[]).filter(e=>e.step_type!=="message"&&(e.step_columns??[]).length)}const oe=o=>!!o.mag_home&&(o.mag_actions??[]).length>0;function st(o){const e={};for(const t of pe(o).filter(_)){const s=t.step_columns[0],r={type:me[t.step_type],description:t.step_prompts?.[0]??s},n=(t.step_options??[]).map(a=>a.label).filter(Boolean);n.length&&(r.enum=n),t.step_type==="email"&&(r.format="email"),e[s]=r}return{type:"object",properties:e}}class ot extends HTMLElement{#e=null;#t=null;#s=null;#o=[];#r={};#n=[];static get observedAttributes(){return["app-guid","src","magnet-id"]}constructor(){super(),this.attachShadow({mode:"open"})}async connectedCallback(){this.shadowRoot.innerHTML="<style>:host { display: none; }</style>",await this.#i()}disconnectedCallback(){for(const e of this.#n)globalThis.navigator?.modelContext?.unregisterTool(e);this.#n=[]}get config(){return this.#t}get captures(){return[...this.#o]}async#i(){this.#a();const e=await et();if(!e){console.warn("machvive-m5t-magnet: the magnet runtime did not load; no tools registered. Common causes: the origin is not in the magnet's allowed domains, or src/app-guid is wrong."),this.dispatchEvent(new CustomEvent("magnet-error",{detail:{reason:"runtime-unavailable"},bubbles:!0,composed:!0}));return}this.#e=e.api;const t=this.getAttribute("magnet-id");if(this.#t=t?e.config.magnets.find(n=>n.mag_id===t):e.config.magnets.find(n=>n.mag_home)??e.config.magnets[0],!this.#t){console.warn(`machvive-m5t-magnet: no magnet "${t}" in this app.`);return}const s=this.#t.mag_id??null;this.#s=this.#e.instances.find(n=>(n.id??null)===s)??this.#e.instances[0];const r=n=>!n?.magnet||n.magnet===this.#t.mag_id||n.magnet===this.#t.mag_guid;for(const n of["open","engage","route","step","cta","call_tap","capture"])this.#e.on?.(n,a=>{r(a)&&(this.#r={...this.#r,[n]:a?.detail??!0,last:n},n==="capture"&&(this.#o.push({at:new Date().toISOString(),...a}),this.dispatchEvent(new CustomEvent("magnet-capture",{detail:a,bubbles:!0,composed:!0}))))});this.#l(),this.dispatchEvent(new CustomEvent("magnet-ready",{detail:{magnetId:this.#t.mag_id,tools:[...this.#n]},bubbles:!0,composed:!0}))}#a(){if(globalThis.window?.machfivemagnet||document.querySelector('script[src*="coreSnippet"]'))return;const e=this.getAttribute("app-guid"),t=this.getAttribute("src");if(!t||!e){console.warn("machvive-m5t-magnet: to load the magnet, set both src and app-guid — or add the magnet snippet to the page yourself and this element will use it.");return}const s=document.createElement("script");s.src=`${t}?appguid=${encodeURIComponent(e)}`,s.async=!0,s.onerror=()=>{console.warn(`machvive-m5t-magnet: could not load the magnet from ${t}. Check that this origin (${globalThis.location?.origin}) is in the magnet's allowed domains.`),this.dispatchEvent(new CustomEvent("magnet-error",{detail:{reason:"snippet-load-failed",origin:globalThis.location?.origin,src:t},bubbles:!0,composed:!0}))},document.head.append(s)}#c(e){globalThis.navigator?.modelContext?.registerTool(e),this.#n.push(e.name)}#l(){const e=this.#t,t=pe(e),s=st(e),r=Object.keys(s.properties),n=se(e),a=oe(e);this.#c({name:"magnet_describe",description:`Describe the "${e.mag_title??e.mag_id}" enquiry form: what it asks, what it collects, and whether it can schedule a meeting. Call this first.`,inputSchema:{type:"object",properties:{}},execute:()=>{const i=[`${e.mag_title??e.mag_id}${e.mag_subtitle?` — ${e.mag_subtitle}`:""}`,e.mag_welcome??"","","It asks:"];for(const d of t){const m=(d.step_options??[]).map(u=>u.label).filter(Boolean);i.push(`- ${d.step_columns[0]} (${d.step_type})${d.step_prompts?.[0]?`: "${d.step_prompts[0]}"`:""}`+(m.length?`
    choices: ${m.join(" | ")}`:""))}const c=t.filter(d=>!_(d));if(c.length){i.push("","The visitor completes these in the widget — you cannot pre-answer them:");for(const d of c)i.push(`- ${d.step_columns[0]} (${d.step_type})`+(d.step_type==="scheduler"?" — a calendar; they pick the slot themselves":""))}const l=n?.eventName?`"${n.eventName}"`:"a meeting";n?.kind==="calendar"?i.push("",`It books ${l}${n.source?` through ${n.source}`:""} on a real calendar, inside the widget. The visitor picks the slot — you cannot book for them.`):n?.kind==="external"?i.push("",`It can book ${l} by sending the visitor to ${n.url}. That opens separately, so neither you nor this page can see whether they went through with it.`):n?.kind==="preferred_time"?i.push("","It asks for a preferred time but books nothing on a calendar — the time is captured with the lead for someone to follow up."):i.push("","It cannot schedule a meeting; it collects details for a follow-up by email.");for(const d of e.mag_actions??[])d?.type==="call"&&d.value&&i.push(`Phone: ${d.value}`),d?.type==="email"&&d.value&&i.push(`Email: ${d.value}`);a&&i.push("","This magnet opens on a welcome card with buttons. Answers are cleared when the visitor taps one, so anything you prefill will not survive — describe what you know to the visitor instead of relying on magnet_start to carry it.");const h=[...new Set((e.mag_actions??[]).map(d=>d?.type).filter(Boolean))];return h.length&&i.push(`Entry points: ${h.join(", ")}.`),i.push("","Use magnet_start to fill in what you know. The visitor reviews and submits — you cannot submit for them."),i.join(`
`)}}),t.some(i=>(i.step_options??[]).length)&&this.#c({name:"magnet_options",description:"List the allowed choices for one of the form's multiple-choice fields.",inputSchema:{type:"object",properties:{field:{type:"string",description:"Field name",enum:t.filter(i=>(i.step_options??[]).length).map(i=>i.step_columns[0])}},required:["field"]},execute:({field:i})=>{const c=t.find(h=>h.step_columns[0]===i);if(!c)return`No such field: ${i}. Try magnet_describe.`;const l=(c.step_options??[]).map(h=>h.label);return l.length?`${i}: ${l.join(" | ")}`:`${i} is free text.`}}),this.#c({name:"magnet_start",description:`Open the enquiry form with answers filled in, so the visitor can review and submit. Known fields: ${r.join(", ")}. This does NOT submit — the visitor confirms. Fields you leave out are asked of the visitor. Extra keys beyond the known fields are allowed and travel with the lead, which is how context like a campaign or source id is passed through.`,inputSchema:s,execute:(i={})=>{if(!this.#s)return{content:[{type:"text",text:"The magnet is not loaded."}],isError:!0};const c=Object.keys(i).filter(u=>!r.includes(u));for(const u of t.filter(_)){const f=i[u.step_columns[0]],g=(u.step_options??[]).map(S=>S.label);if(f!=null&&f!==""&&g.length&&!g.includes(f))return{content:[{type:"text",text:`"${f}" is not a choice for ${u.step_columns[0]}. Options: ${g.join(" | ")}.`}],isError:!0}}const l=Object.fromEntries(Object.entries(i).filter(([,u])=>u!=null&&u!=="").map(([u,f])=>[u,String(f)]));this.#s.open({prefill:l,reset:!0});const h=Object.keys(l),d=r.filter(u=>!h.includes(u)),m=t.filter(u=>!_(u)).map(u=>u.step_columns[0]);return`Opened the enquiry form for the visitor${h.length?` with ${h.join(", ")} filled in`:""}.`+(c.length?` Passed through with the lead: ${c.join(", ")}.`:"")+(d.length?` They will be asked for: ${d.join(", ")}.`:"")+(m.length?` They complete in the widget: ${m.join(", ")}.`:"")+(oe(e)?" Note: this magnet clears answers when the visitor taps a button, so the prefill may not survive.":"")+" They must review and submit it themselves."}}),this.#c({name:"magnet_status",description:"Check how far the visitor has got with the enquiry form: whether it has been opened, engaged with, which step they are on, and whether they submitted.",inputSchema:{type:"object",properties:{}},execute:()=>{if(this.#o.length){const l=se(e)?.kind==="calendar";return`Submitted at ${this.#o.at(-1).at}.`+(l?" Whether the booking itself completed is not reported by the widget.":"")}const i=this.#r;if(!i.last)return"Not opened yet.";const c=[];return i.step&&c.push(`on step ${i.step}`),i.route&&c.push(`took the "${i.route}" route`),i.engage?c.push("has interacted"):i.open&&c.push("opened but not yet engaged"),`Not submitted. The visitor ${c.join(", ")||"has opened it"}.`}}),globalThis.window?.dispatchEvent?.(new CustomEvent(R,{detail:{tools:globalThis.navigator?.modelContext?.tools??[]}}))}}customElements.get("machvive-m5t-magnet")||customElements.define("machvive-m5t-magnet",ot);const B="machvive-webmcp-call",rt="machvive-webmcp",M="calls",nt=1,it=500;class at{#e=null;get available(){return!!globalThis.indexedDB}#t(){return this.available?(this.#e??=new Promise(e=>{let t;try{t=globalThis.indexedDB.open(rt,nt)}catch{return e(null)}t.onupgradeneeded=()=>{const s=t.result;s.objectStoreNames.contains(M)||s.createObjectStore(M,{keyPath:"id"})},t.onsuccess=()=>e(t.result),t.onerror=()=>e(null),t.onblocked=()=>e(null)}),this.#e):Promise.resolve(null)}async#s(e,t){const s=await this.#t();return s?new Promise(r=>{let n;try{n=s.transaction(M,e)}catch{return r(null)}const a=t(n.objectStore(M));n.oncomplete=()=>r(a?a.result:null),n.onerror=()=>r(null),n.onabort=()=>r(null)}):null}all(){return this.#s("readonly",e=>e.getAll()).then(e=>e??[])}put(e){return this.#s("readwrite",t=>t.put(e))}delete(e){return this.#s("readwrite",t=>t.delete(e))}clear(){return this.#s("readwrite",e=>e.clear())}}let ct=0;const re=()=>`call-${Date.now().toString(36)}-${(ct++).toString(36)}`;function D(o){if(o!==void 0)try{return JSON.parse(JSON.stringify(o))}catch{return String(o)}}class lt{#e=new Set;#t=[];#s;#o;#r=new at;ready;constructor({limit:e=it,persist:t=!0}={}){this.#s=e,this.#o=t,this.ready=this.#c()}get persistent(){return this.#o&&this.#r.available}addEventListener(e,t){e==="change"&&typeof t=="function"&&this.#e.add(t)}removeEventListener(e,t){e==="change"&&this.#e.delete(t)}get entries(){return[...this.#t]}get size(){return this.#t.length}add(e){const t={id:re(),...e};this.#t.push(t);let s=[];return this.#t.length>this.#s&&(s=this.#t.splice(0,this.#t.length-this.#s)),this.#i(r=>{r.put(t);for(const n of s)r.delete(n.id)}),this.#n("add",t),t}update(e,t){const s=this.#t.find(r=>r.id===e);return s?(Object.assign(s,t),this.#i(r=>r.put(s)),this.#n("update",s),s):null}remove(e){const t=this.#t.findIndex(r=>r.id===e);if(t===-1)return!1;const[s]=this.#t.splice(t,1);return this.#i(r=>r.delete(s.id)),this.#n("remove",s),!0}clear(){this.#t=[],this.#i(e=>e.clear()),this.#n("clear",null)}toJSON(e=2){return JSON.stringify({version:1,exportedAt:new Date().toISOString(),entries:this.#t},null,e)}import(e){const t=typeof e=="string"?JSON.parse(e):e,s=Array.isArray(t)?t:t?.entries;if(!Array.isArray(s))throw new TypeError("Analytics: import expects an entries array");for(const r of s){const n={...r,id:r.id??re()};this.#t.push(n),this.#i(a=>a.put(n))}return this.#n("import",null),s.length}async replay(e,t){const s=this.#t.find(n=>n.id===e);if(!s)throw new Error(`Analytics: no captured call ${e}`);const r=globalThis.navigator?.modelContext;if(!r?.callTool)throw new Error("Analytics: navigator.modelContext.callTool is unavailable");return r.callTool(s.tool,t??s.params??{})}pushToDataLayer(e){const t=this.#t.find(r=>r.id===e);return t?((globalThis.window.dataLayer||=[]).push({event:"webmcp_tool_call",webmcp_tool:t.tool,webmcp_status:t.status,webmcp_duration_ms:t.durationMs,webmcp_params:t.params,webmcp_error:t.error??void 0}),this.update(e,{pushedToDataLayer:!0}),!0):!1}#n(e,t){const s={reason:e,entry:t,entries:this.entries};for(const r of this.#e)try{r({type:"change",detail:s})}catch{}try{globalThis.window?.dispatchEvent?.(new CustomEvent(B,{detail:{reason:e,entry:t}}))}catch{}}#i(e){this.persistent&&this.#a(e).catch(()=>{})}async#a(e){const t=[];e({put:s=>t.push(["put",s]),delete:s=>t.push(["delete",s]),clear:()=>t.push(["clear"])});for(const[s,r]of t)await this.#r[s](r)}async#c(){if(this.persistent)try{const e=await this.#r.all();if(!Array.isArray(e)||e.length===0)return;const t=new Set(this.#t.map(r=>r.id)),s=[...e.filter(r=>!t.has(r.id)),...this.#t];this.#t=s.slice(-this.#s),this.#n("restore",null)}catch{}}}const fe=new lt;function ne(o,e){if(!o||typeof o!="object"||typeof o.execute!="function"||o.execute.__machviveWrapped)return o;const t=o.execute,s=async function(r,n){const a=new Date().toISOString(),i=Date.now(),c=l=>{try{e.add(l)}catch{}};try{const l=await t.call(this,r,n);return c({tool:o.name,params:D(r),result:D(l),status:"ok",startedAt:a,durationMs:Date.now()-i}),l}catch(l){throw c({tool:o.name,params:D(r),error:String(l?.message??l),status:"error",startedAt:a,durationMs:Date.now()-i}),l}};return s.__machviveWrapped=!0,{...o,execute:s}}let ie=!1;function dt(o=fe){const e=globalThis.navigator?.modelContext;if(!e||ie)return!1;e.tools?.length&&console.warn(`WebMCP analytics: ${e.tools.length} tool(s) were registered before analytics loaded and will not be captured. Import the analytics module earlier.`);const t=e.registerTool.bind(e);if(e.registerTool=s=>t(ne(s,o)),typeof e.provideContext=="function"){const s=e.provideContext.bind(e);e.provideContext=(r={})=>s({...r,tools:(r.tools??[]).map(n=>ne(n,o))})}return ie=!0,!0}const ht=`
  ${U}

  /* Painting the background is not optional: this component sets its own text
     colour per theme, so it cannot rely on inheriting a compatible surface from
     whatever page embeds it. */
  :host { display: block; font: 13px/1.5 system-ui, sans-serif;
          color: var(--mv-fg); background: var(--mv-bg); }
  :host([hidden]) { display: none; }
  .bar { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; margin-bottom: 8px; }
  .count { font-weight: 600; margin-right: auto; }
  /* color is required, not decorative: form controls do not inherit it, so
     without this the UA picks one per theme and white-on-white can result. */
  button { font: inherit; color: var(--mv-fg); padding: 3px 9px;
           border: 1px solid var(--mv-control-border); border-radius: 4px;
           background: var(--mv-bg); cursor: pointer; }
  button:hover { background: var(--mv-hover); }
  button.danger { color: var(--mv-danger); border-color: var(--mv-danger-border); }
  ol { list-style: none; margin: 0; padding: 0; border: 1px solid var(--mv-border);
       border-radius: 6px; max-height: 380px; overflow-y: auto; background: var(--mv-bg); }
  li { border-bottom: 1px solid var(--mv-border-soft); }
  li:last-child { border-bottom: 0; }
  .row { display: flex; gap: 8px; align-items: center; padding: 6px 10px; cursor: pointer; }
  .row:hover { background: var(--mv-hover); }
  .tool { font-family: ui-monospace, monospace; font-weight: 600; }
  .status { font-size: 11px; padding: 1px 6px; border-radius: 10px; }
  .status.ok { background: var(--mv-ok-bg); color: var(--mv-ok-fg); }
  .status.error { background: var(--mv-err-bg); color: var(--mv-err-fg); }
  .ms { color: var(--mv-muted); font-size: 11px; margin-left: auto; }
  .detail { padding: 8px 10px; background: var(--mv-surface);
            border-top: 1px solid var(--mv-border-soft); }
  .detail label { display: block; font-size: 11px; color: var(--mv-muted); margin: 6px 0 2px; }
  textarea { width: 100%; box-sizing: border-box; font-family: ui-monospace, monospace;
             font-size: 12px; color: var(--mv-fg); background: var(--mv-input-bg);
             border: 1px solid var(--mv-control-border); border-radius: 4px; padding: 5px; }
  pre { margin: 0; padding: 6px; color: var(--mv-fg); background: var(--mv-bg);
        border: 1px solid var(--mv-border-soft); border-radius: 4px;
        font-size: 12px; overflow-x: auto; white-space: pre-wrap; word-break: break-word; }
  .empty { padding: 20px; text-align: center; color: var(--mv-faint); }
  .err { color: var(--mv-err-fg); }
`;class ut extends HTMLElement{#e=fe;#t=null;#s=()=>this.#r();static get observedAttributes(){return["datalayer","theme"]}constructor(){super(),this.attachShadow({mode:"open"})}connectedCallback(){this.shadowRoot.innerHTML=`<style>${ht}</style><div id="root"></div>`,this.#e.addEventListener("change",this.#s),globalThis.window.addEventListener(B,this.#o),this.#r(),this.#e.ready?.then(()=>this.isConnected&&this.#r())}disconnectedCallback(){this.#e.removeEventListener("change",this.#s),globalThis.window.removeEventListener(B,this.#o)}get theme(){return this.getAttribute("theme")}set theme(e){e==null?this.removeAttribute("theme"):this.setAttribute("theme",e)}get log(){return this.#e}#o=e=>{this.hasAttribute("datalayer")&&(e.detail?.reason!=="add"||!e.detail.entry||this.#e.pushToDataLayer(e.detail.entry.id))};#r(){const e=this.shadowRoot?.getElementById("root");if(!e)return;const t=this.#e.entries.slice().reverse();e.innerHTML=`
      <div class="bar">
        <span class="count">${t.length} call${t.length===1?"":"s"}</span>
        <button data-act="export">Export</button>
        <button data-act="copy">Copy JSON</button>
        <button data-act="clear" class="danger">Clear</button>
      </div>
      ${t.length===0?'<div class="empty">No WebMCP calls captured yet.</div>':`<ol>${t.map(s=>this.#n(s)).join("")}</ol>`}
    `,e.querySelector(".bar").addEventListener("click",s=>this.#c(s)),e.querySelectorAll("li").forEach(s=>this.#i(s))}#n(e){const t=this.#t===e.id;return`
      <li data-id="${e.id}">
        <div class="row">
          <span class="tool">${I(e.tool??"(unknown)")}</span>
          <span class="status ${e.status}">${e.status}</span>
          <span class="ms">${e.durationMs??0}ms</span>
        </div>
        ${t?`<div class="detail">
                 <label>Params (editable — used on replay)</label>
                 <textarea rows="3" data-role="params">${I(JSON.stringify(e.params??{},null,2))}</textarea>
                 <label>${e.status==="error"?"Error":"Result"}</label>
                 <pre class="${e.status==="error"?"err":""}">${I(e.status==="error"?e.error??"":JSON.stringify(e.result??null,null,2))}</pre>
                 <div class="bar" style="margin-top:8px">
                   <button data-act="save">Save params</button>
                   <button data-act="replay">Replay</button>
                   <button data-act="push">Push to dataLayer</button>
                   <button data-act="remove" class="danger">Delete</button>
                 </div>
               </div>`:""}
      </li>
    `}#i(e){const t=e.dataset.id;e.querySelector(".row").addEventListener("click",()=>{this.#t=this.#t===t?null:t,this.#r()}),e.querySelectorAll("button[data-act]").forEach(s=>{s.addEventListener("click",r=>{r.stopPropagation(),this.#a(s.dataset.act,t,e)})})}async#a(e,t,s){const r=()=>{const n=s.querySelector('[data-role="params"]')?.value??"{}";try{return JSON.parse(n)}catch{return globalThis.window.alert("Params must be valid JSON."),null}};if(e==="save"){const n=r();n&&this.#e.update(t,{params:n})}else if(e==="replay"){const n=r();n&&await this.#e.replay(t,n)}else e==="push"?this.#e.pushToDataLayer(t):e==="remove"&&(this.#t===t&&(this.#t=null),this.#e.remove(t))}#c(e){const t=e.target.dataset?.act;t==="clear"?(this.#t=null,this.#e.clear()):t==="copy"?globalThis.navigator.clipboard?.writeText(this.#e.toJSON()):t==="export"&&this.#l()}#l(){const e=new Blob([this.#e.toJSON()],{type:"application/json"}),t=URL.createObjectURL(e),s=document.createElement("a");s.href=t,s.download=`webmcp-analytics-${Date.now()}.json`,s.click(),URL.revokeObjectURL(t)}}function I(o){return String(o).replace(/[&<>"]/g,e=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"})[e])}dt();customElements.get("machvive-webmcp-analytics")||customElements.define("machvive-webmcp-analytics",ut);const p=o=>document.querySelector(o),mt=()=>[...document.querySelectorAll("machvive-chat-syncopation")];p("#theme").addEventListener("change",o=>{const e=o.target.value;for(const t of mt())t.dataset.lockTheme||(e?t.setAttribute("theme",e):t.removeAttribute("theme"))});document.querySelectorAll("machvive-chat-syncopation[theme]").forEach(o=>{o.dataset.lockTheme="true"});const ge=(o,e)=>document.documentElement.style.setProperty(o,e);p("#accent").addEventListener("input",o=>ge("--mcs-accent",o.target.value));p("#radius").addEventListener("input",o=>ge("--mcs-radius",`${o.target.value}px`));p("#reset").addEventListener("click",()=>{document.documentElement.removeAttribute("style"),p("#accent").value="#1565c0",p("#radius").value="12",p("#theme").value="",p("#theme").dispatchEvent(new Event("change"))});function pt(){const o=Math.random();return o<.2?We({lang:"english",minWords:4,maxWords:9}):o<.75?$({lang:"english",sentences:-1}):$({lang:"english",sentences:3,paragraphs:2+Math.floor(Math.random()*2)})}async function*be(o){for(const e of pt().split(/(\s+)/))await new Promise(t=>setTimeout(t,/[.!?]$/.test(e)?110:16+Math.random()*22)),yield e}const T=p("#stream-services");T.registerTransport("lorem",be);p("#transport").addEventListener("change",o=>{for(const e of document.querySelectorAll("machvive-chat-syncopation-services"))o.target.value==="lorem"?e.registerTransport("lorem",be):e.config.transport="echo"});for(const o of["What does this collection actually give me?","How would I wire it to a real model?","And if I want it to run offline?"])T.conversation.add({role:"user",text:o}),T.conversation.add({role:"assistant",text:$({lang:"english",sentences:3,seed:o.length}),meta:{[k.SOURCE]:"mock"}});T.bus.on("daemon:idle",()=>{T.__stopped=!0});const H=p("#cli-services");customElements.whenDefined("machvive-chat-syncopation-cli").then(()=>{const o=H.querySelector("machvive-chat-syncopation-cli");o.register("order",{describe:"look up an order by number",run:e=>{const t=e.trim();return t?(H.conversation.add({role:"tool",text:`order ${t}: shipped 2 Oct, arriving 7 Oct`}),`looked up order ${t}`):"usage: /order <number>"}}),o.register("inspect",{describe:"emit a custom bus event the inspector will show",run:()=>(H.bus.emit("playground:custom",{from:"the CLI"}),"emitted playground:custom")})});const ft=p("#roles-services");await customElements.whenDefined("machvive-chat-syncopation-canvas");const gt=[{role:"system",text:"Session started. Cart contains 2 items."},{role:"user",text:"Can you summarise what I have in the cart?"},{role:"assistant",text:`Two items: a 12-cup kettle and a pack of filters.

The kettle ships today; the filters are backordered until Friday.`},{role:"tool",text:"cart.read() → { items: 2, total: 48.20 }"},{role:"assistant",text:"Still typing, as it happens",status:"pending"},{role:"assistant",text:"The model endpoint returned 503.",status:"error",meta:{[k.ERROR]:"service unavailable"}},{role:"assistant",text:"<img src=x onerror=alert(1)>"}];for(const o of gt)ft.conversation.add(ce(o));const ae=p("#instrument-services");ae.bus.on("record:added",o=>{ae.bus.emit("playground:counted",{role:o.role,length:o.text.length})});for(const o of document.querySelectorAll("main section h2")){const e=o.closest("section").id;o.insertAdjacentHTML("afterbegin",`<a class="anchor" href="#${e}" aria-label="Link to this section">#</a>`)}
