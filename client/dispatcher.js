/* eslint-disable no-console */
// client/dispatcher.js — event delegation for the SPA. Replaces the
// 60+ inline `onclick="X()"`, `onchange="fn(this.value)"`, etc.
// attributes with declarative `data-action` + `data-args` attributes.
//
// Why: removing onclick removes the need for `window.X = X` re-exports
// in client/main.js, which is a long list that has to be kept in
// sync with the actual module surface. With delegation, only ONE
// global listener is needed and the handler surface lives on the
// IE.* modules naturally.
//
// Wire-up (one-time, at module load): two listeners on `document` —
// one for click, one for change. Both look at the closest
// [data-action] ancestor and dispatch to the matching function on
// window.IE.<someModule>.<fn>.
//
// HTML contract:
//
//   <button data-action="doLogin">Login</button>
//   <button data-action="deleteCode" data-args="ABC123,completed">Delete</button>
//   <select data-action="setExportFilter" data-args="status,__value__">...</select>
//   <input type="checkbox" data-action="toggleAllVisibleCodes" data-args="__checked__">
//
// data-args is a CSV. Each cell is type-coerced:
//   "0"        → 0          (number)
//   "true"     → true       (boolean)
//   "false"    → false      (boolean)
//   "null"     → null
//   "anything" → "anything" (string)
//
// Two sentinels read from the element itself:
//   __value__  → el.value
//   __checked__→ el.checked
//
// Notes / limitations:
// - Keyboard handlers (onkeydown) stay inline. There's no clean
//   declarative way to express `if(event.key==='Enter') X()` and
//   there are only 2 of them. Documented in code-entry.js.

(function (root) {
  // ---- Type coercion for data-args cells ----
  // The full set of values we expect: numbers, true/false, null,
  // and arbitrary strings (with _esc already applied at render time
  // so the string is safe inside an HTML attribute).
  function coerce(v) {
    if (v == null) return v;
    if (v === '') return v;
    if (v === 'null') return null;
    if (v === 'undefined') return undefined;
    if (v === 'true') return true;
    if (v === 'false') return false;
    if (/^-?\d+(\.\d+)?$/.test(v)) return Number(v);
    return v;
  }

  // Split a CSV cell string into coerced values. Currently no
  // quoting (no arg value contains a comma), so a plain split is
  // enough. If quoting is ever needed, swap in a real CSV parser.
  function splitArgs(csv) {
    if (csv == null || csv === '') return [];
    return csv.split(',').map((s) => coerce(s.trim()));
  }

  // Sentinel resolver: replace __value__ and __checked__ sentinels
  // with the live element's value/checked. Other entries pass
  // through unchanged.
  function resolveSentinels(args, el) {
    return args.map((a) => {
      if (a === '__value__') return el.value;
      if (a === '__checked__') return el.checked;
      return a;
    });
  }

  // Find the action handler. The convention is: walk the IE.* modules
  // and return the first one that has a function with the given
  // name. We exclude 'util' (DOM helpers, not actions) and 'state'
  // (state mutators, not actions).
  function lookupAction(name) {
    const IE = root.IE;
    if (!IE) return null;
    for (const mod of Object.keys(IE)) {
      if (mod === 'util' || mod === 'state' || mod === 'dispatcher') continue;
      const fn = IE[mod] && IE[mod][name];
      if (typeof fn === 'function') return { mod, fn };
    }
    return null;
  }

  function dispatch(eventName, e) {
    const el = e.target.closest && e.target.closest('[data-action]');
    if (!el) return;
    const action = el.dataset.action;
    if (!action) return;
    const found = lookupAction(action);
    if (!found) {
      console.warn(`[dispatcher] no handler for action: ${action}`);
      return;
    }
    // Parse args. Prefer data-args="csv" but accept data-arg0, data-arg1,
    // ... as a fallback for handcrafted cases.
    let args;
    if (el.dataset.args !== undefined) {
      args = splitArgs(el.dataset.args);
    } else {
      args = [];
      for (let i = 0; i < 20; i += 1) {
        const v = el.dataset[`arg${i}`];
        if (v === undefined) break;
        args.push(coerce(v));
      }
    }
    args = resolveSentinels(args, el);
    // Some actions call preventDefault themselves; some don't. To stay
    // safe and predictable, do NOT preventDefault here — modules that
    // care (form submits, link clicks) can do it themselves or by
    // wrapping the element. This also keeps <a href="..."> working.
    try {
      const ret = found.fn.apply(null, args);
      // If the handler returned a Promise that rejects, log it.
      if (ret && typeof ret.then === 'function') {
        ret.catch((err) => {
          console.error(`[dispatcher] ${action} rejected:`, err);
        });
      }
    } catch (err) {
      console.error(`[dispatcher] ${action} threw:`, err);
    }
  }

  function onClick(e) { dispatch('click', e); }
  function onChange(e) { dispatch('change', e); }

  // Wire up at module load. We use capture-phase so the dispatcher
  // sees the event before any element-specific listeners (none
  // today, but the convention is useful for future debugging).
  document.addEventListener('click', onClick);
  document.addEventListener('change', onChange);

  root.IE = root.IE || {};
  root.IE.dispatcher = {
    coerce: coerce,
    splitArgs: splitArgs,
    resolveSentinels: resolveSentinels,
    lookupAction: lookupAction,
    onClick: onClick,
    onChange: onChange
  };
})(typeof window !== 'undefined' ? window : globalThis);
