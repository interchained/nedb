#!/usr/bin/env node
// nedb-inspector — deterministic embedder-code checker for NEDB durability.
//
//   nedb-inspector.mjs <pathToTarget.(rs|js|mjs|ts|py)> [more paths...]
//
// It reads how a program EMBEDS NEDB and warns LOUDLY, with the exact correct
// pattern, when a durable database is opened without flush-on-exit wiring — the
// "flush on every put? no; lose data on Ctrl+C? also no" mistake. It is the
// guardrail for the durable-mode auto-flush-on-exit contract.
//
// DETERMINISTIC BY DESIGN — no regex, no AI/LLM:
//   1. A per-language lexer masks comment and string contents (so a `Db::open`
//      inside a comment or a string literal is NEVER matched — the classic
//      regex false-positive), while preserving byte offsets and line numbers.
//   2. Structural token matching over the masked code finds durable-open call
//      sites and the wiring calls that make them safe.
//   3. A fixed rule table decides OK / INFO / WARN and prints the exact fix.
//
// Exit code: 0 = clean (no warnings), 1 = warnings found, 2 = usage/read error.
// Set NO_COLOR=1 for plain output. Importable: `import { inspect } from './nedb-inspector.mjs'`.
//
// © INTERCHAINED LLC × Claude Opus 4.8

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

// ── presentation ─────────────────────────────────────────────────────────────
const COLOR = !process.env.NO_COLOR && (process.stdout.isTTY ?? false);
const sgr = (c) => (s) => (COLOR ? `\x1b[${c}m${s}\x1b[0m` : String(s));
const bold = sgr('1'), dim = sgr('2'), red = sgr('31'), green = sgr('32');
const yellow = sgr('33'), cyan = sgr('36'), magenta = sgr('35');

// ── language table ─────────────────────────────────────────────────────────
const LANGS = {
  rust: { exts: ['.rs'], line: ['//'], block: [['/*', '*/']], nestBlock: true,
          quotes: ['"'], rustRaw: true },
  js:   { exts: ['.js', '.mjs', '.cjs', '.jsx', '.ts', '.tsx', '.mts', '.cts'],
          line: ['//'], block: [['/*', '*/']], nestBlock: false,
          quotes: ['"', "'", '`'] },
  py:   { exts: ['.py', '.pyi'], line: ['#'], block: [], nestBlock: false,
          quotes: ['"', "'"], pyTriple: true },
};

function langForFile(file) {
  const ext = path.extname(file).toLowerCase();
  for (const [name, cfg] of Object.entries(LANGS)) if (cfg.exts.includes(ext)) return name;
  return null;
}

const isIdent = (ch) => ch !== undefined && (
  (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
  (ch >= '0' && ch <= '9') || ch === '_');

// ── lexer: return { masked, strings } ────────────────────────────────────────
// `masked` is `src` with every comment and string body replaced by spaces
// (newlines preserved, so offsets and line numbers are unchanged). `strings` is
// the list of decoded-ish string-literal bodies with their start line — used to
// see import specifiers (which the mask would otherwise blank out).
function maskSource(src, lang) {
  const cfg = LANGS[lang];
  const out = new Array(src.length);
  for (let i = 0; i < src.length; i++) out[i] = src[i] === '\n' ? '\n' : ' ';
  const strings = [];
  let i = 0;
  const n = src.length;
  let line = 1;
  const lineAt = [];            // offset -> line (built lazily below is overkill; track inline)
  const bump = (from, to) => { for (let k = from; k < to; k++) if (src[k] === '\n') line++; };

  const starts = (tok, at) => src.startsWith(tok, at);

  while (i < n) {
    const ch = src[i];

    // line comments
    let matchedLine = null;
    for (const lc of cfg.line) if (starts(lc, i)) { matchedLine = lc; break; }
    if (matchedLine) {
      while (i < n && src[i] !== '\n') i++;
      continue;
    }

    // block comments (with optional nesting for rust)
    let matchedBlock = null;
    for (const [open, close] of cfg.block) if (starts(open, i)) { matchedBlock = [open, close]; break; }
    if (matchedBlock) {
      const [open, close] = matchedBlock;
      let depth = 1;
      bump(i, i + open.length); i += open.length;
      while (i < n && depth > 0) {
        if (cfg.nestBlock && starts(open, i)) { depth++; bump(i, i + open.length); i += open.length; continue; }
        if (starts(close, i)) { depth--; bump(i, i + close.length); i += close.length; continue; }
        if (src[i] === '\n') line++;
        i++;
      }
      continue;
    }

    // python triple-quoted strings
    if (cfg.pyTriple && (starts('"""', i) || starts("'''", i))) {
      const q = src.substr(i, 3);
      const startLine = line;
      bump(i, i + 3); i += 3;
      let body = '';
      while (i < n && !starts(q, i)) { if (src[i] === '\n') line++; body += src[i]; i++; }
      bump(i, i + 3); i += 3;
      strings.push({ value: body, line: startLine });
      continue;
    }

    // rust raw strings: r"...", r#"..."#, r##"..."##
    if (cfg.rustRaw && ch === 'r' && !isIdent(src[i - 1])) {
      let j = i + 1, hashes = 0;
      while (src[j] === '#') { hashes++; j++; }
      if (src[j] === '"') {
        const startLine = line;
        const closeTok = '"' + '#'.repeat(hashes);
        bump(i, j + 1); i = j + 1;
        let body = '';
        while (i < n && !starts(closeTok, i)) { if (src[i] === '\n') line++; body += src[i]; i++; }
        bump(i, i + closeTok.length); i += closeTok.length;
        strings.push({ value: body, line: startLine });
        continue;
      }
    }

    // ordinary quoted strings (with backslash escapes)
    if (cfg.quotes.includes(ch)) {
      const q = ch;
      const startLine = line;
      i++; // opening quote
      let body = '';
      while (i < n && src[i] !== q) {
        if (src[i] === '\\') { if (src[i + 1] === '\n') line++; i += 2; body += ' '; continue; }
        if (src[i] === '\n') { line++; if (q === '`') { body += '\n'; i++; continue; } else break; }
        body += src[i]; i++;
      }
      if (src[i] === q) i++; // closing quote
      strings.push({ value: body, line: startLine });
      continue;
    }

    if (ch === '\n') line++;
    out[i] = ch; // keep code char
    i++;
  }
  return { masked: out.join(''), strings };
}

// ── offset → line map ────────────────────────────────────────────────────────
function lineMapper(src) {
  const starts = [0];
  for (let i = 0; i < src.length; i++) if (src[i] === '\n') starts.push(i + 1);
  return (off) => {
    // binary search
    let lo = 0, hi = starts.length - 1;
    while (lo < hi) { const mid = (lo + hi + 1) >> 1; if (starts[mid] <= off) lo = mid; else hi = mid - 1; }
    return lo + 1;
  };
}

// Find call sites of `pattern` (a literal token sequence like "Db::open",
// ".open", "new NedbCore") in masked code: the sequence must appear with a
// non-identifier left boundary (unless it starts with a non-ident char) and be
// followed — after optional whitespace — by "(". Returns 1-based line numbers.
function findCallSites(masked, pattern, toLine) {
  const lines = [];
  const first = pattern[0];
  const needLeftBoundary = isIdent(first);
  let from = 0;
  for (;;) {
    const idx = masked.indexOf(pattern, from);
    if (idx === -1) break;
    from = idx + pattern.length;
    if (needLeftBoundary && isIdent(masked[idx - 1])) continue;
    // right boundary of the final identifier char, then optional ws, then "("
    let k = idx + pattern.length;
    const lastCharIsIdent = isIdent(pattern[pattern.length - 1]);
    if (lastCharIsIdent && isIdent(masked[k])) continue; // e.g. "opener(" when seeking ".open"
    while (k < masked.length && (masked[k] === ' ' || masked[k] === '\t' || masked[k] === '\n' || masked[k] === '\r')) k++;
    if (masked[k] === '(') lines.push(toLine(idx));
  }
  return lines;
}

// Presence of a bare identifier token (full-word) anywhere in masked code.
function hasIdent(masked, name) {
  let from = 0;
  for (;;) {
    const idx = masked.indexOf(name, from);
    if (idx === -1) return false;
    from = idx + name.length;
    if (!isIdent(masked[idx - 1]) && !isIdent(masked[idx + name.length])) return true;
  }
}

// Does `NEDB(` (python pure engine) carry an argument (→ durable path) rather
// than being empty `NEDB()` (→ in-memory)? Checked on the RAW source, one char
// past the paren, so a blanked string arg still counts.
function pyNedbHasArg(src, masked, toLine) {
  const lines = [];
  let from = 0;
  for (;;) {
    const idx = masked.indexOf('NEDB', from);
    if (idx === -1) break;
    from = idx + 4;
    if (isIdent(masked[idx - 1]) || isIdent(masked[idx + 4])) continue;
    let k = idx + 4;
    while (masked[k] === ' ' || masked[k] === '\t') k++;
    if (masked[k] !== '(') continue;
    k++;
    while (k < src.length && (src[k] === ' ' || src[k] === '\t' || src[k] === '\n' || src[k] === '\r')) k++;
    if (src[k] !== ')') lines.push(toLine(idx)); // has an argument
  }
  return lines;
}

// ── the fixes the inspector teaches ──────────────────────────────────────────
const FIX = {
  rust: [
    'use std::sync::Arc;',
    'use nedb_engine::Db;',
    '',
    'let db = Arc::new(Db::open(path, None)?);',
    'Db::install_exit_flush(Arc::clone(&db));  // flush buffered writes on SIGINT/SIGTERM',
  ].join('\n'),
  js: [
    "// Import the package entry — it arms process.on(SIGINT/SIGTERM) -> flush for you:",
    "import { NedbCore } from 'nedb-engine';",
    'const db = NedbCore.open(path);',
    '',
    '// If you must use the raw native binding, wire the exit flush yourself:',
    "process.on('SIGTERM', () => { db.flush(); process.exit(143); });",
    "process.on('SIGINT',  () => { db.flush(); process.exit(130); });",
    "process.on('exit', () => db.flush());",
  ].join('\n'),
  py: [
    '# nedb._native.NedbCore.open() arms a Python atexit flush for you (nedb >= 2.5.3).',
    '# But os._exit() SKIPS atexit — flush explicitly before it:',
    'db = NedbCore.open(path)',
    'db.flush()',
    'os._exit(0)',
  ].join('\n'),
};

// ── analysis ─────────────────────────────────────────────────────────────────
function analyze(src, lang) {
  const { masked, strings } = maskSource(src, lang);
  const toLine = lineMapper(src);
  const findings = [], infos = [], warnings = [];
  const strvals = strings.map((s) => s.value);

  if (lang === 'rust') {
    const opens = findCallSites(masked, 'Db::open', toLine);
    const mem = findCallSites(masked, 'Db::in_memory', toLine);
    const wired = hasIdent(masked, 'install_exit_flush');
    if (opens.length) {
      findings.push(`durable Db::open() at line ${opens.join(', ')}`);
      if (wired) infos.push('install_exit_flush(...) present — flush-on-exit wired');
      else warnings.push({
        code: 'RUST_NO_EXIT_FLUSH',
        lines: opens,
        msg: 'durable Db::open() with NO install_exit_flush — writes staged since the last flush are LOST on SIGINT/SIGTERM (Drop does not run on a signalled exit).',
        fix: FIX.rust,
      });
    } else if (mem.length) {
      infos.push('only Db::in_memory() — ephemeral, nothing to flush');
    }
  }

  else if (lang === 'js') {
    const importsPkg = strvals.includes('nedb-engine');
    // Import-specifier shapes only — NOT prose that merely contains "native".
    const importsNative = strvals.some((v) =>
      v.endsWith('.node') || v === './native' || v === './native.js' || v.endsWith('/native.js'));
    const durable = findCallSites(masked, 'NedbCore.open', toLine);
    const mem = findCallSites(masked, 'new NedbCore', toLine);
    const manualWired = findCallSites(masked, 'process.on', toLine).length > 0 && hasIdent(masked, 'flush');
    const optOut = hasIdent(masked, 'NEDB_NO_EXIT_FLUSH');
    if (durable.length) {
      findings.push(`durable NedbCore.open() at line ${durable.join(', ')}`);
      if (importsPkg && !importsNative) infos.push("imports 'nedb-engine' — durable open auto-arms flush-on-exit");
      else if (manualWired || optOut) infos.push('manual process.on(...) flush wiring detected');
      else warnings.push({
        code: 'JS_NO_EXIT_FLUSH',
        lines: durable,
        msg: 'durable open via the raw native binding with NO flush-on-exit wiring. Import from \'nedb-engine\' (its wrapper arms process.on -> flush) or wire it yourself.',
        fix: FIX.js,
      });
    } else if (mem.length) {
      infos.push('only new NedbCore() — in-memory, nothing to flush');
    }
  }

  else if (lang === 'py') {
    const nativeOpens = findCallSites(masked, 'NedbCore.open', toLine);
    const pureDurable = pyNedbHasArg(src, masked, toLine);
    const usesUnderExit = hasIdent(masked, 'os') && masked.includes('_exit');
    const atexitWired = hasIdent(masked, 'atexit') || (hasIdent(masked, 'signal') && hasIdent(masked, 'flush'));
    if (nativeOpens.length) {
      findings.push(`native NedbCore.open() at line ${nativeOpens.join(', ')}`);
      infos.push('native open arms a Python atexit flush (nedb >= 2.5.3)');
      if (usesUnderExit && !atexitWired) warnings.push({
        code: 'PY_OS_EXIT_BYPASS',
        lines: nativeOpens,
        msg: 'os._exit() bypasses atexit — the native auto-flush will NOT run. Call db.flush() explicitly before os._exit().',
        fix: FIX.py,
      });
    }
    if (pureDurable.length) {
      findings.push(`pure-Python NEDB(path) at line ${pureDurable.join(', ')}`);
      infos.push('pure-Python NEDB(path=...) is per-op fsync durable — no exit flush needed');
    }
  }

  return { language: lang, findings, infos, warnings, ok: warnings.length === 0 };
}

// Public, testable entry: analyze source text.
export function inspect(source, filename) {
  const lang = langForFile(filename);
  if (!lang) return { language: null, findings: [], infos: [], warnings: [], ok: true, skipped: true };
  return analyze(source, filename ? lang : lang);
}

// ── report ───────────────────────────────────────────────────────────────────
function report(file, res) {
  if (res.skipped) { console.log(`${dim('skip')} ${file} ${dim('(unsupported extension)')}`); return; }
  const tag = res.ok ? green('  OK ') : red(' WARN');
  console.log(`\n${tag} ${bold(file)} ${dim(`[${res.language}]`)}`);
  for (const f of res.findings) console.log(`   ${cyan('•')} ${f}`);
  for (const inf of res.infos) console.log(`   ${dim('· ' + inf)}`);
  for (const w of res.warnings) {
    console.log('');
    console.log(red(bold('   ┌─ NEDB DURABILITY WARNING ─────────────────────────────────')));
    console.log(red(bold(`   │ ${w.code}`)) + dim(`  (line ${w.lines.join(', ')})`));
    console.log(`   ${red('│')} ${yellow(w.msg)}`);
    console.log(red('   │'));
    console.log(`   ${red('│')} ${bold('Use this pattern:')}`);
    for (const ln of w.fix.split('\n')) console.log(`   ${red('│')}   ${green(ln)}`);
    console.log(red(bold('   └────────────────────────────────────────────────────────────')));
  }
}

function usage(code) {
  console.log(`${bold('nedb-inspector')} — deterministic NEDB durability checker

  ${cyan('nedb-inspector.mjs')} <path.(rs|js|mjs|ts|py)> [more paths...]

Warns when a durable NEDB database is opened without flush-on-exit wiring.
Exit: 0 clean · 1 warnings · 2 usage/read error. NO_COLOR=1 for plain output.`);
  process.exit(code);
}

function main(argv) {
  const files = argv.filter((a) => !a.startsWith('-'));
  if (argv.includes('-h') || argv.includes('--help') || files.length === 0) usage(files.length === 0 ? 2 : 0);
  console.log(bold(magenta('\nnedb-inspector')) + dim(' · durable-mode flush-on-exit guardrail'));
  let warned = 0, read = 0;
  for (const file of files) {
    let src;
    try { src = readFileSync(file, 'utf8'); }
    catch (e) { console.log(`${red('ERR ')} ${file} ${dim('(' + (e.code || e.message) + ')')}`); process.exitCode = 2; continue; }
    read++;
    const res = inspect(src, file);
    report(file, res);
    if (!res.ok) warned += res.warnings.length;
  }
  console.log('');
  if (warned > 0) { console.log(red(bold(`✗ ${warned} warning(s) across ${read} file(s)`))); process.exit(1); }
  else { console.log(green(bold(`✓ clean — ${read} file(s) inspected`))); process.exit(process.exitCode === 2 ? 2 : 0); }
}

// Run as CLI when invoked directly (not when imported by the test suite).
const invokedDirectly = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (invokedDirectly) main(process.argv.slice(2));
