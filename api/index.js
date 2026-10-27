// Nullified Casino game server, serverless edition (Vercel functions + Neon Postgres).
// Balances are kept in huEVO coins in Postgres; every game outcome is drawn here, never in the browser.
// There is no long-running process: every piece of shared state (Crash rounds, Duel rooms, the wins
// ticker, unfinished Mines and Blackjack rounds) lives in the database, and the page polls /api/poll.
//
// Players come in through huEVO: the game sends its signed hu_* launch params to /api/login.
// Coins enter through huEVO payments (POST /payments + the player's confirmation + our callback)
// and leave through payouts from the app wallet.
//
// Environment:
//   HUEVO_CLIENT_ID, HUEVO_CLIENT_SECRET  app keys from huEVO (never commit them)
//   DATABASE_URL       Neon Postgres connection string
//   HUEVO_API          default https://huevo.su/api/apps/v1
//   HUEVO_PAYOUT_PATH  payout method path, default /credits (pays the player from the app wallet)
//   HUEVO_VERIFY       strict | log - what to do when the launch signature does not check out
//   ALLOWED_ORIGINS    comma list of game origins allowed to call the API
"use strict";
const crypto = require("crypto");
const { Pool } = require("pg");

const CLIENT_ID = process.env.HUEVO_CLIENT_ID || "";
const CLIENT_SECRET = process.env.HUEVO_CLIENT_SECRET || "";
const HUEVO_API = (process.env.HUEVO_API || "https://huevo.su/api/apps/v1").replace(/\/$/, "");
const PAYOUT_PATH = process.env.HUEVO_PAYOUT_PATH || "/credits";
const VERIFY = process.env.HUEVO_VERIFY || "strict";
const ORIGINS = (process.env.ALLOWED_ORIGINS || "https://nullifiedmember.github.io,http://localhost:8765").split(",").map((s) => s.trim()).filter(Boolean);

const BET_MIN = 10, BET_MAX = 95000;
const DEPOSIT_MIN = 10, DEPOSIT_MAX = 1000000;
// the casino keeps 5% of every top-up and every withdrawal
const CASH_FEE = 0.05;
const afterFee = (amount) => Math.floor(amount * (1 - CASH_FEE));
// share of every bet that comes back to players on average (return to player)
const EDGE = 0.85;
// cases keep more: on average 75% of the price comes back
const COFFIN_EDGE = 0.75;

// ---------- randomness ----------
const rnd = () => crypto.randomBytes(6).readUIntBE(0, 6) / 2 ** 48;
function weighted(entries) {
  let total = 0;
  for (const [, w] of entries) total += w;
  let r = rnd() * total;
  for (const [v, w] of entries) if ((r -= w) < 0) return v;
  return entries[entries.length - 1][0];
}
const validBet = (b) => Number.isInteger(b) && b >= BET_MIN && b <= BET_MAX;
function httpErr(status, message, extra) { const e = new Error(message); e.status = status; e.extra = extra; return e; }

// ---------- database ----------
const DB_URL = process.env.DATABASE_URL || "";
const pool = DB_URL ? new Pool({ connectionString: DB_URL, ssl: /localhost|127\.0\.0\.1/.test(DB_URL) ? false : { rejectUnauthorized: false }, max: 3, idleTimeoutMillis: 10000 }) : null;
const DUEL_STAKES = [100, 500, 1000, 5000, 25000, 95000], DUEL_FEE = 1 - EDGE, DUEL_LEAD_MS = 2500;
const SCHEMA = `
CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY, name TEXT, balance BIGINT NOT NULL DEFAULT 0, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS payments (intent_id TEXT PRIMARY KEY, user_id BIGINT, amount BIGINT, credited BOOLEAN NOT NULL DEFAULT false, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS payouts (id BIGSERIAL PRIMARY KEY, user_id BIGINT, amount BIGINT, ok BOOLEAN, info TEXT, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS games (user_id BIGINT, kind TEXT, state JSONB, PRIMARY KEY (user_id, kind));
CREATE TABLE IF NOT EXISTS wins (id BIGSERIAL PRIMARY KEY, name TEXT, game TEXT, amount BIGINT, mult REAL, at BIGINT);
CREATE TABLE IF NOT EXISTS crash_rounds (id BIGINT PRIMARY KEY, bet_ends BIGINT, fly_start BIGINT, crash_at BIGINT, ends BIGINT, point NUMERIC(10,2), settled BOOLEAN NOT NULL DEFAULT false);
CREATE TABLE IF NOT EXISTS crash_bets (round_id BIGINT, user_id BIGINT, name TEXT, bet BIGINT, auto NUMERIC(10,2) NOT NULL DEFAULT 0, cashed NUMERIC(10,2), PRIMARY KEY (round_id, user_id));
ALTER TABLE crash_bets ADD COLUMN IF NOT EXISTS cashed_at BIGINT;
CREATE TABLE IF NOT EXISTS wheel_rounds (id BIGINT PRIMARY KEY, bet_ends BIGINT, ends BIGINT, seg INT, settled BOOLEAN NOT NULL DEFAULT false);
CREATE TABLE IF NOT EXISTS wheel_bets (round_id BIGINT, user_id BIGINT, name TEXT, pick INT, bet BIGINT, PRIMARY KEY (round_id, user_id, pick));
CREATE TABLE IF NOT EXISTS jp_rounds (id BIGINT PRIMARY KEY, bet_ends BIGINT, ends BIGINT, winner BIGINT, land DOUBLE PRECISION, settled BOOLEAN NOT NULL DEFAULT false);
CREATE TABLE IF NOT EXISTS jp_bets (round_id BIGINT, user_id BIGINT, name TEXT, bet BIGINT, at BIGINT, PRIMARY KEY (round_id, user_id));
CREATE TABLE IF NOT EXISTS duel_rooms (stake BIGINT PRIMARY KEY, user_id BIGINT, name TEXT, since BIGINT);
CREATE TABLE IF NOT EXISTS duel_events (id BIGSERIAL PRIMARY KEY, user_id BIGINT, data JSONB, seen BOOLEAN NOT NULL DEFAULT false);
INSERT INTO duel_rooms (stake) VALUES ${DUEL_STAKES.map((s) => "(" + s + ")").join(",")} ON CONFLICT DO NOTHING;`;
let ready = null;
function init() {
  if (!pool) return Promise.reject(httpErr(503, "База данных не подключена"));
  // two cold starts may race on CREATE TABLE; the loser simply retries once
  return (ready ||= pool.query(SCHEMA).catch(() => pool.query(SCHEMA)).catch((e) => { ready = null; throw e; }));
}
async function tx(fn) {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    const r = await fn(c);
    await c.query("COMMIT");
    return r;
  } catch (e) {
    await c.query("ROLLBACK").catch(() => {});
    throw e;
  } finally { c.release(); }
}
const num = (v) => (v === null || v === undefined ? null : Number(v));
// atomic change; null when it would go below zero
async function change(q, id, delta) {
  const r = await q.query(`UPDATE users SET balance = balance + $2 WHERE id = $1 AND balance + $2 >= 0 RETURNING balance`, [id, delta]);
  return r.rows[0] ? Number(r.rows[0].balance) : null;
}
async function balanceOf(q, id) { const r = await q.query(`SELECT balance FROM users WHERE id = $1`, [id]); return r.rows[0] ? Number(r.rows[0].balance) : 0; }
// per-player game state (free spins, an open Mines field, a Blackjack hand and shoe), locked for the transaction
async function loadGame(c, uid, kind) {
  await c.query(`INSERT INTO games (user_id, kind, state) VALUES ($1, $2, 'null') ON CONFLICT DO NOTHING`, [uid, kind]);
  const r = await c.query(`SELECT state FROM games WHERE user_id = $1 AND kind = $2 FOR UPDATE`, [uid, kind]);
  return r.rows[0].state;
}
const saveGame = (c, uid, kind, st) => c.query(`UPDATE games SET state = $3 WHERE user_id = $1 AND kind = $2`, [uid, kind, JSON.stringify(st)]);

// ---------- wins ticker ----------
async function pushWin(q, name, game, amount, mult) {
  await q.query(`INSERT INTO wins (name, game, amount, mult, at) VALUES ($1, $2, $3, $4, $5)`, [name, game, amount, mult || null, Date.now()]);
  if (rnd() < 0.02) await q.query(`DELETE FROM wins WHERE id < (SELECT max(id) - 200 FROM wins)`);
}
async function winsSince(since) {
  const r = await pool.query(`SELECT id, name, game, amount, mult FROM wins WHERE id > $1 ORDER BY id DESC LIMIT 20`, [Number(since) || 0]);
  return r.rows.map((w) => ({ id: Number(w.id), name: w.name, game: w.game, amount: Number(w.amount), mult: num(w.mult) }));
}

// ---------- huEVO API ----------
async function huevo(method, p, body) {
  const auth = "Basic " + Buffer.from(CLIENT_ID + ":" + CLIENT_SECRET).toString("base64");
  const res = await fetch(HUEVO_API + p, {
    method, headers: { Authorization: auth, "Content-Type": "application/json", Accept: "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  let j = {};
  try { j = await res.json(); } catch {}
  if (!res.ok || j.ok === false) {
    const e = new Error((j.error && j.error.message) || "huEVO HTTP " + res.status);
    e.code = (j.error && j.error.code) || "http_" + res.status;
    console.warn("[huevo]", method, p, res.status, JSON.stringify(j).slice(0, 400));
    throw e;
  }
  return j;
}

// Launch signature. huEVO does not publish the exact recipe, so the common variants are tried and the one
// that matches is logged; with HUEVO_VERIFY=log a mismatch is logged but allowed (test phase only).
const secretKeys = () => {
  const d = crypto.createHash("sha256").update(CLIENT_SECRET).digest();
  return [d, d.toString("hex"), CLIENT_SECRET];
};
function verifyLaunch(params) {
  const given = String(params.hu_hash || "").toLowerCase();
  const keys = Object.keys(params).filter((k) => k.startsWith("hu_") && k !== "hu_hash").sort();
  const data = [keys.map((k) => k + "=" + params[k]).join("&"), keys.map((k) => k + "=" + params[k]).join("\n"),
    new URLSearchParams(keys.map((k) => [k, params[k]])).toString()];
  if (given && CLIENT_SECRET) {
    for (const [ki, key] of secretKeys().entries()) for (const [di, s] of data.entries()) {
      const mac = crypto.createHmac("sha256", key).update(s).digest();
      if (mac.toString("hex") === given || mac.toString("base64url").toLowerCase() === given) return { ok: true, scheme: ki + "/" + di };
    }
  }
  return { ok: false };
}

// ---------- sessions: signed tokens, nothing kept in memory ----------
const sessionKey = () => crypto.createHash("sha256").update("nullified-session:" + CLIENT_SECRET).digest();
function makeToken(u) {
  const p = Buffer.from(JSON.stringify({ id: u.id, hid: u.hid, name: u.name, test: !!u.test, exp: Date.now() + 7 * 864e5 })).toString("base64url");
  return p + "." + crypto.createHmac("sha256", sessionKey()).update(p).digest("base64url");
}
function readToken(t) {
  const [p, s] = String(t || "").split(".");
  if (!p || !s || !CLIENT_SECRET) return null;
  const want = Buffer.from(crypto.createHmac("sha256", sessionKey()).update(p).digest("base64url"));
  const got = Buffer.from(s);
  if (got.length !== want.length || !crypto.timingSafeEqual(got, want)) return null;
  try {
    const u = JSON.parse(Buffer.from(p, "base64url").toString("utf8"));
    return u.exp > Date.now() ? { id: u.id, hid: u.hid || Math.abs(u.id), name: u.name, test: !!u.test } : null;
  } catch { return null; }
}

// ---------- slots ----------
const PAYS = { rott: [0, 0, 0, 65, 260, 1300], pug: [0, 0, 0, 45, 130, 650], poodle: [0, 0, 0, 32, 90, 440], guide: [0, 0, 0, 22, 65, 260],
  bone: [0, 0, 0, 17, 44, 175], A: [0, 0, 0, 11, 26, 90], K: [0, 0, 0, 11, 22, 77], Q: [0, 0, 0, 8, 17, 55], J: [0, 0, 0, 6, 13, 44], T: [0, 0, 0, 4, 11, 33] };
const BASE_WEIGHTS = { rott: 3, pug: 4, poodle: 5, guide: 6, bone: 8, A: 10, K: 10, Q: 12, J: 12, T: 12 };
const WILD_REELS = [1, 2, 3], SCATTER_REELS = [0, 2, 4], WILD_WEIGHT = 2.2, SCATTER_WEIGHT = 3.4, SCATTER_PAY = 5;
const FS_COUNTS = [[9, 35], [12, 30], [15, 20], [18, 10], [21, 5]];
// the reel weights and pay table were tuned for a 97% return; every slot win is scaled down to EDGE
const SLOT_SCALE = EDGE / 0.97;
const LINES = [[1,1,1,1,1],[0,0,0,0,0],[2,2,2,2,2],[0,1,2,1,0],[2,1,0,1,2],[1,0,0,0,1],[1,2,2,2,1],[0,0,1,2,2],[2,2,1,0,0],[1,2,1,0,1],
  [1,0,1,2,1],[0,1,1,1,0],[2,1,1,1,2],[0,1,0,1,0],[2,1,2,1,2],[1,1,0,1,1],[1,1,2,1,1],[0,0,2,0,0],[2,2,0,2,2],[0,2,2,2,0]];
function reelTable(reel, free) {
  const t = Object.entries(BASE_WEIGHTS);
  if (WILD_REELS.includes(reel)) t.push(["W", WILD_WEIGHT]);
  if (!free && SCATTER_REELS.includes(reel)) t.push(["S", SCATTER_WEIGHT]);
  return t;
}
const makeCell = (s) => (s === "W" ? { s, m: rnd() < 0.6 ? 2 : 3 } : { s });
function spinGrid(free, sticky) {
  const grid = [];
  for (let c = 0; c < 5; c++) {
    const col = [], table = reelTable(c, free);
    for (let r = 0; r < 3; r++) { const k = c + ":" + r; col.push(sticky && sticky[k] ? { ...sticky[k], sticky: true } : makeCell(weighted(table))); }
    grid.push(col);
  }
  return grid;
}
function evaluate(grid, totalBet) {
  const lineBet = totalBet / LINES.length, wins = [];
  LINES.forEach((line, li) => {
    const first = grid[0][line[0]];
    if (first.s === "S" || first.s === "W") return;
    let count = 1, mult = 0;
    const cells = [[0, line[0]]];
    for (let c = 1; c < 5; c++) {
      const cell = grid[c][line[c]];
      if (cell.s === first.s || cell.s === "W") { count++; if (cell.s === "W") mult += cell.m; cells.push([c, line[c]]); } else break;
    }
    mult = mult || 1;
    const pay = PAYS[first.s][count];
    if (pay) wins.push({ line: li, sym: first.s, count, mult, amount: Math.floor(pay * lineBet * mult * SLOT_SCALE), cells });
  });
  const scatters = [];
  grid.forEach((col, c) => col.forEach((cell, r) => { if (cell.s === "S") scatters.push([c, r]); }));
  const scatterWin = scatters.length >= 3 ? Math.floor(SCATTER_PAY * totalBet * SLOT_SCALE) : 0;
  return { wins, scatters, scatterWin, total: wins.reduce((a, w) => a + w.amount, 0) + scatterWin };
}
const slotsSpin = (u, body) => tx(async (c) => {
  const st = (await loadGame(c, u.id, "slots")) || { fsLeft: 0, fsWin: 0, bet: 0, sticky: null };
  const free = st.fsLeft > 0;
  const bet = free ? st.bet : Number(body.bet);
  if (!free) {
    if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
    if ((await change(c, u.id, -bet)) === null) throw httpErr(402, "Не хватает монэток на ставку");
  } else st.fsLeft--;
  const grid = spinGrid(free, free ? st.sticky : null);
  const res = evaluate(grid, bet);
  let fsAwarded = 0, fsEnded = false;
  if (free) {
    st.sticky = st.sticky || {};
    grid.forEach((col, ci) => col.forEach((cell, r) => { if (cell.s === "W") st.sticky[ci + ":" + r] = { s: "W", m: cell.m }; }));
    st.fsWin += res.total;
    if (st.fsLeft === 0) { fsEnded = true; st.sticky = null; }
  } else if (res.scatters.length >= 3) {
    fsAwarded = weighted(FS_COUNTS);
    Object.assign(st, { fsLeft: fsAwarded, fsWin: 0, bet, sticky: {} });
  }
  await saveGame(c, u.id, "slots", st);
  const balance = res.total ? await change(c, u.id, res.total) : await balanceOf(c, u.id);
  if (res.total > 0) await pushWin(c, u.name, "Slots", res.total);
  return { grid, ...res, bet, free, fsLeft: st.fsLeft, fsWin: st.fsWin, fsAwarded, fsEnded, balance };
});

// ---------- mines ----------
const MINE_TILES = 25;
function minesMult(picks, count) { let m = EDGE; for (let i = 0; i < picks; i++) m *= (MINE_TILES - i) / (MINE_TILES - count - i); return picks ? m : 1; }
const minesStart = (u, body) => tx(async (c) => {
  if (await loadGame(c, u.id, "mines")) throw httpErr(409, "Раунд уже идёт");
  const bet = Number(body.bet), count = Number(body.count);
  if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
  if (!Number.isInteger(count) || count < 1 || count > 24) throw httpErr(400, "Мин от 1 до 24");
  const balance = await change(c, u.id, -bet);
  if (balance === null) throw httpErr(402, "Не хватает монэток на ставку");
  const swords = new Set();
  while (swords.size < count) swords.add(crypto.randomInt(MINE_TILES));
  await saveGame(c, u.id, "mines", { bet, count, swords: [...swords], opened: [] });
  return { balance };
});
async function minesCashout(c, u, st) {
  await saveGame(c, u.id, "mines", null);
  const mult = minesMult(st.opened.length, st.count), win = Math.floor(st.bet * mult);
  const balance = await change(c, u.id, win);
  await pushWin(c, u.name, "Mines", win, mult);
  return { cashed: true, win, mult, swords: st.swords, opened: st.opened, balance };
}
const minesOpen = (u, body) => tx(async (c) => {
  const st = await loadGame(c, u.id, "mines"), i = Number(body.i);
  if (!st) throw httpErr(409, "Нет активного раунда");
  if (!Number.isInteger(i) || i < 0 || i >= MINE_TILES || st.opened.includes(i)) throw httpErr(400, "Неверная плита");
  if (st.swords.includes(i)) { await saveGame(c, u.id, "mines", null); return { boom: i, swords: st.swords, opened: st.opened, balance: await balanceOf(c, u.id) }; }
  st.opened.push(i);
  if (st.opened.length === MINE_TILES - st.count) return minesCashout(c, u, st);
  await saveGame(c, u.id, "mines", st);
  return { safe: i, opened: st.opened, mult: minesMult(st.opened.length, st.count), next: minesMult(st.opened.length + 1, st.count) };
});
const minesCash = (u) => tx(async (c) => {
  const st = await loadGame(c, u.id, "mines");
  if (!st || !st.opened.length) throw httpErr(409, "Сначала открой плиту");
  return minesCashout(c, u, st);
});

// ---------- blackjack ----------
const SUITS = [["♠", false], ["♥", true], ["♦", true], ["♣", false]];
const RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"];
function newShoe() {
  const shoe = [];
  for (let d = 0; d < 6; d++) for (const [s, red] of SUITS) for (const r of RANKS) shoe.push({ r, s, red });
  for (let i = shoe.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [shoe[i], shoe[j]] = [shoe[j], shoe[i]]; }
  return shoe;
}
function handValue(hand) {
  let total = 0, aces = 0;
  for (const c of hand) { if (c.r === "A") { aces++; total += 11; } else total += ["J", "Q", "K"].includes(c.r) ? 10 : Number(c.r); }
  while (total > 21 && aces) { total -= 10; aces--; }
  return total;
}
const isBJ = (h) => h.length === 2 && handValue(h) === 21;
const cardPoints = (c) => (c.r === "A" ? 11 : ["10", "J", "Q", "K"].includes(c.r) ? 10 : Number(c.r));
function bjDraw(st) { if (st.shoe.length < 60) st.shoe = newShoe(); return st.shoe.pop(); }
function bjView(st, balance) {
  const hide = st.phase === "player";
  return {
    phase: st.phase, active: st.active, balance,
    hands: st.hands.map((h) => ({ cards: h.cards, stake: h.stake, done: h.done, result: h.result || "", total: handValue(h.cards), splitAces: h.splitAces })),
    dealer: hide ? [st.dealer[0], null] : st.dealer,
    dealerTotal: hide ? handValue([st.dealer[0]]) : handValue(st.dealer),
    outcome: st.outcome || null,
  };
}
// the shoe is kept between hands; the round itself only while the player still has to act
function bjSave(c, u, st) {
  const { shoe, ...round } = st;
  return saveGame(c, u.id, "bj", { shoe, round: st.phase === "player" ? round : null });
}
async function bjFinish(c, u, st) {
  const d = handValue(st.dealer), dealerBJ = isBJ(st.dealer);
  let paid = 0, staked = 0;
  for (const h of st.hands) {
    const p = handValue(h.cards), natural = !h.fromSplit && st.hands.length === 1 && isBJ(h.cards);
    if (p > 21) h.result = "lose";
    else if (natural && !dealerBJ) h.result = "bj";
    else if (dealerBJ) h.result = natural ? "push" : "lose";
    else if (d > 21 || p > d) h.result = "win";
    else h.result = p === d ? "push" : "lose";
    paid += Math.floor({ bj: h.stake * 2.2, win: h.stake * 2, push: h.stake, lose: 0 }[h.result]);
    staked += h.stake;
  }
  st.phase = "done";
  st.outcome = { paid, staked, net: paid - staked };
  const balance = paid ? await change(c, u.id, paid) : await balanceOf(c, u.id);
  if (paid > staked) await pushWin(c, u.name, "Blackjack", paid);
  return balance;
}
async function bjAdvance(c, u, st) {
  for (;;) {
    const i = st.hands.findIndex((h) => !h.done);
    if (i === -1) {
      if (st.hands.some((h) => handValue(h.cards) <= 21)) while (handValue(st.dealer) < 17) st.dealer.push(bjDraw(st));
      return bjFinish(c, u, st);
    }
    st.active = i;
    const h = st.hands[i];
    if (h.cards.length === 1) h.cards.push(bjDraw(st));
    if (h.splitAces || handValue(h.cards) === 21) { h.done = true; continue; }
    return balanceOf(c, u.id);
  }
}
const bjDeal = (u, body) => tx(async (c) => {
  const g = (await loadGame(c, u.id, "bj")) || {};
  if (g.round) throw httpErr(409, "Раздача уже идёт");
  const bet = Number(body.bet);
  if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
  if ((await change(c, u.id, -bet)) === null) throw httpErr(402, "Не хватает монэток на ставку");
  const st = { shoe: g.shoe && g.shoe.length >= 60 ? g.shoe : newShoe(), hands: [], dealer: [], active: 0, phase: "player" };
  st.hands = [{ cards: [bjDraw(st), bjDraw(st)], stake: bet, fromSplit: false, splitAces: false, done: false }];
  st.dealer = [bjDraw(st), bjDraw(st)];
  let balance;
  if (isBJ(st.hands[0].cards) || isBJ(st.dealer)) { st.hands[0].done = true; balance = await bjFinish(c, u, st); }
  else balance = await balanceOf(c, u.id);
  await bjSave(c, u, st);
  return bjView(st, balance);
});
const bjAction = (u, body) => tx(async (c) => {
  const g = (await loadGame(c, u.id, "bj")) || {};
  if (!g.round || g.round.phase !== "player") throw httpErr(409, "Нет активной раздачи");
  const st = { ...g.round, shoe: g.shoe };
  const h = st.hands[st.active], a = body.action;
  let balance;
  if (a === "hit") {
    h.cards.push(bjDraw(st));
    if (handValue(h.cards) >= 21) { h.done = true; balance = await bjAdvance(c, u, st); } else balance = await balanceOf(c, u.id);
  } else if (a === "stand") {
    h.done = true; balance = await bjAdvance(c, u, st);
  } else if (a === "double") {
    if (h.cards.length !== 2 || h.splitAces) throw httpErr(400, "Удвоить нельзя");
    if ((await change(c, u.id, -h.stake)) === null) throw httpErr(402, "Не хватает монэток");
    h.stake *= 2; h.cards.push(bjDraw(st)); h.done = true; balance = await bjAdvance(c, u, st);
  } else if (a === "split") {
    if (h.cards.length !== 2 || h.splitAces || st.hands.length >= 4 || cardPoints(h.cards[0]) !== cardPoints(h.cards[1])) throw httpErr(400, "Разделить нельзя");
    if ((await change(c, u.id, -h.stake)) === null) throw httpErr(402, "Не хватает монэток");
    const twin = { cards: [h.cards.pop()], stake: h.stake, fromSplit: true, splitAces: false, done: false };
    h.fromSplit = true;
    if (h.cards[0].r === "A") { h.splitAces = true; twin.splitAces = true; }
    st.hands.splice(st.active + 1, 0, twin);
    h.cards.push(bjDraw(st));
    if (h.splitAces || handValue(h.cards) === 21) { h.done = true; balance = await bjAdvance(c, u, st); } else balance = await balanceOf(c, u.id);
  } else throw httpErr(400, "Неизвестное действие");
  await bjSave(c, u, st);
  return bjView(st, balance);
});

// ---------- upgrader ----------
const UPG_MULTS = [1.2, 1.5, 2, 3, 5, 10, 20, 50, 100];
// the upgrader keeps more than the other games: on average 65% of the stake comes back
const UPG_EDGE = 0.65;
const upgSpin = (u, body) => tx(async (c) => {
  const bet = Number(body.bet), mi = Number(body.mult);
  if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
  if (!Number.isInteger(mi) || mi < 0 || mi >= UPG_MULTS.length) throw httpErr(400, "Неверный множитель");
  if ((await change(c, u.id, -bet)) === null) throw httpErr(402, "Не хватает монэток на ставку");
  const mult = UPG_MULTS[mi], chance = UPG_EDGE / mult, arcStart = -chance * 180;
  const land = rnd() * 360, win = ((land - arcStart + 360) % 360) < chance * 360;
  const prize = Math.floor(bet * mult);
  const balance = win ? await change(c, u.id, prize) : await balanceOf(c, u.id);
  if (win) await pushWin(c, u.name, "Upgrader", prize, mult);
  return { land, win, prize, balance };
});

// ---------- wheel of fortune: shared rounds, like Crash ----------
// 50 sectors: 20 × x2, 8 × x5, 4 × x10, 2 × x20 and 16 skulls (0). Everyone bets on one or more multipliers
// during the betting window, then one spin decides for all: a bet on the sector's multiplier pays bet × it.
// Chance = count / 50, so every pick returns 80% on average. The sector is drawn when the round is created
// and leaves the server only when the bets close. The order here must match WHEEL in the game.
const WHEEL_PICKS = [2, 5, 10, 20];
const WHEEL = (() => {
  const w = new Array(50).fill(0), place = (mult, every, from) => { for (let i = from; i < 50; i += every) w[i] = mult; };
  place(2, 5, 0); place(2, 5, 2); place(5, 25, 1); place(5, 25, 6); place(5, 25, 11); place(5, 25, 16);
  place(10, 25, 3); place(10, 25, 13); place(20, 25, 21);
  return w;
})();
const WHEEL_BET_MS = 15000, WHEEL_SPIN_MS = 6000, WHEEL_PAUSE_MS = 3000;
const toWheelRound = (x) => ({ id: Number(x.id), betEnds: Number(x.bet_ends), ends: Number(x.ends), seg: Number(x.seg) });
async function wheelCurrent() {
  for (let i = 0; i < 4; i++) {
    const r = await pool.query(`SELECT * FROM wheel_rounds ORDER BY id DESC LIMIT 1`);
    const last = r.rows[0] ? toWheelRound(r.rows[0]) : null, now = Date.now();
    if (last && now < last.ends) return last;
    // rounds follow each other back to back while people play; after a quiet spell the next one starts now
    const start = last && now - last.ends < 2000 ? last.ends : now, betEnds = start + WHEEL_BET_MS;
    await pool.query(`INSERT INTO wheel_rounds (id, bet_ends, ends, seg) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
      [last ? last.id + 1 : 1, betEnds, betEnds + WHEEL_SPIN_MS + WHEEL_PAUSE_MS, Math.floor(rnd() * WHEEL.length)]);
    if (last && rnd() < 0.01) {
      await pool.query(`DELETE FROM wheel_bets WHERE round_id < $1`, [last.id - 5000]);
      await pool.query(`DELETE FROM wheel_rounds WHERE id < $1`, [last.id - 5000]);
    }
  }
  throw httpErr(503, "Раунд не создался, попробуй ещё раз");
}
// pays a round once its spin has played out, so the coins arrive when the wheel stops on everyone's screen
async function wheelSettle() {
  const open = await pool.query(`SELECT * FROM wheel_rounds WHERE NOT settled AND bet_ends <= $1 ORDER BY id LIMIT 3`, [Date.now() - WHEEL_SPIN_MS]);
  for (const row of open.rows) {
    const round = toWheelRound(row), mult = WHEEL[round.seg];
    await tx(async (c) => {
      const s = await c.query(`UPDATE wheel_rounds SET settled = true WHERE id = $1 AND NOT settled RETURNING id`, [round.id]);
      if (!s.rows[0] || !mult) return;
      const wins = await c.query(`SELECT user_id, name, bet FROM wheel_bets WHERE round_id = $1 AND pick = $2`, [round.id, mult]);
      for (const b of wins.rows) {
        const prize = Number(b.bet) * mult;
        await change(c, Number(b.user_id), prize);
        await pushWin(c, b.name, "Wheel", prize, mult);
      }
    });
  }
}
async function wheelState(u) {
  const cur = await wheelCurrent();
  await wheelSettle();
  const now = Date.now(), open = now < cur.betEnds;
  const [bets, hist] = await Promise.all([
    pool.query(`SELECT user_id, name, pick, bet FROM wheel_bets WHERE round_id = $1 ORDER BY bet DESC LIMIT 60`, [cur.id]),
    // only spins that have stopped, so the strip never gives away the one still turning
    pool.query(`SELECT seg FROM wheel_rounds WHERE settled ORDER BY id DESC LIMIT 10`),
  ]);
  return {
    id: cur.id, phase: open ? "bet" : now < cur.betEnds + WHEEL_SPIN_MS ? "spin" : "done", betEnds: cur.betEnds,
    spinMs: WHEEL_SPIN_MS, ends: cur.ends, now, seg: open ? null : cur.seg,
    bets: bets.rows.map((b) => ({ id: Number(b.user_id), name: b.name, pick: Number(b.pick), bet: Number(b.bet) })),
    hist: hist.rows.map((h) => WHEEL[Number(h.seg)]),
  };
}
// the last 30 spins that have stopped: what fell, how many played, how much was staked and paid out
async function wheelHistory() {
  const rounds = (await pool.query(`SELECT id, seg FROM wheel_rounds WHERE settled ORDER BY id DESC LIMIT 30`)).rows;
  if (!rounds.length) return { rounds: [] };
  const bets = (await pool.query(`SELECT round_id, user_id, pick, bet FROM wheel_bets WHERE round_id = ANY($1::bigint[])`, [rounds.map((x) => x.id)])).rows;
  return { rounds: rounds.map((x) => {
    const mult = WHEEL[Number(x.seg)], mine = bets.filter((b) => Number(b.round_id) === Number(x.id));
    return { id: Number(x.id), mult, players: new Set(mine.map((b) => Number(b.user_id))).size, staked: mine.reduce((a, b) => a + Number(b.bet), 0),
      paid: mine.filter((b) => Number(b.pick) === mult).reduce((a, b) => a + Number(b.bet) * mult, 0) };
  }) };
}
// a bet on a multiplier while the round takes bets; betting on the same multiplier again adds to it
async function wheelBet(u, body) {
  const bet = Number(body.bet), pick = Number(body.pick);
  if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
  if (!WHEEL_PICKS.includes(pick)) throw httpErr(400, "Нет такого множителя");
  const cur = await wheelCurrent();
  return tx(async (c) => {
    if (Date.now() >= cur.betEnds) throw httpErr(409, "Ставки закрыты, жди следующий раунд");
    const balance = await change(c, u.id, -bet);
    if (balance === null) throw httpErr(402, "Не хватает монэток на ставку");
    const r = await c.query(`INSERT INTO wheel_bets (round_id, user_id, name, pick, bet) VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (round_id, user_id, pick) DO UPDATE SET bet = wheel_bets.bet + EXCLUDED.bet RETURNING bet`, [cur.id, u.id, u.name, pick, bet]);
    if (Number(r.rows[0].bet) > BET_MAX) throw httpErr(400, "На один множитель не больше " + BET_MAX);
    return { balance, round: cur.id };
  });
}

// ---------- roulette: players' stakes go into one pot, one of them takes it ----------
// A round waits for its first two players; the second one starts a JP_BET_MS countdown, and anyone may add
// to their stake (up to BET_MAX in total) until it runs out. The win chance is the player's share of the pot,
// capped at JP_CAP, the excess spread over the others by their shares. The winner gets their own stake back
// plus the others' stakes minus JP_FEE. The draw happens once, at the first look after the countdown.
const JP_BET_MS = 20000, JP_SPIN_MS = 7000, JP_PAUSE_MS = 4000, JP_CAP = 0.65, JP_FEE = 0.2;
function jpChances(bets) {
  const pot = bets.reduce((a, b) => a + b.bet, 0);
  let ch = bets.map((b) => b.bet / pot);
  if (bets.length < 2) return ch;
  // cap, hand the excess to everyone below the cap in proportion to their shares, repeat until nobody is over
  for (let i = 0; i < bets.length; i++) {
    const over = ch.map((c) => c > JP_CAP + 1e-12);
    if (!over.some(Boolean)) break;
    const excess = ch.reduce((a, c, k) => a + (over[k] ? c - JP_CAP : 0), 0);
    const room = ch.reduce((a, c, k) => a + (over[k] ? 0 : c), 0);
    ch = ch.map((c, k) => (over[k] ? JP_CAP : c + excess * c / room));
  }
  return ch;
}
const jpPrize = (bets, winner) => {
  const pot = bets.reduce((a, b) => a + b.bet, 0), own = bets.find((b) => b.id === winner).bet;
  return own + Math.floor((pot - own) * (1 - JP_FEE));
};
const toJp = (x) => ({ id: Number(x.id), betEnds: x.bet_ends === null ? null : Number(x.bet_ends), ends: x.ends === null ? null : Number(x.ends),
  winner: x.winner === null ? null : Number(x.winner), land: x.land === null ? null : Number(x.land), settled: x.settled });
async function jpBets(q, id) {
  const r = await q.query(`SELECT user_id, name, bet FROM jp_bets WHERE round_id = $1 ORDER BY at, user_id`, [id]);
  return r.rows.map((b) => ({ id: Number(b.user_id), name: b.name, bet: Number(b.bet) }));
}
// the round in play, with its winner drawn once the countdown is over
async function jpCurrent() {
  for (let i = 0; i < 4; i++) {
    const r = await pool.query(`SELECT * FROM jp_rounds ORDER BY id DESC LIMIT 1`);
    const last = r.rows[0] ? toJp(r.rows[0]) : null, now = Date.now();
    if (last && !(last.ends && now >= last.ends)) {
      if (!(last.betEnds && now >= last.betEnds && last.winner === null)) return last;
      await jpDraw(last.id);
      return toJp((await pool.query(`SELECT * FROM jp_rounds WHERE id = $1`, [last.id])).rows[0]);
    }
    await pool.query(`INSERT INTO jp_rounds (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, [last ? last.id + 1 : 1]);
    if (last && rnd() < 0.01) {
      await pool.query(`DELETE FROM jp_bets WHERE round_id < $1`, [last.id - 5000]);
      await pool.query(`DELETE FROM jp_rounds WHERE id < $1`, [last.id - 5000]);
    }
  }
  throw httpErr(503, "Раунд не создался, попробуй ещё раз");
}
// land: where on the circle (0..1, clockwise from the top) the pointer stops; players' arcs follow the bet order
const jpDraw = (id) => tx(async (c) => {
  const row = (await c.query(`SELECT * FROM jp_rounds WHERE id = $1 FOR UPDATE`, [id])).rows[0];
  if (!row || row.winner !== null) return;
  const bets = await jpBets(c, id), ch = jpChances(bets), land = rnd();
  let acc = 0, winner = bets[bets.length - 1].id;
  for (let k = 0; k < bets.length; k++) { acc += ch[k]; if (land < acc) { winner = bets[k].id; break; } }
  await c.query(`UPDATE jp_rounds SET winner = $2, land = $3 WHERE id = $1`, [id, winner, land]);
});
// pays the winner once the spin has played out on everyone's screen
async function jpSettle() {
  const open = await pool.query(`SELECT * FROM jp_rounds WHERE NOT settled AND winner IS NOT NULL AND bet_ends <= $1 ORDER BY id LIMIT 3`, [Date.now() - JP_SPIN_MS]);
  for (const row of open.rows) {
    await tx(async (c) => {
      const s = await c.query(`UPDATE jp_rounds SET settled = true WHERE id = $1 AND NOT settled RETURNING winner`, [row.id]);
      if (!s.rows[0]) return;
      const bets = await jpBets(c, Number(row.id)), winner = Number(s.rows[0].winner), prize = jpPrize(bets, winner);
      await change(c, winner, prize);
      await pushWin(c, bets.find((b) => b.id === winner).name, "Roulette", prize);
    });
  }
}
async function jpState(u) {
  const cur = await jpCurrent();
  await jpSettle();
  const now = Date.now(), bets = await jpBets(pool, cur.id), ch = jpChances(bets);
  const phase = !cur.betEnds ? "wait" : now < cur.betEnds ? "bet" : now < cur.betEnds + JP_SPIN_MS ? "spin" : "done";
  const hist = await pool.query(`SELECT r.id, r.winner, b.name FROM jp_rounds r JOIN jp_bets b ON b.round_id = r.id AND b.user_id = r.winner
    WHERE r.settled ORDER BY r.id DESC LIMIT 8`);
  return {
    id: cur.id, phase, betEnds: cur.betEnds, spinMs: JP_SPIN_MS, now, cap: JP_CAP, fee: JP_FEE,
    bets: bets.map((b, k) => ({ ...b, chance: ch[k] })), pot: bets.reduce((a, b) => a + b.bet, 0),
    winner: phase === "wait" || phase === "bet" ? null : cur.winner, land: phase === "wait" || phase === "bet" ? null : cur.land,
    prize: phase === "wait" || phase === "bet" || cur.winner === null ? 0 : jpPrize(bets, cur.winner),
    hist: hist.rows.map((h) => h.name),
  };
}
async function jpBet(u, body) {
  const bet = Number(body.bet);
  if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
  const cur = await jpCurrent();
  return tx(async (c) => {
    const row = toJp((await c.query(`SELECT * FROM jp_rounds WHERE id = $1 FOR UPDATE`, [cur.id])).rows[0]);
    const now = Date.now();
    if (row.betEnds && now >= row.betEnds) throw httpErr(409, "Рулетка уже крутится, жди следующий раунд");
    const balance = await change(c, u.id, -bet);
    if (balance === null) throw httpErr(402, "Не хватает монэток на ставку");
    const r = await c.query(`INSERT INTO jp_bets (round_id, user_id, name, bet, at) VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (round_id, user_id) DO UPDATE SET bet = jp_bets.bet + EXCLUDED.bet RETURNING bet`, [row.id, u.id, u.name, bet, now]);
    if (Number(r.rows[0].bet) > BET_MAX) throw httpErr(400, "За раунд не больше " + BET_MAX);
    // the second player starts the countdown
    if (!row.betEnds) {
      const n = Number((await c.query(`SELECT count(*) FROM jp_bets WHERE round_id = $1`, [row.id])).rows[0].count);
      if (n >= 2) await c.query(`UPDATE jp_rounds SET bet_ends = $2, ends = $3 WHERE id = $1`, [row.id, now + JP_BET_MS, now + JP_BET_MS + JP_SPIN_MS + JP_PAUSE_MS]);
    }
    return { balance };
  });
}
// the last 20 pots that have been paid: who took each, with what chance, how big it was and what they got
async function jpHistory() {
  const rounds = (await pool.query(`SELECT id, winner FROM jp_rounds WHERE settled ORDER BY id DESC LIMIT 20`)).rows;
  const out = [];
  for (const x of rounds) {
    const bets = await jpBets(pool, Number(x.id)), ch = jpChances(bets), k = bets.findIndex((b) => b.id === Number(x.winner));
    if (k < 0) continue;
    out.push({ id: Number(x.id), name: bets[k].name, bet: bets[k].bet, chance: ch[k], players: bets.length,
      pot: bets.reduce((a, b) => a + b.bet, 0), prize: jpPrize(bets, bets[k].id) });
  }
  return { rounds: out };
}
// a player alone in the pot may take the stake back while nobody else has joined
async function jpLeave(u) {
  const cur = await jpCurrent();
  return tx(async (c) => {
    const row = toJp((await c.query(`SELECT * FROM jp_rounds WHERE id = $1 FOR UPDATE`, [cur.id])).rows[0]);
    if (row.betEnds) throw httpErr(409, "Отсчёт уже идёт, ставку не забрать");
    const r = await c.query(`DELETE FROM jp_bets WHERE round_id = $1 AND user_id = $2 RETURNING bet`, [row.id, u.id]);
    const balance = r.rows[0] ? await change(c, u.id, Number(r.rows[0].bet)) : await balanceOf(c, u.id);
    return { balance };
  });
}

// ---------- coffins ----------
const COFFIN_PRICES = [10, 50, 200, 1000, 5000];
// [price multiple, chance %]: the first four pay less than the case, the other five (27% together) pay it back or more;
// the rarest, 0.1%, pays x27
const LOOT = [[0.1, 25], [0.3, 22], [0.5, 16], [0.8, 10], [1.1, 15.2], [1.6, 7], [3, 3.4], [7, 1.3], [27, 0.1]];
const LOOT_W = LOOT.reduce((a, [, w]) => a + w, 0);
const LOOT_K = COFFIN_EDGE / (LOOT.reduce((a, [m, w]) => a + m * w, 0) / LOOT_W);
const coffinOpen = (u, body) => tx(async (c) => {
  const ci = Number(body.coffin);
  if (!Number.isInteger(ci) || ci < 0 || ci >= COFFIN_PRICES.length) throw httpErr(400, "Неверный кейс");
  // up to five cases of the same kind in one go; each one is drawn on its own
  const n = body.count === undefined ? 1 : Number(body.count);
  if (!Number.isInteger(n) || n < 1 || n > 5) throw httpErr(400, "Можно открыть от 1 до 5 кейсов");
  const price = COFFIN_PRICES[ci];
  if ((await change(c, u.id, -price * n)) === null) throw httpErr(402, n > 1 ? "Не хватает монэток на столько кейсов" : "Не хватает монэток на этот кейс");
  const loots = [], pays = [];
  for (let k = 0; k < n; k++) {
    const loot = weighted(LOOT.map(([, w], i) => [i, w]));
    loots.push(loot); pays.push(Math.round(price * LOOT[loot][0] * LOOT_K));
  }
  const balance = await change(c, u.id, pays.reduce((a, p) => a + p, 0));
  for (const pay of pays) if (pay > price) await pushWin(c, u.name, "Coffins", pay, pay / price);
  return { loot: loots[0], pay: pays[0], loots, pays, balance };
});

// ---------- crash: shared rounds kept in the database ----------
// A round is a row: its crash point is drawn when the row is created and never leaves the server until the
// bat falls. Whoever polls first after a round ends creates the next one. The flight is timed from fly_start,
// so every player sees the same multiplier at the same moment.
const CRASH_K = 0.14, CRASH_BET_MS = 6000, CRASH_PAUSE_MS = 3000, CRASH_MAX = 1000;
// Grace for the network: a player's screen runs a little behind the server, so "Забрать" may arrive after the
// real crash moment. For CRASH_GRACE_MS after it the round still reads as flying to everyone, the crash point
// stays secret, and a cash-out is paid at the multiplier the player saw, if that was below the crash point.
// Nobody can learn the point before cash-outs close, so the grace gives no edge to anyone.
// The grace is kept short: the page never draws the flight past (poll time - grace), the last moment the
// server has confirmed the bat alive, so a longer grace would only make the picture lag further behind.
const CRASH_GRACE_MS = 300;
const crashPoint = () => Math.min(CRASH_MAX, Math.max(1, Math.floor(EDGE / (1 - rnd()) * 100) / 100));
const multAt = (r, now) => (now <= r.flyStart ? 1 : Math.exp(CRASH_K * (now - r.flyStart) / 1000));
const toRound = (x) => ({ id: Number(x.id), betEnds: Number(x.bet_ends), flyStart: Number(x.fly_start), crashAt: Number(x.crash_at),
  ends: Number(x.ends), point: Number(x.point), settled: x.settled });
async function crashCurrent() {
  for (let i = 0; i < 4; i++) {
    const r = await pool.query(`SELECT * FROM crash_rounds ORDER BY id DESC LIMIT 1`);
    const last = r.rows[0] ? toRound(r.rows[0]) : null, now = Date.now();
    if (last && now < last.ends) return last;
    // rounds follow each other back to back while people play; after a quiet spell the next one starts now
    const start = last && now - last.ends < 2000 ? last.ends : now;
    const point = crashPoint(), betEnds = start + CRASH_BET_MS, crashAt = betEnds + Math.round(Math.log(point) / CRASH_K * 1000);
    await pool.query(`INSERT INTO crash_rounds (id, bet_ends, fly_start, crash_at, ends, point) VALUES ($1, $2, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`,
      [last ? last.id + 1 : 1, betEnds, crashAt, crashAt + CRASH_GRACE_MS + CRASH_PAUSE_MS, point]);
    if (last && rnd() < 0.01) {
      await pool.query(`DELETE FROM crash_bets WHERE round_id < $1`, [last.id - 5000]);
      await pool.query(`DELETE FROM crash_rounds WHERE id < $1`, [last.id - 5000]);
    }
  }
  throw httpErr(503, "Раунд не создался, попробуй ещё раз");
}
// Once a round is over (grace included): pay the auto cash-outs below the crash point and put every
// Crash win of the round on the ticker. Nothing about the round leaves the server any earlier.
async function crashSettle() {
  const now = Date.now();
  const open = await pool.query(`SELECT * FROM crash_rounds WHERE NOT settled AND crash_at <= $1 ORDER BY id LIMIT 3`, [now - CRASH_GRACE_MS]);
  for (const row of open.rows) {
    const round = toRound(row);
    await tx(async (c) => {
      const s = await c.query(`UPDATE crash_rounds SET settled = true WHERE id = $1 AND NOT settled RETURNING id`, [round.id]);
      if (!s.rows[0]) return;
      const autos = await c.query(`UPDATE crash_bets SET cashed = auto, cashed_at = $3 WHERE round_id = $1 AND cashed IS NULL AND auto > 0 AND auto < $2
        RETURNING user_id`, [round.id, round.point, now]);
      const paid = new Set(autos.rows.map((b) => Number(b.user_id)));
      const wins = await c.query(`SELECT user_id, name, bet, cashed FROM crash_bets WHERE round_id = $1 AND cashed > 0`, [round.id]);
      for (const b of wins.rows) {
        const win = Math.floor(Number(b.bet) * Number(b.cashed));
        if (paid.has(Number(b.user_id))) await change(c, Number(b.user_id), win);
        await pushWin(c, b.name, "Crash", win, Number(b.cashed));
      }
    });
  }
}
async function crashState(u) {
  const cur = await crashCurrent();
  await crashSettle();
  const now = Date.now(), seen = now - CRASH_GRACE_MS;
  const over = now >= cur.crashAt + CRASH_GRACE_MS;
  const phase = now < cur.betEnds ? "bet" : over ? "crashed" : "fly";
  const [bets, hist, queued] = await Promise.all([
    pool.query(`SELECT user_id, name, bet, auto, cashed, cashed_at FROM crash_bets WHERE round_id = $1 ORDER BY bet DESC LIMIT 50`, [cur.id]),
    pool.query(`SELECT point FROM crash_rounds WHERE crash_at <= $1 ORDER BY id DESC LIMIT 8`, [seen]),
    pool.query(`SELECT 1 FROM crash_bets WHERE round_id = $1 AND user_id = $2`, [cur.id + 1, u.id]),
  ]);
  // Others' cash-outs show up only GRACE late (as late as the flight itself may be), so the list never tells
  // anyone more about the crash point than the flight they are watching.
  const mSeen = multAt(cur, seen);
  const players = bets.rows.map((b) => {
    const me = Number(b.user_id) === u.id, auto = Number(b.auto);
    let cashed = 0;
    const done = b.cashed !== null && (over || me || Number(b.cashed_at) <= seen);
    if (done) cashed = Math.max(0, Number(b.cashed));                       // -1: too late, burnt
    else if (auto > 0 && (over ? auto < cur.point : auto <= mSeen)) cashed = auto;
    return { id: Number(b.user_id), name: b.name, bet: Number(b.bet), cashed };
  });
  return { id: cur.id, phase, betEnds: cur.betEnds, flyStart: cur.flyStart, now, point: over ? cur.point : null,
    hist: hist.rows.map((h) => Number(h.point)), players, queued: !!queued.rows[0] };
}
async function crashBet(u, body) {
  const bet = Number(body.bet), auto = Number(body.auto) || 0;
  if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
  const cur = await crashCurrent();
  return tx(async (c) => {
    // during the flight the bet goes to the next round
    const target = Date.now() < cur.betEnds ? cur.id : cur.id + 1;
    const balance = await change(c, u.id, -bet);
    if (balance === null) throw httpErr(402, "Не хватает монэток на ставку");
    const r = await c.query(`INSERT INTO crash_bets (round_id, user_id, name, bet, auto) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING RETURNING round_id`,
      [target, u.id, u.name, bet, auto >= 1.01 ? Math.floor(auto * 100) / 100 : 0]);
    if (!r.rows[0]) throw httpErr(409, "Ставка уже сделана");
    return { balance, queued: target !== cur.id };
  });
}
// body.m is the multiplier on the player's screen when they pressed: it is paid if it is below the crash
// point, never more than the flight has reached on the server, and only until the grace runs out.
async function crashCash(u, body) {
  const cur = await crashCurrent();
  return tx(async (c) => {
    const now = Date.now();
    if (now < cur.flyStart || now >= cur.crashAt + CRASH_GRACE_MS) throw httpErr(409, "Мышь уже сбита");
    const seen = Number(body.m) > 0 ? Number(body.m) : Infinity;
    const m = Math.floor(Math.min(seen, multAt(cur, now)) * 100) / 100;
    if (m < 1 || m >= cur.point) {
      // too late: the bet is burnt on the spot (its auto cash-out still counts if the bat passed it),
      // so a refused try cannot be repeated to feel out where the crash point is
      const b = await c.query(`UPDATE crash_bets SET cashed = CASE WHEN auto > 0 AND auto < $3 THEN auto ELSE -1 END, cashed_at = $4
        WHERE round_id = $1 AND user_id = $2 AND cashed IS NULL RETURNING bet, cashed`, [cur.id, u.id, cur.point, now]);
      if (b.rows[0] && Number(b.rows[0].cashed) > 0) {
        const balance = await change(c, u.id, Math.floor(Number(b.rows[0].bet) * Number(b.rows[0].cashed)));
        return { balance, cashed: Number(b.rows[0].cashed), win: Math.floor(Number(b.rows[0].bet) * Number(b.rows[0].cashed)) };
      }
      throw httpErr(409, "Мышь уже сбита");
    }
    const r = await c.query(`UPDATE crash_bets SET cashed = $3, cashed_at = $4 WHERE round_id = $1 AND user_id = $2 AND cashed IS NULL RETURNING bet`,
      [cur.id, u.id, m, now]);
    if (!r.rows[0]) throw httpErr(409, "Нет ставки в этом раунде");
    const win = Math.floor(Number(r.rows[0].bet) * m);
    const balance = await change(c, u.id, win);   // the ticker gets it when the round is over
    return { balance, cashed: m, win };
  });
}
async function crashUnqueue(u) {
  const cur = await crashCurrent();
  return tx(async (c) => {
    const r = await c.query(`DELETE FROM crash_bets WHERE user_id = $1 AND (round_id > $2 OR (round_id = $2 AND $3 < $4)) RETURNING bet`,
      [u.id, cur.id, Date.now(), cur.betEnds]);
    let balance = null;
    for (const b of r.rows) balance = await change(c, u.id, Number(b.bet));
    return { balance: balance === null ? await balanceOf(c, u.id) : balance, cancelled: r.rows.length > 0 };
  });
}

// ---------- duel: one waiting player per stake room ----------
const duelJoin = (u, body) => tx(async (c) => {
  const stake = Number(body.stake);
  if (!DUEL_STAKES.includes(stake)) throw httpErr(400, "Нет такого зала");
  const mine = await c.query(`SELECT stake FROM duel_rooms WHERE user_id = $1`, [u.id]);
  if (mine.rows[0]) throw httpErr(409, "Ты уже ждёшь в зале " + mine.rows[0].stake);
  const room = (await c.query(`SELECT user_id, name FROM duel_rooms WHERE stake = $1 FOR UPDATE`, [stake])).rows[0];
  const balance = await change(c, u.id, -stake);
  if (balance === null) throw httpErr(402, "Не хватает монэток на ставку");
  if (!room || room.user_id === null) {
    await c.query(`UPDATE duel_rooms SET user_id = $2, name = $3, since = $4 WHERE stake = $1`, [stake, u.id, u.name, Date.now()]);
    return { wait: true, stake, balance };
  }
  const waiter = { id: Number(room.user_id), name: room.name };
  await c.query(`UPDATE duel_rooms SET user_id = NULL, name = NULL, since = NULL WHERE stake = $1`, [stake]);
  const joinerDies = rnd() < 0.5;
  const winner = joinerDies ? waiter : u, prize = Math.floor(stake * 2 * (1 - DUEL_FEE));
  const wb = await change(c, winner.id, prize);
  await pushWin(c, winner.name, "Duel", prize);
  // the waiting player learns the outcome on their next poll (once a second while waiting), so both scenes
  // start together at startAt by the server clock
  const startAt = Date.now() + DUEL_LEAD_MS;
  await c.query(`INSERT INTO duel_events (user_id, data) VALUES ($1, $2)`, [waiter.id, JSON.stringify({ stake, opp: u.name, youDie: !joinerDies, prize, startAt })]);
  return { match: { stake, opp: waiter.name, youDie: joinerDies, prize, startAt }, balance: joinerDies ? balance : wb, now: Date.now() };
});
const duelLeave = (u) => tx(async (c) => {
  const r = await c.query(`UPDATE duel_rooms SET user_id = NULL, name = NULL, since = NULL WHERE user_id = $1 RETURNING stake`, [u.id]);
  let balance = null;
  for (const row of r.rows) balance = await change(c, u.id, Number(row.stake));   // refund the stake
  return { balance: balance === null ? await balanceOf(c, u.id) : balance };
});
async function duelState(u) {
  const [rooms, events] = await Promise.all([
    pool.query(`SELECT stake, user_id, name FROM duel_rooms ORDER BY stake`),
    pool.query(`UPDATE duel_events SET seen = true WHERE user_id = $1 AND NOT seen RETURNING id, data`, [u.id]),
  ]);
  return {
    rooms: rooms.rows.map((r) => ({ stake: Number(r.stake), waiting: r.name, waitingId: num(r.user_id) })),
    events: events.rows.sort((a, b) => Number(a.id) - Number(b.id)).map((e) => e.data),
  };
}

// ---------- one poll for everything live ----------
async function poll(u, body) {
  const out = {};
  if (body.crash) out.crash = await crashState(u);
  else { await crashCurrent(); await crashSettle(); }   // auto cash-outs still get paid while the player is elsewhere
  if (body.wheel) out.wheel = await wheelState(u);
  else await wheelSettle();                               // winners get paid while they are elsewhere too
  if (body.jp) out.jp = await jpState(u);
  else await jpSettle();
  if (body.duel) out.duel = await duelState(u);
  out.wins = await winsSince(body.since);
  out.balance = await balanceOf(pool, u.id);
  out.now = Date.now();
  return out;
}

// ---------- deposits and withdrawals ----------
async function deposit(u, body) {
  const amount = Number(body.amount);
  if (!Number.isInteger(amount) || amount < DEPOSIT_MIN || amount > DEPOSIT_MAX) throw httpErr(400, "Сумма от " + DEPOSIT_MIN + " до " + DEPOSIT_MAX);
  // huEVO requires an idempotency key (1-64 chars), so a retried request never charges twice
  const j = await huevo("POST", "/payments", { user_id: u.hid, amount, description: "Пополнение баланса Nullified Casino", test: !!u.test, idempotency_key: "dep-" + crypto.randomUUID() })
    .catch((e) => { throw httpErr(502, "huEVO: " + e.message); });
  const intent = j.intent_id || j.id || (j.payment && (j.payment.intent_id || j.payment.id)) || (j.intent && (j.intent.id || j.intent.intent_id));
  if (!intent) { console.warn("[deposit] no intent id in", JSON.stringify(j).slice(0, 400)); throw httpErr(502, "huEVO не вернул платёж"); }
  await pool.query(`INSERT INTO payments (intent_id, user_id, amount) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, [intent, u.id, amount]);
  return { intent_id: intent };
}
// credit a payment once, after huEVO confirms it succeeded
async function settlePayment(intent) {
  const j = await huevo("GET", "/payments/" + encodeURIComponent(intent));
  const p = j.payment || j.intent || j;
  if (p.status !== "succeeded") return { status: p.status };
  return tx(async (c) => {
    const r = await c.query(`UPDATE payments SET credited = true WHERE intent_id = $1 AND credited = false RETURNING user_id, amount`, [intent]);
    if (!r.rows[0]) return { status: "succeeded", already: true };
    const userId = Number(r.rows[0].user_id), amount = afterFee(Number(r.rows[0].amount));
    const balance = await change(c, userId, amount);
    console.log("[deposit] credited", amount, "of", r.rows[0].amount, "to", userId);
    return { status: "succeeded", credited: amount, userId, balance };
  });
}
async function depositCheck(u, body) {
  const r = await settlePayment(String(body.intent_id || ""));
  return { status: r.status, credited: r.userId === u.id ? r.credited || 0 : 0, balance: await balanceOf(pool, u.id) };
}
async function withdraw(u, body) {
  const amount = Number(body.amount);
  if (!Number.isInteger(amount) || amount < DEPOSIT_MIN) throw httpErr(400, "Минимум " + DEPOSIT_MIN);
  const balance = await change(pool, u.id, -amount);
  if (balance === null) throw httpErr(402, "На балансе нет столько монэток");
  const log = (ok, info) => pool.query(`INSERT INTO payouts (user_id, amount, ok, info) VALUES ($1, $2, $3, $4)`, [u.id, amount, ok, String(info).slice(0, 500)]);
  const paid = afterFee(amount);
  const req = { user_id: u.hid, amount: paid, description: "Вывод из Nullified Casino (комиссия 5%)", test: !!u.test, idempotency_key: "out-" + crypto.randomUUID() };
  // a lost connection does not tell whether huEVO paid, so the same request is sent again under the same
  // idempotency key (huEVO pays it at most once); only a clear refusal from huEVO returns the coins
  const send = () => huevo("POST", PAYOUT_PATH, req);
  const unclear = (e) => !e.code || /^http_5/.test(e.code) || e.code === "internal" || e.code === "rate_limited";
  try {
    const j = await send().catch((e) => { if (unclear(e)) return new Promise((r) => setTimeout(r, 1500)).then(send); throw e; });
    await log(true, JSON.stringify(j));
    return { ok: true, balance, paid };
  } catch (e) {
    if (unclear(e)) {
      await log(null, "unclear " + (e.code || "") + " " + e.message + " key " + req.idempotency_key);
      throw httpErr(502, "huEVO не ответил. Монэтки списаны и будут выплачены или возвращены после проверки", { balance });
    }
    const back = await change(pool, u.id, amount);
    await log(false, e.code + " " + e.message);
    const msg = e.code === "insufficient_funds" || /кошел|недостат/i.test(e.message) ? "В кассе казино сейчас не хватает монэток, попробуй сумму поменьше" : "huEVO: " + e.message;
    throw httpErr(502, msg, { balance: back });
  }
}
function callbackSignatureOk(raw, sig) {
  if (!sig || !CLIENT_SECRET) return false;
  sig = String(sig).toLowerCase().replace(/^sha256=/, "");
  return secretKeys().slice(0, 2).some((key) => {
    const mac = crypto.createHmac("sha256", key).update(raw).digest();
    return mac.toString("hex") === sig || mac.toString("base64url").toLowerCase() === sig || mac.toString("base64").toLowerCase() === sig;
  });
}

async function login(body) {
  if (!CLIENT_SECRET) throw httpErr(503, "Сервер не настроен: нет ключа huEVO");
  const launch = body.launch || {};
  const id = parseInt(launch.hu_user_id, 10);
  if (!id) throw httpErr(401, "Открой игру через huEVO");
  const v = verifyLaunch(launch);
  if (!v.ok) {
    console.warn("[login] launch signature did not match for user", id, "mode", VERIFY, "keys", Object.keys(launch).join(","));
    if (VERIFY === "strict") throw httpErr(401, "Не удалось проверить вход через huEVO");
  } else console.log("[login] signature ok, scheme", v.scheme);
  const name = String(launch.hu_display_name || launch.hu_username || "Игрок " + id).slice(0, 40);
  // test launches play on a separate balance (stored under the negative id): test coins cost nothing,
  // so they must never mix with real ones or be paid out for real
  const test = launch.hu_mode === "test", uid = test ? -id : id;
  const r = await pool.query(`INSERT INTO users (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING balance`, [uid, name]);
  return { token: makeToken({ id: uid, hid: id, name, test }), user: { id: uid, name }, balance: Number(r.rows[0].balance), test, wins: await winsSince(0) };
}

// ---------- HTTP ----------
const routes = {
  "/api/slots/spin": slotsSpin, "/api/mines/start": minesStart, "/api/mines/open": minesOpen, "/api/mines/cashout": minesCash,
  "/api/bj/deal": bjDeal, "/api/bj/action": bjAction, "/api/upg/spin": upgSpin, "/api/wheel/bet": wheelBet, "/api/wheel/history": wheelHistory,
  "/api/jp/bet": jpBet, "/api/jp/leave": jpLeave, "/api/jp/history": jpHistory, "/api/coffin/open": coffinOpen,
  "/api/crash/bet": crashBet, "/api/crash/cash": crashCash, "/api/crash/unqueue": crashUnqueue,
  "/api/duel/join": duelJoin, "/api/duel/leave": duelLeave, "/api/poll": poll,
  "/api/deposit": deposit, "/api/deposit/check": depositCheck, "/api/withdraw": withdraw,
  "/api/me": async (u) => ({ id: u.id, name: u.name, balance: await balanceOf(pool, u.id) }),
};
function send(res, status, obj) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(JSON.stringify(obj));
}
async function readBody(req) {
  const chunks = []; let size = 0;
  for await (const c of req) { size += c.length; if (size > 65536) throw httpErr(413, "Слишком большой запрос"); chunks.push(c); }
  return Buffer.concat(chunks);
}
module.exports = async (req, res) => {
  const o = req.headers.origin;
  if (o && ORIGINS.includes(o)) { res.setHeader("Access-Control-Allow-Origin", o); res.setHeader("Vary", "Origin"); }
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === "OPTIONS") { res.statusCode = 204; return res.end(); }
  const path = new URL(req.url, "http://x").pathname.replace(/\/+$/, "") || "/";
  try {
    if (req.method === "GET" && (path === "/" || path === "/api")) {
      let db = "none";
      if (pool) db = await init().then(() => "ok", (e) => "error: " + e.message);
      return send(res, 200, { ok: true, service: "nullified-casino", db, verify: VERIFY, keys: !!(CLIENT_ID && CLIENT_SECRET) });
    }
    if (req.method !== "POST") return send(res, 404, { ok: false, error: "not_found" });
    const raw = await readBody(req);
    await init();
    if (path === "/huevo/callback") {
      if (!callbackSignatureOk(raw, req.headers["x-huevo-signature"])) { console.warn("[callback] bad signature"); return send(res, 401, { ok: false }); }
      let j = {}; try { j = JSON.parse(raw.toString("utf8")); } catch {}
      const intent = j.intent_id || (j.payment && j.payment.intent_id) || j.id;
      if (intent) await settlePayment(String(intent)).catch((e) => console.warn("[callback]", e.message));
      return send(res, 200, { ok: true });
    }
    let body = {};
    try { body = raw.length ? JSON.parse(raw.toString("utf8")) : {}; } catch { throw httpErr(400, "Неверный JSON"); }
    if (path === "/api/login") return send(res, 200, { ok: true, ...(await login(body)) });
    const fn = routes[path];
    if (!fn) return send(res, 404, { ok: false, error: "not_found" });
    const u = readToken(String(req.headers.authorization || "").replace(/^Bearer\s+/i, ""));
    if (!u) throw httpErr(401, "session");
    send(res, 200, { ok: true, ...(await fn(u, body)) });
  } catch (e) {
    if (!e.status) console.error(e);
    send(res, e.status || 500, { ok: false, error: e.status ? e.message : "Ошибка сервера", ...(e.extra || {}) });
  }
};
