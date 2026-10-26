// Nullified Casino game server.
// Balances are kept in huEVO coins on this server; every game outcome is drawn here, never in the browser.
// Players come in through huEVO: the game sends its signed hu_* launch params to /api/login.
// Coins enter through huEVO payments (POST /payments + the player's confirmation + our callback)
// and leave through payouts from the app wallet.
//
// Environment:
//   HUEVO_CLIENT_ID, HUEVO_CLIENT_SECRET  app keys from huEVO (never commit them)
//   HUEVO_API          default https://huevo.su/api/apps/v1
//   HUEVO_PAYOUT_PATH  payout method path, default /payouts (not documented by huEVO yet)
//   HUEVO_VERIFY       strict | log | off - what to do when the launch signature does not check out
//   DATABASE_URL       Postgres; without it data lives in data.json (lost on Render restarts)
//   ALLOWED_ORIGINS    comma list of game origins allowed to call the API
"use strict";
const http = require("http");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const CLIENT_ID = process.env.HUEVO_CLIENT_ID || "";
const CLIENT_SECRET = process.env.HUEVO_CLIENT_SECRET || "";
const HUEVO_API = (process.env.HUEVO_API || "https://huevo.su/api/apps/v1").replace(/\/$/, "");
const PAYOUT_PATH = process.env.HUEVO_PAYOUT_PATH || "/payouts";
const VERIFY = process.env.HUEVO_VERIFY || "strict";
const ORIGINS = (process.env.ALLOWED_ORIGINS || "https://nullifiedmember.github.io,http://localhost:8765").split(",").map((s) => s.trim()).filter(Boolean);

const BET_MIN = 10, BET_MAX = 95000;
const DEPOSIT_MIN = 10, DEPOSIT_MAX = 1000000;
const EDGE = 0.97;

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

// ---------- storage ----------
function jsonStore() {
  const file = path.join(__dirname, "data.json");
  let db = { users: {}, payments: {}, payouts: [] };
  try { db = JSON.parse(fs.readFileSync(file, "utf8")); } catch {}
  let dirty = false;
  setInterval(() => { if (dirty) { dirty = false; fs.writeFile(file, JSON.stringify(db), () => {}); } }, 2000);
  const touch = () => { dirty = true; };
  return {
    kind: "json",
    async init() {},
    async upsertUser(id, name) { db.users[id] = db.users[id] || { balance: 0 }; db.users[id].name = name; touch(); return db.users[id].balance; },
    async balance(id) { return db.users[id] ? db.users[id].balance : 0; },
    // atomic change; null when it would go below zero
    async change(id, delta) {
      const u = db.users[id];
      if (!u || u.balance + delta < 0) return null;
      u.balance += delta; touch();
      return u.balance;
    },
    async addPayment(intent, userId, amount) { db.payments[intent] = { userId, amount, credited: false }; touch(); },
    async claimPayment(intent) {
      const p = db.payments[intent];
      if (!p || p.credited) return null;
      p.credited = true; touch();
      return p;
    },
    async addPayout(userId, amount, ok, info) { db.payouts.push({ userId, amount, ok, info, at: Date.now() }); touch(); },
  };
}
function pgStore(url) {
  const { Pool } = require("pg");
  const pool = new Pool({ connectionString: url, ssl: /localhost|127\.0\.0\.1/.test(url) ? false : { rejectUnauthorized: false } });
  return {
    kind: "postgres",
    async init() {
      await pool.query(`CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY, name TEXT, balance BIGINT NOT NULL DEFAULT 0, created_at TIMESTAMPTZ DEFAULT now())`);
      await pool.query(`CREATE TABLE IF NOT EXISTS payments (intent_id TEXT PRIMARY KEY, user_id BIGINT, amount BIGINT, credited BOOLEAN NOT NULL DEFAULT false, created_at TIMESTAMPTZ DEFAULT now())`);
      await pool.query(`CREATE TABLE IF NOT EXISTS payouts (id BIGSERIAL PRIMARY KEY, user_id BIGINT, amount BIGINT, ok BOOLEAN, info TEXT, created_at TIMESTAMPTZ DEFAULT now())`);
    },
    async upsertUser(id, name) {
      const r = await pool.query(`INSERT INTO users (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING balance`, [id, name]);
      return Number(r.rows[0].balance);
    },
    async balance(id) { const r = await pool.query(`SELECT balance FROM users WHERE id = $1`, [id]); return r.rows[0] ? Number(r.rows[0].balance) : 0; },
    async change(id, delta) {
      const r = await pool.query(`UPDATE users SET balance = balance + $2 WHERE id = $1 AND balance + $2 >= 0 RETURNING balance`, [id, delta]);
      return r.rows[0] ? Number(r.rows[0].balance) : null;
    },
    async addPayment(intent, userId, amount) { await pool.query(`INSERT INTO payments (intent_id, user_id, amount) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, [intent, userId, amount]); },
    async claimPayment(intent) {
      const r = await pool.query(`UPDATE payments SET credited = true WHERE intent_id = $1 AND credited = false RETURNING user_id, amount`, [intent]);
      return r.rows[0] ? { userId: Number(r.rows[0].user_id), amount: Number(r.rows[0].amount) } : null;
    },
    async addPayout(userId, amount, ok, info) { await pool.query(`INSERT INTO payouts (user_id, amount, ok, info) VALUES ($1, $2, $3, $4)`, [userId, amount, ok, String(info).slice(0, 500)]); },
  };
}
const store = process.env.DATABASE_URL ? pgStore(process.env.DATABASE_URL) : jsonStore();

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

// ---------- sessions ----------
const sessions = new Map();   // token -> {id, name}
function newSession(id, name) {
  const token = crypto.randomBytes(24).toString("hex");
  sessions.set(token, { id, name });
  return token;
}
const winners = [];   // site-wide ticker, real wins only
let broadcast = () => {};
function pushWin(name, game, amount, mult) {
  const w = { name, game, amount, mult: mult || null, at: Date.now() };
  winners.unshift(w); winners.length = Math.min(winners.length, 20);
  broadcast({ t: "win", win: w });
}

// ---------- slots ----------
const PAYS = { rott: [0, 0, 0, 65, 260, 1300], pug: [0, 0, 0, 45, 130, 650], poodle: [0, 0, 0, 32, 90, 440], guide: [0, 0, 0, 22, 65, 260],
  bone: [0, 0, 0, 17, 44, 175], A: [0, 0, 0, 11, 26, 90], K: [0, 0, 0, 11, 22, 77], Q: [0, 0, 0, 8, 17, 55], J: [0, 0, 0, 6, 13, 44], T: [0, 0, 0, 4, 11, 33] };
const BASE_WEIGHTS = { rott: 3, pug: 4, poodle: 5, guide: 6, bone: 8, A: 10, K: 10, Q: 12, J: 12, T: 12 };
const WILD_REELS = [1, 2, 3], SCATTER_REELS = [0, 2, 4], WILD_WEIGHT = 2.2, SCATTER_WEIGHT = 3.4, SCATTER_PAY = 5;
const FS_COUNTS = [[9, 35], [12, 30], [15, 20], [18, 10], [21, 5]];
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
    if (pay) wins.push({ line: li, sym: first.s, count, mult, amount: Math.floor(pay * lineBet * mult), cells });
  });
  const scatters = [];
  grid.forEach((col, c) => col.forEach((cell, r) => { if (cell.s === "S") scatters.push([c, r]); }));
  const scatterWin = scatters.length >= 3 ? SCATTER_PAY * totalBet : 0;
  return { wins, scatters, scatterWin, total: wins.reduce((a, w) => a + w.amount, 0) + scatterWin };
}
const slotState = new Map();   // userId -> {fsLeft, fsWin, bet, sticky}
async function slotsSpin(u, body) {
  const st = slotState.get(u.id) || { fsLeft: 0, fsWin: 0, bet: 0, sticky: null };
  const free = st.fsLeft > 0;
  const bet = free ? st.bet : Number(body.bet);
  if (!free) {
    if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
    if ((await store.change(u.id, -bet)) === null) throw httpErr(402, "Не хватает монэток на ставку");
  } else st.fsLeft--;
  const grid = spinGrid(free, free ? st.sticky : null);
  const res = evaluate(grid, bet);
  let fsAwarded = 0, fsEnded = false;
  if (free) {
    st.sticky = st.sticky || {};
    grid.forEach((col, c) => col.forEach((cell, r) => { if (cell.s === "W") st.sticky[c + ":" + r] = { s: "W", m: cell.m }; }));
    st.fsWin += res.total;
    if (st.fsLeft === 0) { fsEnded = true; st.sticky = null; }
  } else if (res.scatters.length >= 3) {
    fsAwarded = weighted(FS_COUNTS);
    Object.assign(st, { fsLeft: fsAwarded, fsWin: 0, bet, sticky: {} });
  }
  slotState.set(u.id, st);
  const balance = res.total ? await store.change(u.id, res.total) : await store.balance(u.id);
  if (res.total > 0) pushWin(u.name, "Slots", res.total);
  return { grid, ...res, bet, free, fsLeft: st.fsLeft, fsWin: st.fsWin, fsAwarded, fsEnded, balance };
}

// ---------- mines ----------
const MINE_TILES = 25;
function minesMult(picks, count) { let m = EDGE; for (let i = 0; i < picks; i++) m *= (MINE_TILES - i) / (MINE_TILES - count - i); return picks ? m : 1; }
const mineState = new Map();
async function minesStart(u, body) {
  if (mineState.get(u.id)) throw httpErr(409, "Раунд уже идёт");
  const bet = Number(body.bet), count = Number(body.count);
  if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
  if (!Number.isInteger(count) || count < 1 || count > 24) throw httpErr(400, "Мин от 1 до 24");
  const balance = await store.change(u.id, -bet);
  if (balance === null) throw httpErr(402, "Не хватает монэток на ставку");
  const swords = new Set();
  while (swords.size < count) swords.add(crypto.randomInt(MINE_TILES));
  mineState.set(u.id, { bet, count, swords, opened: new Set() });
  return { balance };
}
async function minesCashout(u, st) {
  mineState.delete(u.id);
  const mult = minesMult(st.opened.size, st.count), win = Math.floor(st.bet * mult);
  const balance = await store.change(u.id, win);
  pushWin(u.name, "Mines", win, mult);
  return { cashed: true, win, mult, swords: [...st.swords], opened: [...st.opened], balance };
}
async function minesOpen(u, body) {
  const st = mineState.get(u.id), i = Number(body.i);
  if (!st) throw httpErr(409, "Нет активного раунда");
  if (!Number.isInteger(i) || i < 0 || i >= MINE_TILES || st.opened.has(i)) throw httpErr(400, "Неверная плита");
  if (st.swords.has(i)) { mineState.delete(u.id); return { boom: i, swords: [...st.swords], opened: [...st.opened], balance: await store.balance(u.id) }; }
  st.opened.add(i);
  if (st.opened.size === MINE_TILES - st.count) return minesCashout(u, st);
  return { safe: i, opened: [...st.opened], mult: minesMult(st.opened.size, st.count), next: minesMult(st.opened.size + 1, st.count) };
}
async function minesCash(u) {
  const st = mineState.get(u.id);
  if (!st || !st.opened.size) throw httpErr(409, "Сначала открой плиту");
  return minesCashout(u, st);
}

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
const bjState = new Map();
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
async function bjFinish(u, st) {
  const d = handValue(st.dealer), dealerBJ = isBJ(st.dealer);
  let paid = 0, staked = 0;
  for (const h of st.hands) {
    const p = handValue(h.cards), natural = !h.fromSplit && st.hands.length === 1 && isBJ(h.cards);
    if (p > 21) h.result = "lose";
    else if (natural && !dealerBJ) h.result = "bj";
    else if (dealerBJ) h.result = natural ? "push" : "lose";
    else if (d > 21 || p > d) h.result = "win";
    else h.result = p === d ? "push" : "lose";
    paid += Math.floor({ bj: h.stake * 2.5, win: h.stake * 2, push: h.stake, lose: 0 }[h.result]);
    staked += h.stake;
  }
  st.phase = "done";
  st.outcome = { paid, staked, net: paid - staked };
  bjState.delete(u.id);
  const balance = paid ? await store.change(u.id, paid) : await store.balance(u.id);
  if (paid > staked) pushWin(u.name, "Blackjack", paid);
  return balance;
}
async function bjAdvance(u, st) {
  for (;;) {
    const i = st.hands.findIndex((h) => !h.done);
    if (i === -1) {
      if (st.hands.some((h) => handValue(h.cards) <= 21)) while (handValue(st.dealer) < 17) st.dealer.push(bjDraw(st));
      return bjFinish(u, st);
    }
    st.active = i;
    const h = st.hands[i];
    if (h.cards.length === 1) h.cards.push(bjDraw(st));
    if (h.splitAces || handValue(h.cards) === 21) { h.done = true; continue; }
    return store.balance(u.id);
  }
}
async function bjDeal(u, body) {
  if (bjState.get(u.id)) throw httpErr(409, "Раздача уже идёт");
  const bet = Number(body.bet);
  if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
  if ((await store.change(u.id, -bet)) === null) throw httpErr(402, "Не хватает монэток на ставку");
  const prev = bjShoes.get(u.id);
  const st = { shoe: prev && prev.length >= 60 ? prev : newShoe(), hands: [], dealer: [], active: 0, phase: "player" };
  st.hands = [{ cards: [bjDraw(st), bjDraw(st)], stake: bet, fromSplit: false, splitAces: false, done: false }];
  st.dealer = [bjDraw(st), bjDraw(st)];
  bjShoes.set(u.id, st.shoe);
  bjState.set(u.id, st);
  let balance;
  if (isBJ(st.hands[0].cards) || isBJ(st.dealer)) { st.hands[0].done = true; balance = await bjFinish(u, st); }
  else balance = await store.balance(u.id);
  return bjView(st, balance);
}
const bjShoes = new Map();
async function bjAction(u, body) {
  const st = bjState.get(u.id);
  if (!st || st.phase !== "player") throw httpErr(409, "Нет активной раздачи");
  const h = st.hands[st.active], a = body.action;
  let balance;
  if (a === "hit") {
    h.cards.push(bjDraw(st));
    if (handValue(h.cards) >= 21) { h.done = true; balance = await bjAdvance(u, st); } else balance = await store.balance(u.id);
  } else if (a === "stand") {
    h.done = true; balance = await bjAdvance(u, st);
  } else if (a === "double") {
    if (h.cards.length !== 2 || h.splitAces) throw httpErr(400, "Удвоить нельзя");
    if ((await store.change(u.id, -h.stake)) === null) throw httpErr(402, "Не хватает монэток");
    h.stake *= 2; h.cards.push(bjDraw(st)); h.done = true; balance = await bjAdvance(u, st);
  } else if (a === "split") {
    if (h.cards.length !== 2 || h.splitAces || st.hands.length >= 4 || cardPoints(h.cards[0]) !== cardPoints(h.cards[1])) throw httpErr(400, "Разделить нельзя");
    if ((await store.change(u.id, -h.stake)) === null) throw httpErr(402, "Не хватает монэток");
    const twin = { cards: [h.cards.pop()], stake: h.stake, fromSplit: true, splitAces: false, done: false };
    h.fromSplit = true;
    if (h.cards[0].r === "A") { h.splitAces = true; twin.splitAces = true; }
    st.hands.splice(st.active + 1, 0, twin);
    h.cards.push(bjDraw(st));
    if (h.splitAces || handValue(h.cards) === 21) { h.done = true; balance = await bjAdvance(u, st); } else balance = await store.balance(u.id);
  } else throw httpErr(400, "Неизвестное действие");
  return bjView(st, balance);
}

// ---------- upgrader ----------
const UPG_MULTS = [1.2, 1.5, 2, 3, 5, 10, 20, 50, 100];
async function upgSpin(u, body) {
  const bet = Number(body.bet), mi = Number(body.mult);
  if (!validBet(bet)) throw httpErr(400, "Ставка от " + BET_MIN + " до " + BET_MAX);
  if (!Number.isInteger(mi) || mi < 0 || mi >= UPG_MULTS.length) throw httpErr(400, "Неверный множитель");
  if ((await store.change(u.id, -bet)) === null) throw httpErr(402, "Не хватает монэток на ставку");
  const mult = UPG_MULTS[mi], chance = EDGE / mult, arcStart = -chance * 180;
  const land = rnd() * 360, win = ((land - arcStart + 360) % 360) < chance * 360;
  const prize = Math.floor(bet * mult);
  const balance = win ? await store.change(u.id, prize) : await store.balance(u.id);
  if (win) pushWin(u.name, "Upgrader", prize, mult);
  return { land, win, prize, balance };
}

// ---------- coffins ----------
const COFFIN_PRICES = [100, 500, 2500, 10000, 50000];
const LOOT = [[0.2, 30], [0.5, 25], [0.8, 18], [1.2, 12], [2, 8], [3, 4], [5, 2], [10, 0.8], [25, 0.2]];
const LOOT_W = LOOT.reduce((a, [, w]) => a + w, 0);
const LOOT_K = EDGE / (LOOT.reduce((a, [m, w]) => a + m * w, 0) / LOOT_W);
async function coffinOpen(u, body) {
  const ci = Number(body.coffin);
  if (!Number.isInteger(ci) || ci < 0 || ci >= COFFIN_PRICES.length) throw httpErr(400, "Неверный гроб");
  const price = COFFIN_PRICES[ci];
  if ((await store.change(u.id, -price)) === null) throw httpErr(402, "Не хватает монэток на этот гроб");
  const loot = weighted(LOOT.map(([m, w], i) => [i, w]));
  const pay = Math.round(price * LOOT[loot][0] * LOOT_K);
  const balance = await store.change(u.id, pay);
  if (pay > price) pushWin(u.name, "Coffins", pay, pay / price);
  return { loot, pay, balance };
}

// ---------- deposits and withdrawals ----------
async function deposit(u, body) {
  const amount = Number(body.amount);
  if (!Number.isInteger(amount) || amount < DEPOSIT_MIN || amount > DEPOSIT_MAX) throw httpErr(400, "Сумма от " + DEPOSIT_MIN + " до " + DEPOSIT_MAX);
  const j = await huevo("POST", "/payments", { user_id: u.id, amount, description: "Пополнение баланса Nullified Casino" });
  const intent = j.intent_id || j.id || (j.payment && (j.payment.intent_id || j.payment.id)) || (j.intent && (j.intent.id || j.intent.intent_id));
  if (!intent) { console.warn("[deposit] no intent id in", JSON.stringify(j).slice(0, 400)); throw httpErr(502, "huEVO не вернул платёж"); }
  await store.addPayment(intent, u.id, amount);
  return { intent_id: intent };
}
// credit a payment once, after huEVO confirms it succeeded
async function settlePayment(intent) {
  const j = await huevo("GET", "/payments/" + encodeURIComponent(intent));
  const p = j.payment || j.intent || j;
  if (p.status !== "succeeded") return { status: p.status };
  const claim = await store.claimPayment(intent);
  if (!claim) return { status: "succeeded", already: true };
  const balance = await store.change(claim.userId, claim.amount);
  console.log("[deposit] credited", claim.amount, "to", claim.userId);
  return { status: "succeeded", credited: claim.amount, userId: claim.userId, balance };
}
async function depositCheck(u, body) {
  const r = await settlePayment(String(body.intent_id || ""));
  return { status: r.status, credited: r.userId === u.id ? r.credited || 0 : 0, balance: await store.balance(u.id) };
}
async function withdraw(u, body) {
  const amount = Number(body.amount);
  if (!Number.isInteger(amount) || amount < DEPOSIT_MIN) throw httpErr(400, "Минимум " + DEPOSIT_MIN);
  const balance = await store.change(u.id, -amount);
  if (balance === null) throw httpErr(402, "На балансе нет столько монэток");
  try {
    const j = await huevo("POST", PAYOUT_PATH, { user_id: u.id, amount, description: "Вывод из Nullified Casino" });
    await store.addPayout(u.id, amount, true, JSON.stringify(j));
    return { ok: true, balance };
  } catch (e) {
    const back = await store.change(u.id, amount);
    await store.addPayout(u.id, amount, false, e.code + " " + e.message);
    throw httpErr(502, e.code === "not_found" ? "Вывод пока недоступен" : "huEVO: " + e.message, { balance: back });
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

// ---------- crash (shared rounds) ----------
const CRASH_K = 0.14, CRASH_BET_MS = 6000, CRASH_PAUSE_MS = 3000, CRASH_MAX = 1000;
const crashPoint = () => Math.min(CRASH_MAX, Math.max(1, Math.floor(EDGE / (1 - rnd()) * 100) / 100));
const crash = { id: 0, phase: "bet", betEnds: 0, flyStart: 0, point: 1, players: new Map(), queue: new Map(), hist: [] };
const crashMult = (ms) => Math.exp(CRASH_K * ms / 1000);
function crashPublic() {
  return { t: "crash", id: crash.id, phase: crash.phase, betEnds: crash.betEnds, flyStart: crash.flyStart, now: Date.now(),
    point: crash.phase === "crashed" ? crash.point : null, hist: crash.hist,
    players: [...crash.players.values()].map((p) => ({ name: p.name, bet: p.bet, cashed: p.cashed, id: p.id })) };
}
async function crashNewRound() {
  crash.id++; crash.phase = "bet"; crash.betEnds = Date.now() + CRASH_BET_MS; crash.point = crashPoint(); crash.players = new Map();
  for (const [id, q] of crash.queue) {
    if ((await store.change(id, -q.bet)) !== null) crash.players.set(id, { ...q, cashed: 0 });
    sendTo(id, { t: "balance", balance: await store.balance(id) });
  }
  crash.queue.clear();
  broadcast(crashPublic());
}
async function crashCashout(p, mult) {
  if (p.cashed) return;
  p.cashed = Math.floor(mult * 100) / 100;
  const win = Math.floor(p.bet * p.cashed);
  const balance = await store.change(p.id, win);
  sendTo(p.id, { t: "balance", balance });
  pushWin(p.name, "Crash", win, p.cashed);
  broadcast(crashPublic());
}
setInterval(async () => {
  const now = Date.now();
  if (crash.phase === "bet" && now >= crash.betEnds) { crash.phase = "fly"; crash.flyStart = now; broadcast(crashPublic()); }
  else if (crash.phase === "fly") {
    const m = crashMult(now - crash.flyStart);
    for (const p of crash.players.values()) if (!p.cashed && p.auto && p.auto < crash.point && m >= p.auto) await crashCashout(p, p.auto);
    if (m >= crash.point) {
      crash.phase = "crashed"; crash.crashedAt = now;
      crash.hist = [crash.point, ...crash.hist].slice(0, 8);
      broadcast(crashPublic());
    }
  } else if (crash.phase === "crashed" && now - crash.crashedAt >= CRASH_PAUSE_MS) await crashNewRound();
}, 50);
async function crashBet(u, msg) {
  const bet = Number(msg.bet), auto = Number(msg.auto) || 0;
  if (!validBet(bet)) return sendTo(u.id, { t: "error", message: "Ставка от " + BET_MIN + " до " + BET_MAX });
  if (crash.players.has(u.id) || crash.queue.has(u.id)) return;
  const entry = { id: u.id, name: u.name, bet, auto: auto >= 1.01 ? auto : 0 };
  if (crash.phase !== "bet") { crash.queue.set(u.id, entry); return sendTo(u.id, { t: "crash_queued" }); }
  const balance = await store.change(u.id, -bet);
  if (balance === null) return sendTo(u.id, { t: "error", message: "Не хватает монэток на ставку" });
  crash.players.set(u.id, { ...entry, cashed: 0 });
  sendTo(u.id, { t: "balance", balance });
  broadcast(crashPublic());
}
async function crashCash(u) {
  const p = crash.players.get(u.id);
  if (!p || p.cashed || crash.phase !== "fly") return;
  const m = crashMult(Date.now() - crash.flyStart);
  if (m >= crash.point) return;
  await crashCashout(p, m);
}

// ---------- duel (one waiting player per stake room) ----------
const DUEL_STAKES = [100, 500, 1000, 5000, 25000, 95000], DUEL_FEE = 0.03;
const duelWaiting = new Map();   // stake -> {id, name}
const duelRooms = () => ({ t: "duel_rooms", rooms: DUEL_STAKES.map((s) => ({ stake: s, waiting: duelWaiting.get(s) ? duelWaiting.get(s).name : null, waitingId: duelWaiting.get(s) ? duelWaiting.get(s).id : null })) });
async function duelJoin(u, msg) {
  const stake = Number(msg.stake);
  if (!DUEL_STAKES.includes(stake)) return sendTo(u.id, { t: "error", message: "Нет такого зала" });
  for (const [s, w] of duelWaiting) if (w.id === u.id) return sendTo(u.id, { t: "error", message: "Ты уже ждёшь в зале " + s });
  const balance = await store.change(u.id, -stake);
  if (balance === null) return sendTo(u.id, { t: "error", message: "Не хватает монэток на ставку" });
  sendTo(u.id, { t: "balance", balance });
  const waiter = duelWaiting.get(stake);
  if (!waiter) { duelWaiting.set(stake, { id: u.id, name: u.name }); sendTo(u.id, { t: "duel_wait", stake }); return broadcast(duelRooms()); }
  duelWaiting.delete(stake);
  const joinerDies = rnd() < 0.5;
  const winner = joinerDies ? waiter : u, prize = Math.floor(stake * 2 * (1 - DUEL_FEE));
  const wb = await store.change(winner.id, prize);
  pushWin(winner.name, "Duel", prize);
  for (const [me, opp, iDie] of [[u, waiter, joinerDies], [waiter, u, !joinerDies]]) {
    sendTo(me.id, { t: "duel_match", stake, opp: opp.name, youDie: iDie, prize, balance: me.id === winner.id ? wb : await store.balance(me.id) });
  }
  broadcast(duelRooms());
}
async function duelLeave(u) {
  for (const [s, w] of duelWaiting) if (w.id === u.id) {
    duelWaiting.delete(s);
    sendTo(u.id, { t: "balance", balance: await store.change(u.id, s) });
    broadcast(duelRooms());
  }
}

// ---------- HTTP ----------
function httpErr(status, message, extra) { const e = new Error(message); e.status = status; e.extra = extra; return e; }
function cors(req, res) {
  const o = req.headers.origin;
  if (o && ORIGINS.includes(o)) { res.setHeader("Access-Control-Allow-Origin", o); res.setHeader("Vary", "Origin"); }
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
}
function send(res, status, obj) { res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" }); res.end(JSON.stringify(obj)); }
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []; let size = 0;
    req.on("data", (c) => { size += c.length; if (size > 65536) { reject(httpErr(413, "Слишком большой запрос")); req.destroy(); } else chunks.push(c); });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}
const routes = {
  "/api/slots/spin": slotsSpin, "/api/mines/start": minesStart, "/api/mines/open": minesOpen, "/api/mines/cashout": minesCash,
  "/api/bj/deal": bjDeal, "/api/bj/action": bjAction, "/api/upg/spin": upgSpin, "/api/coffin/open": coffinOpen,
  "/api/deposit": deposit, "/api/deposit/check": depositCheck, "/api/withdraw": withdraw,
  "/api/me": async (u) => ({ id: u.id, name: u.name, balance: await store.balance(u.id) }),
};
const server = http.createServer(async (req, res) => {
  cors(req, res);
  if (req.method === "OPTIONS") { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, "http://x");
  try {
    if (req.method === "GET" && url.pathname === "/") return send(res, 200, { ok: true, service: "nullified-casino", store: store.kind });
    if (req.method !== "POST") return send(res, 404, { ok: false, error: "not_found" });
    const raw = await readBody(req);
    if (url.pathname === "/huevo/callback") {
      if (!callbackSignatureOk(raw, req.headers["x-huevo-signature"])) { console.warn("[callback] bad signature"); return send(res, 401, { ok: false }); }
      let j = {}; try { j = JSON.parse(raw.toString("utf8")); } catch {}
      const intent = j.intent_id || (j.payment && j.payment.intent_id) || j.id;
      if (intent) {
        const r = await settlePayment(String(intent)).catch((e) => ({ error: e.message }));
        if (r.userId) sendTo(r.userId, { t: "balance", balance: r.balance, deposit: r.credited });
      }
      return send(res, 200, { ok: true });
    }
    let body = {};
    try { body = raw.length ? JSON.parse(raw.toString("utf8")) : {}; } catch { throw httpErr(400, "Неверный JSON"); }
    if (url.pathname === "/api/login") {
      const launch = body.launch || {};
      const id = parseInt(launch.hu_user_id, 10);
      if (!id) throw httpErr(401, "Открой игру через huEVO");
      const v = verifyLaunch(launch);
      if (!v.ok) {
        console.warn("[login] launch signature did not match for user", id, "mode", VERIFY);
        if (VERIFY === "strict") throw httpErr(401, "Не удалось проверить вход через huEVO");
      } else console.log("[login] signature ok, scheme", v.scheme);
      const name = String(launch.hu_display_name || launch.hu_username || "Игрок " + id).slice(0, 40);
      const balance = await store.upsertUser(id, name);
      return send(res, 200, { ok: true, token: newSession(id, name), user: { id, name }, balance, test: launch.hu_mode === "test", winners });
    }
    const fn = routes[url.pathname];
    if (!fn) return send(res, 404, { ok: false, error: "not_found" });
    const tok = String(req.headers.authorization || "").replace(/^Bearer\s+/i, "");
    const u = sessions.get(tok);
    if (!u) throw httpErr(401, "session");
    send(res, 200, { ok: true, ...(await fn(u, body)) });
  } catch (e) {
    if (!e.status) console.error(e);
    send(res, e.status || 500, { ok: false, error: e.status ? e.message : "Ошибка сервера", ...(e.extra || {}) });
  }
});

// ---------- WebSocket: crash, duel, ticker ----------
const wss = new WebSocketServer({ server, path: "/ws" });
const sockets = new Map();   // userId -> Set(ws)
function sendTo(id, msg) { const s = sockets.get(id); if (s) for (const ws of s) if (ws.readyState === 1) ws.send(JSON.stringify(msg)); }
broadcast = (msg) => { const data = JSON.stringify(msg); for (const ws of wss.clients) if (ws.readyState === 1) ws.send(data); };
wss.on("connection", (ws, req) => {
  const o = req.headers.origin;
  if (o && !ORIGINS.includes(o)) return ws.close();
  let u = null;
  ws.send(JSON.stringify(crashPublic())); ws.send(JSON.stringify(duelRooms())); ws.send(JSON.stringify({ t: "wins", wins: winners }));
  ws.on("message", async (data) => {
    let msg; try { msg = JSON.parse(data); } catch { return; }
    if (msg.t === "auth") {
      u = sessions.get(String(msg.token || "")) || null;
      if (u) { if (!sockets.has(u.id)) sockets.set(u.id, new Set()); sockets.get(u.id).add(ws); }
      return ws.send(JSON.stringify({ t: "auth", ok: !!u }));
    }
    if (!u) return;
    try {
      if (msg.t === "crash_bet") await crashBet(u, msg);
      else if (msg.t === "crash_cash") await crashCash(u);
      else if (msg.t === "crash_unqueue") crash.queue.delete(u.id);
      else if (msg.t === "duel_join") await duelJoin(u, msg);
      else if (msg.t === "duel_leave") await duelLeave(u);
    } catch (e) { console.error(e); }
  });
  ws.on("close", async () => {
    if (!u) return;
    const s = sockets.get(u.id); if (s) { s.delete(ws); if (!s.size) { sockets.delete(u.id); await duelLeave(u); } }
  });
});
const ping = setInterval(() => { for (const ws of wss.clients) if (ws.readyState === 1) ws.ping(); }, 25000);
ping.unref();

store.init().then(() => {
  server.listen(PORT, () => console.log("Nullified Casino server on", PORT, "store:", store.kind, "verify:", VERIFY));
  crashNewRound();
}).catch((e) => { console.error("store init failed", e); process.exit(1); });
