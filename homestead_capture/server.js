// Homestead — Home Assistant add-on. The camera brain, ported from Supabase.
//
// Trigger (from an HA automation):  POST /event {camera:"driveway", kind:"vehicle", source:"ha"}
// Pipeline: capture live frame(s) from the Nest camera (headless Chromium WebRTC)
//   → zoomed region crops + known-empty reference crop → Claude via HA's AI task
//   → household / visitor / group rules (config/homestead/visitors.json)
//   → repeat suppression + 45 s follow-up look → deliver via HA services
//   (phone notification with photo, Dubón MP3 on speakers, Alexa if enabled)
//   → event log (media/homestead/events/*.jsonl + frames), ingress page shows it.
//
// Files (Home Assistant config dir, editable in File editor):
//   homestead/cameras.json   labels, device ids, scene text, regions, baseline file
//   homestead/visitors.json  groups (windows/days) + vehicles (household/visitor)
//   homestead/rules.md       base rules with a {{VEHICLES}} placeholder
// Media (/media/homestead): baselines/, audio/, events/, work/
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import puppeteer from "puppeteer-core";

const OPTIONS = JSON.parse(fs.readFileSync("/data/options.json", "utf8"));
const PORT = 8099;
const SDM = "https://smartdevicemanagement.googleapis.com/v1";
const HA = "http://supervisor/core/api";
const HA_TOKEN = process.env.SUPERVISOR_TOKEN;
const CFG_DIR = fs.existsSync("/homeassistant") ? "/homeassistant/homestead" : "/config/homestead";
const MEDIA = "/media/homestead";
const TZ = "America/New_York";
const READY_TIMEOUT_MS = 25_000;
const COOLDOWN_S = 90;
const FOLLOWUP_MS = 45_000;
const REPEAT_HOURS = 3;
const AI_ENTITY = OPTIONS.ai_task_entity || "ai_task.claude_ai_task";
const PAGE_HTML = fs.readFileSync(new URL("./capture.html", import.meta.url), "utf8");
const UI_HTML = fs.readFileSync(new URL("./ui.html", import.meta.url), "utf8");
for (const d of ["baselines", "audio", "events", "work"]) fs.mkdirSync(path.join(MEDIA, d), { recursive: true });
fs.mkdirSync(CFG_DIR, { recursive: true });

const log = (...a) => console.log(new Date().toISOString(), ...a);
const readJson = (p, fallback) => { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return fallback; } };
const cfg = {
  cameras: () => readJson(path.join(CFG_DIR, "cameras.json"), { cameras: [] }),
  visitors: () => readJson(path.join(CFG_DIR, "visitors.json"), { groups: [], vehicles: [] }),
  rules: () => { try { return fs.readFileSync(path.join(CFG_DIR, "rules.md"), "utf8"); } catch { return "Describe what you see."; } },
};

// ---------- Google Device Access (WebRTC offer) ----------
let token = { value: null, expires: 0 };
async function accessToken() {
  if (token.value && token.expires > Date.now() + 120_000) return token.value;
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ client_id: OPTIONS.client_id, client_secret: OPTIONS.client_secret, refresh_token: OPTIONS.refresh_token, grant_type: "refresh_token" }),
  });
  const j = await r.json();
  if (!r.ok || !j.access_token) throw new Error("google token refresh failed: " + JSON.stringify(j).slice(0, 200));
  token = { value: j.access_token, expires: Date.now() + (j.expires_in ?? 3600) * 1000 };
  return token.value;
}
async function sdmOffer(deviceId, offerSdp) {
  const at = await accessToken();
  const r = await fetch(`${SDM}/enterprises/${OPTIONS.project_id}/devices/${deviceId}:executeCommand`, {
    method: "POST", headers: { authorization: `Bearer ${at}`, "content-type": "application/json" },
    body: JSON.stringify({ command: "sdm.devices.commands.CameraLiveStream.GenerateWebRtcStream", params: { offerSdp } }),
  });
  const j = await r.json();
  if (!r.ok) throw new Error(`SDM ${r.status}: ${JSON.stringify(j).slice(0, 300)}`);
  return { answer_sdp: j.results?.answerSdp, media_session_id: j.results?.mediaSessionId };
}

// ---------- Headless browser: capture + crops ----------
let browserP = null;
function browser() {
  if (!browserP) {
    browserP = puppeteer.launch({
      executablePath: process.env.CHROME_BIN, headless: true,
      args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--autoplay-policy=no-user-gesture-required", "--use-fake-ui-for-media-stream", "--mute-audio", "--window-size=1280,800"],
      defaultViewport: { width: 1280, height: 800 },
    }).catch((e) => { browserP = null; throw e; });
  }
  return browserP;
}
let chain = Promise.resolve();
const serialize = (fn) => { const p = chain.then(fn, fn); chain = p.catch(() => {}); return p; };

async function captureFrame(deviceId) {
  const t0 = Date.now();
  const logs = [];
  const page = await (await browser()).newPage();
  try {
    page.on("console", (m) => logs.push(`[${m.type()}] ${m.text()}`));
    await page.goto(`http://127.0.0.1:${PORT}/capture.html?device=${encodeURIComponent(deviceId)}`, { waitUntil: "domcontentloaded", timeout: 15_000 });
    await page.waitForFunction(() => window.__frameReady === true || window.__error != null, { timeout: READY_TIMEOUT_MS, polling: 200 }).catch(() => {});
    const st = await page.evaluate(() => ({ ready: window.__frameReady === true, error: window.__error || null, vw: document.getElementById("v")?.videoWidth, vh: document.getElementById("v")?.videoHeight }));
    if (!st.ready) throw new Error((st.error || `no frame within ${READY_TIMEOUT_MS}ms`) + " | " + logs.slice(-3).join(" ; "));
    const dataUrl = await page.evaluate(() => window.__grabFrame());
    return { jpg: Buffer.from(String(dataUrl).replace(/^data:image\/jpeg;base64,/, ""), "base64"), width: st.vw, height: st.vh, ms: Date.now() - t0 };
  } finally { await page.close().catch(() => {}); }
}
// Crop regions (fractions of the frame) at 3× using a throwaway canvas page.
async function cropRegions(jpg, regions, scale = 3) {
  if (!regions?.length) return {};
  const page = await (await browser()).newPage();
  try {
    await page.setContent("<canvas id=c></canvas><img id=i>");
    const out = await page.evaluate(async (b64, regions, scale) => {
      const img = document.getElementById("i"); img.src = "data:image/jpeg;base64," + b64;
      await new Promise((r) => { img.onload = r; });
      const c = document.getElementById("c"); const ctx = c.getContext("2d"); const res = {};
      for (const reg of regions) {
        const sx = reg.x * img.naturalWidth, sy = reg.y * img.naturalHeight, sw = reg.w * img.naturalWidth, sh = reg.h * img.naturalHeight;
        c.width = Math.round(sw * scale); c.height = Math.round(sh * scale);
        ctx.imageSmoothingEnabled = true; ctx.imageSmoothingQuality = "high";
        ctx.drawImage(img, sx, sy, sw, sh, 0, 0, c.width, c.height);
        res[reg.name] = c.toDataURL("image/jpeg", 0.88).replace(/^data:image\/jpeg;base64,/, "");
      }
      return res;
    }, jpg.toString("base64"), regions, scale);
    return Object.fromEntries(Object.entries(out).map(([k, v]) => [k, Buffer.from(v, "base64")]));
  } finally { await page.close().catch(() => {}); }
}

// ---------- Home Assistant core API (through the Supervisor) ----------
async function ha(method, p, body) {
  const r = await fetch(`${HA}${p}`, { method, headers: { authorization: `Bearer ${HA_TOKEN}`, "content-type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* not json */ }
  if (!r.ok) throw new Error(`HA ${method} ${p} → ${r.status}: ${text.slice(0, 300)}`);
  return j;
}
const haState = async (entity) => (await ha("GET", `/states/${entity}`).catch(() => null))?.state;
const haService = (domain, service, data) => ha("POST", `/services/${domain}/${service}`, data);

// ---------- Rules text (household / visitors / groups for this moment) ----------
function localParts(d) {
  const p = Object.fromEntries(new Intl.DateTimeFormat("en-US", { timeZone: TZ, weekday: "short", hour: "2-digit", minute: "2-digit", hour12: false }).formatToParts(d).map((x) => [x.type, x.value]));
  const isoDay = { Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6, Sun: 7 }[p.weekday] ?? 0;
  return { isoDay, hhmm: `${p.hour === "24" ? "00" : p.hour}:${p.minute}`, pretty: d.toLocaleString("en-US", { timeZone: TZ, weekday: "long", hour: "numeric", minute: "2-digit" }) };
}
const to12h = (hhmm) => { const [h, m] = hhmm.split(":").map(Number); return `${((h + 11) % 12) + 1}:${String(m).padStart(2, "0")} ${h >= 12 ? "PM" : "AM"}`; };
const DAY = ["", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const daysLabel = (days) => { const s = [...days].sort().join(","); return s === "1,2,3,4,5" ? "Mon-Fri" : s === "1,2,3,4,5,6,7" ? "every day" : days.map((d) => DAY[d]).join("/"); };
function activeWindow(g, when) { const { isoDay, hhmm } = localParts(when); if (!g.days?.includes(isoDay)) return null; return (g.windows || []).find((w) => w.start <= hhmm && hhmm <= w.end) ?? null; }
function vehicleRules(v, when) {
  const groups = (v.groups || []).filter((g) => g.enabled !== false);
  const vehicles = (v.vehicles || []).filter((x) => x.enabled !== false);
  const hh = vehicles.filter((x) => x.role === "household"), vis = vehicles.filter((x) => x.role !== "household");
  const now = localParts(when); const out = [];
  out.push(`HOUSEHOLD VEHICLES — the household owns EXACTLY these ${hh.length}, nothing else:`);
  for (const x of hh) out.push(`- ${x.description}`);
  out.push("", `KNOWN VISITOR VEHICLES (alert=false, category "known_visitor", put the owner label in quotes below in the "carrier" field — always the owner, never a group name). It is now ${now.pretty}.`);
  if (!vis.length) out.push("- (none registered)");
  for (const x of vis) {
    const g = groups.find((gg) => gg.id === x.group_id);
    let line = `- ${x.description}. Owner: ${x.name}.`;
    if (g) {
      const win = activeWindow(g, when);
      line += ` Part of the "${g.name}" group (${daysLabel(g.days)}: ${(g.windows || []).map((w) => `${w.label} ${to12h(w.start)}-${to12h(w.end)}`).join(", ")}).${g.notes ? " " + g.notes : ""}`;
      if (win) line += ` RIGHT NOW is inside the ${win.label} window, so this vehicle is expected → if present use carrier "${x.name}".`;
      else if (x.any_time) line += ` RIGHT NOW is outside the group windows, but this owner may come by at any time → if present use carrier "${x.name}".`;
      else line += ` RIGHT NOW is outside the group windows → this vehicle is NOT expected; only accept it as "${x.name}" if the match is unmistakable, otherwise treat it as unknown.`;
    } else line += x.any_time ? ` May arrive at any time → carrier "${x.name}".` : ` Carrier "${x.name}".`;
    out.push(line);
  }
  return out.join("\n");
}
const norm = (t) => String(t || "").toLowerCase().replace(/\s*\(.*$/, "").trim();
function announcementFor(verdict, v) {
  if (verdict.category !== "known_visitor" || typeof verdict.carrier !== "string") return null;
  const veh = (v.vehicles || []).find((x) => x.name.toLowerCase() === verdict.carrier.toLowerCase() || norm(x.name) === norm(verdict.carrier));
  if (!veh) return null;
  return veh.message || `${veh.name.replace(/\s*\(.*$/, "")} is here`;
}

// ---------- Dubón audio (rendered once per message via HA's ElevenLabs TTS) ----------
async function audioFor(text) {
  if (!text) return null;
  const file = path.join(MEDIA, "audio", crypto.createHash("sha1").update(text).digest("hex").slice(0, 12) + ".mp3");
  if (fs.existsSync(file)) return file;
  try {
    const res = await ha("POST", "/tts_get_url", { engine_id: OPTIONS.tts_entity || "tts.elevenlabs_text_to_speech", message: text.replace(/\bGMA\b/g, "G.M.A."), options: OPTIONS.tts_voice ? { voice: OPTIONS.tts_voice } : undefined });
    const r = await fetch(`http://supervisor/core${res.path}`, { headers: { authorization: `Bearer ${HA_TOKEN}` } });
    if (!r.ok) throw new Error("tts download " + r.status);
    fs.writeFileSync(file, Buffer.from(await r.arrayBuffer()));
    return file;
  } catch (e) { log("audio render failed:", e.message); return null; }
}
const mediaId = (file) => "media-source://media_source/local/" + path.relative("/media", file);

// ---------- Event log ----------
const dayKey = (d = new Date()) => new Intl.DateTimeFormat("en-CA", { timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
function appendEvent(ev) { fs.appendFileSync(path.join(MEDIA, "events", `${dayKey(new Date(ev.ts))}.jsonl`), JSON.stringify(ev) + "\n"); }
function readEvents(day) { try { return fs.readFileSync(path.join(MEDIA, "events", `${day}.jsonl`), "utf8").trim().split("\n").filter(Boolean).map((l) => JSON.parse(l)); } catch { return []; } }
function recentEvents(hours) { const since = Date.now() - hours * 3600_000; const days = [dayKey(new Date(since)), dayKey()]; return [...new Set(days)].flatMap(readEvents).filter((e) => new Date(e.ts).getTime() >= since); }
const COLOURS = ["dark blue", "red", "blue", "navy", "white", "black", "gray", "grey", "silver", "tan", "gold", "beige", "green", "brown", "maroon", "orange", "yellow"];
const BODIES = ["pickup", "truck", "suv", "crossover", "sedan", "minivan", "van", "wagon", "hatchback", "coupe", "jeep"];
function vehicleKey(desc) { const d = String(desc).toLowerCase(); const c = COLOURS.find((w) => d.includes(w)) ?? ""; const b = BODIES.find((w) => d.includes(w)) ?? ""; return c || b ? `${c}|${b}` : ""; }

// ---------- The pipeline ----------
const inflight = new Map(); // camera -> ts of last review started
async function analyze(cam, frames, when, eventId) {
  const workDir = path.join(MEDIA, "work", eventId); fs.mkdirSync(workDir, { recursive: true });
  const attachments = [];
  const add = (name, buf, label) => { const f = path.join(workDir, name); fs.writeFileSync(f, buf); attachments.push({ media_content_id: mediaId(f), media_content_type: "image/jpeg" }); return label; };
  const parts = [];
  const last = frames[frames.length - 1];
  parts.push(`IMAGE 1: the full current camera frame${frames.length > 1 ? ` (the LAST of ${frames.length} frames taken ~4s apart after motion)` : ""}.`);
  add("frame.jpg", last);
  let n = 2;
  if (cam.regions?.length) {
    const crops = await Promise.all(frames.map((f) => cropRegions(f, cam.regions)));
    let base = {};
    const baseFile = cam.baseline ? path.join(MEDIA, "baselines", cam.baseline) : null;
    if (baseFile && fs.existsSync(baseFile)) base = await cropRegions(fs.readFileSync(baseFile), cam.regions);
    for (const reg of cam.regions) {
      crops.forEach((c, i) => { if (c[reg.name]) { parts.push(`IMAGE ${n++}: zoomed-in crop of the "${reg.name}" region${frames.length > 1 ? ` — frame ${i + 1} of ${frames.length}` : ""}. Inspect this closely.`); add(`crop-${reg.name}-${i}.jpg`, c[reg.name]); } });
      if (base[reg.name]) { parts.push(`IMAGE ${n++}: the SAME "${reg.name}" region from a KNOWN-EMPTY reference frame (nothing parked, nobody present). Compare the current crop against this to spot anything new.`); add(`base-${reg.name}.jpg`, base[reg.name]); }
    }
  }
  const v = cfg.visitors();
  const rules = cfg.rules().replace("{{VEHICLES}}", vehicleRules(v, when));
  const instructions = `${parts.join("\n")}\n\n${rules}\n\nCAMERA SCENE (where things are in this camera's view): ${cam.scene || "not described"}\n\nContext: Camera "${cam.label}". Local time: ${when.toLocaleString("en-US", { timeZone: TZ })}.\n\nProcedure:\n1. Look at the zoomed crop(s) first. List EVERY vehicle, person, or notable object present in the region (color + type). Partially visible vehicles COUNT. If several frames are given they are a time sequence; judge by the LATEST frame.\n2. If a known-empty reference crop is given, state what is present now that is absent in the reference.\n3. Then apply the rules above. Keep region_contents to at most 4 short phrases.`;
  const res = await ha("POST", "/services/ai_task/generate_data?return_response", {
    task_name: `homestead-${cam.key}`, entity_id: AI_ENTITY, instructions, attachments,
    structure: {
      region_contents: { selector: { text: {} }, description: "up to 4 short phrases: what is in the region(s)" },
      vehicle_count: { selector: { number: {} }, description: "vehicles stopped/parked in the driveway or at the curb in the latest frame (not passing traffic)" },
      alert: { selector: { boolean: {} } },
      category: { selector: { select: { options: ["unknown_vehicle", "delivery", "person", "known_visitor", "household", "nothing"] } } },
      carrier: { selector: { text: {} }, description: "owner label of the recognized visitor vehicle, or the delivery company, else empty" },
      description: { selector: { text: {} }, description: "one sentence" },
    },
  });
  const verdict = res?.service_response?.data ?? res?.data ?? {};
  const household = (v.vehicles || []).filter((x) => x.role === "household" && x.enabled !== false).length || 2;
  const cnt = Number(verdict.vehicle_count);
  const explained = verdict.category === "known_visitor" || verdict.category === "delivery";
  if (Number.isFinite(cnt) && cnt > household && !verdict.alert && !explained) { verdict.alert = true; verdict.category = "unknown_vehicle"; verdict.override = `vehicle_count=${cnt} exceeds ${household} household vehicles`; }
  const ann = announcementFor(verdict, v);
  if (ann) verdict.announcement = ann;
  return verdict;
}

async function deliver(cam, ev, frameFile) {
  const rel = path.relative("/media", frameFile);
  if (ev.alert) {
    const what = String(ev.description ?? "").slice(0, 200);
    if (await haState("input_boolean.alexa_announcements") === "on") await haService("notify", "send_message", { entity_id: "notify.everywhere_announce", message: `${cam.label}: ${what}` }).catch((e) => log("alexa:", e.message));
    await haService("notify", OPTIONS.phone_notify || "mobile_app_josephs_iphone", { title: "Homestead", message: `${cam.label}: ${what}`, data: { image: `/media/local/${rel}` } }).catch((e) => log("phone:", e.message));
  } else if (ev.announcement) {
    const audio = await audioFor(ev.announcement);
    if (audio) await haService("media_player", "play_media", { entity_id: (OPTIONS.speakers || "media_player.home_assistant_voice_0a0948_media_player").split(",").map((s) => s.trim()), media_content_id: mediaId(audio), media_content_type: "music" }).catch((e) => log("speaker:", e.message));
    if (await haState("input_boolean.alexa_announcements") === "on") await haService("notify", "send_message", { entity_id: "notify.everywhere_announce", message: ev.announcement }).catch((e) => log("alexa:", e.message));
  }
}

async function runEvent({ camera, kind, source }, opts = {}) {
  const cams = cfg.cameras().cameras || [];
  const cam = cams.find((c) => c.key === camera || c.label?.toLowerCase() === String(camera).toLowerCase() || c.device_id === camera);
  if (!cam) throw new Error(`unknown camera "${camera}"`);
  const now = Date.now();
  const eventId = opts.eventId || `${cam.key}-${now}`;
  const ev = { id: eventId, ts: new Date(now).toISOString(), camera: cam.key, label: cam.label, kind: kind || "motion", source: source || "ha", status: "received", followup_of: opts.followupOf || null };
  const last = inflight.get(cam.key) || 0;
  if (!opts.followupOf && now - last < COOLDOWN_S * 1000) { ev.status = "skipped"; ev.reason = `duplicate within ${COOLDOWN_S}s`; appendEvent(ev); return ev; }
  inflight.set(cam.key, now);
  try {
    const delays = kind === "vehicle" ? [0] : [0, 4000];
    const frames = [];
    for (const d of delays) { if (d) await new Promise((r) => setTimeout(r, d)); frames.push((await serialize(() => captureFrame(cam.device_id))).jpg); }
    ev.received_ms = Date.now() - now;
    const frameFile = path.join(MEDIA, "events", `${eventId}.jpg`); fs.writeFileSync(frameFile, frames[frames.length - 1]);
    ev.frame = path.relative("/media", frameFile);
    const verdict = await analyze(cam, frames, new Date(now), eventId);
    Object.assign(ev, verdict, { status: "analyzed", reviewed_ms: Date.now() - now });
    // follow-up: only keep if something changed
    if (opts.followupOf) {
      const parent = recentEvents(1).find((e) => e.id === opts.followupOf);
      if (parent && parent.category === ev.category && String(parent.carrier ?? "") === String(ev.carrier ?? "") && Number(parent.vehicle_count ?? -1) === Number(ev.vehicle_count ?? -1)) { fs.rmSync(frameFile, { force: true }); return null; }
    }
    // repeat suppression for unknown vehicles
    if (ev.alert && ev.category === "unknown_vehicle") {
      const key = vehicleKey(ev.description);
      if (key && recentEvents(REPEAT_HOURS).some((e) => e.id !== ev.id && e.alert && e.category === "unknown_vehicle" && vehicleKey(e.description) === key)) ev.repeat_suppressed = true;
    }
    appendEvent(ev);
    if (!ev.repeat_suppressed) await deliver(cam, ev, frameFile);
    fs.rmSync(path.join(MEDIA, "work", eventId), { recursive: true, force: true });
    return ev;
  } catch (e) {
    ev.status = "error"; ev.error = String(e.message || e).slice(0, 300); appendEvent(ev); log("event error:", ev.error); return ev;
  } finally {
    if (!opts.followupOf && kind === "vehicle") setTimeout(() => runEvent({ camera: cam.key, kind, source }, { eventId: `${eventId}-followup`, followupOf: eventId }).catch(() => {}), FOLLOWUP_MS);
  }
}

// ---------- HTTP ----------
const json = (res, code, body) => { res.writeHead(code, { "content-type": "application/json" }); res.end(JSON.stringify(body)); };
const readBody = (req) => new Promise((resolve) => { let d = ""; req.on("data", (c) => (d += c)); req.on("end", () => resolve(d)); });
const isLocal = (req) => /^(127\.|::1|172\.30\.)/.test(req.socket.remoteAddress || "");

http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${PORT}`);
  const p = url.pathname.replace(/^\/api\/hassio_ingress\/[^/]+/, ""); // ingress prefix
  try {
    if (p === "/capture.html") { res.writeHead(200, { "content-type": "text/html" }); return res.end(PAGE_HTML); }
    if (p === "/health") return json(res, 200, { ok: true, browser: !!browserP, cfg_dir: CFG_DIR, cameras: (cfg.cameras().cameras || []).map((c) => c.key) });
    if (p === "/offer" && req.method === "POST") { const { device_id, offer_sdp } = JSON.parse(await readBody(req) || "{}"); return json(res, 200, await sdmOffer(device_id, offer_sdp)); }
    // ---- ingress UI + its JSON ----
    if (p === "/" || p === "") { res.writeHead(200, { "content-type": "text/html" }); return res.end(UI_HTML); }
    if (p === "/ui/events") return json(res, 200, readEvents(url.searchParams.get("day") || dayKey()).reverse());
    if (p === "/ui/visitors" && req.method === "GET") return json(res, 200, cfg.visitors());
    if (p === "/ui/visitors" && req.method === "POST") { const body = JSON.parse(await readBody(req)); fs.writeFileSync(path.join(CFG_DIR, "visitors.json"), JSON.stringify(body, null, 2)); return json(res, 200, { ok: true }); }
    if (p === "/ui/cameras" && req.method === "GET") return json(res, 200, cfg.cameras());
    if (p === "/ui/cameras" && req.method === "POST") { const body = JSON.parse(await readBody(req)); fs.writeFileSync(path.join(CFG_DIR, "cameras.json"), JSON.stringify(body, null, 2)); return json(res, 200, { ok: true }); }
    if (p === "/ui/frame") { const rel = String(url.searchParams.get("f") || "").replace(/\.\./g, ""); const f = path.join("/media", rel); if (!fs.existsSync(f)) return json(res, 404, { error: "no frame" }); res.writeHead(200, { "content-type": "image/jpeg" }); return res.end(fs.readFileSync(f)); }
    if (p === "/ui/audio" && req.method === "POST") { const { text } = JSON.parse(await readBody(req) || "{}"); const f = await audioFor(text); return json(res, f ? 200 : 500, { file: f && path.relative("/media", f) }); }
    if (p === "/ui/baseline" && req.method === "POST") { // {camera, frame:"events/x.jpg"} → copy as the camera's known-empty reference
      const { camera, frame } = JSON.parse(await readBody(req) || "{}"); const src = path.join("/media", String(frame).replace(/\.\./g, ""));
      const c = cfg.cameras(); const cam = (c.cameras || []).find((x) => x.key === camera); if (!cam || !fs.existsSync(src)) return json(res, 400, { error: "bad camera/frame" });
      cam.baseline = `${cam.key}.jpg`; fs.copyFileSync(src, path.join(MEDIA, "baselines", cam.baseline)); fs.writeFileSync(path.join(CFG_DIR, "cameras.json"), JSON.stringify(c, null, 2)); return json(res, 200, { ok: true, baseline: cam.baseline });
    }
    // ---- secret-gated API ----
    const body = req.method === "POST" ? JSON.parse(await readBody(req) || "{}") : {};
    const secret = body.secret || url.searchParams.get("secret");
    if (secret !== OPTIONS.secret) return json(res, 401, { error: "bad secret" });
    if (p === "/event") { // POST JSON, or GET ?camera=&kind=&wait=1 for tests from HA's shell
      const args = req.method === "POST" ? body : { camera: url.searchParams.get("camera"), kind: url.searchParams.get("kind") || "vehicle", source: url.searchParams.get("source") || "test", wait: url.searchParams.get("wait") === "1" };
      const ev = runEvent(args); if (args.wait) return json(res, 200, await ev); ev.catch(() => {}); return json(res, 200, { ok: true });
    }
    if (p === "/import" && req.method === "POST") { // {url, dest:"baselines/front_yard.jpg"} one-time migration helper
      const r = await fetch(body.url); if (!r.ok) return json(res, 502, { error: "fetch " + r.status });
      const dest = path.join(MEDIA, String(body.dest).replace(/\.\./g, "")); fs.mkdirSync(path.dirname(dest), { recursive: true }); fs.writeFileSync(dest, Buffer.from(await r.arrayBuffer())); return json(res, 200, { ok: true, dest });
    }
    if (p === "/frame.jpg" || (p === "/capture" && req.method === "POST")) {
      const deviceId = body.device_id || url.searchParams.get("device"); const name = body.name || url.searchParams.get("name");
      if (!deviceId) return json(res, 400, { error: "device_id required" });
      const out = await serialize(() => captureFrame(deviceId));
      if (name) { const f = path.join(MEDIA, `${String(name).replace(/[^\w.-]/g, "_")}.jpg`); fs.writeFileSync(f, out.jpg); out.file = f; }
      if (p === "/frame.jpg") { res.writeHead(200, { "content-type": "image/jpeg" }); return res.end(out.jpg); }
      return json(res, 200, { jpg_b64: out.jpg.toString("base64"), width: out.width, height: out.height, ms: out.ms, file: out.file });
    }
    return json(res, 404, { error: "not found" });
  } catch (e) { log(req.method, p, String(e?.message || e)); return json(res, 500, { error: String(e?.message || e) }); }
}).listen(PORT, "0.0.0.0", () => log(`homestead listening on ${PORT}; config ${CFG_DIR}`));
