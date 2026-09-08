// Homestead Capture — Home Assistant add-on.
// Grabs a live JPEG frame from a WebRTC-only Nest camera by driving headless
// Chromium through the same offer/answer handshake the Homestead web app uses,
// but with Google Device Access called from here (no Supabase, no Vercel).
//
//   POST /capture  {device_id, secret, name?}  -> {jpg_b64, width, height, ms}
//   GET  /frame.jpg?device=<id>&secret=<s>&name=<file>  -> image/jpeg
// When `name` is given the frame is also written to /media/homestead/<name>.jpg
// so Home Assistant's AI task can attach it (media-source://media_source/local/homestead/<name>.jpg).
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import puppeteer from "puppeteer-core";

const OPTIONS = JSON.parse(fs.readFileSync("/data/options.json", "utf8"));
const PORT = 8099;
const SDM = "https://smartdevicemanagement.googleapis.com/v1";
const MEDIA_DIR = "/media/homestead";
const READY_TIMEOUT_MS = 25_000;
const PAGE_HTML = fs.readFileSync(new URL("./capture.html", import.meta.url), "utf8");

let token = { value: null, expires: 0 };
async function accessToken() {
  if (token.value && token.expires > Date.now() + 120_000) return token.value;
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: OPTIONS.client_id, client_secret: OPTIONS.client_secret,
      refresh_token: OPTIONS.refresh_token, grant_type: "refresh_token",
    }),
  });
  const j = await r.json();
  if (!r.ok || !j.access_token) throw new Error("google token refresh failed: " + JSON.stringify(j).slice(0, 200));
  token = { value: j.access_token, expires: Date.now() + (j.expires_in ?? 3600) * 1000 };
  return token.value;
}

async function sdmOffer(deviceId, offerSdp) {
  const at = await accessToken();
  const r = await fetch(`${SDM}/enterprises/${OPTIONS.project_id}/devices/${deviceId}:executeCommand`, {
    method: "POST",
    headers: { authorization: `Bearer ${at}`, "content-type": "application/json" },
    body: JSON.stringify({ command: "sdm.devices.commands.CameraLiveStream.GenerateWebRtcStream", params: { offerSdp } }),
  });
  const j = await r.json();
  if (!r.ok) throw new Error(`SDM ${r.status}: ${JSON.stringify(j).slice(0, 300)}`);
  return { answer_sdp: j.results?.answerSdp, media_session_id: j.results?.mediaSessionId };
}

// One browser, reused; a fresh page per capture. Serialised so two triggers
// don't race each other for the camera.
let browserP = null;
function browser() {
  if (!browserP) {
    browserP = puppeteer.launch({
      executablePath: process.env.CHROME_BIN,
      headless: true,
      args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--autoplay-policy=no-user-gesture-required",
        "--use-fake-ui-for-media-stream", "--mute-audio", "--window-size=1280,800"],
      defaultViewport: { width: 1280, height: 800 },
    }).catch((e) => { browserP = null; throw e; });
  }
  return browserP;
}
let chain = Promise.resolve();
const serialize = (fn) => { const p = chain.then(fn, fn); chain = p.catch(() => {}); return p; };

async function capture(deviceId) {
  const t0 = Date.now();
  const logs = [];
  const b = await browser();
  const page = await b.newPage();
  try {
    page.on("console", (m) => logs.push(`[${m.type()}] ${m.text()}`));
    page.on("pageerror", (e) => logs.push(`[pageerror] ${e?.message || e}`));
    await page.goto(`http://127.0.0.1:${PORT}/capture.html?device=${encodeURIComponent(deviceId)}`, { waitUntil: "domcontentloaded", timeout: 15_000 });
    await page.waitForFunction(() => window.__frameReady === true || window.__error != null, { timeout: READY_TIMEOUT_MS, polling: 200 }).catch(() => {});
    const state = await page.evaluate(() => ({ ready: window.__frameReady === true, error: window.__error || null, vw: document.getElementById("v")?.videoWidth, vh: document.getElementById("v")?.videoHeight }));
    if (!state.ready) throw new Error((state.error || `no frame within ${READY_TIMEOUT_MS}ms`) + " | " + logs.slice(-4).join(" ; "));
    const dataUrl = await page.evaluate(() => window.__grabFrame());
    return { jpg_b64: String(dataUrl).replace(/^data:image\/jpeg;base64,/, ""), width: state.vw, height: state.vh, ms: Date.now() - t0 };
  } finally {
    await page.close().catch(() => {});
  }
}

const json = (res, code, body) => { res.writeHead(code, { "content-type": "application/json" }); res.end(JSON.stringify(body)); };
const readBody = (req) => new Promise((resolve) => { let d = ""; req.on("data", (c) => (d += c)); req.on("end", () => resolve(d)); });

http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${PORT}`);
  try {
    if (url.pathname === "/capture.html") { res.writeHead(200, { "content-type": "text/html" }); return res.end(PAGE_HTML); }
    if (url.pathname === "/health") return json(res, 200, { ok: true, browser: !!browserP });
    if (url.pathname === "/offer" && req.method === "POST") {
      // Only the local capture page calls this.
      const { device_id, offer_sdp } = JSON.parse(await readBody(req) || "{}");
      return json(res, 200, await sdmOffer(device_id, offer_sdp));
    }
    let deviceId, secret, name;
    if (url.pathname === "/capture" && req.method === "POST") ({ device_id: deviceId, secret, name } = JSON.parse(await readBody(req) || "{}"));
    else if (url.pathname === "/frame.jpg") { deviceId = url.searchParams.get("device"); secret = url.searchParams.get("secret"); name = url.searchParams.get("name"); }
    else return json(res, 404, { error: "not found" });
    if (secret !== OPTIONS.secret) return json(res, 401, { error: "bad secret" });
    if (!deviceId) return json(res, 400, { error: "device_id required" });
    const out = await serialize(() => capture(deviceId));
    if (name) {
      fs.mkdirSync(MEDIA_DIR, { recursive: true });
      const file = path.join(MEDIA_DIR, `${String(name).replace(/[^\w.-]/g, "_")}.jpg`);
      fs.writeFileSync(file, Buffer.from(out.jpg_b64, "base64"));
      out.file = file;
    }
    if (url.pathname === "/frame.jpg") { res.writeHead(200, { "content-type": "image/jpeg" }); return res.end(Buffer.from(out.jpg_b64, "base64")); }
    return json(res, 200, out);
  } catch (e) {
    console.error(new Date().toISOString(), req.method, url.pathname, String(e?.message || e));
    return json(res, 500, { error: String(e?.message || e) });
  }
}).listen(PORT, "0.0.0.0", () => console.log(`homestead-capture listening on ${PORT}`));
