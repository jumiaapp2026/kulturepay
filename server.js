const express = require("express");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;
const API_BASE = "https://kulturepay.co.ke/api/v1";

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

process.on("unhandledRejection", (reason) => {
  console.error("[UNHANDLED_REJECTION]", reason);
});

process.on("uncaughtException", (err) => {
  console.error("[UNCAUGHT_EXCEPTION]", err);
});

function normalizeKenyanPhone(raw) {
  const digits = String(raw || "").replace(/\D/g, "");
  if (digits.startsWith("254") && digits.length === 12) return digits;
  if (digits.startsWith("0") && digits.length === 10) return "254" + digits.slice(1);
  if ((digits.startsWith("7") || digits.startsWith("1")) && digits.length === 9) {
    return "254" + digits;
  }
  return null;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function parseJsonSafe(res) {
  const text = await res.text();
  try {
    return { text, json: text ? JSON.parse(text) : null };
  } catch {
    return { text, json: null };
  }
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 15000) {
  return fetch(url, {
    ...options,
    signal: AbortSignal.timeout(timeoutMs),
  });
}

async function triggerStkPush({ phone, amount, reference, stepIndex }) {
  const secretKey = process.env.KULTUREPAY_SECRET_KEY;
  if (!secretKey) {
    throw new Error("Missing KULTUREPAY_SECRET_KEY environment variable");
  }

  const headers = {
    Authorization: `Bearer ${secretKey}`,
    "Content-Type": "application/json",
    Accept: "application/json",
  };

  const createPayload = {
    amount: Number(amount),
    amountMinor: Math.round(Number(amount) * 100),
    currency: "KES",
    payment_method: "mpesa",
    phone_number: phone,
    phone,
    reference: reference || `BATCH-${Date.now()}`,
  };

  console.log(`[STK_CREATE_REQ] #${stepIndex} phone=${phone} amount=${amount} ref=${createPayload.reference}`);

  const createRes = await fetchWithTimeout(`${API_BASE}/payment_intents`, {
    method: "POST",
    headers,
    body: JSON.stringify(createPayload),
  });

  const createParsed = await parseJsonSafe(createRes);

  if (!createRes.ok) {
    console.error(
      `[STK_CREATE_FAIL] #${stepIndex} phone=${phone} status=${createRes.status} body=${JSON.stringify(
        createParsed.json ?? createParsed.text
      )}`
    );
    throw new Error(
      createParsed.json?.message ||
        createParsed.json?.error ||
        `Create intent failed (HTTP ${createRes.status}): ${createParsed.text.slice(0, 200)}`
    );
  }

  const intent = createParsed.json || {};
  console.log(`[STK_CREATE_OK] #${stepIndex} phone=${phone} intentId=${intent.id} raw=${JSON.stringify(intent)}`);

  if (intent.id) {
    const confirmRes = await fetchWithTimeout(`${API_BASE}/payment_intents/${intent.id}/confirm`, {
      method: "POST",
      headers,
      body: JSON.stringify({ phone, phone_number: phone }),
    });

    const confirmParsed = await parseJsonSafe(confirmRes);

    if (!confirmRes.ok) {
      console.error(
        `[STK_CONFIRM_WARN] #${stepIndex} phone=${phone} intentId=${intent.id} status=${confirmRes.status} body=${JSON.stringify(
          confirmParsed.json ?? confirmParsed.text
        )}`
      );
    } else {
      console.log(
        `[STK_CONFIRM_OK] #${stepIndex} phone=${phone} intentId=${intent.id} raw=${JSON.stringify(
          confirmParsed.json ?? confirmParsed.text
        )}`
      );
      return confirmParsed.json || intent;
    }
  }

  return intent;
}

app.post("/api/batch-stk", async (req, res) => {
  const { numbers = [], amount = 10, reference = "STK-BATCH", intervalSeconds = 4 } = req.body;

  const adminPass = process.env.ADMIN_PASSWORD;
  if (adminPass && req.headers["x-admin-password"] !== adminPass) {
    console.warn("[AUTH_FAIL] Invalid x-admin-password on /api/batch-stk");
    return res.status(401).json({ error: "Invalid dashboard password." });
  }

  const rawList = Array.isArray(numbers) ? numbers : String(numbers).split(/[\n,]+/);
  const cleaned = rawList
    .map((n) => ({ raw: String(n), normalized: normalizeKenyanPhone(n) }))
    .filter((item) => item.raw.trim() !== "");

  if (cleaned.length === 0) {
    return res.status(400).json({ error: "No phone numbers provided." });
  }

  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  if (typeof res.flushHeaders === "function") {
    res.flushHeaders();
  }

  let clientDisconnected = false;
  req.on("close", () => {
    if (!res.writableEnded) {
      clientDisconnected = true;
      console.warn("[SSE_CLIENT_CLOSED] Client connection closed before batch finished");
    }
  });

  const sendEvent = (payload) => {
    if (!clientDisconnected && !res.writableEnded) {
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    }
  };

  const delayMs = Math.max(4000, Number(intervalSeconds) * 1000 || 4000);
  console.log(`[BATCH_START] count=${cleaned.length} amount=${amount} intervalMs=${delayMs}`);

  for (let i = 0; i < cleaned.length; i++) {
    const stepIndex = i + 1;
    const { raw, normalized } = cleaned[i];

    if (!normalized) {
      console.warn(`[INVALID_PHONE] #${stepIndex} raw="${raw}"`);
      sendEvent({
        index: stepIndex,
        total: cleaned.length,
        phone: raw,
        status: "invalid",
        message: "Invalid Kenyan phone number format",
      });
    } else {
      sendEvent({
        index: stepIndex,
        total: cleaned.length,
        phone: normalized,
        status: "sending",
        message: "Dispatching STK push...",
      });

      try {
        const intent = await triggerStkPush({
          phone: normalized,
          amount,
          reference: `${reference}-${stepIndex}`,
          stepIndex,
        });

        sendEvent({
          index: stepIndex,
          total: cleaned.length,
          phone: normalized,
          status: "sent",
          intentId: intent.id || "N/A",
          message: `STK sent (${intent.payment_status || intent.status || "pending"})`,
        });
      } catch (err) {
        console.error(`[BATCH_ITEM_ERR] #${stepIndex} phone=${normalized}:`, err.message);
        sendEvent({
          index: stepIndex,
          total: cleaned.length,
          phone: normalized,
          status: "error",
          message: err.message,
        });
      }
    }

    if (i < cleaned.length - 1) {
      await sleep(delayMs);
    }
  }

  console.log("[BATCH_DONE]");
  sendEvent({ done: true });
  if (!res.writableEnded) {
    res.end();
  }
});

app.post("/api/webhook/kulturepay", (req, res) => {
  console.log("[WEBHOOK_RECEIVED]", JSON.stringify(req.body));
  res.sendStatus(200);
});

app.listen(PORT, () => {
  console.log(`[SERVER_START] Listening on port ${PORT}`);
});
