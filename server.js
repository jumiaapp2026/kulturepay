const express = require("express");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;
const API_BASE = "https://kulturepay.co.ke/api/v1";

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// Normalize Kenyan phone numbers to 2547XXXXXXXX / 2541XXXXXXXX
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

// Send a single STK Push / Payment Intent request
async function triggerStkPush({ phone, amount, reference }) {
  const secretKey = process.env.KULTUREPAY_SECRET_KEY;
  if (!secretKey) {
    throw new Error("KULTUREPAY_SECRET_KEY is not set in environment variables.");
  }

  const res = await fetch(`${API_BASE}/payment_intents`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${secretKey}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({
      amount: Number(amount),
      currency: "KES",
      payment_method: "mpesa",
      phone_number: phone,
      reference: reference || `BATCH-${Date.now()}`,
    }),
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.message || data.error || `HTTP ${res.status}`);
  }
  return data;
}

// SSE endpoint that processes numbers 4 seconds apart and streams live status
app.post("/api/batch-stk", async (req, res) => {
  const { numbers = [], amount = 10, reference = "STK-BATCH", intervalSeconds = 4 } = req.body;

  // Optional dashboard access password check
  const adminPass = process.env.ADMIN_PASSWORD;
  if (adminPass && req.headers["x-admin-password"] !== adminPass) {
    return res.status(401).json({ error: "Invalid dashboard password." });
  }

  const cleaned = numbers
    .map((n) => ({ raw: n, normalized: normalizeKenyanPhone(n) }))
    .filter((item) => item.raw.trim() !== "");

  if (cleaned.length === 0) {
    return res.status(400).json({ error: "No phone numbers provided." });
  }

  // Set up Server-Sent Events stream
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  let aborted = false;
  req.on("close", () => {
    aborted = true;
  });

  const sendEvent = (payload) => {
    if (!aborted) {
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    }
  };

  const delayMs = Math.max(4000, Number(intervalSeconds) * 1000);

  for (let i = 0; i < cleaned.length; i++) {
    if (aborted) break;

    const { raw, normalized } = cleaned[i];
    if (!normalized) {
      sendEvent({
        index: i + 1,
        total: cleaned.length,
        phone: raw,
        status: "invalid",
        message: "Invalid Kenyan phone number format",
      });
    } else {
      sendEvent({
        index: i + 1,
        total: cleaned.length,
        phone: normalized,
        status: "sending",
        message: "Dispatching STK push...",
      });

      try {
        const intent = await triggerStkPush({
          phone: normalized,
          amount,
          reference: `${reference}-${i + 1}`,
        });

        sendEvent({
          index: i + 1,
          total: cleaned.length,
          phone: normalized,
          status: "sent",
          intentId: intent.id || "N/A",
          message: `STK sent (${intent.payment_status || "pending"})`,
        });
      } catch (err) {
        sendEvent({
          index: i + 1,
          total: cleaned.length,
          phone: normalized,
          status: "error",
          message: err.message,
        });
      }
    }

    // Wait 4 seconds before dispatching the next number
    if (i < cleaned.length - 1 && !aborted) {
      await sleep(delayMs);
    }
  }

  sendEvent({ done: true });
  res.end();
});

// Webhook endpoint to receive final M-Pesa confirmations asynchronously
app.post("/api/webhook/kulturepay", (req, res) => {
  console.log("KulturePay Webhook Event:", JSON.stringify(req.body));
  res.sendStatus(200);
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
