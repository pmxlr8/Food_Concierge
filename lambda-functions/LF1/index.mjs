/**
 * LF1 - Lex V2 Code Hook Lambda (Dining Concierge Bot)
 * 
 * Invoked by Amazon Lex V2 as a code hook for:
 *   - DialogCodeHook  → validates each slot as the user fills it
 *   - FulfillmentCodeHook → pushes the completed request to SQS
 * 
 * Intents handled:
 *   - GreetingIntent          → friendly greeting
 *   - ThankYouIntent          → polite goodbye
 *   - DiningSuggestionsIntent → collects 6 slots, validates, sends to SQS
 * 
 * Environment Variables:
 *   SQS_QUEUE_URL  - Full URL of the DiningRequestsQueue
 *   REGION         - AWS region (default: us-east-1)
 */

import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { DynamoDBClient, GetItemCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";

const sqsClient = new SQSClient({ region: process.env.REGION || "us-east-1" });
const dynamoClient = new DynamoDBClient({ region: process.env.REGION || "us-east-1" });

// ============================================================
// CONFIGURATION
// ============================================================
const VALID_CUISINES = ["chinese", "japanese", "italian", "mexican", "indian", "thai"];

// Accept many natural ways to say "New York / Manhattan area"
// Normalized → all stored as "manhattan" for the Yelp-scraped data
const LOCATION_ALIASES = {
  "manhattan":           "manhattan",
  "new york":            "manhattan",
  "new york city":       "manhattan",
  "nyc":                 "manhattan",
  "ny":                  "manhattan",
  "brooklyn":            "brooklyn",
  "queens":              "queens",
  "bronx":               "bronx",
  "the bronx":           "bronx",
  "staten island":       "staten island",
};

/**
 * Normalize a user-supplied location to our canonical form.
 * Returns the canonical name or null if not recognized.
 */
function normalizeLocation(raw) {
  if (!raw) return null;
  const key = raw.toLowerCase().trim();
  return LOCATION_ALIASES[key] || null;
}

// ============================================================
// HANDLER
// ============================================================
export const handler = async (event) => {
  console.log("LF1 event:", JSON.stringify(event));

  const intentName       = event.sessionState?.intent?.name;
  const invocationSource = event.invocationSource; // DialogCodeHook | FulfillmentCodeHook
  const slots            = event.sessionState?.intent?.slots || {};
  const sessionAttrs     = event.sessionState?.sessionAttributes || {};

  console.log(`Intent: ${intentName} | Source: ${invocationSource}`);

  try {
    // ---- Greeting ----
    if (intentName === "GreetingIntent") {
      return buildFulfillmentResponse(
        event,
        "Hi there! I can help you find restaurant suggestions. " +
        "Just say something like \"I'm looking for Italian food\" to get started."
      );
    }

    // ---- Thank You ----
    if (intentName === "ThankYouIntent") {
      return buildFulfillmentResponse(
        event,
        "You're welcome! If you want to search again, just say " +
        "\"find me a restaurant\" anytime. Enjoy your meal! 😊"
      );
    }

    // ---- Dining Suggestions ----
    if (intentName === "DiningSuggestionsIntent") {
      if (invocationSource === "DialogCodeHook") {
        return handleDialogValidation(event, slots, sessionAttrs);
      }
      if (invocationSource === "FulfillmentCodeHook") {
        return await handleFulfillment(event, slots, sessionAttrs);
      }
    }

    // ---- FallbackIntent or unknown ----
    return buildFulfillmentResponse(
      event,
      "I'm not sure I understood that. I can help you find restaurant suggestions — " +
      "just say something like \"I want to eat\" or \"find me a restaurant\". " +
      "If you already made a request, I can't modify it, but you can start a new one!"
    );
  } catch (err) {
    // Catch-all – never let the Lambda crash ungracefully
    console.error("Unhandled error in LF1 handler:", err);
    return buildFulfillmentResponse(
      event,
      "Oops, something went wrong on my end. Please try again in a moment."
    );
  }
};

// ============================================================
// DIALOG VALIDATION  (called for every turn while slots are being filled)
// ============================================================
function handleDialogValidation(event, slots, sessionAttrs) {
  const location      = getSlotValue(slots, "Location");
  const cuisine       = getSlotValue(slots, "Cuisine");
  const numberOfPeople = getSlotValue(slots, "NumberOfPeople");
  const diningDate    = getSlotValue(slots, "DiningDate");
  const diningTime    = getSlotValue(slots, "DiningTime");
  const email         = getSlotValue(slots, "Email");

  // ---------- Location ----------
  if (location) {
    const normalized = normalizeLocation(location);
    if (!normalized) {
      return buildElicitSlotResponse(
        event,
        "Location",
        `Sorry, we only serve the New York City area right now. ` +
        `"${location}" isn't in our coverage. ` +
        `Try Manhattan, Brooklyn, Queens, or just "NYC".`
      );
    }
    // If user said "new york" we silently resolve it to "manhattan"
    // by updating the slot value in-place so downstream sees the canonical form
  }

  // ---------- Cuisine ----------
  if (cuisine) {
    if (!VALID_CUISINES.includes(cuisine.toLowerCase().trim())) {
      return buildElicitSlotResponse(
        event,
        "Cuisine",
        `Hmm, I don't have "${cuisine}" in my list yet. ` +
        `I can help with: ${VALID_CUISINES.map(c => c.charAt(0).toUpperCase() + c.slice(1)).join(", ")}. ` +
        `Which one sounds good?`
      );
    }
  }

  // ---------- Number of People ----------
  if (numberOfPeople) {
    const num = parseInt(numberOfPeople, 10);
    if (isNaN(num) || num < 1) {
      return buildElicitSlotResponse(
        event,
        "NumberOfPeople",
        "That doesn't look right. How many people will be dining? Please enter a number (e.g., 2)."
      );
    }
    if (num > 20) {
      return buildElicitSlotResponse(
        event,
        "NumberOfPeople",
        "That's a large party! For groups over 20, please contact the restaurant directly. " +
        "How many people (1–20)?"
      );
    }
  }

  // ---------- Dining Date ----------
  if (diningDate) {
    // Lex resolves "today"→"2026-02-21", "tomorrow"→"2026-02-22", etc.
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const requested = new Date(diningDate + "T00:00:00");

    if (isNaN(requested.getTime())) {
      return buildElicitSlotResponse(
        event,
        "DiningDate",
        "I couldn't understand that date. Could you try again? " +
        "You can say \"today\", \"tomorrow\", or a date like \"March 5th\"."
      );
    }
    if (requested < today) {
      return buildElicitSlotResponse(
        event,
        "DiningDate",
        `That date (${diningDate}) is in the past! ` +
        `Please pick today or a future date.`
      );
    }
    // Optional: warn if date is more than 90 days out
    const maxFuture = new Date(today);
    maxFuture.setDate(maxFuture.getDate() + 90);
    if (requested > maxFuture) {
      return buildElicitSlotResponse(
        event,
        "DiningDate",
        "That's quite far out! I can only take reservations up to 90 days ahead. " +
        "Could you pick a closer date?"
      );
    }
  }

  // ---------- Dining Time ----------
  if (diningTime) {
    const timeRegex = /^\d{2}:\d{2}$/;
    if (!timeRegex.test(diningTime)) {
      return buildElicitSlotResponse(
        event,
        "DiningTime",
        "I couldn't parse that time. Try something like \"7 pm\" or \"19:30\"."
      );
    }
    const [hours, minutes] = diningTime.split(":").map(Number);
    // Check if time is reasonable (not 3 AM)
    if (hours < 6 || hours > 23) {
      return buildElicitSlotResponse(
        event,
        "DiningTime",
        "Most restaurants are open between 6 AM and 11 PM. " +
        "Could you pick a time in that range?"
      );
    }
    // If date is today, reject times that have already passed
    if (diningDate) {
      const now = new Date();
      const todayStr = now.toISOString().split("T")[0]; // "YYYY-MM-DD"
      if (diningDate === todayStr) {
        const nowHours = now.getHours();
        const nowMinutes = now.getMinutes();
        if (hours < nowHours || (hours === nowHours && minutes <= nowMinutes)) {
          return buildElicitSlotResponse(
            event,
            "DiningTime",
            `It's already past ${diningTime} today! Please pick a later time, ` +
            `or choose a future date.`
          );
        }
      }
    }
  }

  // ---------- Email ----------
  if (email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return buildElicitSlotResponse(
        event,
        "Email",
        "That doesn't look like a valid email. " +
        "Please enter an email like yourname@example.com."
      );
    }
  }

  // Everything currently filled is valid → let Lex keep eliciting the next slot
  return buildDelegateResponse(event);
}

// ============================================================
// FULFILLMENT  (all slots are filled and validated)
// ============================================================
async function handleFulfillment(event, slots, sessionAttrs) {
  const locationRaw   = getSlotValue(slots, "Location");
  const cuisine       = getSlotValue(slots, "Cuisine");
  const numberOfPeople = getSlotValue(slots, "NumberOfPeople");
  const diningDate    = getSlotValue(slots, "DiningDate");
  const diningTime    = getSlotValue(slots, "DiningTime");
  const email         = getSlotValue(slots, "Email");

  // Normalize location for downstream processing
  const location = normalizeLocation(locationRaw) || locationRaw;

  console.log("Fulfilling DiningSuggestionsIntent:", {
    location, cuisine, numberOfPeople, diningDate, diningTime, email,
  });

  // Build the SQS message
  const sqsMessage = {
    Location: location,
    Cuisine: cuisine.toLowerCase().trim(),
    NumberOfPeople: numberOfPeople,
    DiningDate: diningDate,
    DiningTime: diningTime,
    Email: email,
  };

  try {
    const sqsQueueUrl = process.env.SQS_QUEUE_URL;
    if (!sqsQueueUrl) {
      console.error("FATAL: SQS_QUEUE_URL env var is not set!");
      return buildFulfillmentResponse(
        event,
        "I'm sorry, the restaurant suggestion service isn't configured yet. " +
        "Please try again later."
      );
    }

    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: sqsQueueUrl,
        MessageBody: JSON.stringify(sqsMessage),
      })
    );
    console.log("SQS message sent:", JSON.stringify(sqsMessage));

    // --- Extra Credit: persist user preferences for next visit ---
    await saveUserState(email, location, cuisine, numberOfPeople, diningDate, diningTime);

    const cuisineDisplay = cuisine.charAt(0).toUpperCase() + cuisine.slice(1).toLowerCase();
    return buildFulfillmentResponse(
      event,
      `You're all set! Expect my ${cuisineDisplay} restaurant suggestions ` +
      `for ${numberOfPeople} people on ${diningDate} around ${diningTime} ` +
      `in your inbox at ${email} shortly. Have a great day!`
    );
  } catch (error) {
    console.error("Fulfillment error:", error);
    return buildFulfillmentResponse(
      event,
      "I'm sorry, something went wrong while processing your request. " +
      "Please try again in a moment."
    );
  }
}

// ============================================================
// EXTRA CREDIT - save user state so we can pre-fill next time
// ============================================================
async function saveUserState(email, location, cuisine, numberOfPeople, diningDate, diningTime) {
  try {
    await dynamoClient.send(new PutItemCommand({
      TableName: "user-state",
      Item: {
        Email:               { S: email },
        Location:            { S: location },
        Cuisine:             { S: cuisine },
        NumberOfPeople:      { S: String(numberOfPeople) },
        DiningDate:          { S: diningDate },
        DiningTime:          { S: diningTime },
        LastSearchTimestamp:  { S: new Date().toISOString() },
      },
    }));
    console.log("Extra credit: user state saved for", email);
  } catch (err) {
    // Non-fatal — if the user-state table doesn't exist yet, just log and move on
    console.warn("Extra credit state save skipped:", err.message);
  }
}

// ============================================================
// HELPER — extract interpreted value from a Lex V2 slot
// ============================================================
function getSlotValue(slots, slotName) {
  const slot = slots?.[slotName];
  if (!slot?.value) return null;
  return slot.value.interpretedValue || slot.value.originalValue || null;
}

// ============================================================
// RESPONSE BUILDERS
// ============================================================

/** Close the intent with a message (Fulfilled). */
function buildFulfillmentResponse(event, messageContent) {
  return {
    sessionState: {
      dialogAction: { type: "Close" },
      intent: {
        name:  event.sessionState.intent.name,
        slots: event.sessionState.intent.slots,
        state: "Fulfilled",
      },
      sessionAttributes: event.sessionState.sessionAttributes || {},
    },
    messages: [{ contentType: "PlainText", content: messageContent }],
  };
}

/** Re-ask the user for a specific slot with a validation message. */
function buildElicitSlotResponse(event, slotToElicit, messageContent) {
  const slots = { ...event.sessionState.intent.slots };
  slots[slotToElicit] = null; // clear the bad value so Lex re-prompts

  return {
    sessionState: {
      dialogAction: {
        type: "ElicitSlot",
        slotToElicit,
      },
      intent: {
        name:  event.sessionState.intent.name,
        slots,
        state: "InProgress",
      },
      sessionAttributes: event.sessionState.sessionAttributes || {},
    },
    messages: [{ contentType: "PlainText", content: messageContent }],
  };
}

/** Hand control back to Lex to continue the conversation. */
function buildDelegateResponse(event) {
  return {
    sessionState: {
      dialogAction: { type: "Delegate" },
      intent: {
        name:  event.sessionState.intent.name,
        slots: event.sessionState.intent.slots,
        state: "InProgress",
      },
      sessionAttributes: event.sessionState.sessionAttributes || {},
    },
  };
}
