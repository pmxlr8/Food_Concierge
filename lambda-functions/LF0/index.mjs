/**
 * LF0 - Chat API Lambda Function
 * 
 * This Lambda is triggered by API Gateway (POST /chatbot).
 * It extracts the user's message, sends it to Amazon Lex V2,
 * and returns Lex's response in the BotResponse format.
 * 
 * Environment Variables:
 *   BOT_ID       - Your Lex V2 Bot ID
 *   BOT_ALIAS_ID - Your Lex V2 Bot Alias ID  
 *   REGION       - AWS region (default: us-east-1)
 */

import { LexRuntimeV2Client, RecognizeTextCommand } from "@aws-sdk/client-lex-runtime-v2";

// Initialize the Lex V2 client
const lexClient = new LexRuntimeV2Client({
  region: process.env.REGION || "us-east-1",
});

export const handler = async (event) => {
  console.log("LF0 received event:", JSON.stringify(event));

  // --- CORS headers (needed for API Gateway proxy integration) ---
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
    "Access-Control-Allow-Methods": "OPTIONS,POST",
    "Content-Type": "application/json",
  };

  try {
    // Parse the incoming request body
    let body;
    if (typeof event.body === "string") {
      body = JSON.parse(event.body);
    } else {
      body = event.body || event;
    }

    // Extract user message text
    const messages = body.messages || [];
    if (messages.length === 0 || !messages[0].unstructured) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({
          messages: [
            {
              type: "unstructured",
              unstructured: {
                id: "1",
                text: "I didn't receive a message. Could you try again?",
                timestamp: new Date().toISOString(),
              },
            },
          ],
        }),
      };
    }

    const userMessage = messages[0].unstructured.text;
    console.log("User message:", userMessage);

    // --- Check environment variables ---
    const botId = process.env.BOT_ID;
    const botAliasId = process.env.BOT_ALIAS_ID;

    if (!botId || !botAliasId) {
      // Boilerplate fallback if Lex is not configured yet
      console.log("Lex not configured, returning boilerplate response");
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          messages: [
            {
              type: "unstructured",
              unstructured: {
                id: "1",
                text: "I'm still under development. Please come back later.",
                timestamp: new Date().toISOString(),
              },
            },
          ],
        }),
      };
    }

    // --- Send message to Lex V2 ---
    // Use a session ID based on some identifier. 
    // For simplicity, we use a static session or extract from headers.
    // In production, you'd use a user-specific session ID (e.g., from Cognito).
    const sessionId = event.requestContext?.identity?.sourceIp || "default-user";

    const lexCommand = new RecognizeTextCommand({
      botId: botId,
      botAliasId: botAliasId,
      localeId: "en_US",
      sessionId: sessionId,
      text: userMessage,
    });

    const lexResponse = await lexClient.send(lexCommand);
    console.log("Lex response:", JSON.stringify(lexResponse));

    // Extract Lex's response messages
    const lexMessages = lexResponse.messages || [];
    let responseText = "Sorry, I didn't understand that. Could you try again?";

    if (lexMessages.length > 0) {
      // Combine all Lex messages into one response
      responseText = lexMessages.map((m) => m.content).join(" ");
    }

    // Return the BotResponse
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        messages: [
          {
            type: "unstructured",
            unstructured: {
              id: "1",
              text: responseText,
              timestamp: new Date().toISOString(),
            },
          },
        ],
      }),
    };
  } catch (error) {
    console.error("Error in LF0:", error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        messages: [
          {
            type: "unstructured",
            unstructured: {
              id: "1",
              text: "Oops, something went wrong. Please try again.",
              timestamp: new Date().toISOString(),
            },
          },
        ],
      }),
    };
  }
};
