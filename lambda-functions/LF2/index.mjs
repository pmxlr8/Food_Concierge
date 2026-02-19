/**
 * LF2 - Queue Worker Lambda Function
 * 
 * Triggered every minute by EventBridge/CloudWatch Events.
 * Pulls a message from SQS (Q1), queries OpenSearch for restaurant IDs
 * matching the requested cuisine, fetches full details from DynamoDB,
 * formats the results, and sends an email to the user via SES.
 * 
 * Environment Variables:
 *   SQS_QUEUE_URL          - URL of DiningRequestsQueue
 *   OPENSEARCH_ENDPOINT    - OpenSearch domain endpoint (e.g., https://xxx.us-east-1.es.amazonaws.com)
 *   OPENSEARCH_USERNAME    - OpenSearch master username
 *   OPENSEARCH_PASSWORD    - OpenSearch master password
 *   SES_SENDER_EMAIL       - Verified SES sender email address
 *   REGION                 - AWS region (default: us-east-1)
 */

import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import { DynamoDBClient, GetItemCommand } from "@aws-sdk/client-dynamodb";
import { SESClient, SendEmailCommand } from "@aws-sdk/client-ses";
import https from "https";
import http from "http";

const region = process.env.REGION || "us-east-1";
const sqsClient = new SQSClient({ region });
const dynamoClient = new DynamoDBClient({ region });
const sesClient = new SESClient({ region });

export const handler = async (event) => {
  console.log("LF2 invoked - Queue Worker");

  const sqsQueueUrl = process.env.SQS_QUEUE_URL;
  if (!sqsQueueUrl) {
    console.error("SQS_QUEUE_URL not configured");
    return { statusCode: 500, body: "SQS_QUEUE_URL not configured" };
  }

  try {
    // 1. Pull a message from SQS
    const receiveResult = await sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: sqsQueueUrl,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 0, // Short poll since we're on a schedule
      })
    );

    if (!receiveResult.Messages || receiveResult.Messages.length === 0) {
      console.log("No messages in queue. Nothing to process.");
      return { statusCode: 200, body: "No messages to process" };
    }

    const message = receiveResult.Messages[0];
    const messageBody = JSON.parse(message.Body);
    console.log("Processing message:", JSON.stringify(messageBody));

    const { Location, Cuisine, NumberOfPeople, DiningDate, DiningTime, Email } = messageBody;

    // 2. Query OpenSearch for restaurant IDs matching the cuisine
    const restaurantIds = await searchOpenSearch(Cuisine);
    console.log(`Found ${restaurantIds.length} restaurants from OpenSearch`);

    if (restaurantIds.length === 0) {
      console.log("No restaurants found in OpenSearch for cuisine:", Cuisine);
      // Still delete the message so it doesn't get re-processed
      await deleteSQSMessage(sqsQueueUrl, message.ReceiptHandle);
      return { statusCode: 200, body: "No restaurants found" };
    }

    // 3. Pick 3 random restaurants
    const selectedIds = getRandomItems(restaurantIds, 3);
    console.log("Selected restaurant IDs:", selectedIds);

    // 4. Fetch full details from DynamoDB
    const restaurants = await Promise.all(
      selectedIds.map((id) => getRestaurantFromDynamoDB(id))
    );
    console.log("Restaurant details:", JSON.stringify(restaurants));

    // 5. Format and send email via SES
    try {
      await sendEmailViaSES(
        Email,
        Cuisine,
        NumberOfPeople,
        DiningDate,
        DiningTime,
        restaurants
      );
      console.log("Email sent successfully to:", Email);
    } catch (emailError) {
      console.error("Failed to send email:", emailError.message);
      // Still delete the message to avoid infinite retry loop
      // (e.g., unverified recipient in SES sandbox mode)
    }

    // 6. Delete the processed message from SQS (always, even if email failed)
    await deleteSQSMessage(sqsQueueUrl, message.ReceiptHandle);
    console.log("SQS message deleted");

    return { statusCode: 200, body: "Message processed" };
  } catch (error) {
    console.error("Error in LF2:", error);
    return { statusCode: 500, body: error.message };
  }
};

// ============================================================
// OPENSEARCH QUERY
// ============================================================
async function searchOpenSearch(cuisine) {
  const endpoint = process.env.OPENSEARCH_ENDPOINT;
  const username = process.env.OPENSEARCH_USERNAME;
  const password = process.env.OPENSEARCH_PASSWORD;

  if (!endpoint) {
    throw new Error("OPENSEARCH_ENDPOINT not configured");
  }

  // Search for restaurants matching the cuisine
  const query = {
    size: 50, // Get up to 50 matches, we'll randomly pick 3
    query: {
      match: {
        Cuisine: cuisine.toLowerCase(),
      },
    },
  };

  const url = new URL(`${endpoint}/restaurants/_search`);
  const auth = Buffer.from(`${username}:${password}`).toString("base64");

  const responseBody = await makeHttpRequest({
    hostname: url.hostname,
    port: url.port || 443,
    path: url.pathname,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Basic ${auth}`,
    },
    body: JSON.stringify(query),
    protocol: url.protocol,
  });

  const result = JSON.parse(responseBody);

  if (!result.hits || !result.hits.hits) {
    return [];
  }

  // Extract RestaurantIDs from the search results
  return result.hits.hits.map((hit) => hit._source.RestaurantID);
}

// ============================================================
// DYNAMODB LOOKUP
// ============================================================
async function getRestaurantFromDynamoDB(businessId) {
  const result = await dynamoClient.send(
    new GetItemCommand({
      TableName: "yelp-restaurants",
      Key: {
        BusinessID: { S: businessId },
      },
    })
  );

  if (!result.Item) {
    return { name: "Unknown Restaurant", address: "Address not available" };
  }

  return {
    name: result.Item.Name?.S || "Unknown Restaurant",
    address: result.Item.Address?.S || "Address not available",
    rating: result.Item.Rating?.N || "N/A",
    numberOfReviews: result.Item.NumberOfReviews?.N || "N/A",
    zipCode: result.Item.ZipCode?.S || "",
  };
}

// ============================================================
// SES EMAIL
// ============================================================
async function sendEmailViaSES(recipientEmail, cuisine, numberOfPeople, diningDate, diningTime, restaurants) {
  const senderEmail = process.env.SES_SENDER_EMAIL;
  if (!senderEmail) {
    throw new Error("SES_SENDER_EMAIL not configured");
  }

  // Format the restaurant list
  const restaurantList = restaurants
    .map((r, i) => `${i + 1}. ${r.name}, located at ${r.address}`)
    .join("\n");

  const emailBody = `Hello! Here are my ${cuisine} restaurant suggestions for ${numberOfPeople} people, for ${diningDate} at ${diningTime}:\n\n${restaurantList}\n\nEnjoy your meal!`;

  const emailHtml = `
    <html>
      <body>
        <p>Hello!</p>
        <p>Here are my <strong>${cuisine}</strong> restaurant suggestions for <strong>${numberOfPeople}</strong> people, for <strong>${diningDate}</strong> at <strong>${diningTime}</strong>:</p>
        <ol>
          ${restaurants.map((r) => `<li><strong>${r.name}</strong>, located at ${r.address} (Rating: ${r.rating}/5, ${r.numberOfReviews} reviews)</li>`).join("")}
        </ol>
        <p>Enjoy your meal!</p>
      </body>
    </html>
  `;

  await sesClient.send(
    new SendEmailCommand({
      Source: senderEmail,
      Destination: {
        ToAddresses: [recipientEmail],
      },
      Message: {
        Subject: {
          Data: `Your ${cuisine} Restaurant Suggestions`,
          Charset: "UTF-8",
        },
        Body: {
          Text: {
            Data: emailBody,
            Charset: "UTF-8",
          },
          Html: {
            Data: emailHtml,
            Charset: "UTF-8",
          },
        },
      },
    })
  );
}

// ============================================================
// HELPER FUNCTIONS
// ============================================================

/**
 * Delete a message from SQS
 */
async function deleteSQSMessage(queueUrl, receiptHandle) {
  await sqsClient.send(
    new DeleteMessageCommand({
      QueueUrl: queueUrl,
      ReceiptHandle: receiptHandle,
    })
  );
}

/**
 * Pick N random items from an array
 */
function getRandomItems(array, n) {
  const shuffled = [...array].sort(() => 0.5 - Math.random());
  return shuffled.slice(0, Math.min(n, shuffled.length));
}

/**
 * Make an HTTPS/HTTP request (used for OpenSearch queries)
 */
function makeHttpRequest(options) {
  return new Promise((resolve, reject) => {
    const protocol = options.protocol === "http:" ? http : https;
    const body = options.body;
    delete options.body;
    delete options.protocol;

    const req = protocol.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(data);
        } else {
          reject(new Error(`OpenSearch request failed: ${res.statusCode} - ${data}`));
        }
      });
    });

    req.on("error", reject);
    if (body) {
      req.write(body);
    }
    req.end();
  });
}
