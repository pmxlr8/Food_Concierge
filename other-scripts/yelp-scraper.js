/**
 * Yelp Restaurant Scraper
 * 
 * Scrapes 5000+ restaurants from Manhattan using the Yelp Fusion API.
 * Queries 6 cuisine types across 10 Manhattan neighborhoods.
 * Deduplicates by business ID. Stores results in DynamoDB and exports
 * to JSON for OpenSearch bulk upload.
 * 
 * PREREQUISITES:
 *   1. Install dependencies:  npm install
 *   2. Create a Yelp Fusion API app at: https://www.yelp.com/developers/v3/manage_app
 *   3. Set your Yelp API key below or as environment variable YELP_API_KEY
 *   4. Configure AWS CLI:  aws configure  (set region to us-east-1)
 *   5. Create DynamoDB table "yelp-restaurants" with partition key "BusinessID" (String)
 * 
 * USAGE:
 *   node yelp-scraper.js
 */

const { DynamoDBClient, PutItemCommand } = require("@aws-sdk/client-dynamodb");
const https = require("https");

// ============================================================
// CONFIGURATION - CHANGE THESE
// ============================================================
const YELP_API_KEY = process.env.YELP_API_KEY || "YOUR_YELP_API_KEY_HERE"; // <-- SET THIS
const AWS_REGION = process.env.AWS_REGION || "us-east-1";
const DYNAMODB_TABLE = "yelp-restaurants";

const CUISINES = ["chinese", "japanese", "italian", "mexican", "indian", "thai"];
const LOCATIONS = [
  "Manhattan, NY",
  "Midtown Manhattan, NY",
  "Lower Manhattan, NY",
  "Upper East Side, NY",
  "Upper West Side, NY",
  "East Village, NY",
  "West Village, NY",
  "SoHo, NY",
  "Chinatown Manhattan, NY",
  "Hell's Kitchen, NY",
];
const RESULTS_PER_PAGE = 50; // Yelp max per request
const PAGES_PER_LOCATION = 5;  // 5 pages per cuisine per location (offset 0-200)
const fs = require("fs");

// ============================================================
// MAIN
// ============================================================
const dynamoClient = new DynamoDBClient({ region: AWS_REGION });

async function main() {
  console.log("=== Yelp Restaurant Scraper ===\n");

  if (YELP_API_KEY === "YOUR_YELP_API_KEY_HERE") {
    console.error("ERROR: Please set your Yelp API key!");
    console.error("  Option 1: Edit this file and replace YOUR_YELP_API_KEY_HERE");
    console.error("  Option 2: Run with:  YELP_API_KEY=your_key node yelp-scraper.js");
    process.exit(1);
  }

  const allRestaurants = new Map(); // businessId -> restaurant data (for dedup)
  const opensearchData = []; // For OpenSearch bulk upload

  for (const cuisine of CUISINES) {
    console.log(`\n--- Scraping ${cuisine} restaurants ---`);
    let cuisineCount = 0;

    for (const location of LOCATIONS) {
      console.log(`  Location: ${location}`);

      for (let page = 0; page < PAGES_PER_LOCATION; page++) {
        const offset = page * RESULTS_PER_PAGE;
        // Yelp caps total at offset+limit <= 1000
        if (offset + RESULTS_PER_PAGE > 1000) break;

        try {
          const businesses = await searchYelp(cuisine, location, offset);

          if (!businesses || businesses.length === 0) {
            console.log(`    No more results at offset ${offset}`);
            break;
          }

          let newThisPage = 0;
          for (const biz of businesses) {
            // Skip duplicates
            if (allRestaurants.has(biz.id)) continue;

            const restaurant = {
              BusinessID: biz.id,
              Name: biz.name,
              Address: formatAddress(biz.location),
              Coordinates: {
                Latitude: String(biz.coordinates?.latitude || ""),
                Longitude: String(biz.coordinates?.longitude || ""),
              },
              NumberOfReviews: String(biz.review_count || 0),
              Rating: String(biz.rating || 0),
              ZipCode: biz.location?.zip_code || "",
              Cuisine: cuisine,
              insertedAtTimestamp: new Date().toISOString(),
            };

            allRestaurants.set(biz.id, restaurant);
            opensearchData.push({
              RestaurantID: biz.id,
              Cuisine: cuisine,
            });
            cuisineCount++;
            newThisPage++;
          }

          console.log(`    Page ${page + 1}: ${businesses.length} results, ${newThisPage} new (total: ${allRestaurants.size})`);

          // If very few new results, skip to next location
          if (newThisPage === 0) break;

          // Respect Yelp rate limits
          await sleep(300);
        } catch (error) {
          console.error(`    Error: ${error.message}`);
          await sleep(2000);
        }
      }
    }

    console.log(`  Total unique ${cuisine} restaurants: ${cuisineCount}`);
  }

  console.log(`\n=== Total unique restaurants collected: ${allRestaurants.size} ===\n`);

  // Save to JSON files for reference
  const allData = Array.from(allRestaurants.values());
  fs.writeFileSync("restaurants-dynamodb.json", JSON.stringify(allData, null, 2));
  fs.writeFileSync("restaurants-opensearch.json", JSON.stringify(opensearchData, null, 2));
  console.log("Saved restaurants-dynamodb.json and restaurants-opensearch.json\n");

  // Upload to DynamoDB
  console.log("Uploading to DynamoDB...");
  let uploaded = 0;
  let failed = 0;

  for (const restaurant of allData) {
    try {
      await dynamoClient.send(
        new PutItemCommand({
          TableName: DYNAMODB_TABLE,
          Item: {
            BusinessID: { S: restaurant.BusinessID },
            Name: { S: restaurant.Name },
            Address: { S: restaurant.Address },
            Coordinates: {
              M: {
                Latitude: { S: restaurant.Coordinates.Latitude },
                Longitude: { S: restaurant.Coordinates.Longitude },
              },
            },
            NumberOfReviews: { N: restaurant.NumberOfReviews },
            Rating: { N: restaurant.Rating },
            ZipCode: { S: restaurant.ZipCode },
            Cuisine: { S: restaurant.Cuisine },
            insertedAtTimestamp: { S: restaurant.insertedAtTimestamp },
          },
        })
      );
      uploaded++;

      if (uploaded % 50 === 0) {
        console.log(`  Uploaded ${uploaded}/${allData.length}`);
      }

      // Small delay to avoid throttling
      if (uploaded % 25 === 0) {
        await sleep(100);
      }
    } catch (error) {
      console.error(`  Failed to upload ${restaurant.BusinessID}: ${error.message}`);
      failed++;
    }
  }

  console.log(`\nDynamoDB upload complete: ${uploaded} succeeded, ${failed} failed`);
  console.log("\nNext step: Run opensearch-bulk-upload.js to load data into OpenSearch");
}

// ============================================================
// YELP API
// ============================================================
function searchYelp(cuisine, location, offset) {
  return new Promise((resolve, reject) => {
    const params = new URLSearchParams({
      term: `${cuisine} restaurants`,
      location: location,
      limit: String(RESULTS_PER_PAGE),
      offset: String(offset),
    });

    const options = {
      hostname: "api.yelp.com",
      path: `/v3/businesses/search?${params.toString()}`,
      method: "GET",
      headers: {
        Authorization: `Bearer ${YELP_API_KEY}`,
        Accept: "application/json",
      },
    };

    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.error) {
            reject(new Error(parsed.error.description || parsed.error.code));
          } else {
            resolve(parsed.businesses || []);
          }
        } catch (e) {
          reject(new Error(`Failed to parse Yelp response: ${e.message}`));
        }
      });
    });

    req.on("error", reject);
    req.end();
  });
}

// ============================================================
// HELPERS
// ============================================================
function formatAddress(location) {
  if (!location) return "Address not available";
  const parts = [
    location.address1,
    location.address2,
    location.address3,
    location.city,
    location.state,
    location.zip_code,
  ].filter(Boolean);
  return parts.join(", ") || "Address not available";
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Run
main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
