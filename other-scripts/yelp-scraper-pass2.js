/**
 * Yelp Scraper - Second Pass
 * 
 * Adds more restaurants from additional neighborhoods and sub-cuisine terms
 * to reach the 5000+ target. Deduplicates against the first pass data.
 */

const { DynamoDBClient, PutItemCommand } = require("@aws-sdk/client-dynamodb");
const https = require("https");
const fs = require("fs");
const path = require("path");

const YELP_API_KEY = process.env.YELP_API_KEY;
const AWS_REGION = process.env.AWS_REGION || "us-east-1";
const DYNAMODB_TABLE = "yelp-restaurants";
const dynamoClient = new DynamoDBClient({ region: AWS_REGION });

// Sub-cuisine terms mapped to parent cuisine category
const SEARCH_TERMS = [
  { term: "dim sum", cuisine: "chinese" },
  { term: "szechuan", cuisine: "chinese" },
  { term: "cantonese", cuisine: "chinese" },
  { term: "sushi", cuisine: "japanese" },
  { term: "ramen", cuisine: "japanese" },
  { term: "izakaya", cuisine: "japanese" },
  { term: "pizza", cuisine: "italian" },
  { term: "pasta", cuisine: "italian" },
  { term: "tacos", cuisine: "mexican" },
  { term: "burritos", cuisine: "mexican" },
  { term: "taqueria", cuisine: "mexican" },
  { term: "curry", cuisine: "indian" },
  { term: "biryani", cuisine: "indian" },
  { term: "tandoori", cuisine: "indian" },
  { term: "pad thai", cuisine: "thai" },
  { term: "thai curry", cuisine: "thai" },
];

const LOCATIONS = [
  "Harlem, NY",
  "Financial District, NY",
  "Tribeca, NY",
  "Chelsea, NY",
  "Gramercy, NY",
  "Murray Hill, NY",
  "Flatiron, NY",
  "NoHo, NY",
  "Nolita, NY",
  "Little Italy Manhattan, NY",
  "Koreatown Manhattan, NY",
  "Washington Heights, NY",
  "Inwood Manhattan, NY",
  "Morningside Heights, NY",
  "Kips Bay, NY",
];

const RESULTS_PER_PAGE = 50;
const PAGES_PER_LOCATION = 4;

async function main() {
  console.log("=== Yelp Scraper - Second Pass ===\n");

  // Load existing data for deduplication
  // JSON files are in the project root (parent of other-scripts/)
  const DATA_DIR = path.resolve(__dirname, "..");
  const DYNAMO_JSON = path.join(DATA_DIR, "restaurants-dynamodb.json");
  const OS_JSON = path.join(DATA_DIR, "restaurants-opensearch.json");

  let existingIds = new Set();
  let opensearchData = [];
  try {
    const existing = JSON.parse(fs.readFileSync(DYNAMO_JSON, "utf-8"));
    for (const r of existing) existingIds.add(r.BusinessID);
    const existingOS = JSON.parse(fs.readFileSync(OS_JSON, "utf-8"));
    opensearchData = [...existingOS];
    console.log(`Loaded ${existingIds.size} existing restaurants for dedup\n`);
  } catch (e) {
    console.log("No existing data found, starting fresh\n");
  }

  const newRestaurants = [];
  let totalNew = 0;

  for (const { term, cuisine } of SEARCH_TERMS) {
    console.log(`\n--- Searching "${term}" (→ ${cuisine}) ---`);
    let termCount = 0;

    for (const location of LOCATIONS) {
      for (let page = 0; page < PAGES_PER_LOCATION; page++) {
        const offset = page * RESULTS_PER_PAGE;
        if (offset + RESULTS_PER_PAGE > 1000) break;

        try {
          const businesses = await searchYelp(term, location, offset);
          if (!businesses || businesses.length === 0) break;

          let newThisPage = 0;
          for (const biz of businesses) {
            if (existingIds.has(biz.id)) continue;

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

            existingIds.add(biz.id);
            newRestaurants.push(restaurant);
            opensearchData.push({ RestaurantID: biz.id, Cuisine: cuisine });
            termCount++;
            newThisPage++;
          }

          if (newThisPage === 0) break;
          await sleep(300);
        } catch (error) {
          console.error(`    Error: ${error.message}`);
          if (error.message.includes("TOO_MANY_REQUESTS") || error.message.includes("ACCESS_LIMIT")) {
            console.log("    Rate limited! Waiting 60s...");
            await sleep(60000);
          } else {
            await sleep(2000);
          }
        }
      }
    }

    totalNew += termCount;
    console.log(`  New from "${term}": ${termCount} (running total new: ${totalNew}, grand total: ${existingIds.size})`);

    // If we've hit 5000+, we can stop early
    if (existingIds.size >= 5500) {
      console.log(`\n  Reached ${existingIds.size} restaurants, stopping early!`);
      break;
    }
  }

  console.log(`\n=== Second pass: ${totalNew} new restaurants. Grand total: ${existingIds.size} ===\n`);

  // Merge with existing data and save
  let existingData = [];
  try {
    existingData = JSON.parse(fs.readFileSync(DYNAMO_JSON, "utf-8"));
  } catch (e) {
    console.log("No existing dynamodb JSON to merge, saving new data only.");
  }
  const allData = [...existingData, ...newRestaurants];
  fs.writeFileSync(DYNAMO_JSON, JSON.stringify(allData, null, 2));
  fs.writeFileSync(OS_JSON, JSON.stringify(opensearchData, null, 2));
  console.log(`Saved merged data: ${allData.length} total restaurants\n`);

  // Upload new restaurants to DynamoDB
  console.log("Uploading new restaurants to DynamoDB...");
  let uploaded = 0, failed = 0;
  for (const restaurant of newRestaurants) {
    try {
      await dynamoClient.send(new PutItemCommand({
        TableName: DYNAMODB_TABLE,
        Item: {
          BusinessID: { S: restaurant.BusinessID },
          Name: { S: restaurant.Name },
          Address: { S: restaurant.Address },
          Coordinates: { M: {
            Latitude: { S: restaurant.Coordinates.Latitude },
            Longitude: { S: restaurant.Coordinates.Longitude },
          }},
          NumberOfReviews: { N: restaurant.NumberOfReviews },
          Rating: { N: restaurant.Rating },
          ZipCode: { S: restaurant.ZipCode },
          Cuisine: { S: restaurant.Cuisine },
          insertedAtTimestamp: { S: restaurant.insertedAtTimestamp },
        },
      }));
      uploaded++;
      if (uploaded % 100 === 0) console.log(`  Uploaded ${uploaded}/${newRestaurants.length}`);
      if (uploaded % 25 === 0) await sleep(100);
    } catch (error) {
      console.error(`  Failed: ${restaurant.BusinessID}: ${error.message}`);
      failed++;
    }
  }
  console.log(`\nDynamoDB upload: ${uploaded} succeeded, ${failed} failed`);
  console.log(`Grand total in DynamoDB: ${existingIds.size} restaurants`);
}

function searchYelp(term, location, offset) {
  return new Promise((resolve, reject) => {
    const params = new URLSearchParams({
      term: term,
      location: location,
      limit: String(RESULTS_PER_PAGE),
      offset: String(offset),
    });
    const options = {
      hostname: "api.yelp.com",
      path: `/v3/businesses/search?${params.toString()}`,
      method: "GET",
      headers: { Authorization: `Bearer ${YELP_API_KEY}`, Accept: "application/json" },
    };
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.error) reject(new Error(parsed.error.description || parsed.error.code));
          else resolve(parsed.businesses || []);
        } catch (e) { reject(new Error(`Parse error: ${e.message}`)); }
      });
    });
    req.on("error", reject);
    req.end();
  });
}

function formatAddress(loc) {
  if (!loc) return "N/A";
  return [loc.address1, loc.address2, loc.address3, loc.city, loc.state, loc.zip_code].filter(Boolean).join(", ") || "N/A";
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

main().catch((e) => { console.error("Fatal:", e); process.exit(1); });
