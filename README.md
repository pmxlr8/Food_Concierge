# Dining Concierge Chatbot

A serverless, microservice-driven web application that provides restaurant dining suggestions through a conversational chatbot interface.

## Architecture

```
User → S3 (Frontend) → API Gateway → Lambda (LF0) → Amazon Lex V2
                                                       ↓
                                                 Lambda (LF1) → SQS Queue
                                                                    ↓
                                        EventBridge (1 min) → Lambda (LF2)
                                                                ↓
                                                OpenSearch + DynamoDB → SES (Email)
```

## Repository Structure

```
├── frontend/           # Chat UI (hosted on S3)
├── lambda-functions/
│   ├── LF0/           # Chat API handler (API Gateway → Lex)
│   ├── LF1/           # Lex code hook (validation + SQS)
│   └── LF2/           # Queue worker (OpenSearch + DynamoDB → SES email)
├── other-scripts/
│   ├── yelp-scraper.js              # Scrapes restaurants from Yelp API
│   └── opensearch-bulk-upload.js    # Uploads data to OpenSearch
└── README.md
```

## AWS Services Used

| Service | Purpose |
|---------|---------|
| S3 | Host frontend static website |
| API Gateway | REST API for chat endpoint |
| Lambda | 3 functions (LF0, LF1, LF2) |
| Amazon Lex V2 | NLU chatbot with intents |
| SQS | Message queue for dining requests |
| DynamoDB | Store restaurant data + user state |
| OpenSearch | Search restaurants by cuisine |
| SES | Send recommendation emails |
| EventBridge | Schedule LF2 every minute |

## Setup Instructions

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for detailed step-by-step instructions.
