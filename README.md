# Debit Credit Monitoring Application

This is a Spring Boot application for monitoring debit and credit events from Azure Event Hubs.

## Features

- Consumes events from Azure Event Hubs using Event Processor Host
- Provides a REST endpoint to send test events
- Logs received events for monitoring
- Integrated with Application Insights for logging

## Prerequisites

- Java 17
- Maven
- Azure Event Hubs namespace
- Azure Storage account for checkpointing

## Configuration

Set the following environment variables:

- `AZURE_EVENTHUB_CONNECTION_STRING`: Connection string for Event Hubs
- `AZURE_STORAGE_CONNECTION_STRING`: Connection string for Azure Storage

## Running the Application

1. Clone the repository
2. Set environment variables
3. Run `mvn spring-boot:run`

## API

- `POST /send-event`: Send a test event to Event Hubs

Example:
```bash
curl -X POST http://localhost:8080/send-event -H "Content-Type: application/json" -d '"Test event"'
```

## Monitoring

Events are logged to the console and Application Insights.