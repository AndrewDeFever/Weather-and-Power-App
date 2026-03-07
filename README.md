# Weather-and-Power-App

A serverless weather and power situational-awareness app built to support operational decision-making during severe weather and outage events.

This project was created for two reasons:

1. To give my co-workers a fast, practical way to check weather and outage context in one place.
2. To demonstrate AWS and serverless application design in a real-world use case.

## Overview

The app combines weather observations, weather alerts, and nearby utility outage information into a single status lookup.

Users can search by site ID and quickly see:

- current weather conditions
- active weather alerts
- utility/provider information
- nearby outage activity and estimated restoration context when available

This is designed for fast operational awareness rather than deep analytics.

## Why I Built It

In operations, weather and power issues often show up together, but the information is usually spread across multiple systems and websites.

This app reduces that friction by pulling the relevant context into one place. It is especially useful during high-impact weather events when speed and clarity matter.

It also serves as a practical AWS portfolio project that demonstrates how I design, deploy, and harden cloud-hosted applications.

## Features

- site ID-based lookup for operationally relevant locations
- current weather conditions from NWS sources
- weather alert visibility
- nearby utility outage context
- provider-aware outage integration
- simple frontend for quick status checks
- API-first backend design

## Demo Site IDs

The repo includes demo/test site IDs for quick validation:

- `TULSATEST`
- `OKCTEST`
- `DALLASTEST`
- `KCKTEST`

## Architecture

### Frontend
- static frontend hosted separately
- Amazon S3
- Amazon CloudFront

### Backend
- FastAPI application
- AWS Lambda
- Amazon API Gateway
- Mangum adapter for ASGI/Lambda integration

### External Data Sources
- National Weather Service / weather.gov
- utility outage provider endpoints

## Security / Hardening Highlights

This project includes practical hardening measures appropriate for a public-facing serverless application:

- input validation and bounds checking
- utility override allowlist
- latitude/longitude validation
- security response headers
- API Gateway throttling
- outbound concurrency bulkhead
- outbound host allowlist
- pinned dependency versions
- CI dependency audit
- reduced exception detail leakage in client responses

## Local Development

Install dependencies:

```bash
pip install -r requirements.txt -r requirements-dev.txt
```

Run tests:

```bash
PYTHONPATH=. pytest -q tests
```

Run locally (example):

```bash
uvicorn app.api:app --reload
```

## Deployment

This project is deployed as a serverless application using AWS SAM and GitHub Actions.

High-level deployment flow:

- code pushed to GitHub
- GitHub Actions runs tests and validation
- backend deploys through SAM
- frontend assets deploy to S3
- CloudFront serves the frontend and routes API traffic appropriately

## Notes

- provider latency can vary during severe weather events
- outage accuracy and restoration estimates depend on third-party provider data
- some provider paths warm up faster than others due to caching behavior
- API throttling and fallback behavior are intentional protections

## Purpose as a Portfolio Project

This repo is intended to show practical AWS and application engineering skills, including:

- serverless API design
- frontend/backend separation
- deployment automation
- operational resiliency thinking
- security-minded hardening
- debugging and performance testing under realistic conditions

## License

GPL-3.0
