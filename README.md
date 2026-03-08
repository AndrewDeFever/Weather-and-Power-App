## Overview

**Weather & Power App** is an AWS-hosted operational lookup tool built for fast situational awareness during weather and utility-impact events.

It gives operations users a single place to check:

- current weather conditions
- active weather alerts
- utility / provider context
- nearby outage activity and restoration details when available

The goal is not deep analytics. The goal is **fast operational context** — the kind of answer you want in seconds when weather and power risk may affect a site.

## Why This Project Exists

In real operations work, weather and power issues often show up together, but the information is usually scattered across multiple systems, provider maps, and public websites.

This project reduces that friction by combining the most useful context into a single workflow:

1. look up a known site
2. identify the utility serving that site
3. pull current weather and alert data
4. check for nearby outage activity
5. return a concise operational summary

It was built both as a practical internal-style tool and as a portfolio project demonstrating cloud, serverless, and application hardening skills in a real-world use case.

## Core Workflow

A user enters a known site ID, and the app returns a consolidated status view showing:

- resolved site information
- utility / provider mapping
- current weather observation
- alert presence
- nearby outage context
- raw response data for troubleshooting or validation

This supports quick triage and decision-making without forcing the user to pivot between weather pages, outage maps, and internal lookup references.

## Architecture

### Frontend

- Static single-page frontend
- Hosted through **Amazon S3**
- Delivered through **Amazon CloudFront**

### Backend

- **FastAPI** application running on **AWS Lambda**
- **Amazon API Gateway** for the public API layer
- **Mangum** adapter for ASGI-to-Lambda integration

### External Data Sources

- National Weather Service / weather.gov
- Provider-specific outage endpoints

### Deployment

- Infrastructure defined with **AWS SAM**
- CI/CD handled with **GitHub Actions**
- Frontend and backend deployed independently

## Request Flow

1. The user searches for a site ID in the frontend.
2. The frontend sends a request to the public API.
3. The backend resolves the site and provider metadata.
4. Weather data is retrieved from NWS sources.
5. Outage context is retrieved from the matching utility provider integration.
6. Results are normalized into a consistent response shape and returned to the UI.
7. The frontend renders an operational summary, detail tabs, and supporting links.

## Engineering Decisions

This project intentionally prioritizes **practical reliability over novelty**.

Key design choices include:

- **Serverless deployment** for low operational overhead and simple public hosting
- **Frontend / backend separation** to keep the UI lightweight and the API independently deployable
- **Provider-aware outage routing** so the backend can select the correct integration path for each site
- **Consistent response shaping** so the frontend can handle success, fallback, and degraded responses without breaking
- **Security-focused hardening** appropriate for a public-facing lookup service

## Security and Hardening Highlights

The backend includes several defensive controls to improve safety and resilience:

- input validation and bounds checking
- utility override allowlist
- latitude / longitude validation
- outbound host allowlist
- outbound concurrency bulkhead
- API Gateway throttling
- security response headers
- pinned dependencies
- CI dependency auditing
- reduced exception detail leakage in client responses

## Performance Notes

Response times vary by provider and cache state.

Warm requests are generally faster, while first-hit or slower third-party provider paths can take longer. This reflects a real constraint of distributed systems that depend on external utility and weather sources.

For this reason, the app is designed to:

- fail predictably
- preserve response structure
- expose useful status information even when some provider paths are slower than others

## Known Limitations

- Provider latency can vary, especially during severe weather or high-traffic outage events.
- Outage accuracy and estimated restoration times depend on third-party provider data quality.
- Coverage is limited to the providers and demo sites currently supported by the project.
- This application is intended for operational lookup and awareness, not authoritative incident management.

## Why This Matters as a Portfolio Project

This project is meant to demonstrate more than API wiring. It shows how I approach:

- serverless application design on AWS
- practical operations-focused tooling
- frontend / backend separation
- CI/CD automation
- defensive programming and service hardening
- external dependency risk and latency handling
- building a real, public-facing tool with a clear use case
