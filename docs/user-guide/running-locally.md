# Running the Backend Locally

**Scope statement:** This guide explains how to start the FastAPI backend in development.

## Step 1 — Install dependencies

**Question:** _How do I install backend dependencies?_

**Answer:** Use the workspace’s Python tooling (see project README for exact commands). The backend is designed to run with Python 3.12+.

## Step 2 — Configure environment

**Question:** _Do I need a `.env` file?_

**Answer:** No. The backend reads environment variables directly. If your tooling loads `.env` files (e.g., a shell wrapper or IDE), you may use the optional template at `backend/src/ldaca_web_app/resources/configs/.env.example` and copy it to `.env` in your working directory.

## Step 3 — Start the server

**Question:** _How do I run the API in dev mode?_

**Answer:** Use the FastAPI entrypoint command in the backend README. The default port is `8001`.

## Step 4 — Verify health

**Question:** _How do I know the backend is up?_

**Answer:** Open the health or docs endpoint (`/docs` and `/redoc` are enabled by default).

## Recap

**Question:** _What’s next after the server is running?_

**Answer:** Read the configuration and background task references, then try the API tutorial for a first request.
