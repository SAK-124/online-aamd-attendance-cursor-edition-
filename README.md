# Zoom Attendance Web (Netlify)

- Frontend: React + Vite (Netlify build)
- Backend: Python FastAPI via Netlify Functions (AWS Lambda with Mangum)

## Local Dev
- Frontend: `cd frontend && npm install && npm run dev`
- Backend: Deploy-only (Netlify Functions); for local testing use Netlify CLI: `netlify dev`

## Deploy
- Connect repo to Netlify. Build command and functions directory are configured in `netlify.toml`.
