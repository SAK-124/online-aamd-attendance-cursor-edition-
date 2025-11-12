# Zoom Attendance Web (Netlify)

- Frontend: React + Vite (Netlify build)
- Backend: TypeScript Netlify Functions implementing the attendance engine natively

## Local Dev
- Frontend: `cd frontend && npm install && npm run dev`
- Backend: `cd netlify/functions && npm install && npm run build`; for local testing use Netlify CLI: `netlify dev`

## Deploy
- Connect repo to Netlify. Build command and functions directory are configured in `netlify.toml`.
