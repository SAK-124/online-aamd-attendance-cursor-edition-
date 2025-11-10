# Zoom Attendance Web (Netlify)

- Frontend: React + Vite (Netlify build)
- Backend: TypeScript Netlify Functions wrapping the original Python attendance engine

## Local Dev
- Frontend: `cd frontend && npm install && npm run dev`
- Backend: Build with `npm --prefix netlify/functions run build`; install Python deps with `pip install -r netlify/functions/python_backend/requirements.txt`; for local testing use Netlify CLI: `netlify dev`

## Deploy
- Connect repo to Netlify. Build command and functions directory are configured in `netlify.toml`.
