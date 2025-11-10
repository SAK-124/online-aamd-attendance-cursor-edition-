from mangum import Mangum
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import Response, JSONResponse
import json
from . import logic

app = FastAPI()

@app.get("/api/health")
def health():
    return {"ok": True}

@app.post("/api/process")
async def process(
    zoom_csv: UploadFile = File(...),
    roster: UploadFile | None = File(None),
    params: str = Form("{}"),
    exemptions: str = Form("{}"),
):
    try:
        params_obj = json.loads(params or "{}")
    except Exception:
        params_obj = {}
    try:
        exemptions_obj = json.loads(exemptions or "{}")
    except Exception:
        exemptions_obj = {}

    zoom_bytes = await zoom_csv.read()
    roster_bytes = await roster.read() if roster is not None else None

    try:
        out_bytes, meta = logic.process_request(
            zoom_bytes, roster_bytes, params_obj, exemptions_obj
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

    headers = {
        "Content-Disposition": "attachment; filename=zoom_attendance_processed.xlsx",
        "X-Zoom-Attendance-Meta": json.dumps(meta),
    }
    return Response(
        content=out_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@app.post("/api/keys")
async def list_keys(zoom_csv: UploadFile = File(...)):
    zoom_bytes = await zoom_csv.read()
    try:
        items = logic.extract_keys_from_bytes(zoom_bytes)
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    return items

handler = Mangum(app)
