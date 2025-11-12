import { HandlerEvent, HandlerResponse } from '@netlify/functions'
import {
  badRequest,
  cleanupFiles,
  jsonResponse,
  parseMultipart,
  withCors,
} from './shared.js'
import {
  processRequest,
  extractKeysFromCsv,
  bufferToBase64,
} from './logic.js'

export async function handleProcess(event: HandlerEvent): Promise<HandlerResponse> {
  const { fields, files } = await parseMultipart(event)
  const zoomFile = files['zoom_csv']
  if (!zoomFile) {
    await cleanupFiles(Object.values(files))
    return badRequest('zoom_csv file is required')
  }
  const rosterFile = files['roster']
  const paramsJson = fields['params'] || '{}'
  const exemptionsJson = fields['exemptions'] || '{}'
  try {
    let params: any
    let exemptions: any
    try {
      params = JSON.parse(paramsJson)
    } catch {
      params = {}
    }
    try {
      exemptions = JSON.parse(exemptionsJson)
    } catch {
      exemptions = {}
    }
    const result = await processRequest(zoomFile.path, rosterFile?.path ?? null, params, exemptions)
    const buffer = result.buffer
    const meta = result.meta
    return withCors({
      statusCode: 200,
      isBase64Encoded: true,
      body: bufferToBase64(buffer),
      headers: {
        'Content-Type':
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': `attachment; filename=${meta.output_xlsx}`,
        'X-Zoom-Attendance-Meta': JSON.stringify(meta),
      },
    })
  } finally {
    await cleanupFiles(Object.values(files))
  }
}

export async function handleKeys(event: HandlerEvent): Promise<HandlerResponse> {
  const { files } = await parseMultipart(event)
  const zoomFile = files['zoom_csv']
  if (!zoomFile) {
    await cleanupFiles(Object.values(files))
    return badRequest('zoom_csv file is required')
  }
  try {
    const items = await extractKeysFromCsv(zoomFile.path)
    return jsonResponse(200, items)
  } finally {
    await cleanupFiles(Object.values(files))
  }
}

export function handleHealth(): HandlerResponse {
  return jsonResponse(200, { ok: true })
}
