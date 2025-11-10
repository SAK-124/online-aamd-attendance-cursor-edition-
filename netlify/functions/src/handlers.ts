import { HandlerEvent, HandlerResponse } from '@netlify/functions'
import {
  badRequest,
  cleanupFiles,
  jsonResponse,
  parseMultipart,
  runPython,
  withCors,
} from './shared.js'

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
    const args = [
      'process',
      '--zoom',
      zoomFile.path,
      '--params-json',
      paramsJson,
      '--exemptions-json',
      exemptionsJson,
    ]
    if (rosterFile) {
      args.push('--roster', rosterFile.path)
    }
    const result = await runPython(args)
    if (result.status !== 0) {
      throw new Error(result.stderr || 'Python processing failed')
    }
    let payload: any
    try {
      payload = JSON.parse(result.stdout.trim() || '{}')
    } catch (err) {
      throw new Error(`Unable to parse Python response: ${err instanceof Error ? err.message : String(err)}`)
    }
    if (!payload || payload.ok !== true) {
      const message = payload?.error || 'Processing failed'
      throw new Error(message)
    }
    if (typeof payload.data !== 'string') {
      throw new Error('Invalid response payload: data missing')
    }
    const buffer = Buffer.from(payload.data, 'base64')
    const meta = payload.meta ?? {}
    return withCors({
      statusCode: 200,
      isBase64Encoded: true,
      body: buffer.toString('base64'),
      headers: {
        'Content-Type':
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': 'attachment; filename=zoom_attendance_processed.xlsx',
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
    const result = await runPython(['keys', '--zoom', zoomFile.path])
    if (result.status !== 0) {
      throw new Error(result.stderr || 'Python key extraction failed')
    }
    let payload: any
    try {
      payload = JSON.parse(result.stdout.trim() || '{}')
    } catch (err) {
      throw new Error(`Unable to parse Python response: ${err instanceof Error ? err.message : String(err)}`)
    }
    if (!payload || payload.ok !== true || !Array.isArray(payload.items)) {
      const message = payload?.error || 'Unable to read keys'
      throw new Error(message)
    }
    return jsonResponse(200, payload.items)
  } finally {
    await cleanupFiles(Object.values(files))
  }
}

export function handleHealth(): HandlerResponse {
  return jsonResponse(200, { ok: true })
}
