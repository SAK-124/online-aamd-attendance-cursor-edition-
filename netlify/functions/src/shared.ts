import Busboy from 'busboy'
import { HandlerEvent, HandlerResponse } from '@netlify/functions'
import { randomUUID } from 'crypto'
import { createWriteStream } from 'fs'
import { promises as fs } from 'fs'
import { tmpdir } from 'os'
import { basename, join } from 'path'
import { dirname } from 'path'
import { fileURLToPath } from 'url'
import { spawn } from 'child_process'

export interface UploadedFile {
  fieldName: string
  path: string
  filename: string
  contentType: string
}

export interface MultipartResult {
  fields: Record<string, string>
  files: Record<string, UploadedFile>
}

export async function parseMultipart(event: HandlerEvent): Promise<MultipartResult> {
  const contentType = event.headers['content-type'] || event.headers['Content-Type']
  if (!contentType || !contentType.toLowerCase().startsWith('multipart/form-data')) {
    throw new Error('Expected multipart/form-data request')
  }
  const bodyBuffer = Buffer.from(event.body || '', event.isBase64Encoded ? 'base64' : 'utf8')
  return new Promise((resolve, reject) => {
    const busboy = Busboy({ headers: { 'content-type': contentType } })
    const fields: Record<string, string> = {}
    const files: Record<string, UploadedFile> = {}
    const writePromises: Promise<void>[] = []

    busboy.on('field', (name, value) => {
      fields[name] = value
    })

    busboy.on('file', (fieldName, stream, info) => {
      const filename = info.filename || `${fieldName}-${randomUUID()}`
      const tmpPath = join(tmpdir(), `upload-${Date.now()}-${randomUUID()}-${basename(filename)}`)
      const outStream = createWriteStream(tmpPath)
      const record: UploadedFile = {
        fieldName,
        path: tmpPath,
        filename,
        contentType: info.mimeType || 'application/octet-stream',
      }
      const writePromise = new Promise<void>((res, rej) => {
        stream.on('error', rej)
        outStream.on('error', rej)
        outStream.on('finish', res)
      }).then(() => {
        files[fieldName] = record
      })
      stream.pipe(outStream)
      writePromises.push(writePromise)
    })

    busboy.on('error', reject)
    busboy.on('finish', () => {
      Promise.all(writePromises)
        .then(() => resolve({ fields, files }))
        .catch(reject)
    })

    busboy.end(bodyBuffer)
  })
}

export async function cleanupFiles(files: Iterable<UploadedFile>): Promise<void> {
  for (const file of files) {
    try {
      await fs.unlink(file.path)
    } catch {
      /* ignore */
    }
  }
}

export function normalisePath(event: HandlerEvent): string {
  let rawPath: string
  if (event.rawUrl) {
    try {
      rawPath = new URL(event.rawUrl).pathname
    } catch {
      rawPath = event.path || '/'
    }
  } else {
    rawPath = event.path || '/'
  }
  const prefixes = ['/.netlify/functions/process', '/.netlify/functions', '/api']
  for (const prefix of prefixes) {
    if (rawPath === prefix) {
      rawPath = '/'
      break
    }
    if (rawPath.startsWith(`${prefix}/`)) {
      rawPath = rawPath.slice(prefix.length)
      break
    }
  }
  if (!rawPath.startsWith('/')) rawPath = `/${rawPath}`
  if (rawPath === '/' || rawPath === '') {
    return '/process'
  }
  return rawPath
}

function getModuleDir(): string {
  if (typeof import.meta.url !== 'undefined') {
    return dirname(fileURLToPath(import.meta.url))
  }
  return process.cwd()
}

const moduleDir = getModuleDir()
const pythonCli = join(moduleDir, 'python_backend', 'cli.py')
const pythonExecutable =
  process.env.PYTHON_EXECUTABLE || process.env.PYTHON || process.env.PYTHON_BIN || 'python3'

export interface PythonResult {
  status: number | null
  stdout: string
  stderr: string
}

export async function runPython(args: string[]): Promise<PythonResult> {
  return new Promise<PythonResult>((resolve, reject) => {
    let stdout = ''
    let stderr = ''
    const child = spawn(pythonExecutable, [pythonCli, ...args], {
      cwd: moduleDir,
    })
    child.stdout.setEncoding('utf8')
    child.stdout.on('data', (chunk) => {
      stdout += chunk
    })
    child.stderr.setEncoding('utf8')
    child.stderr.on('data', (chunk) => {
      stderr += chunk
    })
    child.on('error', (err) => reject(err))
    child.on('close', (code) => {
      resolve({ status: code, stdout, stderr })
    })
  })
}

export function withCors(response: HandlerResponse): HandlerResponse {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'content-type',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    ...(response.headers || {}),
  }
  return { ...response, headers }
}

export function jsonResponse(statusCode: number, body: unknown): HandlerResponse {
  return withCors({
    statusCode,
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  })
}

export function optionsResponse(): HandlerResponse {
  return withCors({ statusCode: 204, body: '' })
}

export function methodNotAllowed(): HandlerResponse {
  return withCors({ statusCode: 405, body: 'Method Not Allowed' })
}

export function notFound(): HandlerResponse {
  return withCors({ statusCode: 404, body: 'Not Found' })
}

export function badRequest(message: string): HandlerResponse {
  return jsonResponse(400, { error: message })
}
