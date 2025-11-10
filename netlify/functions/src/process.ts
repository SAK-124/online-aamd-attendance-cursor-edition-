import { Handler } from '@netlify/functions'
import {
  badRequest,
  methodNotAllowed,
  normalisePath,
  notFound,
  optionsResponse,
  withCors,
} from './shared.js'
import { handleHealth, handleKeys, handleProcess } from './handlers.js'

const handler: Handler = async (event) => {
  const method = (event.httpMethod || 'GET').toUpperCase()
  if (method === 'OPTIONS') {
    return optionsResponse()
  }
  const path = normalisePath(event)
  try {
    if (method === 'GET' && (path === '/health' || path === '/api/health')) {
      return handleHealth()
    }
    if (method === 'POST' && (path === '/process' || path === '/')) {
      return await handleProcess(event)
    }
    if (method === 'POST' && path === '/keys') {
      return await handleKeys(event)
    }
    if (method === 'GET' && path === '/process') {
      return badRequest('POST required for /process')
    }
    if (method === 'GET' && path === '/keys') {
      return badRequest('POST required for /keys')
    }
    if (method === 'GET' && path === '/api/keys') {
      return badRequest('POST required for /api/keys')
    }
    if (method === 'POST' && path === '/api/process') {
      return await handleProcess(event)
    }
    if (method === 'POST' && path === '/api/keys') {
      return await handleKeys(event)
    }
    if (method !== 'GET' && method !== 'POST') {
      return methodNotAllowed()
    }
    return notFound()
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    return withCors({
      statusCode: 400,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: message }),
    })
  }
}

export { handler }
