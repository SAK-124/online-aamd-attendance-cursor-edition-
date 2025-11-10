import { Handler } from '@netlify/functions'
import { badRequest, methodNotAllowed, optionsResponse, withCors } from './shared.js'
import { handleKeys } from './handlers.js'

const handler: Handler = async (event) => {
  const method = (event.httpMethod || 'GET').toUpperCase()
  if (method === 'OPTIONS') {
    return optionsResponse()
  }
  if (method !== 'POST') {
    if (method === 'GET') {
      return badRequest('POST required for /keys')
    }
    return methodNotAllowed()
  }
  try {
    return await handleKeys(event)
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
