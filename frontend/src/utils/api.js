const ENV_BASES = (import.meta.env.VITE_API_BASE_URL || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean)

const DEV_ORIGIN = import.meta.env.VITE_DEV_API_ORIGIN || 'http://localhost:8888'

const BASE_CANDIDATES = [
  ...ENV_BASES,
  import.meta.env.DEV ? `${DEV_ORIGIN}/api` : null,
  import.meta.env.DEV ? `${DEV_ORIGIN}/.netlify/functions` : null,
  import.meta.env.DEV ? `${DEV_ORIGIN}/.netlify/functions/process` : null,
  '/api',
  '/.netlify/functions/process',
  '/.netlify/functions',
].filter(Boolean)

function normalizeBase(base) {
  if (!base || base === '/') return ''
  return base.endsWith('/') ? base.slice(0, -1) : base
}

function buildUrl(base, path) {
  const normalizedBase = normalizeBase(base)
  const normalizedPath = path.startsWith('/') ? path : `/${path}`
  if (!normalizedBase) return normalizedPath
  return `${normalizedBase}${normalizedPath}`
}

function extractTitleFromHtml(html) {
  const match = html.match(/<title>(.*?)<\/title>/i)
  return match ? match[1].trim() : null
}

function summariseHtmlResponse(text, url) {
  const title = extractTitleFromHtml(text)
  if (title) {
    return `Unexpected HTML response from ${url} (${title}). Ensure the API rewrite is configured.`
  }
  return `Unexpected HTML response from ${url}. Ensure the API rewrite is configured.`
}

function summariseErrorPayload(text, status, url) {
  if (!text) {
    return `Request to ${url} failed with status ${status}.`
  }
  try {
    const parsed = JSON.parse(text)
    if (parsed && typeof parsed.error === 'string' && parsed.error.trim()) {
      return parsed.error.trim()
    }
  } catch (err) {
    /* ignore */
  }
  const trimmed = text.trim()
  if (!trimmed) {
    return `Request to ${url} failed with status ${status}.`
  }
  if (/<!DOCTYPE html>/i.test(trimmed) || /<html/i.test(trimmed)) {
    return summariseHtmlResponse(trimmed, url)
  }
  return trimmed.length > 400 ? `${trimmed.slice(0, 397)}…` : trimmed
}

export async function apiFetch(path, options = {}) {
  const candidates = Array.from(new Set(BASE_CANDIDATES.map(normalizeBase)))
  const errors = []
  for (const base of candidates) {
    const url = buildUrl(base, path)
    try {
      const res = await fetch(url, options)
      const contentType = res.headers.get('content-type') || ''
      const looksHtml = /text\/html/i.test(contentType)
      if (res.ok && !looksHtml) {
        return res
      }
      const responseText = await res.clone().text().catch(() => '')
      if (res.ok && looksHtml) {
        errors.push(summariseHtmlResponse(responseText, url))
        continue
      }
      const message = summariseErrorPayload(responseText, res.status, url)
      if ((res.status === 404 || res.status === 502 || res.status === 503) && base !== '/.netlify/functions') {
        errors.push(message)
        continue
      }
      throw new Error(message)
    } catch (err) {
      if (err instanceof Error) {
        errors.push(`${url}: ${err.message}`)
      } else {
        errors.push(`${url}: ${String(err)}`)
      }
    }
  }
  const unique = Array.from(new Set(errors.filter(Boolean)))
  const fallback =
    unique.length > 0
      ? unique.join('\n')
      : 'All API base URLs failed. Check that the backend is reachable.'
  throw new Error(fallback)
}
