import { parse } from 'csv-parse/sync'
import iconv from 'iconv-lite'
import { Workbook, Worksheet } from 'exceljs'
import { DateTime, Duration } from 'luxon'
import { promises as fs } from 'fs'
import { extname } from 'path'
import * as XLSX from 'xlsx'

const APP_FILE_DEFAULT = 'zoom_attendance_processed.xlsx'
const EXCLUDE_NAME_PATTERNS = [
  /^\s*meeting analytics from read\s*$/i,
  /^\s*ta\s*$/i,
  /^\s*saboor'?s fathom notetaker\s*$/i,
  /^\s*hassaan khalid\s*$/i,
]

const RECONNECT_OVERLAP_TOLERANCE_SECONDS = 2

export interface ProcessParams {
  threshold_ratio?: number
  buffer_minutes?: number
  break_minutes?: number
  override_total_minutes?: number | null
  penalty_tolerance_minutes?: number
  rounding_mode?: 'none' | 'ceil_attendance' | 'ceil_both'
}

export interface ProcessMeta {
  output_xlsx: string
  total_class_minutes: number
  adjusted_total_minutes: number
  threshold_minutes_raw: number
  effective_threshold_minutes: number
  buffer_minutes: number
  rows: number
  rounding_mode: 'none' | 'ceil_attendance' | 'ceil_both'
  roster_used: boolean
  total_class_minutes_source: string
}

export interface ProcessPayload {
  buffer: Buffer
  meta: ProcessMeta
}

export type ExemptionsMap = Record<string, Record<string, boolean>>

export interface ExtractedKey {
  key: string
  erp: string | null
  name: string
  display: string
}

type Interval = { start: DateTime; end: DateTime }

type SessionRecord = {
  joinTs: DateTime | null
  leaveTs: DateTime | null
  joinRaw: string
  leaveRaw: string
  rawName: string
}

type ReconnectEvent = {
  index: number
  disconnectTs: DateTime | null
  reconnectTs: DateTime | null
  gap: Duration
  disconnectSeg: SessionRecord | null
  reconnectSeg: SessionRecord | null
}

type KeyAggregates = {
  key: string
  cleanName: string
  canon: string
  erp: string | null
  rawNames: Set<string>
  matchSource: string
  intervals: Interval[]
  goodIntervals: Interval[]
  badIntervals: Interval[]
  durationsGood: number[]
  durationsBad: number[]
  sessionRecords: SessionRecord[]
}

type ZoomRow = Record<string, string>

type RosterRow = {
  ERP: string
  RosterName: string
  RosterCanon: string
  Email: string
}

function decodeBuffer(buffer: Buffer): { text: string; encoding: string } {
  if (buffer.length === 0) {
    return { text: '', encoding: 'utf8' }
  }
  const first = buffer[0]
  const second = buffer[1]
  if (first === 0xff && second === 0xfe) {
    return { text: iconv.decode(buffer, 'utf16-le'), encoding: 'utf16le' }
  }
  if (first === 0xfe && second === 0xff) {
    return { text: iconv.decode(buffer, 'utf16-be'), encoding: 'utf16be' }
  }
  if (buffer.slice(0, 3).equals(Buffer.from([0xef, 0xbb, 0xbf]))) {
    return { text: iconv.decode(buffer, 'utf8'), encoding: 'utf8-sig' }
  }
  try {
    return { text: iconv.decode(buffer, 'utf8'), encoding: 'utf8' }
  } catch {
    return { text: iconv.decode(buffer, 'latin1'), encoding: 'latin1' }
  }
}

function detectDelimiter(sample: string): string {
  const candidates = [',', ';', '\t']
  const lines = sample.split(/\r?\n/).slice(0, 5)
  let best = ','
  let bestScore = -1
  for (const cand of candidates) {
    const counts = lines.map((ln) => (ln.includes(cand) ? ln.split(cand).length : 0))
    const avg = counts.reduce((a, b) => a + b, 0) / (counts.length || 1)
    if (avg > bestScore) {
      best = cand
      bestScore = avg
    }
  }
  return best
}

function parseCsv(buffer: Buffer, opts?: { columns?: boolean }): { records: any[]; headers: string[] } {
  const { text } = decodeBuffer(buffer)
  const delim = detectDelimiter(text)
  const records = parse(text, {
    columns: opts?.columns ?? true,
    skip_empty_lines: true,
    delimiter: delim,
    relax_column_count: true,
    trim: true,
  }) as any[]
  const headers: string[] = Array.isArray(records) && records.length > 0 ? Object.keys(records[0]) : []
  return { records, headers }
}

function parseZoomCsv(buffer: Buffer): ZoomRow[] {
  const { text } = decodeBuffer(buffer)
  const headerMatch = /\bName\s*\(original name\)\s*,/i.exec(text)
  let payload = text
  if (headerMatch) {
    const idx = text.lastIndexOf('\n', headerMatch.index)
    payload = text.slice(idx >= 0 ? idx + 1 : 0)
  } else {
    const lines = text.split(/\r?\n/).filter((ln) => ln.trim())
    let headerIndex = -1
    for (let i = 0; i < lines.length; i++) {
      const low = lines[i].toLowerCase()
      if (low.includes('join time') && low.includes('leave time')) {
        headerIndex = i
        break
      }
    }
    if (headerIndex === -1) {
      throw new Error('Could not locate the participants header row in the CSV.')
    }
    payload = lines.slice(headerIndex).join('\n')
  }
  const payloadBuffer = Buffer.from(payload, 'utf8')
  const { records } = parseCsv(payloadBuffer)
  return records as ZoomRow[]
}

function normaliseZoom(buffer: Buffer): ZoomRow[] {
  return parseZoomCsv(buffer)
}

function parseDate(value: string | undefined): DateTime | null {
  if (!value) return null
  const trimmed = value.trim()
  if (!trimmed) return null
  const dt = DateTime.fromISO(trimmed, { zone: 'utc' })
  if (dt.isValid) return dt.toUTC()
  const parsed = DateTime.fromFormat(trimmed, 'M/d/yyyy, h:mm:ss a', { zone: 'utc' })
  if (parsed.isValid) return parsed.toUTC()
  const fallback = DateTime.fromJSDate(new Date(trimmed))
  return fallback.isValid ? fallback.toUTC() : null
}

function mergeIntervals(intervals: Interval[]): Interval[] {
  const filtered = intervals
    .filter((i) => i.start && i.end && i.end > i.start)
    .sort((a, b) => a.start.toMillis() - b.start.toMillis())
  if (filtered.length === 0) return []
  const merged: Interval[] = []
  let current = { ...filtered[0] }
  for (const interval of filtered.slice(1)) {
    if (interval.start <= current.end) {
      if (interval.end > current.end) {
        current.end = interval.end
      }
    } else {
      merged.push(current)
      current = { ...interval }
    }
  }
  merged.push(current)
  return merged
}

function minutes(intervals: Interval[]): number {
  return intervals.reduce((acc, i) => acc + i.end.diff(i.start, 'minutes').minutes, 0)
}

function intervalUnionMinutes(intervals: Interval[]): number {
  return minutes(mergeIntervals(intervals))
}

function hasAnyOverlap(intervals: Interval[]): boolean {
  const filtered = intervals
    .filter((i) => i.start && i.end && i.end > i.start)
    .sort((a, b) => a.start.toMillis() - b.start.toMillis())
  for (let i = 1; i < filtered.length; i++) {
    if (filtered[i].start < filtered[i - 1].end) {
      return true
    }
  }
  return false
}

function intervalsOverlapOrClose(aIntervals: Interval[], bIntervals: Interval[], maxGapMinutes = 7): boolean {
  const A = mergeIntervals(aIntervals)
  const B = mergeIntervals(bIntervals)
  if (!A.length || !B.length) return false
  let i = 0
  let j = 0
  const gap = maxGapMinutes * 60 * 1000
  while (i < A.length && j < B.length) {
    const a = A[i]
    const b = B[j]
    if (a.end >= b.start && b.end >= a.start) {
      return true
    }
    if (a.end.toMillis() < b.start.toMillis()) {
      if (b.start.toMillis() - a.end.toMillis() <= gap) {
        return true
      }
      i += 1
    } else if (b.end.toMillis() < a.start.toMillis()) {
      if (a.start.toMillis() - b.end.toMillis() <= gap) {
        return true
      }
      j += 1
    } else if (a.end < b.end) {
      i += 1
    } else {
      j += 1
    }
  }
  return false
}

function minutesMinus(aIntervals: Interval[], bIntervals: Interval[]): number {
  const A = mergeIntervals(aIntervals)
  const B = mergeIntervals(bIntervals)
  if (!A.length) return 0
  if (!B.length) return minutes(A)
  let total = 0
  let j = 0
  for (const a of A) {
    let cur = a.start
    while (cur < a.end && j < B.length) {
      const b = B[j]
      if (b.end <= cur) {
        j += 1
        continue
      }
      if (b.start >= a.end) {
        break
      }
      if (b.start > cur) {
        total += b.start.diff(cur, 'minutes').minutes
      }
      cur = b.end > cur ? b.end : cur
      if (b.end <= cur) {
        j += 1
      }
    }
    if (cur < a.end) {
      total += a.end.diff(cur, 'minutes').minutes
    }
  }
  return total
}

function tsToExcelString(ts: DateTime | null): string {
  if (!ts) return ''
  return ts.toUTC().toFormat('yyyy-MM-dd HH:mm:ss')
}

function durationToHMS(duration: Duration | null): string {
  if (!duration) return ''
  const totalSeconds = Math.max(0, Math.floor(duration.as('seconds')))
  const hrs = Math.floor(totalSeconds / 3600)
  const rem = totalSeconds % 3600
  const mins = Math.floor(rem / 60)
  const secs = rem % 60
  return `${hrs.toString().padStart(2, '0')}:${mins.toString().padStart(2, '0')}:${secs
    .toString()
    .padStart(2, '0')}`
}

function cleanRaw(val: unknown): string {
  if (val === undefined || val === null) return ''
  if (typeof val === 'string') return val
  return String(val ?? '')
}

function canonName(raw: string): string {
  if (typeof raw !== 'string') return ''
  let s = raw.toLowerCase()
  s = s.replace(/\([^)]*\)/g, ' ')
  s = s.replace(/\d{5}/g, ' ')
  s = s.replace(/[_-]/g, ' ')
  s = s.replace(/[^a-z]+/g, ' ')
  s = s.replace(/\s+/g, ' ').trim()
  return s
}

function normNameSpacesOnly(raw: string): string {
  return String(raw ?? '').trim().toLowerCase().replace(/\s+/g, ' ')
}

function detectColumns(rows: ZoomRow[]): {
  nameCol: string
  joinCol: string | null
  leaveCol: string | null
  durationCol: string | null
  emailCol: string | null
  pidCol: string | null
} {
  if (!rows.length) {
    throw new Error('Zoom CSV appears to be empty')
  }
  const header = Object.keys(rows[0])
  const lookup = new Map(header.map((h) => [h.toLowerCase(), h]))
  const pick = (cands: string[]): string | null => {
    for (const cand of cands) {
      const found = lookup.get(cand)
      if (found) return found
    }
    return null
  }
  const nameCol = pick([
    'name (original name)',
    'name',
    'participant',
    'user name',
    'full name',
    'display name',
  ])
  if (!nameCol) {
    throw new Error(`Could not detect participant name column. Found: ${header.join(', ')}`)
  }
  const joinCol = pick([
    'join time',
    'join time (timezone)',
    'join time (yyyy-mm-dd hh:mm:ss)',
    'join time (utc)',
    'first join time',
    'first join time (utc)',
  ])
  const leaveCol = pick([
    'leave time',
    'leave time (timezone)',
    'leave time (yyyy-mm-dd hh:mm:ss)',
    'leave time (utc)',
    'last leave time',
    'last leave time (utc)',
  ])
  const durationCol = pick([
    'duration (minutes)',
    'total duration (minutes)',
    'time in meeting (minutes)',
  ])
  const emailCol = pick(['user email', 'email', 'attendee email'])
  const pidCol = pick(['participant id', 'user id', 'unique id', 'id'])
  if (!joinCol && !leaveCol && !durationCol) {
    throw new Error('No join/leave or duration columns found in the CSV.')
  }
  return { nameCol, joinCol, leaveCol, durationCol, emailCol, pidCol }
}

function parseName(name: string | undefined | null): [string | null, string, number] {
  if (!name || typeof name !== 'string') return [null, '', -1]
  const trimmed = name.trim()
  const match = /^\s*(\d{5})[\s-_]+(.+?)\s*$/.exec(trimmed)
  if (match) {
    return [match[1], match[2].trim(), 0]
  }
  return [null, trimmed, -1]
}

function detectRosterColumns(rows: Record<string, string>[]): {
  erpCol: string | null
  nameCol: string | null
  emailCol: string | null
} {
  if (!rows.length) return { erpCol: null, nameCol: null, emailCol: null }
  const header = Object.keys(rows[0])
  const lookup = new Map(header.map((h) => [h.toLowerCase(), h]))
  const isFiveDigit = (val: string) => /^\s*\d{5}\s*$/.test(val ?? '')
  let bestCol: string | null = null
  let bestHits = -1
  for (const col of header) {
    let hits = 0
    for (const row of rows.slice(0, 500)) {
      if (isFiveDigit(String(row[col] ?? ''))) hits += 1
    }
    if (hits > bestHits) {
      bestHits = hits
      bestCol = col
    }
  }
  const erpCol = bestHits > 0 ? bestCol : null
  let nameCol: string | null = null
  for (const key of ['name', 'student name', 'full name', 'official name']) {
    const found = lookup.get(key)
    if (found) {
      nameCol = found
      break
    }
  }
  if (!nameCol) {
    nameCol = header.find((col) => col !== erpCol) ?? null
  }
  let emailCol: string | null = null
  for (const key of ['email', 'user email', 'attendee email', 'e-mail']) {
    const found = lookup.get(key)
    if (found) {
      emailCol = found
      break
    }
  }
  return { erpCol, nameCol, emailCol }
}

async function loadRoster(path: string | null): Promise<RosterRow[]> {
  if (!path) return []
  const buf = await fs.readFile(path)
  const ext = extname(path).toLowerCase()
  if (ext === '.xlsx' || ext === '.xlsm' || ext === '.xls') {
    const workbook = XLSX.read(buf, { type: 'buffer' })
    const sheetName = workbook.SheetNames[0]
    if (!sheetName) return []
    const rows = XLSX.utils.sheet_to_json<Record<string, string>>(workbook.Sheets[sheetName], {
      defval: '',
    })
    return normalizeRosterRows(rows)
  }
  const { records } = parseCsv(buf, { columns: true })
  return normalizeRosterRows(records as Record<string, string>[])
}

function normalizeRosterRows(rows: Record<string, string>[]): RosterRow[] {
  const cols = detectRosterColumns(rows)
  const { erpCol, nameCol, emailCol } = cols
  if (!erpCol || !nameCol) {
    throw new Error('Could not detect ERP/Name columns in roster. Make sure it has 5-digit ERP and a Name column.')
  }
  const out: RosterRow[] = []
  const seen = new Set<string>()
  for (const row of rows) {
    const erpMatch = /(\d{5})/.exec(String(row[erpCol] ?? ''))
    const erp = erpMatch ? erpMatch[1] : null
    const name = String(row[nameCol] ?? '').trim()
    if (!erp || !name) continue
    if (seen.has(erp)) continue
    seen.add(erp)
    out.push({
      ERP: erp,
      RosterName: name,
      RosterCanon: canonName(name),
      Email: emailCol ? String(row[emailCol] ?? '').trim() : '',
    })
  }
  return out
}

function shouldExclude(name: string): boolean {
  for (const pattern of EXCLUDE_NAME_PATTERNS) {
    if (pattern.test(name ?? '')) return true
  }
  return false
}

function prepareSegments(records: SessionRecord[]): SessionRecord[] {
  const segments = records
    .map((rec) => {
      let start = rec.joinTs
      let end = rec.leaveTs
      if (!start && end) start = end
      if (!end && start) end = start
      if (start && end && end < start) {
        const tmp = start
        start = end
        end = tmp
      }
      return {
        ...rec,
        joinTs: start,
        leaveTs: end,
      }
    })
    .filter((rec) => rec.joinTs && rec.leaveTs) as SessionRecord[]
  segments.sort((a, b) => {
    const aStart = a.joinTs?.toMillis() ?? 0
    const bStart = b.joinTs?.toMillis() ?? 0
    if (aStart === bStart) {
      const aEnd = a.leaveTs?.toMillis() ?? aStart
      const bEnd = b.leaveTs?.toMillis() ?? bStart
      return aEnd - bEnd
    }
    return aStart - bStart
  })
  return segments
}

function computeReconnectEvents(segments: SessionRecord[]): ReconnectEvent[] {
  if (!segments.length) return []
  const events: ReconnectEvent[] = []
  let coverageSeg = segments[0]
  let coverageEnd = coverageSeg.leaveTs
  let counter = 0
  for (const seg of segments.slice(1)) {
    const start = seg.joinTs ?? seg.leaveTs
    if (!start) {
      if (seg.leaveTs && (!coverageEnd || seg.leaveTs > coverageEnd)) {
        coverageSeg = seg
        coverageEnd = seg.leaveTs
      }
      continue
    }
    if (!coverageEnd) {
      coverageSeg = seg
      coverageEnd = seg.leaveTs
      continue
    }
    const toleranceEnd = coverageEnd.plus({ seconds: RECONNECT_OVERLAP_TOLERANCE_SECONDS })
    if (start < toleranceEnd) {
      if (seg.leaveTs && (!coverageEnd || seg.leaveTs > coverageEnd)) {
        coverageSeg = seg
        coverageEnd = seg.leaveTs
      }
      continue
    }
    const disconnectTs = coverageEnd
    const reconnectTs = start
    const gap = reconnectTs.diff(disconnectTs ?? reconnectTs)
    counter += 1
    events.push({
      index: counter,
      disconnectTs,
      reconnectTs,
      gap,
      disconnectSeg: coverageSeg ?? null,
      reconnectSeg: seg ?? null,
    })
    coverageSeg = seg
    coverageEnd = seg.leaveTs
  }
  return events
}

interface ProcessedKeySummary {
  attendanceRows: Record<string, any>[]
  issuesRows: Record<string, any>[]
  absentRows: Record<string, any>[]
  penaltiesRows: Record<string, any>[]
  matchesRows: Record<string, any>[]
  reconnectRows: Record<string, any>[]
  presentErps: Set<string>
  ambiguousNameKeys: Set<string>
  aliasMerges: Array<{ source: string; target: string }>
}

function analyseZoom(
  rows: ZoomRow[],
  rosterRows: RosterRow[],
  params: Required<ProcessParams>,
  exemptions: ExemptionsMap,
): {
  aggregates: Map<string, KeyAggregates>
  hasTimes: boolean
  totalMinutes: number
  totalSource: string
  adjustedTotal: number
  effectiveThreshold: number
  thresholdRaw: number
  rosterDf: RosterRow[]
  aliasMerges: Array<{ source: string; target: string }>
  ambiguous: Set<string>
  reconnectMap: Map<string, ReconnectEvent[]>
} {
  const cols = detectColumns(rows)
  const nameCol = cols.nameCol
  const joinCol = cols.joinCol
  const leaveCol = cols.leaveCol
  const durationCol = cols.durationCol
  const emailCol = cols.emailCol
  const pidCol = cols.pidCol

  const filteredRows = rows.filter((row) => !shouldExclude(String(row[nameCol] ?? '')))
  const hasTimes = Boolean(
    joinCol &&
      leaveCol &&
      filteredRows.some((row) => parseDate(row[joinCol!])) &&
      filteredRows.some((row) => parseDate(row[leaveCol!])),
  )

  const aggregates = new Map<string, KeyAggregates>()

  const joinValues = hasTimes
    ? filteredRows.map((row) => parseDate(joinCol ? row[joinCol] : undefined)).filter((dt) => dt) as DateTime[]
    : []
  const leaveValues = hasTimes
    ? filteredRows.map((row) => parseDate(leaveCol ? row[leaveCol] : undefined)).filter((dt) => dt) as DateTime[]
    : []

  let totalMinutes = 0
  let totalSource = ''
  if (params.override_total_minutes && params.override_total_minutes > 0) {
    totalMinutes = params.override_total_minutes
    totalSource = 'override'
  } else if (hasTimes) {
    if (!joinValues.length || !leaveValues.length) {
      throw new Error('Timestamps present but unparsable. Check the Zoom CSV encoding/format.')
    }
    const minJoin = joinValues.reduce((a, b) => (a < b ? a : b))
    const maxLeave = leaveValues.reduce((a, b) => (a > b ? a : b))
    totalMinutes = maxLeave.diff(minJoin, 'minutes').minutes
    totalSource = 'auto (timestamps)'
  } else if (durationCol) {
    let maxDur = 0
    for (const row of filteredRows) {
      const num = Number.parseFloat(String(row[durationCol] ?? '0'))
      if (!Number.isNaN(num) && num > maxDur) {
        maxDur = num
      }
    }
    totalMinutes = maxDur
    totalSource = 'auto (max duration)'
  } else {
    throw new Error('Could not determine total class duration.')
  }

  const breakMinutes = Math.max(0, params.break_minutes)
  const adjustedTotal = Math.max(totalMinutes - breakMinutes, 1)
  const thresholdRaw = params.threshold_ratio * adjustedTotal
  const bufferMinutes = Math.max(0, params.buffer_minutes)
  const effectiveThreshold = Math.max(0, thresholdRaw - bufferMinutes)

  const reconnectMap = new Map<string, ReconnectEvent[]>()

  for (const row of filteredRows) {
    const [erp, cleanName, penFlag] = parseName(row[nameCol])
    const canon = canonName(cleanName)
    const rawName = String(row[nameCol] ?? '')
    const key = erp ? `ERP:${erp}` : `NAME:${normNameSpacesOnly(cleanName)}`
    const aggregate = aggregates.get(key) ?? {
      key,
      cleanName,
      canon,
      erp,
      rawNames: new Set<string>(),
      matchSource: erp ? 'erp_in_name' : 'name_only',
      intervals: [],
      goodIntervals: [],
      badIntervals: [],
      durationsGood: [],
      durationsBad: [],
      sessionRecords: [],
    }
    aggregate.rawNames.add(rawName)
    if (erp && !aggregate.erp) aggregate.erp = erp
    aggregate.canon = aggregate.canon || canon
    const join = hasTimes && joinCol ? parseDate(row[joinCol]) : null
    const leave = hasTimes && leaveCol ? parseDate(row[leaveCol]) : null
    const joinRaw = joinCol ? String(row[joinCol] ?? '') : ''
    const leaveRaw = leaveCol ? String(row[leaveCol] ?? '') : ''
    const sessionRecord: SessionRecord = {
      joinTs: join,
      leaveTs: leave,
      joinRaw,
      leaveRaw,
      rawName,
    }
    aggregate.sessionRecords.push(sessionRecord)
    if (hasTimes && join && leave) {
      aggregate.intervals.push({ start: join, end: leave })
      if (penFlag === -1) {
        aggregate.badIntervals.push({ start: join, end: leave })
      } else {
        aggregate.goodIntervals.push({ start: join, end: leave })
      }
    } else if (!hasTimes && durationCol) {
      const dur = Number.parseFloat(String(row[durationCol] ?? '0')) || 0
      if (penFlag === -1) {
        aggregate.durationsBad.push(dur)
      } else {
        aggregate.durationsGood.push(dur)
      }
    }
    aggregates.set(key, aggregate)
  }

  const aliasMerges: Array<{ source: string; target: string }> = []
  const ambiguous = new Set<string>()

  const erpByCanon = new Map<string, string[]>()
  const nameByCanon = new Map<string, string[]>()
  for (const [key, aggregate] of aggregates.entries()) {
    const canon = aggregate.canon
    if (!canon) continue
    if (key.startsWith('ERP:')) {
      const list = erpByCanon.get(canon) ?? []
      list.push(key)
      erpByCanon.set(canon, list)
    } else {
      const list = nameByCanon.get(canon) ?? []
      list.push(key)
      nameByCanon.set(canon, list)
    }
  }

  for (const [canon, nameKeys] of nameByCanon.entries()) {
    const erpKeys = erpByCanon.get(canon) ?? []
    if (!erpKeys.length) continue
    if (hasTimes) {
      for (const nameKey of [...nameKeys]) {
        const aggregate = aggregates.get(nameKey)
        if (!aggregate) continue
        let chosen: string | null = null
        if (erpKeys.length === 1) {
          chosen = erpKeys[0]
        } else {
          const nameIntervals = aggregate.intervals
          for (const erpKey of erpKeys) {
            const erpAgg = aggregates.get(erpKey)
            if (!erpAgg) continue
            if (intervalsOverlapOrClose(nameIntervals, erpAgg.intervals, 7)) {
              chosen = erpKey
              break
            }
          }
        }
        if (!chosen) {
          ambiguous.add(nameKey)
          continue
        }
        const target = aggregates.get(chosen)
        if (!target) continue
        for (const interval of aggregate.intervals) target.intervals.push(interval)
        for (const interval of aggregate.goodIntervals) target.goodIntervals.push(interval)
        for (const interval of aggregate.badIntervals) target.badIntervals.push(interval)
        for (const dur of aggregate.durationsGood) target.durationsGood.push(dur)
        for (const dur of aggregate.durationsBad) target.durationsBad.push(dur)
        for (const rec of aggregate.sessionRecords) target.sessionRecords.push(rec)
        aggregate.rawNames.forEach((n) => target.rawNames.add(n))
        target.matchSource = 'alias_merge'
        aggregates.delete(nameKey)
        aliasMerges.push({ source: nameKey, target: chosen })
      }
    } else {
      if (erpKeys.length === 1) {
        const targetKey = erpKeys[0]
        const target = aggregates.get(targetKey)
        if (!target) continue
        for (const nameKey of [...nameKeys]) {
          const aggregate = aggregates.get(nameKey)
          if (!aggregate) continue
          for (const dur of aggregate.durationsGood) target.durationsGood.push(dur)
          for (const dur of aggregate.durationsBad) target.durationsBad.push(dur)
          aggregate.rawNames.forEach((n) => target.rawNames.add(n))
          target.matchSource = 'alias_merge'
          aggregates.delete(nameKey)
          aliasMerges.push({ source: nameKey, target: targetKey })
        }
      } else {
        for (const nameKey of nameKeys) {
          ambiguous.add(nameKey)
        }
      }
    }
  }

  for (const [key, aggregate] of aggregates.entries()) {
    if (hasTimes) {
      const segments = prepareSegments(aggregate.sessionRecords)
      const events = computeReconnectEvents(segments)
      if (events.length) {
        reconnectMap.set(key, events)
      }
    }
  }

  return {
    aggregates,
    hasTimes,
    totalMinutes,
    totalSource,
    adjustedTotal,
    effectiveThreshold,
    thresholdRaw,
    rosterDf: rosterRows,
    aliasMerges,
    ambiguous,
    reconnectMap,
  }
}

function summarise(
  aggregates: Map<string, KeyAggregates>,
  reconnectMap: Map<string, ReconnectEvent[]>,
  params: Required<ProcessParams>,
  effectiveThreshold: number,
  thresholdRaw: number,
  adjustedTotal: number,
  totalMinutes: number,
  rosterRows: RosterRow[],
  aliasMerges: Array<{ source: string; target: string }>,
  ambiguousNameKeys: Set<string>,
  exemptions: ExemptionsMap,
  hasTimes: boolean,
): ProcessedKeySummary {
  const attendanceRows: Record<string, any>[] = []
  const issuesRows: Record<string, any>[] = []
  const absentRows: Record<string, any>[] = []
  const penaltiesRows: Record<string, any>[] = []
  const matchesRows: Record<string, any>[] = []
  const reconnectRows: Record<string, any>[] = []
  const presentErps = new Set<string>()

  const attHeader = 'Attendance Status'

  for (const [key, aggregate] of aggregates.entries()) {
    const erp = aggregate.erp
    const name = aggregate.cleanName
    const zoomNamesRaw = Array.from(aggregate.rawNames).join('; ')
    const totalGood = hasTimes ? intervalUnionMinutes(aggregate.goodIntervals) : aggregate.durationsGood.reduce((a, b) => a + b, 0)
    const totalBad = hasTimes ? intervalUnionMinutes(aggregate.badIntervals) : aggregate.durationsBad.reduce((a, b) => a + b, 0)
    const unionMinutesRaw = Math.min(totalGood + totalBad, adjustedTotal)
    const segCount = hasTimes
      ? aggregate.goodIntervals.length + aggregate.badIntervals.length
      : aggregate.durationsGood.length + aggregate.durationsBad.length
    const isDual = unionMinutesRaw > adjustedTotal + 0.1
    const isReconnect = segCount > 1 && !isDual
    const reconnectCount = isReconnect ? Math.max(0, segCount - 1) : 0
    const badOnlyMinutes = totalBad

    const events = reconnectMap.get(key) ?? []
    for (const ev of events) {
      const gapMinutes = Math.round(ev.gap.as('minutes') * 100) / 100
      const gapSeconds = Math.max(0, Math.round(ev.gap.as('seconds')))
      reconnectRows.push({
        Key: key,
        ERP: erp,
        Name: name,
        'Zoom Names (raw)': zoomNamesRaw,
        'Event # (per student)': ev.index,
        'Disconnect Time': tsToExcelString(ev.disconnectTs),
        'Reconnect Time': tsToExcelString(ev.reconnectTs),
        'Gap (minutes)': gapMinutes,
        'Gap (seconds)': gapSeconds,
        'Gap Duration (hh:mm:ss)': durationToHMS(ev.gap),
        'Disconnect Raw Name': cleanRaw(ev.disconnectSeg?.rawName ?? ''),
        'Reconnect Raw Name': cleanRaw(ev.reconnectSeg?.rawName ?? ''),
        'Disconnect Join (raw)': cleanRaw(ev.disconnectSeg?.joinRaw ?? ''),
        'Disconnect Leave (raw)': cleanRaw(ev.disconnectSeg?.leaveRaw ?? ''),
        'Reconnect Join (raw)': cleanRaw(ev.reconnectSeg?.joinRaw ?? ''),
        'Reconnect Leave (raw)': cleanRaw(ev.reconnectSeg?.leaveRaw ?? ''),
      })
    }

    let unionDecision = unionMinutesRaw
    let thrDecision = effectiveThreshold
    if (params.rounding_mode === 'ceil_attendance') {
      unionDecision = Math.ceil(unionDecision)
    } else if (params.rounding_mode === 'ceil_both') {
      unionDecision = Math.ceil(unionDecision)
      thrDecision = Math.ceil(thrDecision)
    }

    const meets = unionDecision >= thrDecision
    const isAmb = ambiguousNameKeys.has(key)
    const attendanceStatus = isAmb ? 'Needs Review' : meets ? 'Present' : 'Absent'

    const penaltyTolerance = params.penalty_tolerance_minutes
    const badPct = unionMinutesRaw > 0 ? (badOnlyMinutes / unionMinutesRaw) * 100 : 0
    let penaltyApplied = badOnlyMinutes > penaltyTolerance ? -1 : 0

    const ex = exemptions[key] || {}
    if (ex.naming) penaltyApplied = 0
    const exOverlap = Boolean(ex.overlap)
    const exReconnect = Boolean(ex.reconnect)

    const issues: string[] = []
    if (isDual && !exOverlap) {
      issues.push('Duplicate account — overlapping (two devices)')
    }
    if (isReconnect && !exReconnect) {
      if (reconnectCount > 0) {
        issues.push(`Duplicate account — reconnects (non-overlapping x${reconnectCount})`)
      } else {
        issues.push('Duplicate account — reconnects (non-overlapping)')
      }
    }
    if (isAmb) {
      issues.push('Ambiguous duplicate name (no ERP / alias ambiguous)')
    }
    for (const merge of aliasMerges.filter((m) => m.target === key)) {
      issues.push(`Merged alias ${merge.source} into ${merge.target}`)
    }

    attendanceRows.push({
      Key: key,
      'Zoom Names (raw)': zoomNamesRaw,
      'Attended Minutes (RAW)': round2(unionMinutesRaw),
      'Threshold Minutes (RAW)': round2(effectiveThreshold),
      'Attended Minutes (DECISION)': round2(unionDecision),
      'Threshold Minutes (DECISION)': round2(thrDecision),
      [attHeader]: attendanceStatus,
      'Naming Penalty': penaltyApplied === -1 ? -1 : 0,
      Issues: issues.join('; '),
    })

    issuesRows.push({
      Key: key,
      ERP: erp,
      Name: name,
      'Zoom Names (raw)': zoomNamesRaw,
      'Match Source': aggregate.matchSource,
      'Issue Detail': issues.join('; '),
      'Intervals/Segments': segCount,
      'Dual Devices?': isDual ? 'Yes' : 'No',
      'Reconnects?': isReconnect ? 'Yes' : 'No',
      'Reconnect Count': reconnectCount,
      'Ambiguous Name?': isAmb ? 'Yes' : 'No',
      'Total Minutes Counted (Union RAW)': round2(unionMinutesRaw),
      'Override Attendance': '',
    })

    if (!meets || isAmb) {
      const shortfall = Math.max(0, thrDecision - unionDecision)
      absentRows.push({
        Key: key,
        ERP: erp,
        Name: name,
        'Zoom Names (raw)': zoomNamesRaw,
        'Attended Minutes (DECISION)': round2(unionDecision),
        'Threshold Minutes (DECISION)': round2(thrDecision),
        'Shortfall Minutes (DECISION)': round2(shortfall),
        'Dual Devices?': isDual ? 'Yes' : 'No',
        'Reconnects?': isReconnect ? 'Yes' : 'No',
        'Reconnect Count': reconnectCount,
        'Is Ambiguous?': isAmb ? 'Yes' : 'No',
        Reason: isAmb ? 'Needs Review (ambiguous)' : '',
        'Override (from Issues)': '',
        'Final Status': '',
      })
    }

    penaltiesRows.push({
      Key: key,
      'Zoom Names (raw)': zoomNamesRaw,
      'Bad-Name Minutes': round2(badOnlyMinutes),
      'Bad-Name %': round2(badPct),
      'Penalty Tolerance (min)': penaltyTolerance,
      'Penalty Applied': penaltyApplied,
    })

    matchesRows.push({
      Key: key,
      ERP: erp,
      Name: name,
      'Zoom Names (raw)': zoomNamesRaw,
      'Match Source': aggregate.matchSource,
    })

    if (erp) presentErps.add(erp)
  }

  if (rosterRows.length) {
    const zoomCanonNames = new Set<string>()
    for (const aggregate of aggregates.values()) {
      aggregate.rawNames.forEach((n) => zoomCanonNames.add(canonName(n)))
    }
    for (const row of rosterRows) {
      const erpKey = `ERP:${row.ERP}`
      if (presentErps.has(row.ERP)) continue
      if (aggregates.has(erpKey)) continue
      if (zoomCanonNames.has(row.RosterCanon)) continue
      const thrDecision = params.rounding_mode === 'ceil_both' ? Math.ceil(effectiveThreshold) : effectiveThreshold
      attendanceRows.push({
        Key: erpKey,
        'Zoom Names (raw)': `${row.RosterName} (roster)`,
        'Attended Minutes (RAW)': 0,
        'Threshold Minutes (RAW)': round2(effectiveThreshold),
        'Attended Minutes (DECISION)': 0,
        'Threshold Minutes (DECISION)': round2(thrDecision),
        [attHeader]: 'Absent',
        'Naming Penalty': 0,
        Issues: 'Not in Zoom log (Roster)',
      })
      issuesRows.push({
        Key: erpKey,
        ERP: row.ERP,
        Name: row.RosterName,
        'Zoom Names (raw)': row.RosterName,
        'Match Source': 'roster-only',
        'Issue Detail': 'Not in Zoom log (Roster)',
        'Intervals/Segments': 0,
        'Dual Devices?': 'No',
        'Reconnects?': 'No',
        'Reconnect Count': 0,
        'Ambiguous Name?': 'No',
        'Total Minutes Counted (Union RAW)': 0,
        'Override Attendance': '',
      })
      absentRows.push({
        Key: erpKey,
        ERP: row.ERP,
        Name: row.RosterName,
        'Zoom Names (raw)': row.RosterName,
        'Attended Minutes (DECISION)': 0,
        'Threshold Minutes (DECISION)': round2(thrDecision),
        'Shortfall Minutes (DECISION)': round2(thrDecision),
        'Dual Devices?': 'No',
        'Reconnects?': 'No',
        'Reconnect Count': 0,
        'Is Ambiguous?': 'No',
        Reason: 'Absent from Zoom log (roster)',
        'Override (from Issues)': '',
        'Final Status': 'Absent',
      })
    }
  }

  return {
    attendanceRows,
    issuesRows,
    absentRows,
    penaltiesRows,
    matchesRows,
    reconnectRows,
    presentErps,
    ambiguousNameKeys,
    aliasMerges,
  }
}

function round2(value: number): number {
  return Math.round(value * 100) / 100
}

function createWorksheetFromRows(workbook: Workbook, name: string, rows: Record<string, any>[], columns?: string[]): Worksheet {
  const worksheet = workbook.addWorksheet(name)
  const cols = columns ?? (rows.length ? Object.keys(rows[0]) : [])
  if (cols.length) {
    worksheet.columns = cols.map((key) => ({ header: key, key }))
    for (const row of rows) {
      worksheet.addRow(cols.map((col) => row[col] ?? ''))
    }
  }
  return worksheet
}

function headerNames(sheet: Worksheet): string[] {
  const headers: string[] = []
  const firstRow = sheet.getRow(1)
  firstRow.eachCell({ includeEmpty: true }, (cell, colNumber) => {
    const value = cell.value
    headers[colNumber] = value === undefined || value === null ? '' : String((value as any).result ?? value)
  })
  return headers
}

async function buildWorkbook(
  rawRows: ZoomRow[],
  summary: ProcessedKeySummary,
  rosterRows: RosterRow[],
  params: Required<ProcessParams>,
  totalSource: string,
  totalMinutes: number,
  breakMinutes: number,
  adjustedTotal: number,
  thresholdRaw: number,
  effectiveThreshold: number,
): Promise<Buffer> {
  const workbook = new Workbook()
  workbook.creator = 'Zoom Attendance'
  const rawSheet = workbook.addWorksheet('Raw Zoom CSV')
  if (rawRows.length) {
    const headers = Object.keys(rawRows[0])
    rawSheet.columns = headers.map((h) => ({ header: h, key: h }))
    for (const row of rawRows) {
      rawSheet.addRow(headers.map((h) => row[h] ?? ''))
    }
  }

  const attendanceSheet = createWorksheetFromRows(workbook, 'Attendance', summary.attendanceRows)
  const issuesSheet = createWorksheetFromRows(workbook, 'Issues', summary.issuesRows)
  const reconnectSheet = createWorksheetFromRows(workbook, 'Reconnects', summary.reconnectRows)
  const absentSheet = createWorksheetFromRows(workbook, 'Absent', summary.absentRows)
  const penaltiesSheet = createWorksheetFromRows(workbook, 'Penalties', summary.penaltiesRows)
  const matchesSheet = createWorksheetFromRows(workbook, 'Matches', summary.matchesRows)
  const erpSheet = workbook.addWorksheet('ERPs')
  const erps = rosterRows.length
    ? Array.from(new Set(rosterRows.map((r) => r.ERP))).sort()
    : Array.from(summary.presentErps).sort()
  erpSheet.columns = [{ header: 'ERP', key: 'ERP' }]
  for (const erp of erps) {
    erpSheet.addRow([erp])
  }

  const metaSheet = workbook.addWorksheet('Meta')
  metaSheet.columns = [
    { header: 'Metric', key: 'Metric' },
    { header: 'Value', key: 'Value' },
  ]
  const metaRows = [
    ['Total class minutes (source)', totalSource],
    ['Total class minutes (before break)', round2(totalMinutes)],
    ['Break minutes deducted', round2(breakMinutes)],
    ['Adjusted total class minutes', round2(adjustedTotal)],
    ['Attendance threshold ratio', params.threshold_ratio],
    ['Raw threshold minutes (ratio * adjusted total)', round2(thresholdRaw)],
    ['Leniency buffer minutes', round2(params.buffer_minutes)],
    ['EFFECTIVE threshold minutes (raw - buffer)', round2(effectiveThreshold)],
    ['Decision rule', 'Present if DECISION Attended >= DECISION Threshold'],
    [
      'Rounding mode',
      {
        none: 'None',
        ceil_attendance: 'Ceil attendance only',
        ceil_both: 'Ceil attendance & threshold',
      }[params.rounding_mode],
    ],
    ['Naming penalty tolerance (minutes)', params.penalty_tolerance_minutes],
    ['Roster provided', rosterRows.length ? 'Yes' : 'No'],
    [
      'Excluded names patterns',
      EXCLUDE_NAME_PATTERNS.map((re) => re.source.replace(/^\^\s*|\s*\$/g, '')).join('; '),
    ],
  ]
  for (const row of metaRows) metaSheet.addRow(row)

  const summarySheet = workbook.addWorksheet('Summary')
  summarySheet.columns = [
    { header: 'Metric', key: 'Metric' },
    { header: 'Value', key: 'Value' },
  ]
  summarySheet.addRow(['(Formulas inserted by app)', ''])

  if (issuesSheet.columnCount && summary.issuesRows.length) {
    const headers = headerNames(issuesSheet)
    const overrideIndex = headers.indexOf('Override Attendance')
    if (overrideIndex > 0) {
      for (let r = 2; r <= issuesSheet.rowCount; r++) {
        const cell = issuesSheet.getRow(r).getCell(overrideIndex)
        cell.dataValidation = {
          type: 'list',
          allowBlank: true,
          formulae: ['"Present,Absent"'],
        }
      }
    }
  }

  if (absentSheet.columnCount && summary.absentRows.length) {
    const absentHeaders = headerNames(absentSheet)
    const issuesHeaders = headerNames(issuesSheet)
    const keyCol = absentHeaders.indexOf('Key')
    const isAmbCol = absentHeaders.indexOf('Is Ambiguous?')
    const overrideFromIssuesCol = absentHeaders.indexOf('Override (from Issues)')
    const finalStatusCol = absentHeaders.indexOf('Final Status')
    const issuesKeyCol = issuesHeaders.indexOf('Key')
    const issuesOverrideCol = issuesHeaders.indexOf('Override Attendance')
    if (keyCol > 0 && issuesKeyCol > 0 && issuesOverrideCol > 0 && overrideFromIssuesCol > 0 && finalStatusCol > 0) {
      const keyLetter = columnLetter(keyCol)
      const ovLetter = columnLetter(overrideFromIssuesCol)
      const finalLetter = columnLetter(finalStatusCol)
      const issuesKeyLetter = columnLetter(issuesKeyCol)
      const issuesOvLetter = columnLetter(issuesOverrideCol)
      const isAmbLetter = isAmbCol > 0 ? columnLetter(isAmbCol) : ''
      for (let r = 2; r <= absentSheet.rowCount; r++) {
        const keyCell = `${keyLetter}${r}`
        const ovCell = absentSheet.getRow(r).getCell(overrideFromIssuesCol)
        ovCell.value = {
          formula: `IFERROR(XLOOKUP(${keyCell},Issues!${issuesKeyLetter}:${issuesKeyLetter},Issues!${issuesOvLetter}:${issuesOvLetter},""),"")`,
        }
        const finalCell = absentSheet.getRow(r).getCell(finalStatusCol)
        if (isAmbLetter) {
          finalCell.value = {
            formula: `IF(${ovLetter}${r}<>"",${ovLetter}${r},IF(${isAmbLetter}${r}="Yes","Needs Review","Absent"))`,
          }
        } else {
          finalCell.value = {
            formula: `IF(${ovLetter}${r}<>"",${ovLetter}${r},"Absent")`,
          }
        }
      }
      absentSheet.addConditionalFormatting({
        ref: `${finalLetter}2:${finalLetter}${absentSheet.rowCount}`,
        rules: [
          {
            type: 'expression',
            formulae: [`${finalLetter}2="Needs Review"`],
            priority: 1,
            style: { fill: { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF59D' } } },
          },
        ],
      })
      summarySheet.addRow(['Total Absent (final)', { formula: `COUNTIF(Absent!${finalLetter}:${finalLetter},"Absent")` }])
      summarySheet.addRow(['Total Needs Review', { formula: `COUNTIF(Absent!${finalLetter}:${finalLetter},"Needs Review")` }])
    }
  } else {
    summarySheet.addRow(['Total Absent (final)', 0])
    summarySheet.addRow(['Total Needs Review', 0])
  }

  if (penaltiesSheet.columnCount) {
    const headers = headerNames(penaltiesSheet)
    const penCol = headers.indexOf('Penalty Applied')
    if (penCol > 0) {
      const letter = columnLetter(penCol)
      summarySheet.addRow([
        'Total Naming Penalties (-1)',
        { formula: `COUNTIF(Penalties!${letter}:${letter},-1)` },
      ])
    }
  }

  if (issuesSheet.columnCount) {
    const headers = headerNames(issuesSheet)
    const dualCol = headers.indexOf('Dual Devices?')
    const recCol = headers.indexOf('Reconnects?')
    const ambCol = headers.indexOf('Ambiguous Name?')
    if (dualCol > 0) {
      const letter = columnLetter(dualCol)
      summarySheet.addRow(['Total Dual-Device Flags', { formula: `COUNTIF(Issues!${letter}:${letter},"Yes")` }])
    }
    if (recCol > 0) {
      const letter = columnLetter(recCol)
      summarySheet.addRow(['Total Reconnect Flags', { formula: `COUNTIF(Issues!${letter}:${letter},"Yes")` }])
    }
    if (ambCol > 0) {
      const letter = columnLetter(ambCol)
      summarySheet.addRow(['Total Ambiguous Names', { formula: `COUNTIF(Issues!${letter}:${letter},"Yes")` }])
    }
    const recCountCol = headers.indexOf('Reconnect Count')
    if (recCountCol > 0) {
      const letter = columnLetter(recCountCol)
      summarySheet.addRow(['Total Reconnect Events', { formula: `SUM(Issues!${letter}:${letter})` }])
    }
  }

  const buffer = await workbook.xlsx.writeBuffer()
  return Buffer.from(buffer)
}

function columnLetter(index: number): string {
  let result = ''
  let n = index
  while (n > 0) {
    const rem = (n - 1) % 26
    result = String.fromCharCode(65 + rem) + result
    n = Math.floor((n - 1) / 26)
  }
  return result
}

export async function processRequest(
  zoomPath: string,
  rosterPath: string | null,
  params: ProcessParams,
  exemptions: ExemptionsMap,
): Promise<ProcessPayload> {
  const rawBuffer = await fs.readFile(zoomPath)
  const rawRows = normaliseZoom(rawBuffer)
  const rosterRows = await loadRoster(rosterPath)

  const processedParams: Required<ProcessParams> = {
    threshold_ratio: params.threshold_ratio ?? 0.8,
    buffer_minutes: params.buffer_minutes ?? 0,
    break_minutes: params.break_minutes ?? 0,
    override_total_minutes: params.override_total_minutes ?? null,
    penalty_tolerance_minutes: params.penalty_tolerance_minutes ?? 0,
    rounding_mode: params.rounding_mode ?? 'none',
  }

  const analysis = analyseZoom(rawRows, rosterRows, processedParams, exemptions)
  const summary = summarise(
    analysis.aggregates,
    analysis.reconnectMap,
    processedParams,
    analysis.effectiveThreshold,
    analysis.thresholdRaw,
    analysis.adjustedTotal,
    analysis.totalMinutes,
    rosterRows,
    analysis.aliasMerges,
    analysis.ambiguous,
    exemptions,
    analysis.hasTimes,
  )

  const workbookBuffer = await buildWorkbook(
    rawRows,
    summary,
    rosterRows,
    processedParams,
    analysis.totalSource,
    analysis.totalMinutes,
    processedParams.break_minutes,
    analysis.adjustedTotal,
    analysis.thresholdRaw,
    analysis.effectiveThreshold,
  )

  return {
    buffer: workbookBuffer,
    meta: {
      output_xlsx: APP_FILE_DEFAULT,
      total_class_minutes: round2(analysis.totalMinutes),
      adjusted_total_minutes: round2(analysis.adjustedTotal),
      threshold_minutes_raw: round2(analysis.thresholdRaw),
      effective_threshold_minutes: round2(analysis.effectiveThreshold),
      buffer_minutes: round2(processedParams.buffer_minutes),
      rows: summary.attendanceRows.length,
      rounding_mode: processedParams.rounding_mode,
      roster_used: rosterRows.length > 0,
      total_class_minutes_source: analysis.totalSource,
    },
  }
}

export async function extractKeysFromCsv(zoomPath: string): Promise<ExtractedKey[]> {
  const buffer = await fs.readFile(zoomPath)
  const rows = normaliseZoom(buffer)
  const cols = detectColumns(rows)
  const nameCol = cols.nameCol
  const seen = new Set<string>()
  const items: ExtractedKey[] = []
  for (const row of rows) {
    const name = String(row[nameCol] ?? '')
    if (shouldExclude(name)) continue
    const [erp, clean] = parseName(name)
    const key = erp ? `ERP:${erp}` : `NAME:${normNameSpacesOnly(clean)}`
    if (seen.has(key)) continue
    seen.add(key)
    const displayErp = erp ? `${erp} · ` : ''
    const display = `${displayErp}${clean}`.trim()
    items.push({ key, erp, name: clean, display })
  }
  return items
}

export function bufferToBase64(buffer: Buffer): string {
  return buffer.toString('base64')
}

export { APP_FILE_DEFAULT }
