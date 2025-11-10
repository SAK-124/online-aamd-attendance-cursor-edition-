import React, { useEffect, useMemo, useState } from 'react'
import ExemptionsPanel from '../components/ExemptionsPanel.jsx'

const PREF_KEY = 'za_prefs_v1'

function formatFileInfo(file) {
  if (!file) return null
  const size = file.size
  let label
  if (size < 1024) label = `${size} B`
  else if (size < 1024 * 1024) label = `${(size / 1024).toFixed(1)} KB`
  else label = `${(size / (1024 * 1024)).toFixed(2)} MB`
  return `${file.name} · ${label}`
}

export default function ProcessPage() {
  const [zoomCsv, setZoomCsv] = useState(null)
  const [rosterFile, setRosterFile] = useState(null)
  const [threshold, setThreshold] = useState('0.8')
  const [bufferMinutes, setBufferMinutes] = useState('0')
  const [breakMinutes, setBreakMinutes] = useState('0')
  const [overrideMinutes, setOverrideMinutes] = useState('')
  const [namingPenalty, setNamingPenalty] = useState('2')
  const [roundingMode, setRoundingMode] = useState('none')
  const [exemptions, setExemptions] = useState({})
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [success, setSuccess] = useState('')
  const [meta, setMeta] = useState(null)
  const [exemptionOptions, setExemptionOptions] = useState([])
  const [exemptionLoading, setExemptionLoading] = useState(false)
  const [exemptionError, setExemptionError] = useState('')

  useEffect(() => {
    try {
      const s = localStorage.getItem(PREF_KEY)
      if (s) {
        const j = JSON.parse(s)
        if (j.threshold !== undefined) setThreshold(String(j.threshold))
        if (j.bufferMinutes !== undefined) setBufferMinutes(String(j.bufferMinutes))
        if (j.breakMinutes !== undefined) setBreakMinutes(String(j.breakMinutes))
        if (j.overrideMinutes !== undefined) setOverrideMinutes(j.overrideMinutes === null ? '' : String(j.overrideMinutes))
        if (j.namingPenalty !== undefined) setNamingPenalty(String(j.namingPenalty))
        if (j.roundingMode !== undefined) setRoundingMode(String(j.roundingMode))
        if (j.exemptions) setExemptions(j.exemptions)
      }
    } catch {}
  }, [])

  useEffect(() => {
    try {
      const payload = {
        threshold,
        bufferMinutes,
        breakMinutes,
        overrideMinutes: overrideMinutes === '' ? null : overrideMinutes,
        namingPenalty,
        roundingMode,
        exemptions,
      }
      localStorage.setItem(PREF_KEY, JSON.stringify(payload))
    } catch {}
  }, [threshold, bufferMinutes, breakMinutes, overrideMinutes, namingPenalty, roundingMode, exemptions])

  useEffect(() => {
    let cancelled = false
    if (!zoomCsv) {
      setExemptionOptions([])
      setExemptionLoading(false)
      setExemptionError('')
      return () => {}
    }
    setExemptionLoading(true)
    setExemptionError('')
    const fd = new FormData()
    fd.append('zoom_csv', zoomCsv)
    fetch('/api/keys', { method: 'POST', body: fd })
      .then(async (res) => {
        if (!res.ok) {
          const text = await res.text().catch(() => '')
          throw new Error(text || `Request failed (${res.status})`)
        }
        return res.json()
      })
      .then((items) => {
        if (cancelled) return
        setExemptionOptions(items)
        const valid = new Set(items.map((i) => i.key))
        setExemptions((prev) => {
          const next = {}
          for (const [key, flags] of Object.entries(prev || {})) {
            if (valid.has(key) && (flags.naming || flags.overlap || flags.reconnect)) {
              next[key] = flags
            }
          }
          return next
        })
      })
      .catch((err) => {
        if (cancelled) return
        setExemptionError(err.message || 'Unable to read CSV. Ensure it is the Zoom participant export.')
      })
      .finally(() => {
        if (!cancelled) setExemptionLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [zoomCsv])

  const effectiveExemptions = useMemo(() => {
    const out = {}
    for (const [key, flags] of Object.entries(exemptions || {})) {
      if (flags.naming || flags.overlap || flags.reconnect) out[key] = flags
    }
    return out
  }, [exemptions])

  async function onProcess() {
    setError('')
    setSuccess('')
    setMeta(null)
    if (!zoomCsv) {
      setError('Please choose a Zoom participants CSV file.')
      return
    }
    const params = {
      threshold_ratio: parseFloat(threshold || '0'),
      buffer_minutes: parseFloat(bufferMinutes || '0'),
      break_minutes: parseFloat(breakMinutes || '0'),
      override_total_minutes: overrideMinutes ? parseFloat(overrideMinutes) : null,
      penalty_tolerance_minutes: parseFloat(namingPenalty || '0'),
      rounding_mode: roundingMode,
    }

    const fd = new FormData()
    fd.append('zoom_csv', zoomCsv)
    if (rosterFile) fd.append('roster', rosterFile)
    fd.append('params', JSON.stringify(params))
    fd.append('exemptions', JSON.stringify(effectiveExemptions))

    setBusy(true)
    try {
      const res = await fetch('/api/process', { method: 'POST', body: fd })
      if (!res.ok) {
        const t = await res.text().catch(() => '')
        throw new Error(t || `Server returned ${res.status}`)
      }
      const metaHeader = res.headers.get('x-zoom-attendance-meta')
      if (metaHeader) {
        try {
          setMeta(JSON.parse(metaHeader))
        } catch {
          setMeta(null)
        }
      }
      const blob = await res.blob()
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = 'zoom_attendance_processed.xlsx'
      document.body.appendChild(a)
      a.click()
      a.remove()
      window.URL.revokeObjectURL(url)
      setSuccess('Workbook generated successfully.')
    } catch (e) {
      setError(e.message || 'Processing failed.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="page">
      <header className="page-header">
        <h2>Process attendance</h2>
        <p className="lead">Upload the Zoom participants CSV, optional roster, and configure the same parameters from the desktop tool.</p>
      </header>

      <div className="grid-two stack-lg">
        <div className="card">
          <h3>Files</h3>
          <label className="field">
            <span>Zoom participants CSV <span className="required">*</span></span>
            <input type="file" accept=".csv" onChange={(e) => setZoomCsv(e.target.files?.[0] || null)} />
            {formatFileInfo(zoomCsv) && <small className="muted">{formatFileInfo(zoomCsv)}</small>}
          </label>
          <label className="field">
            <span>Roster (Excel/CSV, optional)</span>
            <input type="file" accept=".xlsx,.xls,.csv" onChange={(e) => setRosterFile(e.target.files?.[0] || null)} />
            {formatFileInfo(rosterFile) && <small className="muted">{formatFileInfo(rosterFile)}</small>}
          </label>
          <p className="muted tiny">Roster entries are used only to flag absentees when no ERP or canonical match is present in Zoom data.</p>
        </div>

        <div className="card meta-card">
          <h3>Last run summary</h3>
          {meta ? (
            <ul className="meta-list">
              <li><span>Total minutes</span><strong>{meta.total_class_minutes}</strong></li>
              <li><span>Adjusted minutes</span><strong>{meta.adjusted_total_minutes}</strong></li>
              <li><span>Effective threshold</span><strong>{meta.effective_threshold_minutes}</strong></li>
              <li><span>Rows in attendance</span><strong>{meta.rows}</strong></li>
              <li><span>Roster used</span><strong>{meta.roster_used ? 'Yes' : 'No'}</strong></li>
              <li><span>Writer engine</span><strong>{meta.engine}</strong></li>
            </ul>
          ) : (
            <p className="muted">Process a CSV to see runtime metrics, just like the terminal output from the Python script.</p>
          )}
        </div>
      </div>

      <div className="card">
        <h3>Parameters</h3>
        <div className="grid">
          <label>Threshold (0–1)
            <input value={threshold} onChange={(e) => setThreshold(e.target.value)} />
          </label>
          <label>Buffer minutes
            <input value={bufferMinutes} onChange={(e) => setBufferMinutes(e.target.value)} />
          </label>
          <label>Break minutes
            <input value={breakMinutes} onChange={(e) => setBreakMinutes(e.target.value)} />
          </label>
          <label>Override minutes
            <input value={overrideMinutes} placeholder="Leave blank to auto-detect" onChange={(e) => setOverrideMinutes(e.target.value)} />
          </label>
          <label>Naming penalty tolerance (minutes)
            <input value={namingPenalty} onChange={(e) => setNamingPenalty(e.target.value)} />
          </label>
          <label>Rounding mode
            <select value={roundingMode} onChange={(e) => setRoundingMode(e.target.value)}>
              <option value="none">None</option>
              <option value="ceil_attendance">Ceil attendance only</option>
              <option value="ceil_both">Ceil attendance &amp; threshold</option>
            </select>
          </label>
        </div>
      </div>

      <ExemptionsPanel
        options={exemptionOptions}
        loading={exemptionLoading}
        error={exemptionError}
        value={exemptions}
        onChange={setExemptions}
      />

      <div className="action-bar">
        <button className="button button-primary" onClick={onProcess} disabled={busy}>
          {busy ? 'Processing…' : 'Process & download workbook'}
        </button>
        <span className="muted tiny">The server runs the full Python engine and streams the Excel file to your browser.</span>
      </div>

      {error && <div className="toast error">{error}</div>}
      {success && <div className="toast success">{success}</div>}
    </section>
  )
}
