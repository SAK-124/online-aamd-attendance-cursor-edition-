import React, { useEffect, useState } from 'react'
import ExemptionsPanel from '../components/ExemptionsPanel.jsx'

const PREF_KEY = 'za_prefs_v1'

export default function ProcessPage() {
  const [zoomCsv, setZoomCsv] = useState(null)
  const [rosterFile, setRosterFile] = useState(null)
  const [threshold, setThreshold] = useState('0.8')
  const [bufferMinutes, setBufferMinutes] = useState('0')
  const [breakMinutes, setBreakMinutes] = useState('0')
  const [overrideMinutes, setOverrideMinutes] = useState('')
  const [namingPenalty, setNamingPenalty] = useState('0')
  const [roundingMode, setRoundingMode] = useState('none')
  const [exemptions, setExemptions] = useState({})
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    try {
      const s = localStorage.getItem(PREF_KEY)
      if (s) {
        const j = JSON.parse(s)
        if (j.threshold) setThreshold(String(j.threshold))
        if (j.bufferMinutes) setBufferMinutes(String(j.bufferMinutes))
        if (j.breakMinutes) setBreakMinutes(String(j.breakMinutes))
        if (j.overrideMinutes !== undefined) setOverrideMinutes(j.overrideMinutes === null ? '' : String(j.overrideMinutes))
        if (j.namingPenalty) setNamingPenalty(String(j.namingPenalty))
        if (j.roundingMode) setRoundingMode(String(j.roundingMode))
        if (j.exemptions) setExemptions(j.exemptions)
      }
    } catch {}
  }, [])

  useEffect(() => {
    try {
      const j = {
        threshold,
        bufferMinutes,
        breakMinutes,
        overrideMinutes: overrideMinutes === '' ? null : overrideMinutes,
        namingPenalty,
        roundingMode,
        exemptions
      }
      localStorage.setItem(PREF_KEY, JSON.stringify(j))
    } catch {}
  }, [threshold, bufferMinutes, breakMinutes, overrideMinutes, namingPenalty, roundingMode, exemptions])

  async function onProcess() {
    setError('')
    if (!zoomCsv) { setError('Please choose Zoom CSV'); return }
    const params = {
      threshold_ratio: parseFloat(threshold || '0'),
      buffer_minutes: parseFloat(bufferMinutes || '0'),
      break_minutes: parseFloat(breakMinutes || '0'),
      override_total_minutes: overrideMinutes ? parseFloat(overrideMinutes) : null,
      penalty_tolerance_minutes: parseFloat(namingPenalty || '0'),
      rounding_mode: roundingMode
    }

    const fd = new FormData()
    fd.append('zoom_csv', zoomCsv)
    if (rosterFile) fd.append('roster', rosterFile)
    fd.append('params', JSON.stringify(params))
    fd.append('exemptions', JSON.stringify(exemptions))

    setBusy(true)
    try {
      const res = await fetch('/api/process', { method: 'POST', body: fd })
      if (!res.ok) {
        const t = await res.text().catch(() => '')
        throw new Error(`Server returned ${res.status}${t ? `: ${t}` : ''}`)
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
    } catch (e) {
      setError(e.message || 'Failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <section>
      <h2>Process Attendance</h2>
      <div className="card" style={{ marginBottom: 16 }}>
        <label>Zoom Participants CSV
          <input type="file" accept=".csv" onChange={(e) => setZoomCsv(e.target.files?.[0] || null)} />
        </label>
        <label>Roster (Excel/CSV, optional)
          <input type="file" accept=".xlsx,.xls,.csv" onChange={(e) => setRosterFile(e.target.files?.[0] || null)} />
        </label>
      </div>

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
        <label>Override minutes (optional)
          <input value={overrideMinutes} onChange={(e) => setOverrideMinutes(e.target.value)} />
        </label>
        <label>Naming penalty tolerance (min)
          <input value={namingPenalty} onChange={(e) => setNamingPenalty(e.target.value)} />
        </label>
        <label>Rounding mode
          <select value={roundingMode} onChange={(e) => setRoundingMode(e.target.value)}>
            <option value="none">None</option>
            <option value="ceil_attendance">Ceil attendance only</option>
            <option value="ceil_both">Ceil attendance & threshold</option>
          </select>
        </label>
      </div>

      <div style={{ marginTop: 24 }}>
        <ExemptionsPanel zoomCsvFile={zoomCsv} value={exemptions} onChange={setExemptions} />
      </div>

      <div style={{ marginTop: 16 }}>
        <button onClick={onProcess} disabled={busy}>{busy ? 'Processing…' : 'Process and Download Excel'}</button>
      </div>
      {error && <p style={{ color: 'salmon' }}>{error}</p>}
    </section>
  )
}
