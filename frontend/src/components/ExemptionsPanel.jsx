import React, { useMemo, useState, useEffect } from 'react'

function parseCsvNames(file) {
  return new Promise((resolve) => {
    const reader = new FileReader()
    reader.onload = () => {
      try {
        const text = String(reader.result || '')
        const lines = text.split(/\r?\n/).filter(Boolean)
        if (lines.length === 0) return resolve([])
        const header = lines[0].split(',').map(h => h.trim().toLowerCase())
        const nameIdx = header.findIndex(h => h === 'name' || h.includes('participant') || h.includes('screen name'))
        if (nameIdx === -1) return resolve([])
        const set = new Set()
        for (let i = 1; i < lines.length; i++) {
          const cols = lines[i].split(',')
          const nm = (cols[nameIdx] || '').trim()
          if (nm) set.add(nm)
        }
        resolve(Array.from(set).sort())
      } catch { resolve([]) }
    }
    reader.onerror = () => resolve([])
    reader.readAsText(file)
  })
}

export default function ExemptionsPanel({ zoomCsvFile, value, onChange }) {
  const [names, setNames] = useState([])
  const [query, setQuery] = useState('')

  useEffect(() => {
    let mounted = true
    if (zoomCsvFile) {
      parseCsvNames(zoomCsvFile).then(n => { if (mounted) setNames(n) })
    } else {
      setNames([])
    }
    return () => { mounted = false }
  }, [zoomCsvFile])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return names
    return names.filter(n => n.toLowerCase().includes(q))
  }, [names, query])

  function toggle(name, key) {
    const k = `NAME:${name}`
    const cur = value[k] || { naming: false, overlap: false, reconnect: false }
    const next = { ...value, [k]: { ...cur, [key]: !cur[key] } }
    onChange(next)
  }

  return (
    <div className="card">
      <h3>Exemptions</h3>
      <input placeholder="Search name…" value={query} onChange={(e) => setQuery(e.target.value)} />
      <div style={{ maxHeight: 280, overflow: 'auto', marginTop: 8 }}>
        {filtered.map(n => {
          const k = `NAME:${n}`
          const ex = value[k] || { naming: false, overlap: false, reconnect: false }
          return (
            <div key={k} style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '6px 0', borderBottom: '1px solid #202845' }}>
              <div style={{ flex: 1 }}>{n}</div>
              <label><input type="checkbox" checked={ex.naming} onChange={() => toggle(n, 'naming')} /> Naming</label>
              <label><input type="checkbox" checked={ex.overlap} onChange={() => toggle(n, 'overlap')} /> Overlap</label>
              <label><input type="checkbox" checked={ex.reconnect} onChange={() => toggle(n, 'reconnect')} /> Reconnect</label>
            </div>
          )
        })}
      </div>
    </div>
  )
}
