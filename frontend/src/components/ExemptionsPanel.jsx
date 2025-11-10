import React, { useMemo, useState } from 'react'

const EMPTY_FLAGS = { naming: false, overlap: false, reconnect: false }

export default function ExemptionsPanel({ options, loading, error, value, onChange }) {
  const [query, setQuery] = useState('')

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return options
    return options.filter(({ display, key }) => (
      display.toLowerCase().includes(q) || key.toLowerCase().includes(q)
    ))
  }, [options, query])

  function toggle(key, flag) {
    const current = value[key] || EMPTY_FLAGS
    const next = { ...value, [key]: { ...current, [flag]: !current[flag] } }
    onChange(next)
  }

  const activeCount = Object.values(value || {}).reduce((acc, flags) => (
    acc + (flags.naming || flags.overlap || flags.reconnect ? 1 : 0)
  ), 0)

  return (
    <div className="card panel">
      <div className="panel-header">
        <div>
          <h3>Exemptions</h3>
          <p className="muted">Override penalties or duplicate warnings for specific students.</p>
        </div>
        <span className="badge">{activeCount} active</span>
      </div>

      <div className="panel-toolbar">
        <input
          placeholder="Search ERP, name, or key…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
      </div>

      <div className="panel-body">
        {loading && <p className="muted">Scanning CSV… this matches the desktop app&apos;s key list.</p>}
        {!loading && error && <p className="error-text">{error}</p>}
        {!loading && !error && options.length === 0 && (
          <p className="muted">Upload a Zoom participants CSV to configure exemptions.</p>
        )}

        {!loading && !error && options.length > 0 && (
          <ul className="exemption-list">
            {filtered.map(({ key, display }) => {
              const flags = value[key] || EMPTY_FLAGS
              return (
                <li key={key}>
                  <div className="exemption-main">
                    <strong>{display}</strong>
                    <span className="muted mono">{key}</span>
                  </div>
                  <div className="exemption-flags">
                    <label>
                      <input
                        type="checkbox"
                        checked={flags.naming}
                        onChange={() => toggle(key, 'naming')}
                      />
                      Naming
                    </label>
                    <label>
                      <input
                        type="checkbox"
                        checked={flags.overlap}
                        onChange={() => toggle(key, 'overlap')}
                      />
                      Dual devices
                    </label>
                    <label>
                      <input
                        type="checkbox"
                        checked={flags.reconnect}
                        onChange={() => toggle(key, 'reconnect')}
                      />
                      Reconnects
                    </label>
                  </div>
                </li>
              )
            })}
          </ul>
        )}
      </div>
    </div>
  )
}
