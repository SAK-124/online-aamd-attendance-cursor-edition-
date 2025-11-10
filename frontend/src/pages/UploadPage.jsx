import React from 'react'
import { Link } from 'react-router-dom'

const highlights = [
  'Zoom-only alias merging using canonical names: handles ERP prefixes, parentheses, punctuation, and reconnects.',
  'Attendance workbook ships with Attendance, Issues, Absent, Penalties, Matches, Reconnects, ERPs, Meta, and Summary sheets.',
  'Excel formulas auto-populate overrides, conditional formatting, and dropdowns exactly like the original desktop script.',
  'Roster upload is optional and only injects missing students—never merging roster names into Zoom logs.',
]

export default function UploadPage() {
  return (
    <section className="page">
      <div className="hero">
        <div>
          <h2>Automate attendance without leaving the browser</h2>
          <p className="lead">
            This web interface wraps the official v4.1 Python engine. Drop in the same Zoom participant CSV and optional roster
            you would use with the desktop GUI and download the identical workbook in seconds.
          </p>
          <div className="cta-group">
            <Link className="button button-primary" to="/process">Start processing</Link>
            <a className="button button-ghost" href="https://github.com/" target="_blank" rel="noreferrer">View source on GitHub</a>
          </div>
        </div>
        <div className="hero-card card">
          <h3>What you&apos;ll get</h3>
          <ul className="feature-list">
            {highlights.map(item => (
              <li key={item}>{item}</li>
            ))}
          </ul>
          <div className="stat-grid">
            <div>
              <span className="stat-number">4.1</span>
              <span className="muted">Engine version</span>
            </div>
            <div>
              <span className="stat-number">9</span>
              <span className="muted">Excel sheets</span>
            </div>
            <div>
              <span className="stat-number">0</span>
              <span className="muted">Roster merges</span>
            </div>
          </div>
        </div>
      </div>

      <div className="grid-two">
        <div className="card">
          <h3>Before you begin</h3>
          <ol className="step-list">
            <li>Download the <strong>Zoom participants CSV</strong> from the meeting report.</li>
            <li>Optionally export your <strong>roster</strong> as CSV or Excel (needs ERP + Name columns).</li>
            <li>Decide on threshold, buffer, break minutes, and naming penalty tolerance.</li>
          </ol>
        </div>
        <div className="card">
          <h3>Why it&apos;s reliable</h3>
          <p className="muted">
            The same Python code handles canonicalisation, reconnect detection, penalties, and Excel post-processing. We simply run
            it in a serverless function and stream the generated workbook back to you.
          </p>
          <p className="muted">
            The UI remembers your parameters locally, and exemptions sync with the keys extracted by the engine—not by a fragile
            CSV string match.
          </p>
        </div>
      </div>
    </section>
  )
}
