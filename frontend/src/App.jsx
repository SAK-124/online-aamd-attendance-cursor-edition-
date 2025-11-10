import React from 'react'
import { Routes, Route, NavLink } from 'react-router-dom'
import UploadPage from './pages/UploadPage.jsx'
import ProcessPage from './pages/ProcessPage.jsx'

export default function App() {
  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="brand">
          <div className="brand-icon">ZA</div>
          <div>
            <h1>Zoom Attendance Automator</h1>
            <p className="muted">Powered by the v4.1 processing engine from the original Python app.</p>
          </div>
        </div>
        <nav className="nav-links">
          <NavLink to="/" end className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>Overview</NavLink>
          <NavLink to="/process" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>Process CSV</NavLink>
        </nav>
      </header>
      <main className="app-main">
        <Routes>
          <Route path="/" element={<UploadPage />} />
          <Route path="/process" element={<ProcessPage />} />
        </Routes>
      </main>
      <footer className="app-footer">
        <span className="muted">Excel workbooks include Attendance, Issues, Reconnects, Penalties, Matches, Meta, and Summary tabs—just like the desktop utility.</span>
      </footer>
    </div>
  )
}
