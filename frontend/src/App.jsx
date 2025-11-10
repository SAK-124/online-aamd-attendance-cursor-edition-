import React from 'react'
import { Routes, Route, Link } from 'react-router-dom'
import UploadPage from './pages/UploadPage.jsx'
import ProcessPage from './pages/ProcessPage.jsx'

export default function App() {
  return (
    <div className="container">
      <header className="header">
        <h1>Zoom Attendance</h1>
        <nav>
          <Link to="/">Upload</Link>
          <Link to="/process">Process</Link>
        </nav>
      </header>
      <main>
        <Routes>
          <Route path="/" element={<UploadPage />} />
          <Route path="/process" element={<ProcessPage />} />
        </Routes>
      </main>
    </div>
  )
}
