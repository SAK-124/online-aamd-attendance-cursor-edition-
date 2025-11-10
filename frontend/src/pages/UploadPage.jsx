import React, { useState } from 'react'

export default function UploadPage() {
  const [zoomCsv, setZoomCsv] = useState(null)
  const [rosterFile, setRosterFile] = useState(null)

  return (
    <section>
      <h2>Upload Files</h2>
      <div className="card">
        <label>Zoom Participants CSV
          <input type="file" accept=".csv" onChange={(e) => setZoomCsv(e.target.files?.[0] || null)} />
        </label>
        <label>Roster (Excel/CSV, optional)
          <input type="file" accept=".xlsx,.xls,.csv" onChange={(e) => setRosterFile(e.target.files?.[0] || null)} />
        </label>
      </div>
    </section>
  )
}
