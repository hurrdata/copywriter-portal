'use client'

import { useState, useEffect } from 'react'
import { Search, MapPin, CheckCircle2, Clock, Map, Navigation, Car, Landmark, Download, Crosshair, Clipboard, TreePine, Building2, GraduationCap, Home, Route, BarChart3 } from 'lucide-react'
import { saveDraftContent } from '@/app/actions'

// ─── Helpers ────────────────────────────────────────────────────────────────

/** Parse pipe-delimited POI string: "Name (0.5 mi) | Name2 ★ (1.2 mi)" */
function parsePOIList(raw: string | undefined): { name: string; dist: string; starred: boolean }[] {
  if (!raw || raw === 'N/A') return []
  return raw.split('|').map(s => s.trim()).filter(Boolean).map(item => {
    const starred = item.includes('★')
    const clean = item.replace('★', '').trim()
    const match = clean.match(/^(.+?)\s*\(([^)]+)\)\s*$/)
    return match
      ? { name: match[1].trim(), dist: match[2].trim(), starred }
      : { name: clean, dist: '', starred }
  })
}

/** Segment → color badge config */
function segmentBadge(segment: string) {
  const s = (segment || '').toLowerCase()
  if (s.includes('misaligned')) return { label: 'Misaligned', bg: 'bg-red-100', text: 'text-red-800', border: 'border-red-200' }
  if (s.includes('a') || s.includes('home dominant')) return { label: 'Seg A — Home Dominant', bg: 'bg-green-100', text: 'text-green-800', border: 'border-green-200' }
  if (s.includes('b') || s.includes('mixed')) return { label: 'Seg B — Mixed', bg: 'bg-blue-100', text: 'text-blue-800', border: 'border-blue-200' }
  if (s.includes('c') || s.includes('distributed')) return { label: 'Seg C — Distributed', bg: 'bg-yellow-100', text: 'text-yellow-800', border: 'border-yellow-200' }
  return { label: segment, bg: 'bg-gray-100', text: 'text-gray-700', border: 'border-gray-200' }
}

// ─── Sub-components ──────────────────────────────────────────────────────────

function ContextCard({ icon, title, children, accent }: { icon: React.ReactNode; title: string; children: React.ReactNode; accent?: string }) {
  return (
    <div className={`bg-white rounded-2xl border border-gray-200 shadow-sm overflow-hidden ${accent ? `border-l-4 ${accent}` : ''}`}>
      <div className="flex items-center gap-3 px-5 py-4 border-b border-gray-100">
        {icon}
        <h4 className="font-bold text-gray-900 text-sm">{title}</h4>
      </div>
      <div className="px-5 py-4">{children}</div>
    </div>
  )
}

function POIRow({ name, dist, starred }: { name: string; dist: string; starred?: boolean }) {
  return (
    <div className="flex justify-between items-center py-2 border-b border-gray-50 last:border-0">
      <span className="text-sm text-gray-800 font-medium">
        {name}{starred && <span className="ml-1 text-amber-500">★</span>}
      </span>
      {dist && <span className="text-xs font-bold text-gray-400 shrink-0 ml-2">{dist}</span>}
    </div>
  )
}

// ─── Main Component ──────────────────────────────────────────────────────────

export default function Dashboard({ initialFacilities }: { initialFacilities: any[] }) {
  const [facilities, setFacilities] = useState(initialFacilities)
  const [activeId, setActiveId] = useState<number | null>(initialFacilities[0]?.id || null)
  const [searchTerm, setSearchTerm] = useState('')
  const [filter, setFilter] = useState('All')
  const [copySuccess, setCopySuccess] = useState(false)

  const BULLET_CATEGORIES = [
    'Home Community', 'Nearby Neighborhoods', 'Second City Draw',
    'Interstate/Highway Exit', 'Airport Proximity', 'University/College Proximity',
    'Military Base/Community', 'Local Schools', 'Notable Nearby Landmark',
    'Marina/Waterfront', 'RV Park/Outdoor Recreation', 'Urban Residential Communities'
  ]

  const activeFacility = facilities.find(f => f.id === activeId)

  const [editForm, setEditForm] = useState({
    introParagraph: '', bullet1: '', bullet1Tag: '', bullet2: '', bullet2Tag: '',
    bullet3: '', bullet3Tag: '', bullet4: '', bullet4Tag: ''
  })

  useEffect(() => {
    if (activeFacility?.draft) {
      setEditForm({
        introParagraph: activeFacility.draft.introParagraph || '',
        bullet1: activeFacility.draft.bullet1 || '',
        bullet1Tag: activeFacility.draft.bullet1Tag || '',
        bullet2: activeFacility.draft.bullet2 || '',
        bullet2Tag: activeFacility.draft.bullet2Tag || '',
        bullet3: activeFacility.draft.bullet3 || '',
        bullet3Tag: activeFacility.draft.bullet3Tag || '',
        bullet4: activeFacility.draft.bullet4 || '',
        bullet4Tag: activeFacility.draft.bullet4Tag || ''
      })
    } else {
      setEditForm({ introParagraph: '', bullet1: '', bullet1Tag: '', bullet2: '', bullet2Tag: '', bullet3: '', bullet3Tag: '', bullet4: '', bullet4Tag: '' })
    }
  }, [activeFacility])

  const filteredFacilities = facilities
    .filter(f => {
      const matchesSearch = f.storeNumber.includes(searchTerm) || f.city.toLowerCase().includes(searchTerm.toLowerCase())
      if (filter === 'All') return matchesSearch
      return matchesSearch && f.draft?.status === filter
    })
    .sort((a, b) => parseInt(a.storeNumber, 10) - parseInt(b.storeNumber, 10))

  const handleSave = async () => {
    if (!activeFacility?.draft) return
    setFacilities(facilities.map(f =>
      f.id === activeFacility.id ? { ...f, draft: { ...f.draft, ...editForm, status: 'Approved' } } : f
    ))
    await saveDraftContent(activeFacility.draft.id, editForm)
  }

  const generateHTML = () =>
    `<p>${editForm.introParagraph}</p>\n<ul>\n` +
    `  <li><strong>${editForm.bullet1Tag}:</strong> ${editForm.bullet1}</li>\n` +
    `  <li><strong>${editForm.bullet2Tag}:</strong> ${editForm.bullet2}</li>\n` +
    `  <li><strong>${editForm.bullet3Tag}:</strong> ${editForm.bullet3}</li>\n` +
    `  <li><strong>${editForm.bullet4Tag}:</strong> ${editForm.bullet4}</li>\n` +
    `</ul>`

  const copyToClipboard = () => {
    navigator.clipboard.writeText(generateHTML())
    setCopySuccess(true)
    setTimeout(() => setCopySuccess(false), 2000)
  }

  const handleExportCSV = () => {
    const headers = ['Store Number', 'Status', 'HTML Output']
    const rows = facilities.map(f => {
      const html = `<p>${f.draft?.introParagraph || ''}</p><ul>` +
        `<li><strong>${f.draft?.bullet1Tag || ''}:</strong> ${f.draft?.bullet1 || ''}</li>` +
        `<li><strong>${f.draft?.bullet2Tag || ''}:</strong> ${f.draft?.bullet2 || ''}</li>` +
        `<li><strong>${f.draft?.bullet3Tag || ''}:</strong> ${f.draft?.bullet3 || ''}</li>` +
        `<li><strong>${f.draft?.bullet4Tag || ''}:</strong> ${f.draft?.bullet4 || ''}</li></ul>`
      return [f.storeNumber, f.draft?.status || 'Pending', `"${html.replace(/"/g, '""')}"`]
    })
    const csv = [headers, ...rows].map(e => e.join(',')).join('\n')
    const link = document.createElement('a')
    link.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8;' }))
    link.setAttribute('download', 'exr_pro_copy_expansion.csv')
    document.body.appendChild(link); link.click(); document.body.removeChild(link)
  }

  let geo: any = null
  let demo: any = null
  if (activeFacility?.geoData) {
    try { geo = typeof activeFacility.geoData === 'string' ? JSON.parse(activeFacility.geoData) : activeFacility.geoData } catch (e) {}
  }
  if (activeFacility?.demographicData) {
    try { demo = typeof activeFacility.demographicData === 'string' ? JSON.parse(activeFacility.demographicData) : activeFacility.demographicData } catch (e) {}
  }

  // Parse all POI lists
  const schools = geo ? parsePOIList(geo['POIs — Schools']) : []
  const neighborhoods = geo ? parsePOIList(geo['POIs — Neighborhoods']) : []
  const parks = geo ? parsePOIList(geo['POIs — Parks/Greenspace']) : []
  const residentialAreas = geo ? parsePOIList(geo['POIs — Residential Areas']) : []
  const interstateExits = geo ? parsePOIList(geo['POIs — Interstate Exits']) : []
  const zipMix: { zip: string; share: number; type: string }[] = geo?.zip_customer_mix || []
  const segment = geo?.Segment || ''
  const badge = segment ? segmentBadge(segment) : null

  return (
    <div className="flex h-screen bg-[#f3f4f6] text-gray-800 font-sans overflow-hidden">

      {/* ── SIDEBAR ── */}
      <div className="w-80 bg-white border-r border-gray-200 flex flex-col shadow-sm z-10">
        <div className="p-6 border-b border-gray-100">
          <div className="flex items-center gap-3 text-green-700 mb-6">
            <div className="w-8 h-8 rounded-lg bg-green-100 flex items-center justify-center">
              <MapPin className="w-5 h-5" />
            </div>
            <h1 className="text-xl font-bold tracking-tight">EXR Content</h1>
          </div>
          <div className="relative mb-4">
            <Search className="w-4 h-4 absolute left-3 top-3 text-gray-400" />
            <input type="text" placeholder="Search store or city..."
              value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-9 pr-4 py-2 bg-gray-50 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-green-500 focus:bg-white transition-all" />
          </div>
          <div className="flex gap-2">
            {['All', 'Pending', 'Approved'].map(f => (
              <button key={f} onClick={() => setFilter(f)}
                className={`px-3 py-1.5 rounded-full text-xs font-semibold tracking-wide transition-colors ${filter === f ? 'bg-green-700 text-white shadow-sm' : 'bg-gray-100 text-gray-500 hover:bg-gray-200'}`}>
                {f}
              </button>
            ))}
          </div>
        </div>
        <div className="flex-1 overflow-y-auto px-4 py-2">
          {filteredFacilities.map((facility) => (
            <button key={facility.id} onClick={() => setActiveId(facility.id)}
              className={`w-full text-left p-4 rounded-xl mb-2 transition-all flex flex-col gap-2 ${activeId === facility.id ? 'bg-green-50 border-green-200 border shadow-sm ring-1 ring-green-500 ring-opacity-20' : 'bg-white hover:bg-gray-50 border border-transparent'}`}>
              <div className="flex justify-between items-center">
                <span className="font-bold text-gray-900">Store {facility.storeNumber}</span>
                {facility.draft?.status === 'Approved' ? <CheckCircle2 className="w-4 h-4 text-green-600" /> : <Clock className="w-4 h-4 text-amber-500" />}
              </div>
              <span className="text-xs text-gray-500 truncate">{facility.city}, {facility.state}</span>
            </button>
          ))}
        </div>
      </div>

      {/* ── MAIN AREA ── */}
      {activeFacility ? (
        <div className="flex-1 flex flex-col h-full bg-[#fbfbfb]">

          {/* Header */}
          <div className="h-20 border-b border-gray-200 bg-white px-8 flex justify-between items-center shrink-0">
            <div>
              <h2 className="text-2xl font-black text-gray-900 tracking-tight">
                Store {activeFacility.storeNumber} • {activeFacility.city}, {activeFacility.state}
              </h2>
              <p className="text-sm text-gray-500 font-medium mt-1">{activeFacility.address} • {activeFacility.zip}</p>
            </div>
            <div className="flex gap-3 items-center">
              {badge && (
                <span className={`px-3 py-1.5 rounded-lg text-xs font-black border ${badge.bg} ${badge.text} ${badge.border}`}>
                  {badge.label}
                </span>
              )}
              <span className={`px-4 py-2 rounded-lg text-sm font-bold flex items-center gap-2 ${activeFacility.draft?.status === 'Approved' ? 'bg-green-100 text-green-800' : 'bg-amber-100 text-amber-800'}`}>
                {activeFacility.draft?.status === 'Approved' ? 'Approved' : 'Needs Review'}
              </span>
              <button onClick={handleExportCSV}
                className="bg-gray-100 hover:bg-gray-200 text-gray-700 px-4 py-2 rounded-lg text-sm font-bold border border-gray-200 flex items-center gap-2 transition-all">
                <Download size={16} /> Export All (CSV)
              </button>
              <button onClick={handleSave}
                className="bg-green-700 hover:bg-green-800 text-white px-6 py-2 rounded-lg text-sm font-bold shadow-md shadow-green-700/20 transition-all border border-green-800">
                Save & Approve
              </button>
            </div>
          </div>

          <div className="flex-1 overflow-hidden flex">

            {/* ── EDITOR (Left) ── */}
            <div className="w-3/5 border-r border-gray-200 overflow-y-auto p-8 bg-white relative">

              {(!activeFacility.draft || !(activeFacility.draft as any).bullet1Tag) && (
                <div className="absolute inset-0 bg-white flex items-center justify-center z-20">
                  <div className="flex flex-col items-center max-w-sm text-center p-8">
                    <div className="w-16 h-16 rounded-3xl bg-amber-50 flex items-center justify-center mb-8 border border-amber-100 shadow-sm">
                      <Clock className="w-8 h-8 text-amber-500" />
                    </div>
                    <h3 className="text-2xl font-black text-gray-900 tracking-tight leading-loose">Expansion Content Scheduled</h3>
                    <p className="text-sm text-gray-500 mt-4 leading-relaxed">
                      This facility is scheduled for the next phase of the 879-store expansion.
                      <span className="block mt-4 font-bold text-green-700">Please review one of the Top 30 stores for the 'Pro' content demo.</span>
                    </p>
                  </div>
                </div>
              )}

              <div className="space-y-6 max-w-2xl mx-auto">
                <div>
                  <label className="block text-xs font-bold text-gray-400 uppercase tracking-wider mb-2">Introductory Paragraph</label>
                  <textarea value={editForm.introParagraph}
                    onChange={(e) => setEditForm({ ...editForm, introParagraph: e.target.value })}
                    className="w-full p-4 bg-gray-50 border border-gray-200 rounded-xl text-gray-800 font-medium leading-relaxed resize-none h-32 focus:outline-none focus:ring-2 focus:ring-green-500 focus:bg-white transition-colors" />
                </div>

                {[1, 2, 3, 4].map(num => (
                  <div key={num} className="relative">
                    <label className="block mb-2 flex items-center gap-3">
                      <div className="w-6 h-6 rounded-full bg-green-100 flex items-center justify-center text-green-800 font-bold shrink-0 text-xs">{num}</div>
                      <select value={(editForm as any)[`bullet${num}Tag`]}
                        onChange={(e) => setEditForm({ ...editForm, [`bullet${num}Tag`]: e.target.value })}
                        className="text-sm font-black text-green-700 uppercase tracking-tight bg-transparent border-none p-0 focus:ring-0 cursor-pointer hover:text-green-800">
                        <option value="">Select Category...</option>
                        {BULLET_CATEGORIES.map(cat => <option key={cat} value={cat}>{cat}</option>)}
                      </select>
                    </label>
                    <textarea value={(editForm as any)[`bullet${num}`]}
                      onChange={(e) => setEditForm({ ...editForm, [`bullet${num}`]: e.target.value })}
                      className="w-full p-4 bg-gray-50 border border-gray-200 rounded-xl text-gray-800 leading-relaxed resize-none h-28 focus:outline-none focus:ring-2 focus:ring-green-500 focus:bg-white transition-colors" />
                  </div>
                ))}

                {/* Production HTML */}
                <div className="mt-12 pt-12 border-t border-gray-100">
                  <div className="flex justify-between items-center mb-4">
                    <label className="block text-xs font-bold text-gray-400 uppercase tracking-wider">Production HTML Output</label>
                    <button onClick={copyToClipboard} className="text-xs font-bold text-green-700 hover:text-green-800 flex items-center gap-2">
                      {copySuccess ? <CheckCircle2 size={14} /> : <Clipboard size={14} />}
                      {copySuccess ? 'Copied!' : 'Copy Code'}
                    </button>
                  </div>
                  <textarea readOnly value={generateHTML()}
                    className="w-full p-4 bg-gray-900 text-green-400 font-mono text-xs rounded-xl h-48 border border-gray-800 focus:outline-none" />
                </div>
              </div>
            </div>

            {/* ── CONTEXT RAIL (Right) ── */}
            <div className="w-2/5 bg-gray-50 overflow-y-auto p-6 border-l border-gray-200">
              <h3 className="text-xs font-black text-gray-400 uppercase tracking-widest mb-5">Source Context Data</h3>

              {geo ? (
                <div className="space-y-4">

                  {/* 1. CUSTOMER ZIP MIX */}
                  {zipMix.length > 0 && (
                    <ContextCard icon={<div className="bg-indigo-100 p-2 rounded-lg text-indigo-700"><BarChart3 size={16} /></div>} title="Customer ZIP Mix">
                      <div className="space-y-2">
                        {zipMix.map((z, i) => (
                          <div key={i} className="flex items-center gap-3">
                            <div className="flex-1">
                              <div className="flex justify-between items-center mb-1">
                                <span className="text-sm font-bold text-gray-800">{z.zip}</span>
                                <span className="text-xs font-black text-indigo-700">{z.share}%</span>
                              </div>
                              <div className="w-full bg-gray-100 rounded-full h-1.5">
                                <div className={`h-1.5 rounded-full ${z.type === 'Home' ? 'bg-indigo-600' : 'bg-indigo-300'}`}
                                  style={{ width: `${Math.min(z.share * 1.5, 100)}%` }} />
                              </div>
                            </div>
                            <span className={`text-[10px] font-black px-2 py-0.5 rounded-full ${z.type === 'Home' ? 'bg-indigo-100 text-indigo-700' : 'bg-gray-100 text-gray-500'}`}>
                              {z.type}
                            </span>
                          </div>
                        ))}
                      </div>
                    </ContextCard>
                  )}

                  {/* 2. SEGMENT */}
                  {badge && (
                    <ContextCard icon={<div className="bg-gray-100 p-2 rounded-lg text-gray-600"><Map size={16} /></div>} title="Market Segment">
                      <div className="flex items-center gap-3">
                        <span className={`px-4 py-2 rounded-lg text-sm font-black border ${badge.bg} ${badge.text} ${badge.border}`}>{badge.label}</span>
                        {geo['Misaligned'] === 'Yes' && (
                          <span className="text-xs font-bold text-red-600 bg-red-50 px-3 py-1.5 rounded-lg border border-red-100">
                            ⚠ {geo['Misaligned Sub-Type'] || 'Misaligned'}
                          </span>
                        )}
                      </div>
                      {geo['Second City Draw'] && (
                        <p className="text-xs text-gray-600 mt-3 leading-relaxed">
                          <span className="font-black text-blue-700">Regional Draw: </span>{geo['Second City Draw']}
                        </p>
                      )}
                    </ContextCard>
                  )}

                  {/* 3. TRANSPORTATION & ACCESS */}
                  <ContextCard icon={<div className="bg-blue-100 p-2 rounded-lg text-blue-700"><Car size={16} /></div>} title="Transportation & Access">
                    <div className="space-y-3">
                      <div className="flex justify-between items-start">
                        <div>
                          <span className="text-[10px] uppercase text-gray-400 font-black block">Nearest Interstate</span>
                          <span className="text-sm font-bold text-gray-800">{geo['nearest_interstate'] || 'N/A'}</span>
                          <span className="text-xs text-gray-400 ml-2">{Number(geo['interstate_distance_mi'] || 0).toFixed(2)} mi</span>
                        </div>
                        <div className="text-right">
                          <span className="text-[10px] uppercase text-gray-400 font-black block">Airport</span>
                          <span className="text-sm font-bold text-gray-800">{geo['nearest_airport'] || 'N/A'}</span>
                          <span className="text-xs text-gray-400 ml-1">{Number(geo['airport_distance_mi'] || 0).toFixed(1)} mi</span>
                        </div>
                      </div>
                      {geo['nearest_major_intersection'] && (
                        <div className="pt-3 border-t border-gray-100">
                          <span className="text-[10px] uppercase text-gray-400 font-black flex items-center gap-1 mb-1"><Crosshair size={9} /> Front Door Intersection</span>
                          <span className="text-sm font-bold text-gray-800">{geo['nearest_major_intersection']}</span>
                        </div>
                      )}
                    </div>
                  </ContextCard>

                  {/* 4. INTERSTATE EXITS */}
                  {interstateExits.length > 0 && (
                    <ContextCard icon={<div className="bg-blue-100 p-2 rounded-lg text-blue-700"><Route size={16} /></div>} title="Interstate Exits">
                      {interstateExits.map((exit, i) => (
                        <POIRow key={i} name={`Exit ${exit.name}`} dist={exit.dist} />
                      ))}
                      {geo['Interstate Access'] && (
                        <p className="text-[11px] text-gray-500 mt-2 pt-2 border-t border-gray-100 italic">{geo['Interstate Access']}</p>
                      )}
                    </ContextCard>
                  )}

                  {/* 5. NEIGHBORHOODS */}
                  {neighborhoods.length > 0 && (
                    <ContextCard icon={<div className="bg-emerald-100 p-2 rounded-lg text-emerald-700"><Home size={16} /></div>} title="Nearby Neighborhoods">
                      {neighborhoods.slice(0, 8).map((n, i) => <POIRow key={i} name={n.name} dist={n.dist} starred={n.starred} />)}
                    </ContextCard>
                  )}

                  {/* 6. SCHOOLS */}
                  {schools.length > 0 && (
                    <ContextCard icon={<div className="bg-purple-100 p-2 rounded-lg text-purple-700"><GraduationCap size={16} /></div>} title="Nearby Schools">
                      {schools.slice(0, 8).map((s, i) => <POIRow key={i} name={s.name} dist={s.dist} starred={s.starred} />)}
                    </ContextCard>
                  )}

                  {/* 7. RESIDENTIAL AREAS */}
                  {residentialAreas.length > 0 && (
                    <ContextCard icon={<div className="bg-orange-100 p-2 rounded-lg text-orange-700"><Building2 size={16} /></div>} title="Residential Communities">
                      {residentialAreas.slice(0, 8).map((r, i) => <POIRow key={i} name={r.name} dist={r.dist} starred={r.starred} />)}
                    </ContextCard>
                  )}

                  {/* 8. PARKS */}
                  {parks.length > 0 && (
                    <ContextCard icon={<div className="bg-green-100 p-2 rounded-lg text-green-700"><TreePine size={16} /></div>} title="Parks & Greenspace">
                      {parks.slice(0, 8).map((p, i) => <POIRow key={i} name={p.name} dist={p.dist} starred={p.starred} />)}
                    </ContextCard>
                  )}

                  {/* 9. DEMOGRAPHICS */}
                  <ContextCard icon={<div className="bg-purple-100 p-2 rounded-lg text-purple-700"><Landmark size={16} /></div>} title="Demographics & Insights">
                    <div className="space-y-3">
                      <div className="flex justify-between items-center">
                        <div>
                          <span className="text-[10px] uppercase text-gray-400 font-black block mb-1">Target Persona</span>
                          <span className={`inline-block px-3 py-1 rounded-md font-bold text-xs border ${badge?.bg || 'bg-purple-50'} ${badge?.text || 'text-purple-800'} ${badge?.border || 'border-purple-100'}`}>
                            {demo?.persona || geo['Demographic Persona'] || 'General'}
                          </span>
                        </div>
                        <div className="text-right">
                          <span className="text-[10px] uppercase text-gray-400 font-black block mb-1">Median Income</span>
                          <span className="text-sm font-bold text-gray-800">${Number(geo['Wtd. Median Income'] || 0).toLocaleString()}</span>
                        </div>
                      </div>
                      <div className="grid grid-cols-3 gap-2 pt-3 border-t border-gray-100">
                        {[
                          { label: 'Median Age', val: geo['Wtd. Median Age'] },
                          { label: 'Renter %', val: geo['Wtd. Renter Rate %'] ? `${geo['Wtd. Renter Rate %']}%` : null },
                          { label: 'Under 18', val: geo['Wtd. % Under 18'] ? `${geo['Wtd. % Under 18']}%` : null },
                        ].map((stat, i) => stat.val && (
                          <div key={i} className="bg-gray-50 rounded-lg p-2 text-center">
                            <span className="text-[10px] text-gray-400 font-black uppercase block">{stat.label}</span>
                            <span className="text-sm font-black text-gray-800">{stat.val}</span>
                          </div>
                        ))}
                      </div>
                      {(geo['Content Hook (Bullet 2)'] || geo['Demographic Content Hook']) && (
                        <div className="pt-3 border-t border-gray-100">
                          <span className="text-[10px] uppercase text-gray-400 font-black block mb-1">Strategic Hook</span>
                          <p className="text-xs text-gray-600 leading-relaxed italic">"{geo['Content Hook (Bullet 2)'] || geo['Demographic Content Hook']}"</p>
                        </div>
                      )}
                    </div>
                  </ContextCard>

                </div>
              ) : (
                <div className="text-sm text-gray-500 text-center mt-10">Context Data Unavailable</div>
              )}
            </div>
          </div>
        </div>
      ) : (
        <div className="flex-1 flex flex-col items-center justify-center bg-[#fbfbfb]">
          <Navigation className="w-16 h-16 text-gray-200 mb-4" />
          <h2 className="text-xl font-bold text-gray-400">Select a location from the sidebar</h2>
        </div>
      )}
    </div>
  )
}
