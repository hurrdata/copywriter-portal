'use client'

import { useState, useEffect } from 'react'
import { Search, MapPin, CheckCircle2, Clock, Map, Navigation, Car, Landmark, Users, Download, FileText, Crosshair, Clipboard } from 'lucide-react'
import { updateDraftStatus, saveDraftContent } from '@/app/actions'

export default function Dashboard({ initialFacilities }: { initialFacilities: any[] }) {
  const [facilities, setFacilities] = useState(initialFacilities)
  const [activeId, setActiveId] = useState<number | null>(initialFacilities[0]?.id || null)
  const [searchTerm, setSearchTerm] = useState('')
  const [filter, setFilter] = useState('All') // All, Pending, Approved
  const [copySuccess, setCopySuccess] = useState(false)

  const BULLET_CATEGORIES = [
    'Home Community',
    'Nearby Neighborhoods',
    'Second City Draw',
    'Interstate/Highway Exit',
    'Airport Proximity',
    'University/College Proximity',
    'Military Base/Community',
    'Local Schools',
    'Notable Nearby Landmark',
    'Marina/Waterfront',
    'RV Park/Outdoor Recreation',
    'Urban Residential Communities'
  ]

  const activeFacility = facilities.find(f => f.id === activeId)
  
  // Local state for the editor forms to allow real-time typing
  const [editForm, setEditForm] = useState({
    introParagraph: '',
    bullet1: '',
    bullet1Tag: '',
    bullet2: '',
    bullet2Tag: '',
    bullet3: '',
    bullet3Tag: '',
    bullet4: '',
    bullet4Tag: ''
  })

  // Sync editor when switching facilities
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
      setEditForm({
        introParagraph: '', bullet1: '', bullet1Tag: '', bullet2: '', bullet2Tag: '', bullet3: '', bullet3Tag: '', bullet4: '', bullet4Tag: ''
      })
    }
  }, [activeFacility])

  const filteredFacilities = facilities
    .filter(f => {
      const matchesSearch = f.storeNumber.includes(searchTerm) || f.city.toLowerCase().includes(searchTerm.toLowerCase())
      if (filter === 'All') return matchesSearch
      return matchesSearch && f.draft?.status === filter
    })
    .sort((a, b) => {
      return parseInt(a.storeNumber, 10) - parseInt(b.storeNumber, 10)
    })

  const handleSave = async () => {
    if (!activeFacility?.draft) return
    
    // Optimistic UI update
    const updatedFacilities = facilities.map(f => {
      if (f.id === activeFacility.id) {
        return { 
          ...f, 
          draft: { ...f.draft, ...editForm, status: 'Approved' }
        }
      }
      return f
    })
    setFacilities(updatedFacilities)
    
    // Server action
    await saveDraftContent(activeFacility.draft.id, editForm)
  }

  const generateHTML = () => {
    return `<p>${editForm.introParagraph}</p>\n<ul>\n` +
      `  <li><strong>${editForm.bullet1Tag}:</strong> ${editForm.bullet1}</li>\n` +
      `  <li><strong>${editForm.bullet2Tag}:</strong> ${editForm.bullet2}</li>\n` +
      `  <li><strong>${editForm.bullet3Tag}:</strong> ${editForm.bullet3}</li>\n` +
      `  <li><strong>${editForm.bullet4Tag}:</strong> ${editForm.bullet4}</li>\n` +
      `</ul>`;
  };

  const copyToClipboard = () => {
    navigator.clipboard.writeText(generateHTML());
    setCopySuccess(true);
    setTimeout(() => setCopySuccess(false), 2000);
  };

  const handleExportCSV = () => {
    const headers = ['Store Number', 'Status', 'HTML Output'];
    const rows = facilities.map(f => {
      const html = `<p>${f.draft?.introParagraph || ''}</p><ul>` +
          `<li><strong>${f.draft?.bullet1Tag || ''}:</strong> ${f.draft?.bullet1 || ''}</li>` +
          `<li><strong>${f.draft?.bullet2Tag || ''}:</strong> ${f.draft?.bullet2 || ''}</li>` +
          `<li><strong>${f.draft?.bullet3Tag || ''}:</strong> ${f.draft?.bullet3 || ''}</li>` +
          `<li><strong>${f.draft?.bullet4Tag || ''}:</strong> ${f.draft?.bullet4 || ''}</li></ul>`;
      return [f.storeNumber, f.draft?.status || 'Pending', `"${html.replace(/"/g, '""')}"`];
    });

    const csvContent = [headers, ...rows].map(e => e.join(",")).join("\n");
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.setAttribute("download", "exr_pro_copy_expansion.csv");
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  let geo = null
  let demo = null
  if (activeFacility?.geoData) {
    try { geo = typeof activeFacility.geoData === 'string' ? JSON.parse(activeFacility.geoData) : activeFacility.geoData } catch(e){}
  }
  if (activeFacility?.demographicData) {
    try { demo = typeof activeFacility.demographicData === 'string' ? JSON.parse(activeFacility.demographicData) : activeFacility.demographicData } catch(e){}
  }

  return (
    <div className="flex h-screen bg-[#f3f4f6] text-gray-800 font-sans overflow-hidden">
      
      {/* SIDEBAR */}
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
            <input 
              type="text" 
              placeholder="Search store or city..." 
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-9 pr-4 py-2 bg-gray-50 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-green-500 focus:bg-white transition-all"
            />
          </div>
          
          <div className="flex gap-2">
            {['All', 'Pending', 'Approved'].map(f => (
              <button 
                key={f}
                onClick={() => setFilter(f)}
                className={`px-3 py-1.5 rounded-full text-xs font-semibold tracking-wide transition-colors ${
                  filter === f ? 'bg-green-700 text-white shadow-sm' : 'bg-gray-100 text-gray-500 hover:bg-gray-200'
                }`}
              >
                {f}
              </button>
            ))}
          </div>
        </div>

        <div className="flex-1 overflow-y-auto px-4 py-2 scrollbar-hide">
          {filteredFacilities.map((facility) => (
            <button
              key={facility.id}
              onClick={() => setActiveId(facility.id)}
              className={`w-full text-left p-4 rounded-xl mb-2 transition-all flex flex-col gap-2 ${
                activeId === facility.id 
                  ? 'bg-green-50 border-green-200 border shadow-sm ring-1 ring-green-500 ring-opacity-20' 
                  : 'bg-white hover:bg-gray-50 border border-transparent'
              }`}
            >
              <div className="flex justify-between items-center">
                <span className="font-bold text-gray-900">Store {facility.storeNumber}</span>
                {facility.draft?.status === 'Approved' ? (
                  <CheckCircle2 className="w-4 h-4 text-green-600" />
                ) : (
                  <Clock className="w-4 h-4 text-amber-500" />
                )}
              </div>
              <span className="text-xs text-gray-500 truncate">{facility.city}, {facility.state}</span>
            </button>
          ))}
        </div>
      </div>

      {/* MAIN CONTENT AREA */}
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
            <div className="flex gap-3">
               <span className={`px-4 py-2 rounded-lg text-sm font-bold flex items-center gap-2 ${
                  activeFacility.draft?.status === 'Approved' 
                  ? 'bg-green-100 text-green-800' : 'bg-amber-100 text-amber-800'
               }`}>
                 {activeFacility.draft?.status === 'Approved' ? 'Approved' : 'Needs Review'}
               </span>
               <button 
                onClick={handleExportCSV}
                className="bg-gray-100 hover:bg-gray-200 text-gray-700 px-4 py-2 rounded-lg text-sm font-bold border border-gray-200 flex items-center gap-2 transition-all"
               >
                 <Download size={16} /> Export All (CSV)
               </button>
               <button 
                onClick={handleSave}
                className="bg-green-700 hover:bg-green-800 text-white px-6 py-2 rounded-lg text-sm font-bold shadow-md shadow-green-700/20 transition-all border border-green-800 focus:ring-4 focus:ring-green-500/20"
               >
                 Save & Approve Code
               </button>
            </div>
          </div>

          <div className="flex-1 overflow-hidden flex">
            {/* Editor Side (Left) */}
            <div className="w-3/5 border-r border-gray-200 overflow-y-auto p-8 bg-white relative">
              
              {(!activeFacility.draft || !(activeFacility.draft as any).bullet1Tag) && (
                <div className="absolute inset-0 bg-white flex items-center justify-center z-20">
                  <div className="flex flex-col items-center max-w-sm text-center p-8">
                    <div className="w-16 h-16 rounded-3xl bg-amber-50 flex items-center justify-center mb-8 border border-amber-100 shadow-sm">
                      <Clock className="w-8 h-8 text-amber-500" />
                    </div>
                    <h3 className="text-2xl font-black text-gray-900 tracking-tight leading-loose">Expansion Content Scheduled</h3>
                    <p className="text-sm text-gray-500 mt-4 leading-relaxed tracking-wide">
                      This facility is scheduled for the next phase of the 879-store expansion. 
                      <span className="block mt-4 font-bold text-green-700">Please review one of the Top 30 stores for the 'Pro' content demo.</span>
                    </p>
                  </div>
                </div>
              )}

              <div className="space-y-6 max-w-2xl mx-auto">
                <div>
                   <label className="block text-xs font-bold text-gray-400 uppercase tracking-wider mb-2">Introductory Paragraph</label>
                   <textarea 
                     value={editForm.introParagraph}
                     onChange={(e) => setEditForm({...editForm, introParagraph: e.target.value})}
                     className="w-full p-4 bg-gray-50 border border-gray-200 rounded-xl text-gray-800 font-medium leading-relaxed resize-none h-32 focus:outline-none focus:ring-2 focus:ring-green-500 focus:bg-white transition-colors"
                   />
                </div>
                
                {[1, 2, 3, 4].map(num => {
                  return (
                    <div key={num} className="relative group">
                       <label className="block mb-2 flex items-center justify-between">
                         <div className="flex items-center gap-3">
                           <div className="w-6 h-6 rounded-full bg-green-100 flex items-center justify-center text-green-800 font-bold shrink-0" style={{ fontSize: '11px' }}>{num}</div>
                           <select 
                            value={(editForm as any)[`bullet${num}Tag`]}
                            onChange={(e) => setEditForm({...editForm, [`bullet${num}Tag`]: e.target.value})}
                            className="text-sm font-black text-green-700 uppercase tracking-tight bg-transparent border-none p-0 focus:ring-0 cursor-pointer hover:text-green-800"
                           >
                             <option value="">Select Category...</option>
                             {BULLET_CATEGORIES.map(cat => (
                               <option key={cat} value={cat}>{cat}</option>
                             ))}
                           </select>
                         </div>
                       </label>
                       <textarea 
                         value={(editForm as any)[`bullet${num}`]}
                         onChange={(e) => setEditForm({...editForm, [`bullet${num}`]: e.target.value})}
                         className="w-full p-4 bg-gray-50 border border-gray-200 rounded-xl text-gray-800 leading-relaxed resize-none h-28 focus:outline-none focus:ring-2 focus:ring-green-500 focus:bg-white transition-colors"
                       />
                    </div>
                  );
                })}

                {/* Production HTML Preview */}
                <div className="mt-12 pt-12 border-t border-gray-100">
                   <div className="flex justify-between items-center mb-4">
                     <label className="block text-xs font-bold text-gray-400 uppercase tracking-wider">Production HTML Output</label>
                     <button 
                      onClick={copyToClipboard}
                      className="text-xs font-bold text-green-700 hover:text-green-800 flex items-center gap-2"
                     >
                       {copySuccess ? <CheckCircle2 size={14} /> : <Clipboard size={14} />}
                       {copySuccess ? 'Copied!' : 'Copy Code'}
                     </button>
                   </div>
                   <textarea 
                     readOnly
                     value={generateHTML()}
                     className="w-full p-4 bg-gray-900 text-green-400 font-mono text-xs rounded-xl h-48 border border-gray-800 focus:outline-none"
                   />
                </div>
              </div>
            </div>

            {/* Context Side (Right) */}
            <div className="w-2/5 bg-gray-50 overflow-y-auto p-8 border-l border-gray-200 shadow-inner">
               <h3 className="text-xs font-black text-gray-400 uppercase tracking-widest mb-6">Source Context Data</h3>
               
               {geo ? (
                 <div className="space-y-4">
                    <div className="bg-white p-5 rounded-2xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow">
                      <div className="flex items-center gap-3 mb-4">
                         <div className="bg-blue-100 p-2 rounded-lg text-blue-700"><Car size={18} /></div>
                         <h4 className="font-bold text-gray-900">Transportation & Access</h4>
                      </div>
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <span className="text-xs text-gray-500 font-medium whitespace-nowrap">Interstate</span>
                          <p className="font-bold text-sm text-gray-800 truncate">{geo['nearest_interstate'] || 'N/A'}</p>
                          <p className="text-[10px] text-gray-400 font-bold uppercase">{Number(geo['interstate_distance_mi'] || 0).toFixed(2)} mi</p>
                        </div>
                        <div>
                          <span className="text-xs text-gray-500 font-medium whitespace-nowrap">Airport</span>
                          <p className="font-bold text-sm text-gray-800 truncate">{geo['nearest_airport'] || 'N/A'}</p>
                          <p className="text-[10px] text-gray-400 font-bold uppercase">{Number(geo['airport_distance_mi'] || 0).toFixed(2)} mi</p>
                        </div>
                        {geo['nearest_major_intersection'] && (
                          <div className="col-span-2 pt-2 border-t border-gray-100">
                            <span className="text-xs text-gray-500 font-medium flex items-center gap-1"><Crosshair size={10} /> Nearest Major Intersection</span>
                            <p className="font-bold text-sm text-gray-800 mt-1">{geo['nearest_major_intersection']}</p>
                          </div>
                        )}
                      </div>
                    </div>

                    {geo['Second City Draw'] && (
                      <div className="bg-white p-5 rounded-2xl border border-gray-200 shadow-sm border-l-4 border-l-blue-500">
                        <div className="flex items-center gap-3 mb-2">
                           <div className="bg-blue-50 p-2 rounded-lg text-blue-600"><Map size={18} /></div>
                           <h4 className="font-bold text-gray-900 text-sm">Regional Market Draw</h4>
                        </div>
                        <p className="text-xs text-gray-600 leading-relaxed font-medium">
                          Confirmed traffic draw from <span className="text-blue-700 font-bold">{geo['Second City Draw']}</span>. 
                          Writers should acknowledge this community in the intro or bullets.
                        </p>
                      </div>
                    )}

                    <div className="bg-white p-5 rounded-2xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow">
                      <div className="flex items-center gap-3 mb-4">
                         <div className="bg-purple-100 p-2 rounded-lg text-purple-700"><Landmark size={18} /></div>
                         <h4 className="font-bold text-gray-900">Demographics & Insights</h4>
                      </div>
                      <div className="space-y-4">
                        <div className="flex justify-between items-start">
                          <div>
                            <span className="text-xs text-gray-500 font-medium block mb-1">Target Persona</span>
                            <div className="inline-block px-3 py-1 bg-purple-50 text-purple-800 rounded-md font-bold text-xs border border-purple-100">
                               {demo?.persona || geo['Demographic Persona'] || 'General'}
                            </div>
                          </div>
                          {(geo['Home Share'] > 0.1) && (
                            <div className="text-right">
                              <span className="text-xs text-gray-500 font-medium block mb-1">Customer Mix</span>
                              <div className="inline-block px-3 py-1 bg-green-50 text-green-800 rounded-md font-bold text-xs border border-green-100">
                                 {geo['Home Zip']} ({Math.round(geo['Home Share'] * 100)}%)
                              </div>
                            </div>
                          )}
                        </div>
                        <div>
                          <span className="text-xs text-gray-500 font-medium">Strategic Hook</span>
                          <p className="font-semibold text-[13px] text-gray-700 mt-1 leading-relaxed italic">"{geo['Content Hook (Bullet 2)'] || geo['Demographic Content Hook'] || 'N/A'}"</p>
                        </div>
                      </div>
                    </div>

                    <div className="bg-white p-5 rounded-2xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow">
                      <div className="flex items-center gap-3 mb-4">
                         <div className="bg-orange-100 p-2 rounded-lg text-orange-700"><MapPin size={18} /></div>
                         <h4 className="font-bold text-gray-900">POI Proximity Audit</h4>
                      </div>
                      <div className="space-y-2">
                        {[
                          { label: 'University', value: geo['nearest_university_verified'] || geo['nearest_university'], dist: geo['university_distance_mi'] },
                          { label: 'Military Base', value: geo['nearest_military_base'], dist: geo['military_base_distance_mi'] },
                          { label: 'High School', value: geo['nearest_school_verified'] },
                          { label: 'Reg. Park', value: geo['nearest_park_verified'] },
                          { label: 'Hospital', value: geo['nearest_hospital_verified'] },
                          { label: 'Stadium', value: geo['nearest_stadium_verified'] },
                        ].filter(item => item.value && item.value !== 'None').map((item, idx) => (
                          <div key={idx} className="flex justify-between items-center py-2 border-b border-gray-50 last:border-0">
                            <div>
                              <span className="text-[10px] uppercase text-gray-400 font-black block">{item.label}</span>
                              <span className="text-sm font-bold text-gray-800">{item.value}</span>
                            </div>
                            {item.dist && (
                              <span className="text-[10px] font-bold text-gray-500">{Number(item.dist).toFixed(1)} mi</span>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
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
