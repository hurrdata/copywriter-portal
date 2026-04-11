'use client'

import { useState, useEffect } from 'react'
import { Search, MapPin, CheckCircle2, Clock, Map, Navigation, Car, Landmark } from 'lucide-react'
import { updateDraftStatus, saveDraftContent } from '@/app/actions'

export default function Dashboard({ initialFacilities }: { initialFacilities: any[] }) {
  const [facilities, setFacilities] = useState(initialFacilities)
  const [activeId, setActiveId] = useState<number | null>(initialFacilities[0]?.id || null)
  const [searchTerm, setSearchTerm] = useState('')
  const [filter, setFilter] = useState('All') // All, Pending, Approved

  const activeFacility = facilities.find(f => f.id === activeId)
  
  // Local state for the editor forms to allow real-time typing
  const [editForm, setEditForm] = useState({
    introParagraph: '',
    bullet1: '',
    bullet2: '',
    bullet3: '',
    bullet4: ''
  })

  // Sync editor when switching facilities
  useEffect(() => {
    if (activeFacility?.draft) {
      setEditForm({
        introParagraph: activeFacility.draft.introParagraph || '',
        bullet1: activeFacility.draft.bullet1 || '',
        bullet2: activeFacility.draft.bullet2 || '',
        bullet3: activeFacility.draft.bullet3 || '',
        bullet4: activeFacility.draft.bullet4 || ''
      })
    } else {
      setEditForm({
        introParagraph: '', bullet1: '', bullet2: '', bullet3: '', bullet4: ''
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
      // Numeric sort for store numbers (101, 102, 1013)
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

  // Parse Geodata from DB safely
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
          {filteredFacilities.length === 0 && (
            <div className="text-center text-gray-400 text-sm mt-10">No locations found.</div>
          )}
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
              
              {!activeFacility.draft && (
                <div className="absolute inset-0 bg-white/80 backdrop-blur-sm flex items-center justify-center z-10">
                  <div className="bg-white p-6 rounded-2xl shadow-xl flex flex-col items-center border border-gray-100">
                    <Clock className="w-12 h-12 text-gray-300 mb-4 animate-pulse" />
                    <h3 className="text-lg font-bold">AI Generation Pending</h3>
                    <p className="text-sm text-gray-500 mt-2">The background script hasn't reached this store yet.</p>
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
                
                {[1, 2, 3, 4].map(num => (
                  <div key={num} className="relative group">
                     <label className="block text-xs font-bold text-green-700 uppercase tracking-wider mb-2 flex items-center gap-2">
                       <div className="w-5 h-5 rounded-full bg-green-100 flex items-center justify-center text-green-800" style={{ fontSize: '10px' }}>{num}</div>
                       Bullet {num}
                     </label>
                     <textarea 
                       value={(editForm as any)[`bullet${num}`]}
                       onChange={(e) => setEditForm({...editForm, [`bullet${num}`]: e.target.value})}
                       className="w-full p-4 bg-gray-50 border border-gray-200 rounded-xl text-gray-800 leading-relaxed resize-none h-28 focus:outline-none focus:ring-2 focus:ring-green-500 focus:bg-white transition-colors"
                     />
                  </div>
                ))}
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
                        <h4 className="font-bold text-gray-900">Transportation</h4>
                     </div>
                     <div className="grid grid-cols-2 gap-4">
                       <div>
                         <span className="text-xs text-gray-500 font-medium">Nearest Interstate</span>
                         <p className="font-bold text-sm text-gray-800">{geo['nearest_interstate'] || 'N/A'}</p>
                       </div>
                       <div>
                         <span className="text-xs text-gray-500 font-medium">Distance</span>
                         <p className="font-bold text-sm text-gray-800">{Number(geo['interstate_distance_mi'] || 0).toFixed(2)} mi</p>
                       </div>
                       <div className="col-span-2 pt-2 border-t border-gray-100">
                         <span className="text-xs text-gray-500 font-medium">Nearest Airport</span>
                         <p className="font-bold text-sm text-gray-800">{geo['nearest_airport'] || 'N/A'} <span className="text-gray-400 font-normal">({Number(geo['airport_distance_mi'] || 0).toFixed(2)} mi)</span></p>
                       </div>
                     </div>
                   </div>

                   <div className="bg-white p-5 rounded-2xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow">
                     <div className="flex items-center gap-3 mb-4">
                        <div className="bg-purple-100 p-2 rounded-lg text-purple-700"><Landmark size={18} /></div>
                        <h4 className="font-bold text-gray-900">Demographics & Insights</h4>
                     </div>
                     <div className="space-y-4">
                       <div>
                         <span className="text-xs text-gray-500 font-medium block mb-1">Target Persona</span>
                         <div className="inline-block px-3 py-1 bg-purple-50 text-purple-800 rounded-md font-bold text-sm border border-purple-100">
                            {demo?.persona || geo['Demographic Persona'] || 'General'}
                         </div>
                       </div>
                       <div>
                         <span className="text-xs text-gray-500 font-medium">Content Hook</span>
                         <p className="font-semibold text-sm text-gray-800 mt-1">{geo['Content Hook (Bullet 2)'] || 'N/A'}</p>
                       </div>
                       <div>
                         <span className="text-xs text-gray-500 font-medium block">County</span>
                         <p className="font-bold text-sm text-gray-800">{geo['county_name'] || 'N/A'}</p>
                       </div>
                     </div>
                   </div>
                   
                   <div className="bg-white p-5 rounded-2xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow">
                     <div className="flex items-center gap-3 mb-4">
                        <div className="bg-orange-100 p-2 rounded-lg text-orange-700"><Map size={18} /></div>
                        <h4 className="font-bold text-gray-900">Local Anchors</h4>
                     </div>
                     <div className="space-y-3">
                       <div className="flex justify-between items-center pb-2 border-b border-gray-50">
                         <span className="text-sm font-medium text-gray-600">University</span>
                         <span className="text-sm font-bold text-gray-900">{geo['nearest_university'] || 'None'}</span>
                       </div>
                       <div className="flex justify-between items-center">
                         <span className="text-sm font-medium text-gray-600">Military Base</span>
                         <span className="text-sm font-bold text-gray-900">{geo['nearest_military_base'] || 'None'}</span>
                       </div>
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
