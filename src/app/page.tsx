import Dashboard from '@/components/Dashboard'
import { getFacilitiesWithDrafts } from './actions'

export const dynamic = 'force-dynamic'

export default async function Home() {
  const facilities = await getFacilitiesWithDrafts()
  
  return (
    <main className="min-h-screen bg-white">
      <Dashboard initialFacilities={facilities} />
    </main>
  )
}
