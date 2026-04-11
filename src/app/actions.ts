'use server'

import { prisma } from '@/lib/prisma'
import { revalidatePath } from 'next/cache'

export async function getFacilitiesWithDrafts() {
  const facilities = await prisma.facility.findMany({
    include: {
      draft: true,
    },
    orderBy: {
      storeNumber: 'asc',
    },
  })
  
  // Debug log for Vercel logs to confirm schema visibility
  if (facilities[0]?.draft) {
    console.log("DB Sample Check (#102):", facilities.find(f => f.storeNumber === '102')?.draft);
  }
  
  return facilities
}

export async function updateDraftStatus(draftId: number, status: string) {
  await prisma.copyDraft.update({
    where: { id: draftId },
    data: { status },
  })
  revalidatePath('/')
}

export async function saveDraftContent(draftId: number, data: {
  introParagraph: string,
  bullet1: string,
  bullet2: string,
  bullet3: string,
  bullet4: string
}, writerName?: string) {
  await prisma.copyDraft.update({
    where: { id: draftId },
    data: {
      introParagraph: data.introParagraph,
      bullet1: data.bullet1,
      bullet2: data.bullet2,
      bullet3: data.bullet3,
      bullet4: data.bullet4,
      status: 'Approved', // Auto approve on manual save
      approvedBy: writerName || 'Unknown Writer'
    },
  })
  revalidatePath('/')
}

export async function verifyPassword(name: string, pwd: string) {
  if (pwd === process.env.AUTH_PASS) {
    const { cookies } = await import('next/headers')
    // Set cookie expiry for 30 days
    const THIRTY_DAYS = 30 * 24 * 60 * 60 * 1000
    const cookieStore = await cookies()
    cookieStore.set('exr_auth', 'true', { expires: Date.now() + THIRTY_DAYS })
    cookieStore.set('exr_writer_name', name, { expires: Date.now() + THIRTY_DAYS })
    return true
  }
  return false
}
