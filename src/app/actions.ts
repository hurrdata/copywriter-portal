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

export async function saveDraftContent(facilityId: number, data: {
  introParagraph: string,
  bullet1: string,
  bullet1Tag?: string,
  bullet2: string,
  bullet2Tag?: string,
  bullet3: string,
  bullet3Tag?: string,
  bullet4: string,
  bullet4Tag?: string
}, writerName?: string) {
  try {
    await prisma.copyDraft.update({
      where: { facilityId: facilityId },
      data: {
        introParagraph: data.introParagraph,
        bullet1: data.bullet1,
        bullet1Tag: data.bullet1Tag,
        bullet2: data.bullet2,
        bullet2Tag: data.bullet2Tag,
        bullet3: data.bullet3,
        bullet3Tag: data.bullet3Tag,
        bullet4: data.bullet4,
        bullet4Tag: data.bullet4Tag,
        status: 'Approved', // Auto approve on manual save
        approvedBy: writerName || 'Unknown Writer'
      },
    })
    revalidatePath('/')
  } catch (error) {
    console.error("Failed to save draft content for facility:", facilityId, error)
    throw new Error("Failed to save changes. Please try again.")
  }
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
