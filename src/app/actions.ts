'use server'

import { prisma } from '@/lib/prisma'
import { revalidatePath } from 'next/cache'

export async function getFacilitiesWithDrafts() {
  return await prisma.facility.findMany({
    include: {
      draft: true,
    },
    orderBy: {
      storeNumber: 'asc',
    },
  })
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
}) {
  await prisma.copyDraft.update({
    where: { id: draftId },
    data: {
      introParagraph: data.introParagraph,
      bullet1: data.bullet1,
      bullet2: data.bullet2,
      bullet3: data.bullet3,
      bullet4: data.bullet4,
      status: 'Approved' // Auto approve on manual save
    },
  })
  revalidatePath('/')
}
