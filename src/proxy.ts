import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

export function middleware(req: NextRequest) {
  const url = req.nextUrl
  const isAuthenticated = req.cookies.has('exr_auth')

  // Allow access to the login page and Next.js static assets
  if (url.pathname === '/login' || url.pathname.startsWith('/api/') || url.pathname.startsWith('/_next/')) {
    return NextResponse.next()
  }

  // Redirect to login page instantly if the cookie is missing
  if (!isAuthenticated) {
    url.pathname = '/login'
    return NextResponse.redirect(url)
  }

  return NextResponse.next()
}

export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
}
