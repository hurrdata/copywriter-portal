import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

export function middleware(req: NextRequest) {
  const basicAuth = req.headers.get('authorization')
  const url = req.nextUrl

  // Require auth by default for all routes
  if (basicAuth) {
    const authValue = basicAuth.split(' ')[1]
    const [user, pwd] = atob(authValue).split(':')

    if (user === process.env.AUTH_USER && pwd === process.env.AUTH_PASS) {
      return NextResponse.next()
    }
  }

  url.pathname = '/api/auth'
  return new NextResponse('Auth required', {
    status: 401,
    headers: { 'WWW-Authenticate': 'Basic realm="Secure Area"' },
  })
}

// Config to protect all routes (ignore next static files and images)
export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
}
