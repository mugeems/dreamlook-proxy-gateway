# DreamLook Proxy Gateway

Proxy gateway service that routes Flowith API requests through 922proxy for IP rotation.

## Purpose

Cloudflare Workers cannot use HTTP/SOCKS5 proxies directly. This gateway acts as an intermediary:

```
Cloudflare Worker → Railway (Proxy Gateway) → 922proxy → Flowith
```

This ensures Flowith sees rotating residential IPs instead of Cloudflare's static IPs.

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/proxy` | POST | Forward single request through proxy |
| `/proxy/batch` | POST | Forward multiple requests in parallel |
| `/health` | GET | Health check |
| `/test-proxy` | GET | Test proxy connection and get current IP |

## Usage

### Single Request

```javascript
const response = await fetch('https://your-gateway.railway.app/proxy', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'X-Gateway-Secret': 'your-secret-key'
  },
  body: JSON.stringify({
    url: 'https://edge.flowith.net/user/credits',
    method: 'GET',
    headers: {
      'Authorization': 'Bearer your-token',
      'User-Agent': 'Mozilla/5.0...'
    }
  })
});

const data = await response.json();
// data.ok, data.status, data.body
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GATEWAY_SECRET` | Secret key for authenticating requests |
| `PROXY_HOST` | 922proxy hostname |
| `PROXY_PORT` | 922proxy port |
| `PROXY_USER` | 922proxy username |
| `PROXY_PASS` | 922proxy password |
| `PORT` | Server port (set by Railway) |

## Deploy to Railway

```bash
railway login
railway init
railway up
```

Then set environment variables in Railway dashboard.
