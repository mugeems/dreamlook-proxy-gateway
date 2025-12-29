/**
 * DreamLook Proxy Gateway
 * Routes Flowith API requests through 922proxy for IP rotation
 * 
 * Deploy to Railway for production use
 */

const express = require('express');
const fetch = require('node-fetch');
const { HttpsProxyAgent } = require('https-proxy-agent');

const app = express();

// Handle large payloads (for image uploads, etc.)
app.use(express.json({ limit: '50mb' }));
app.use(express.text({ limit: '50mb' }));

// Environment variables
const GATEWAY_SECRET = process.env.GATEWAY_SECRET || 'dreamlook-gateway-secret-2024';
const PROXY_HOST = process.env.PROXY_HOST || 'na.proxys5.net';
const PROXY_PORT = process.env.PROXY_PORT || '6200';
const PROXY_USER = process.env.PROXY_USER || '15012685-zone-custom-region-US';
const PROXY_PASS = process.env.PROXY_PASS || 'iEw5guU4';

// Build proxy URL
const PROXY_URL = `http://${PROXY_USER}:${PROXY_PASS}@${PROXY_HOST}:${PROXY_PORT}`;

// Request counter for logging
let requestCounter = 0;

/**
 * Log with timestamp and request ID
 */
function log(requestId, message, data = null) {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] [#${requestId}] ${message}`;
    if (data) {
        console.log(logMessage, typeof data === 'object' ? JSON.stringify(data).substring(0, 500) : data);
    } else {
        console.log(logMessage);
    }
}

/**
 * Main proxy endpoint
 * Receives request details from Cloudflare Worker and forwards via 922proxy
 */
app.post('/proxy', async (req, res) => {
    const requestId = ++requestCounter;
    const startTime = Date.now();

    // Verify request from Worker
    const authHeader = req.headers['x-gateway-secret'];
    if (authHeader !== GATEWAY_SECRET) {
        log(requestId, '❌ Unauthorized request');
        return res.status(401).json({ error: 'Unauthorized' });
    }

    const { url, method, headers, body } = req.body;

    if (!url) {
        log(requestId, '❌ Missing URL in request');
        return res.status(400).json({ error: 'URL is required' });
    }

    log(requestId, `📤 ${method || 'GET'} ${url}`);
    log(requestId, `   User-Agent: ${headers?.['User-Agent'] || headers?.['user-agent'] || 'none'}`);

    try {
        // Create proxy agent with 922proxy credentials
        const agent = new HttpsProxyAgent(PROXY_URL);

        // Prepare request body
        let requestBody = null;
        if (body) {
            requestBody = typeof body === 'string' ? body : JSON.stringify(body);
        }

        // Forward request with ALL headers from Worker
        const response = await fetch(url, {
            method: method || 'GET',
            headers: headers || {},
            body: requestBody,
            agent: agent,
            timeout: 120000 // 2 minute timeout
        });

        const responseText = await response.text();
        const duration = Date.now() - startTime;

        // Collect response headers
        const responseHeaders = {};
        response.headers.forEach((value, key) => {
            responseHeaders[key] = value;
        });

        log(requestId, `📥 Response: ${response.status} ${response.statusText} (${duration}ms)`);

        // Return response to Worker
        res.json({
            ok: response.ok,
            status: response.status,
            statusText: response.statusText,
            headers: responseHeaders,
            body: responseText
        });

    } catch (error) {
        const duration = Date.now() - startTime;
        log(requestId, `❌ Error after ${duration}ms: ${error.message}`);

        res.status(500).json({
            error: error.message,
            code: error.code || 'UNKNOWN',
            duration: duration
        });
    }
});

/**
 * Batch proxy endpoint for multiple requests
 * Useful for parallel polling
 */
app.post('/proxy/batch', async (req, res) => {
    const requestId = ++requestCounter;
    const startTime = Date.now();

    // Verify request from Worker
    const authHeader = req.headers['x-gateway-secret'];
    if (authHeader !== GATEWAY_SECRET) {
        log(requestId, '❌ Unauthorized batch request');
        return res.status(401).json({ error: 'Unauthorized' });
    }

    const { requests } = req.body;

    if (!Array.isArray(requests) || requests.length === 0) {
        log(requestId, '❌ Invalid batch request');
        return res.status(400).json({ error: 'requests array is required' });
    }

    log(requestId, `📤 Batch request with ${requests.length} items`);

    try {
        const agent = new HttpsProxyAgent(PROXY_URL);

        const results = await Promise.all(
            requests.map(async (reqItem, index) => {
                try {
                    const { url, method, headers, body } = reqItem;

                    let requestBody = null;
                    if (body) {
                        requestBody = typeof body === 'string' ? body : JSON.stringify(body);
                    }

                    const response = await fetch(url, {
                        method: method || 'GET',
                        headers: headers || {},
                        body: requestBody,
                        agent: agent,
                        timeout: 60000
                    });

                    const responseText = await response.text();
                    const responseHeaders = {};
                    response.headers.forEach((value, key) => {
                        responseHeaders[key] = value;
                    });

                    return {
                        index,
                        ok: response.ok,
                        status: response.status,
                        statusText: response.statusText,
                        headers: responseHeaders,
                        body: responseText
                    };
                } catch (err) {
                    return {
                        index,
                        ok: false,
                        error: err.message
                    };
                }
            })
        );

        const duration = Date.now() - startTime;
        log(requestId, `📥 Batch completed in ${duration}ms`);

        res.json({ results });

    } catch (error) {
        const duration = Date.now() - startTime;
        log(requestId, `❌ Batch error after ${duration}ms: ${error.message}`);

        res.status(500).json({
            error: error.message,
            duration: duration
        });
    }
});

/**
 * Health check endpoint
 */
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        requests_processed: requestCounter,
        proxy_configured: !!PROXY_HOST
    });
});

/**
 * Sticky Proxy endpoint for Referral Booster
 * Uses custom proxy credentials per request for session persistence
 * Request body should include proxyConfig with host, port, username, password
 */
app.post('/proxy-sticky', async (req, res) => {
    const requestId = ++requestCounter;
    const startTime = Date.now();

    // Verify request from Worker
    const authHeader = req.headers['x-gateway-secret'];
    if (authHeader !== GATEWAY_SECRET) {
        log(requestId, '❌ Unauthorized sticky proxy request');
        return res.status(401).json({ error: 'Unauthorized' });
    }

    const { url, method, headers, body, proxyConfig } = req.body;

    if (!url) {
        log(requestId, '❌ Missing URL in sticky proxy request');
        return res.status(400).json({ error: 'URL is required' });
    }

    if (!proxyConfig || !proxyConfig.host || !proxyConfig.username || !proxyConfig.password) {
        log(requestId, '❌ Missing proxyConfig in sticky proxy request');
        return res.status(400).json({ error: 'proxyConfig with host, port, username, password is required' });
    }

    // Build custom proxy URL from provided config
    const customProxyUrl = `http://${proxyConfig.username}:${proxyConfig.password}@${proxyConfig.host}:${proxyConfig.port || 6200}`;

    log(requestId, `📤 [STICKY] ${method || 'GET'} ${url}`);
    log(requestId, `   Proxy Session: ${proxyConfig.username.substring(0, 30)}...`);
    log(requestId, `   User-Agent: ${headers?.['User-Agent'] || headers?.['user-agent'] || 'none'}`);

    try {
        // Create proxy agent with custom sticky session credentials
        const agent = new HttpsProxyAgent(customProxyUrl);

        // Prepare request body
        let requestBody = null;
        if (body) {
            requestBody = typeof body === 'string' ? body : JSON.stringify(body);
        }

        // Forward request with ALL headers from Worker
        const response = await fetch(url, {
            method: method || 'GET',
            headers: headers || {},
            body: requestBody,
            agent: agent,
            timeout: 120000 // 2 minute timeout
        });

        const responseText = await response.text();
        const duration = Date.now() - startTime;

        // Collect response headers
        const responseHeaders = {};
        response.headers.forEach((value, key) => {
            responseHeaders[key] = value;
        });

        log(requestId, `📥 [STICKY] Response: ${response.status} ${response.statusText} (${duration}ms)`);

        // Return response to Worker
        res.json({
            ok: response.ok,
            status: response.status,
            statusText: response.statusText,
            headers: responseHeaders,
            body: responseText
        });

    } catch (error) {
        const duration = Date.now() - startTime;
        log(requestId, `❌ [STICKY] Error after ${duration}ms: ${error.message}`);

        res.status(500).json({
            error: error.message,
            code: error.code || 'UNKNOWN',
            duration: duration
        });
    }
});

/**
 * Test proxy connection
 */
app.get('/test-proxy', async (req, res) => {
    const requestId = ++requestCounter;
    log(requestId, '🧪 Testing proxy connection...');

    try {
        const agent = new HttpsProxyAgent(PROXY_URL);

        const response = await fetch('https://api.ipify.org?format=json', {
            agent: agent,
            timeout: 30000
        });

        const data = await response.json();
        log(requestId, `✅ Proxy test successful, IP: ${data.ip}`);

        res.json({
            status: 'ok',
            proxy_ip: data.ip,
            message: 'Proxy connection successful'
        });

    } catch (error) {
        log(requestId, `❌ Proxy test failed: ${error.message}`);

        res.status(500).json({
            status: 'error',
            error: error.message,
            message: 'Proxy connection failed'
        });
    }
});

/**
 * Root endpoint
 */
app.get('/', (req, res) => {
    res.json({
        name: 'DreamLook Proxy Gateway',
        version: '1.1.0',
        endpoints: {
            '/proxy': 'POST - Forward single request through proxy',
            '/proxy-sticky': 'POST - Forward request with custom sticky proxy config',
            '/proxy/batch': 'POST - Forward multiple requests through proxy',
            '/health': 'GET - Health check',
            '/test-proxy': 'GET - Test proxy connection'
        }
    });
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log('='.repeat(60));
    console.log('DreamLook Proxy Gateway');
    console.log('='.repeat(60));
    console.log(`Server running on port ${PORT}`);
    console.log(`Proxy Host: ${PROXY_HOST}:${PROXY_PORT}`);
    console.log(`Proxy User: ${PROXY_USER.substring(0, 10)}...`);
    console.log('='.repeat(60));
});
