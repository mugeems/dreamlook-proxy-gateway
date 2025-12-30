/**
 * DreamLook Batch Processor
 * Runs on Render to process batch generation jobs
 * 
 * This module:
 * - Polls Cloudflare Workers API every 10 seconds for pending jobs
 * - Processes up to 15 jobs in parallel
 * - Submits to Flowith via 922proxy for IP rotation
 * - Polls for results (max 5 minutes per job)
 * - Updates job status via Workers API
 * 
 * STEALTH FEATURES:
 * - Consistent User-Agent per Flowith account
 * - All requests go through 922proxy (residential IPs)
 * - Human-like delays between requests
 * - Proper browser headers (Origin, Referer, sec-ch-ua)
 */

const fetch = require('node-fetch');
const { HttpsProxyAgent } = require('https-proxy-agent');

// ==========================================
// CONFIGURATION
// ==========================================

const CONFIG = {
    // Workers API
    API_URL: process.env.DREAMLOOK_API_URL || 'https://dreamlook-backend.anthrasite.workers.dev',
    INTERNAL_SECRET: process.env.INTERNAL_BATCH_SECRET || '',

    // Batch processing
    CONCURRENT_JOBS: 15,          // Max parallel jobs
    POLL_INTERVAL: 10000,         // Poll for new jobs every 10 seconds

    // Flowith polling
    FLOWITH_POLL_INTERVAL: 5000,  // Poll Flowith every 5 seconds
    MAX_POLL_ATTEMPTS: 60,        // Max 60 x 5s = 5 minutes
    JOB_TIMEOUT: 300000,          // 5 minutes per job

    // Proxy (922proxy)
    PROXY_HOST: process.env.PROXY_HOST || 'na.proxys5.net',
    PROXY_PORT: process.env.PROXY_PORT || '6200',
    PROXY_USER: process.env.PROXY_USER || '15012685-zone-custom-region-US',
    PROXY_PASS: process.env.PROXY_PASS || 'iEw5guU4',

    // Retry
    MAX_REFUND_RETRIES: 10,       // Retry refund API up to 10 times
    RETRY_DELAY_BASE: 2000,       // Base delay for exponential backoff
};

// Build proxy URL
const PROXY_URL = `http://${CONFIG.PROXY_USER}:${CONFIG.PROXY_PASS}@${CONFIG.PROXY_HOST}:${CONFIG.PROXY_PORT}`;

// ==========================================
// STEALTH UTILITIES
// ==========================================

// User-Agent pool (same as backend)
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
];

/**
 * Get consistent User-Agent based on account ID (same algorithm as backend)
 */
function getConsistentUserAgent(accountId) {
    if (!accountId) return USER_AGENTS[0];

    let hash = 0;
    for (let i = 0; i < accountId.length; i++) {
        const char = accountId.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash;
    }

    const index = Math.abs(hash) % USER_AGENTS.length;
    return USER_AGENTS[index];
}

/**
 * Extract Chrome version from User-Agent
 */
function parseChromeVersion(userAgent) {
    const match = userAgent.match(/Chrome\/(\d+)/);
    return match ? match[1] : '124';
}

/**
 * Detect platform from User-Agent
 */
function parsePlatform(userAgent) {
    if (userAgent.includes('Windows')) return 'Windows';
    if (userAgent.includes('Macintosh')) return 'macOS';
    if (userAgent.includes('Linux')) return 'Linux';
    return 'Windows';
}

/**
 * Generate sec-ch-ua header
 */
function generateSecChUa(version) {
    const verNum = parseInt(version);
    if (verNum >= 124) {
        return `"Chromium";v="${version}", "Not.A/Brand";v="8", "Google Chrome";v="${version}"`;
    }
    return `"Not_A Brand";v="24", "Chromium";v="${version}", "Google Chrome";v="${version}"`;
}

/**
 * Generate stealth headers for Flowith
 */
function generateStealthHeaders(token, userAgent) {
    const version = parseChromeVersion(userAgent);
    const platform = parsePlatform(userAgent);
    const isChrome = userAgent.includes('Chrome') && !userAgent.includes('Edg');

    const headers = {
        'User-Agent': userAgent,
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/plain, */*',
        'Origin': 'https://flowith.io',
        'Referer': 'https://flowith.io/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'DNT': '1',
        'Priority': 'u=1, i',
    };

    if (isChrome) {
        headers['sec-ch-ua'] = generateSecChUa(version);
        headers['sec-ch-ua-mobile'] = '?0';
        headers['sec-ch-ua-platform'] = `"${platform}"`;
    }

    if (token) {
        headers['Authorization'] = token.startsWith('Bearer ') ? token : `Bearer ${token}`;
    }

    return headers;
}

/**
 * Random delay (human-like)
 */
function getRandomDelay(min = 500, max = 1500) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Sleep utility
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ==========================================
// LOGGING
// ==========================================

function log(message, data = null) {
    const timestamp = new Date().toISOString();
    if (data) {
        console.log(`[${timestamp}] [BatchProcessor] ${message}`, typeof data === 'object' ? JSON.stringify(data).substring(0, 200) : data);
    } else {
        console.log(`[${timestamp}] [BatchProcessor] ${message}`);
    }
}

// ==========================================
// STATS & MEMORY MONITORING
// ==========================================

const stats = {
    startTime: null,
    totalProcessed: 0,
    totalCompleted: 0,
    totalRefunded: 0,
    totalErrors: 0,
    currentlyProcessing: 0,
    lastPollTime: null,
    // Memory tracking
    peakMemoryUsed: 0,
    peakMemoryTime: null,
    memoryHistory: [], // Last 30 samples for trend
};

/**
 * Get current memory usage in MB
 */
function getMemoryUsage() {
    const mem = process.memoryUsage();
    return {
        heapUsed: Math.round(mem.heapUsed / 1024 / 1024 * 100) / 100,  // MB
        heapTotal: Math.round(mem.heapTotal / 1024 / 1024 * 100) / 100,  // MB
        external: Math.round(mem.external / 1024 / 1024 * 100) / 100,  // MB
        rss: Math.round(mem.rss / 1024 / 1024 * 100) / 100,  // MB (Resident Set Size - total memory allocated)
        arrayBuffers: Math.round((mem.arrayBuffers || 0) / 1024 / 1024 * 100) / 100,  // MB
    };
}

/**
 * Update peak memory tracking
 */
function updateMemoryStats() {
    const mem = getMemoryUsage();
    const now = new Date().toISOString();

    // Update peak memory
    if (mem.rss > stats.peakMemoryUsed) {
        stats.peakMemoryUsed = mem.rss;
        stats.peakMemoryTime = now;
    }

    // Keep last 30 samples for trend analysis
    stats.memoryHistory.push({
        timestamp: now,
        rss: mem.rss,
        heapUsed: mem.heapUsed,
        processing: stats.currentlyProcessing
    });

    if (stats.memoryHistory.length > 30) {
        stats.memoryHistory.shift();
    }

    return mem;
}

/**
 * Calculate memory trend (increasing/stable/decreasing)
 */
function getMemoryTrend() {
    if (stats.memoryHistory.length < 5) return 'insufficient_data';

    const recent = stats.memoryHistory.slice(-5);
    const older = stats.memoryHistory.slice(-10, -5);

    if (older.length === 0) return 'insufficient_data';

    const recentAvg = recent.reduce((sum, m) => sum + m.rss, 0) / recent.length;
    const olderAvg = older.reduce((sum, m) => sum + m.rss, 0) / older.length;

    const diff = ((recentAvg - olderAvg) / olderAvg) * 100;

    if (diff > 10) return '📈 INCREASING';
    if (diff < -10) return '📉 DECREASING';
    return '📊 STABLE';
}

function getStats() {
    const mem = getMemoryUsage();
    const renderFreeLimit = 512; // Render free tier = 512MB

    return {
        ...stats,
        uptime: stats.startTime ? Math.floor((Date.now() - stats.startTime) / 1000) : 0,
        status: stats.currentlyProcessing > 0 ? 'processing' : 'idle',
        memory: {
            current: mem,
            peak: {
                rss: stats.peakMemoryUsed,
                recordedAt: stats.peakMemoryTime
            },
            trend: getMemoryTrend(),
            renderLimit: renderFreeLimit,
            usagePercent: Math.round((mem.rss / renderFreeLimit) * 100),
            peakPercent: Math.round((stats.peakMemoryUsed / renderFreeLimit) * 100),
            available: Math.round((renderFreeLimit - mem.rss) * 100) / 100,
            warning: mem.rss > (renderFreeLimit * 0.8) ? '⚠️ HIGH MEMORY USAGE!' : null
        },
        recentMemoryHistory: stats.memoryHistory.slice(-10) // Last 10 samples
    };
}

// ==========================================
// ERROR MESSAGE TRANSFORMATION
// ==========================================

/**
 * Transform technical error messages to user-friendly messages
 * This prevents exposing internal URLs and technical details to users
 */
function getUserFriendlyErrorMessage(technicalError) {
    const errorLower = (technicalError || '').toLowerCase();

    // Network/Connection errors
    if (errorLower.includes('socket hang up') ||
        errorLower.includes('socket disconnected') ||
        errorLower.includes('econnreset') ||
        errorLower.includes('econnrefused')) {
        return 'Connection interrupted during generation. Credits refunded.';
    }

    if (errorLower.includes('tls') ||
        errorLower.includes('ssl') ||
        errorLower.includes('secure connection')) {
        return 'Secure connection failed. Please try again. Credits refunded.';
    }

    if (errorLower.includes('timeout') ||
        errorLower.includes('timed out') ||
        errorLower.includes('etimedout')) {
        return 'Generation took too long and was cancelled. Credits refunded.';
    }

    if (errorLower.includes('network') ||
        errorLower.includes('fetch failed') ||
        errorLower.includes('enotfound')) {
        return 'Network error occurred. Please try again. Credits refunded.';
    }

    // Flowith/Generation errors
    if (errorLower.includes('insufficient_credits') ||
        errorLower.includes('insufficient credits')) {
        return 'Service temporarily busy. Credits refunded.';
    }

    if (errorLower.includes('accounts exhausted') ||
        errorLower.includes('no available')) {
        return 'Service temporarily at capacity. Credits refunded.';
    }

    if (errorLower.includes('content') &&
        (errorLower.includes('moderation') ||
            errorLower.includes('filter') ||
            errorLower.includes('blocked'))) {
        return 'Your prompt was blocked by content filter. Please modify your prompt. Credits refunded.';
    }

    if (errorLower.includes('rate limit') ||
        errorLower.includes('too many requests') ||
        errorLower.includes('429')) {
        return 'Service is busy. Please try again in a moment. Credits refunded.';
    }

    if (errorLower.includes('overload') ||
        errorLower.includes('capacity') ||
        errorLower.includes('503') ||
        errorLower.includes('500 internal server error')) {
        return 'Server is temporarily overloaded. Credits refunded.';
    }

    if (errorLower.includes('login failed') ||
        errorLower.includes('authentication') ||
        errorLower.includes('unauthorized') ||
        errorLower.includes('401')) {
        return 'Authentication error. Please try again. Credits refunded.';
    }

    if (errorLower.includes('generation failed') && !errorLower.includes(':')) {
        return 'Image generation failed. Credits refunded.';
    }

    // Provider errors from Flowith
    if (errorLower.includes('provider_error') ||
        errorLower.includes('provider error')) {
        return 'AI provider error. Credits refunded.';
    }

    // Generic fallback - hide any URL patterns
    if (technicalError && (technicalError.includes('http://') || technicalError.includes('https://'))) {
        return 'Generation failed due to a temporary issue. Credits refunded.';
    }

    // If it's already user-friendly (short and no URLs), keep it
    if (technicalError && technicalError.length < 100 && !technicalError.includes('://')) {
        // Append refund message if not already present
        if (!errorLower.includes('refund')) {
            return technicalError + '. Credits refunded.';
        }
        return technicalError;
    }

    // Default fallback
    return 'Generation failed. Credits have been refunded to your account.';
}

// ==========================================
// WORKERS API CALLS
// ==========================================

/**
 * Call Workers internal API
 */
async function callWorkersAPI(endpoint, method = 'GET', body = null) {
    const url = `${CONFIG.API_URL}${endpoint}`;

    const options = {
        method,
        headers: {
            'Content-Type': 'application/json',
            'X-Internal-Secret': CONFIG.INTERNAL_SECRET,
        },
    };

    if (body) {
        options.body = JSON.stringify(body);
    }

    const response = await fetch(url, options);
    const data = await response.json();

    if (!response.ok) {
        throw new Error(data.error || `API returned ${response.status}`);
    }

    return data;
}

/**
 * Fetch pending jobs from Workers
 */
async function fetchPendingJobs(limit = CONFIG.CONCURRENT_JOBS) {
    return callWorkersAPI(`/internal/batch/pending-jobs?limit=${limit}`);
}

/**
 * Claim jobs for processing
 */
async function claimJobs(jobIds) {
    return callWorkersAPI('/internal/batch/claim', 'POST', { jobIds });
}

/**
 * Get Flowith account for job
 */
async function getFlowithAccount(jobId, excludeAccountIds = []) {
    return callWorkersAPI('/internal/batch/get-account', 'POST', { jobId, excludeAccountIds });
}

/**
 * Mark job as complete
 */
async function markJobComplete(jobId, resultUrl, processingTime, flowithAccountId, flowithSessionId, accessToken, refreshToken) {
    return callWorkersAPI('/internal/batch/complete', 'POST', {
        jobId,
        resultUrl,
        processingTime,
        flowithAccountId,
        flowithSessionId,
        accessToken,
        refreshToken,
    });
}

/**
 * Refund job with retry (CRITICAL - must not lose user credits)
 */
async function refundJob(jobId, errorMessage) {
    for (let attempt = 1; attempt <= CONFIG.MAX_REFUND_RETRIES; attempt++) {
        try {
            const result = await callWorkersAPI('/internal/batch/refund', 'POST', {
                jobId,
                errorMessage,
            });

            if (result.success) {
                log(`✅ Refund successful for job ${jobId} (attempt ${attempt})`);
                return result;
            }
        } catch (error) {
            log(`⚠️ Refund attempt ${attempt} failed for job ${jobId}: ${error.message}`);

            if (attempt < CONFIG.MAX_REFUND_RETRIES) {
                const delay = CONFIG.RETRY_DELAY_BASE * Math.pow(2, attempt - 1);
                log(`   Retrying in ${delay}ms...`);
                await sleep(delay);
            }
        }
    }

    // All retries failed - log critical error
    log(`🚨 CRITICAL: Refund FAILED after ${CONFIG.MAX_REFUND_RETRIES} attempts for job ${jobId}`);
    stats.totalErrors++;
    return { success: false };
}

/**
 * Upload to R2
 */
async function uploadToR2(jobId, userId, sourceUrl) {
    return callWorkersAPI('/internal/batch/upload-r2', 'POST', {
        jobId,
        userId,
        sourceUrl,
        type: 'image',
    });
}

// ==========================================
// FLOWITH API CALLS (VIA 922PROXY)
// ==========================================

/**
 * Flowith API configuration
 */
const FLOWITH_CONFIG = {
    apiKey: 'sb_publishable_qPCinc8LE8ChpdT7Pf79tQ_eryz5udr',
    edgeUrl: 'https://edge.flowith.net',
    supabaseUrl: 'https://aibdxsebwhalbnugsqel.supabase.co',
};

/**
 * Fetch via 922proxy
 */
async function fetchViaProxy(url, options = {}) {
    const agent = new HttpsProxyAgent(PROXY_URL);

    // Add human-like delay
    await sleep(getRandomDelay(200, 500));

    return fetch(url, {
        ...options,
        agent,
        timeout: 120000,
    });
}

/**
 * Ensure valid token (refresh if needed)
 */
async function ensureValidToken(account) {
    // Check if token is expired
    const tokenExpiry = account.token_expiry;
    const isExpired = !tokenExpiry || Date.now() >= (tokenExpiry - 300000);

    if (!isExpired && account.access_token) {
        return account.access_token;
    }

    // Need to refresh token
    if (account.refresh_token) {
        log(`   🔄 Token expired, refreshing for ${account.email}...`);

        const userAgent = getConsistentUserAgent(account.id);
        const headers = {
            ...generateStealthHeaders(null, userAgent),
            'apikey': FLOWITH_CONFIG.apiKey,
        };

        try {
            const response = await fetchViaProxy(
                `${FLOWITH_CONFIG.supabaseUrl}/auth/v1/token?grant_type=refresh_token`,
                {
                    method: 'POST',
                    headers,
                    body: JSON.stringify({ refresh_token: account.refresh_token }),
                }
            );

            if (response.ok) {
                const data = await response.json();
                account.access_token = data.access_token;
                account.refresh_token = data.refresh_token || account.refresh_token;

                // Parse new expiry
                try {
                    const payload = JSON.parse(atob(data.access_token.split('.')[1]));
                    account.token_expiry = payload.exp ? payload.exp * 1000 : null;
                } catch (e) { }

                log(`   ✅ Token refreshed for ${account.email}`);
                return account.access_token;
            }
        } catch (error) {
            log(`   ⚠️ Token refresh failed: ${error.message}`);
        }
    }

    // Fall back to login
    log(`   🔐 Logging in for ${account.email}...`);

    const userAgent = getConsistentUserAgent(account.id);
    const headers = {
        ...generateStealthHeaders(null, userAgent),
        'apikey': FLOWITH_CONFIG.apiKey,
    };

    const response = await fetchViaProxy(
        `${FLOWITH_CONFIG.supabaseUrl}/auth/v1/token?grant_type=password`,
        {
            method: 'POST',
            headers,
            body: JSON.stringify({
                email: account.email,
                password: account.password_decrypted,
            }),
        }
    );

    if (!response.ok) {
        throw new Error(`Login failed for ${account.email}: ${response.status}`);
    }

    const data = await response.json();
    account.access_token = data.access_token;
    account.refresh_token = data.refresh_token;

    // Parse expiry
    try {
        const payload = JSON.parse(atob(data.access_token.split('.')[1]));
        account.token_expiry = payload.exp ? payload.exp * 1000 : null;
    } catch (e) { }

    log(`   ✅ Login successful for ${account.email}`);
    return account.access_token;
}

/**
 * Submit generation to Flowith
 */
async function submitToFlowith(job, account) {
    const userAgent = getConsistentUserAgent(account.id);
    const token = await ensureValidToken(account);
    const headers = generateStealthHeaders(token, userAgent);
    headers['apikey'] = FLOWITH_CONFIG.apiKey;

    const nodeId = generateUUID();
    const convId = account.session?.conv_id || generateUUID();

    // Step 1: Create node
    log(`   📤 Creating node for job ${job.id.substring(0, 8)}...`);

    const nodePayload = {
        nodes: [{
            id: nodeId,
            conv_id: convId,
            type: '2',
            position: JSON.stringify({}),
            data: JSON.stringify({ model: job.model_id }),
            c_ids: [],
            p_id: convId,
        }],
    };

    const nodeResponse = await fetchViaProxy(
        `${FLOWITH_CONFIG.edgeUrl}/user/batch-upsert-nodes`,
        {
            method: 'POST',
            headers,
            body: JSON.stringify(nodePayload),
        }
    );

    if (!nodeResponse.ok) {
        const error = await nodeResponse.text();
        throw new Error(`Node creation failed: ${nodeResponse.status} - ${error}`);
    }

    // Step 2: Submit generation
    log(`   🎨 Submitting generation for job ${job.id.substring(0, 8)}...`);

    const genPayload = {
        model: job.model_id,
        nodeId: nodeId,
        convId: convId,
        messages: [{ content: job.prompt, role: 'user' }],
        aspect_ratio: job.aspect_ratio || '1:1',
        image_size: job.image_size || '2k',
    };

    const genResponse = await fetchViaProxy(
        `${FLOWITH_CONFIG.edgeUrl}/image_gen/async`,
        {
            method: 'POST',
            headers,
            body: JSON.stringify(genPayload),
        }
    );

    if (!genResponse.ok) {
        const error = await genResponse.text();

        // Check for insufficient credits
        if (genResponse.status === 402 || error.includes('insufficient credits')) {
            throw new Error('INSUFFICIENT_CREDITS');
        }

        throw new Error(`Generation submit failed: ${genResponse.status} - ${error}`);
    }

    log(`   ✅ Submitted successfully. NodeId: ${nodeId.substring(0, 8)}...`);

    return {
        nodeId,
        convId,
        sessionId: account.session?.id,
    };
}

/**
 * Poll Flowith for result
 */
async function pollFlowithResult(nodeId, account) {
    const userAgent = getConsistentUserAgent(account.id);
    const token = account.access_token;
    const headers = generateStealthHeaders(token, userAgent);
    headers['apikey'] = FLOWITH_CONFIG.apiKey;
    headers['accept-profile'] = 'public';

    log(`   🔄 Polling for result... (max ${CONFIG.MAX_POLL_ATTEMPTS} attempts)`);

    for (let attempt = 0; attempt < CONFIG.MAX_POLL_ATTEMPTS; attempt++) {
        await sleep(CONFIG.FLOWITH_POLL_INTERVAL);

        try {
            const url = `${FLOWITH_CONFIG.supabaseUrl}/rest/v1/flow_node?id=eq.${nodeId}&select=data`;

            const response = await fetchViaProxy(url, {
                method: 'GET',
                headers,
            });

            if (!response.ok) {
                continue;
            }

            const rows = await response.json();

            if (!rows || rows.length === 0) {
                continue;
            }

            const node = rows[0];
            let nodeData;

            try {
                nodeData = typeof node.data === 'string' ? JSON.parse(node.data) : node.data;
            } catch (e) {
                continue;
            }

            // Check if completed with image URL
            if (nodeData.value && nodeData.value.includes('http') && !nodeData.value.includes('provider_error')) {
                log(`   ✅ Generation completed! (attempt ${attempt + 1})`);
                return { success: true, imageUrl: nodeData.value };
            }

            // Check if failed
            if (nodeData.status === 'failed' || nodeData.error) {
                let errorMsg = nodeData.error || 'Generation failed';

                // Try to parse value for error details
                if (nodeData.value) {
                    try {
                        const parsed = JSON.parse(nodeData.value);
                        errorMsg = parsed.value || parsed.error || errorMsg;
                    } catch (e) { }
                }

                log(`   ❌ Generation failed: ${errorMsg}`);
                return { success: false, error: errorMsg };
            }

            // Log progress every 10 attempts
            if (attempt > 0 && attempt % 10 === 0) {
                log(`   ⏳ Still processing... (attempt ${attempt + 1}/${CONFIG.MAX_POLL_ATTEMPTS})`);
            }

        } catch (error) {
            // Continue polling on error
        }
    }

    log(`   ⏰ Polling timeout after ${CONFIG.MAX_POLL_ATTEMPTS} attempts`);
    return { success: false, error: 'Generation timeout after 5 minutes' };
}

/**
 * Generate UUID
 */
function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

// ==========================================
// JOB PROCESSING
// ==========================================

/**
 * Process a single job
 */
async function processJob(job) {
    const startTime = Date.now();
    log(`\n📦 Processing job ${job.id.substring(0, 8)}...`);
    log(`   Prompt: ${job.prompt?.substring(0, 50)}...`);
    log(`   Model: ${job.model_id}`);

    stats.currentlyProcessing++;

    const MAX_ACCOUNT_ATTEMPTS = 5;
    let excludeAccountIds = [];

    try {
        for (let accountAttempt = 1; accountAttempt <= MAX_ACCOUNT_ATTEMPTS; accountAttempt++) {
            try {
                // Get Flowith account
                const accountResult = await getFlowithAccount(job.id, excludeAccountIds);

                if (!accountResult.success || !accountResult.account) {
                    throw new Error('No available Flowith accounts');
                }

                const account = accountResult.account;
                log(`   📧 Using account: ${account.email} (attempt ${accountAttempt})`);

                // Submit to Flowith
                const submitResult = await submitToFlowith(job, account);

                // Poll for result
                const pollResult = await pollFlowithResult(submitResult.nodeId, account);

                if (pollResult.success) {
                    // Upload to R2
                    let finalUrl = pollResult.imageUrl;

                    try {
                        const r2Result = await uploadToR2(job.id, job.user_id, pollResult.imageUrl);
                        if (r2Result.success) {
                            finalUrl = r2Result.r2Url;
                            log(`   📦 Uploaded to R2`);
                        }
                    } catch (r2Error) {
                        log(`   ⚠️ R2 upload failed, using original URL`);
                    }

                    // Mark as complete
                    const processingTime = Date.now() - startTime;
                    await markJobComplete(
                        job.id,
                        finalUrl,
                        processingTime,
                        account.id,
                        submitResult.sessionId,
                        account.access_token,
                        account.refresh_token
                    );

                    log(`   ✅ Job completed in ${Math.floor(processingTime / 1000)}s`);
                    stats.totalCompleted++;
                    stats.currentlyProcessing--;
                    return { success: true };
                } else {
                    throw new Error(pollResult.error || 'Generation failed');
                }

            } catch (error) {
                if (error.message === 'INSUFFICIENT_CREDITS') {
                    log(`   ⚠️ Account has insufficient credits, trying another...`);
                    // Get last account ID and exclude it
                    try {
                        const lastAccount = await getFlowithAccount(job.id, excludeAccountIds);
                        if (lastAccount.account?.id) {
                            excludeAccountIds.push(lastAccount.account.id);
                        }
                    } catch (e) { }
                    continue; // Try next account
                }

                // Other error - fail immediately
                throw error;
            }
        }

        // All account attempts exhausted
        throw new Error('All Flowith accounts exhausted');

    } catch (error) {
        log(`   ❌ Job failed: ${error.message}`);

        // Transform technical error to user-friendly message
        const userFriendlyError = getUserFriendlyErrorMessage(error.message);
        log(`   📝 User message: ${userFriendlyError}`);

        // Refund the job with user-friendly error
        await refundJob(job.id, userFriendlyError);
        stats.totalRefunded++;
        stats.currentlyProcessing--;

        return { success: false, error: error.message };
    }
}

// ==========================================
// MAIN LOOP
// ==========================================

/**
 * Process pending batch
 */
async function processPendingBatch() {
    try {
        // Fetch pending jobs
        const result = await fetchPendingJobs();

        if (!result.success || !result.jobs || result.jobs.length === 0) {
            return 0;
        }

        const jobs = result.jobs;
        log(`📋 Found ${jobs.length} pending jobs`);

        // Claim jobs
        const jobIds = jobs.map(j => j.id);
        const claimResult = await claimJobs(jobIds);

        if (!claimResult.success) {
            log(`⚠️ Failed to claim jobs`);
            return 0;
        }

        log(`✅ Claimed ${claimResult.claimed} jobs`);

        // Process jobs in parallel (with concurrency limit)
        const claimedJobs = claimResult.jobs || [];

        const results = await Promise.allSettled(
            claimedJobs.map(job => processJob(job))
        );

        // Count results
        let completed = 0;
        let failed = 0;

        results.forEach(result => {
            if (result.status === 'fulfilled' && result.value.success) {
                completed++;
            } else {
                failed++;
            }
        });

        log(`\n📊 Batch results: ${completed} completed, ${failed} failed`);
        stats.totalProcessed += claimedJobs.length;

        return claimedJobs.length;

    } catch (error) {
        log(`❌ Error processing batch: ${error.message}`);
        return 0;
    }
}

/**
 * Start the batch processor
 */
async function startBatchProcessor() {
    log('🚀 Starting DreamLook Batch Processor');
    log(`   API URL: ${CONFIG.API_URL}`);
    log(`   Concurrent Jobs: ${CONFIG.CONCURRENT_JOBS}`);
    log(`   Poll Interval: ${CONFIG.POLL_INTERVAL}ms`);
    log(`   Proxy: ${CONFIG.PROXY_HOST}:${CONFIG.PROXY_PORT}`);

    if (!CONFIG.INTERNAL_SECRET) {
        log('⚠️ WARNING: INTERNAL_BATCH_SECRET not set!');
    }

    stats.startTime = Date.now();

    // Log initial memory
    const initialMem = getMemoryUsage();
    log(`📊 Initial Memory: RSS=${initialMem.rss}MB, Heap=${initialMem.heapUsed}/${initialMem.heapTotal}MB`);

    // Memory logging interval (every 30 seconds)
    let memoryLogCounter = 0;

    // Main loop
    while (true) {
        try {
            stats.lastPollTime = Date.now();

            const processed = await processPendingBatch();

            // Update memory stats
            const mem = updateMemoryStats();
            memoryLogCounter++;

            // Log memory every 3 cycles (~30 seconds) or when processing
            const shouldLogMemory = memoryLogCounter >= 3 || stats.currentlyProcessing > 0;

            if (shouldLogMemory) {
                memoryLogCounter = 0;
                const renderFreeLimit = 512;
                const usagePercent = Math.round((mem.rss / renderFreeLimit) * 100);
                const trend = getMemoryTrend();

                log(`📊 Memory: RSS=${mem.rss}MB (${usagePercent}%) | Heap=${mem.heapUsed}/${mem.heapTotal}MB | Peak=${stats.peakMemoryUsed}MB | Jobs=${stats.currentlyProcessing} | ${trend}`);

                // Warning if approaching limit
                if (mem.rss > renderFreeLimit * 0.8) {
                    log(`⚠️ WARNING: Memory usage at ${usagePercent}% of Render free tier limit (512MB)!`);
                }
                if (mem.rss > renderFreeLimit * 0.9) {
                    log(`🚨 CRITICAL: Memory at ${usagePercent}%! Risk of OOM crash. Consider reducing CONCURRENT_JOBS.`);
                }
            }

            // If no jobs, wait before next poll
            if (processed === 0) {
                await sleep(CONFIG.POLL_INTERVAL);
            } else {
                // Small delay between batches
                await sleep(2000);
            }

        } catch (error) {
            log(`❌ Loop error: ${error.message}`);
            await sleep(CONFIG.POLL_INTERVAL);
        }
    }
}

// ==========================================
// EXPORTS
// ==========================================

module.exports = {
    startBatchProcessor,
    getStats,
    CONFIG,
};
