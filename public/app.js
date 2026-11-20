let socket;
let latencyChart;
const latencyHistory = [];

// Tab Switcher Logic
function switchTab(tabId) {
    document.querySelectorAll('.menu-item').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));

    const tabBtn = Array.from(document.querySelectorAll('.menu-item')).find(btn => btn.getAttribute('onclick').includes(tabId));
    if (tabBtn) tabBtn.classList.add('active');

    const targetPane = document.getElementById('tab-' + tabId);
    if (targetPane) targetPane.classList.add('active');

    const titles = {
        'dashboard': 'Distributed Telemetry & Stress Dashboard',
        'architecture': 'Distributed System Architecture & Event Pipeline',
        'playground': 'REST API Developer Sandbox & Swagger Playground',
        'database': 'PostgreSQL Relational Ledger & Async Ingestion'
    };
    document.getElementById('current-tab-title').innerText = titles[tabId] || 'Distributed Console';
}

// Interactive Architecture Hover Logic
function showNodeInfo(nodeType) {
    const detailsBox = document.getElementById('node-details');
    const nodes = {
        'rate-limit': {
            title: '1. Token Bucket Rate Limiting Shield',
            desc: 'Tracks discrete user tokens per IP/UserId in a high-speed Redis bucket (20 max capacity, 5 refill/sec). If a bot tries to spam thousands of requests, tokens exhaust instantly, returning HTTP 429 Too Many Requests in <0.2ms before touching any database resources.'
        },
        'idempotency': {
            title: '2. Cryptographic Idempotency Filter',
            desc: 'Validates unique client transaction tokens (`idemp_<userId>_<nonce>`). If a client network drops and retries an existing order, the cached response is immediately replayed, mathematically preventing double-charging or duplicate order insertions.'
        },
        'redis': {
            title: '3. In-Memory Atomic Reservation (Redis Lua CAS)',
            desc: 'Executes atomic stock decrement scripts directly in Redis RAM memory. Instead of traditional SQL row-locks (`SELECT FOR UPDATE`) which deadlock under 1,000+ threads, Lua scripts execute as an uninterrupted single-threaded transaction, guaranteeing zero inventory overselling.'
        },
        'kafka': {
            title: '4. Asynchronous Event Broker (Apache Kafka / RabbitMQ)',
            desc: 'Once the atomic stock is reserved, an `ORDER_CREATED` event is immediately queued into the Kafka topic. The client receives an instant 201 Created acknowledgment in <1ms, decoupling the user from slow downstream disk writes.'
        },
        'db': {
            title: '5. Relational PostgreSQL Batch Ingestion Worker',
            desc: 'Background consumer worker threads read off Kafka partitions in buffered batches, generating ACID-compliant records and invoice documents without causing database connection saturation or transaction contention.'
        }
    };

    const info = nodes[nodeType];
    if (info) {
        detailsBox.innerHTML = `
            <h4>${info.title}</h4>
            <p>${info.desc}</p>
        `;
    }
}

// WebSocket Live Telemetry Stream
function initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    socket = new WebSocket(`${protocol}//${window.location.host}`);

    socket.onopen = () => {
        document.getElementById('ws-status').innerHTML = '<span class="dot"></span> LIVE TELEMETRY';
    };

    socket.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.type === 'TELEMETRY_UPDATE') {
                updateUI(data);
            }
        } catch (e) {
            console.error('WS parse error:', e);
        }
    };

    socket.onclose = () => {
        document.getElementById('ws-status').innerHTML = '⚠️ RECONNECTING...';
        setTimeout(initWebSocket, 2000);
    };
}

function updateUI(data) {
    // 1. KPI updates
    const totalOps = (data.redisStats.totalHits || 0) + (data.redisStats.totalMisses || 0);
    const hitRatio = totalOps > 0 ? ((data.redisStats.totalHits / totalOps) * 100).toFixed(1) : 100.0;
    document.getElementById('metric-cache-hit').innerText = `${hitRatio}%`;
    document.getElementById('metric-atomic-ops').innerText = Number(data.redisStats.atomicOps).toLocaleString();
    document.getElementById('metric-queue-depth').innerText = data.queueStats.queueDepth;
    document.getElementById('metric-queue-processed').innerText = `${Number(data.queueStats.totalProcessed).toLocaleString()} processed (${data.queueStats.avgLatencyMs || 0}ms avg)`;
    document.getElementById('metric-rate-limited').innerText = Number(data.redisStats.rateLimitedCount).toLocaleString();

    // 2. Render Products
    const prodContainer = document.getElementById('products-container');
    prodContainer.innerHTML = '';
    data.products.forEach((p) => {
        const percent = Math.max(0, Math.min(100, (p.availableStock / p.initialStock) * 100));
        let barColor = 'var(--accent)';
        if (percent < 20) barColor = 'var(--danger)';
        else if (percent < 50) barColor = 'var(--warn)';

        const card = document.createElement('div');
        card.className = 'product-card';
        card.innerHTML = `
            <div class="product-head">
                <span>${p.name}</span>
                <span class="product-price">$${p.price.toFixed(2)}</span>
            </div>
            <div class="stock-bar-container">
                <div class="stock-bar" style="width: ${percent}%; background: ${barColor};"></div>
            </div>
            <div class="product-meta">
                <span>Available: <strong>${p.availableStock}</strong> / ${p.initialStock}</span>
                <span>Sold: ${p.totalSold}</span>
            </div>
        `;
        prodContainer.appendChild(card);
    });

    // 3. Render Order Stream
    const streamContainer = document.getElementById('order-stream-list');
    streamContainer.innerHTML = '';
    data.recentOrders.forEach((o) => {
        const item = document.createElement('div');
        item.className = 'stream-item';
        item.innerHTML = `
            <div>
                <span class="stream-id">${o.id}</span>
                <span style="color: #cbd5e1; margin-left: 6px;">${o.productName}</span>
            </div>
            <div>
                <span class="badge" style="color: #34d399;">$${o.totalAmount}</span>
            </div>
        `;
        streamContainer.appendChild(item);
    });

    // 4. Render Database Table
    const dbBody = document.getElementById('db-invoices-body');
    if (data.recentOrders.length > 0) {
        dbBody.innerHTML = '';
        data.recentOrders.forEach((o) => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><strong style="color: var(--cyan);">${o.id}</strong></td>
                <td>${o.userId}</td>
                <td>${o.productId}</td>
                <td>${o.quantity}</td>
                <td>$${o.totalAmount}</td>
                <td><span class="badge" style="color: #34d399;">COMMITTED</span></td>
                <td><span class="badge" style="color: #38bdf8;">ORDER_CREATED</span></td>
                <td><span class="badge" style="color: #10b981;">PERSISTED</span></td>
            `;
            dbBody.appendChild(tr);
        });
    }

    // 5. Update Latency Chart
    if (latencyChart) {
        latencyHistory.push(parseFloat(data.queueStats.avgLatencyMs) || 0.5);
        if (latencyHistory.length > 20) latencyHistory.shift();
        latencyChart.data.labels = latencyHistory.map((_, i) => `${i}s`);
        latencyChart.data.datasets[0].data = latencyHistory;
        latencyChart.update();
    }
}

// Playground API Execution
async function triggerPlaygroundCheckout() {
    const term = document.getElementById('playground-terminal');
    const log = (msg, cls = 'system') => {
        const line = document.createElement('div');
        line.className = `term-line ${cls}`;
        line.innerText = `[${new Date().toLocaleTimeString()}] ${msg}`;
        term.appendChild(line);
        term.scrollTop = term.scrollHeight;
    };

    log('Dispatching POST /api/checkout ...', 'request');
    const payload = {
        userId: 'user_2023001287',
        productId: 'prod_ps5_pro',
        quantity: 1,
        idempotencyKey: `idemp_${Date.now()}`
    };

    try {
        const res = await fetch('/api/checkout', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        log(`Response Status: HTTP ${res.status}`, res.status === 201 ? 'response' : 'error');
        log(JSON.stringify(data, null, 2), res.status === 201 ? 'response' : 'error');
    } catch (e) {
        log(`Network Exception: ${e.message}`, 'error');
    }
}

function clearConsole() {
    document.getElementById('playground-terminal').innerHTML = '<div class="term-line system">[SYSTEM] Terminal logs cleared.</div>';
}

// Manual User Checkout
async function handleManualCheckout() {
    const userId = document.getElementById('manual-user-id').value || 'guest_user';
    const productId = document.getElementById('manual-product-select').value;
    const alertBox = document.getElementById('manual-checkout-alert');

    try {
        const res = await fetch('/api/checkout', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                userId,
                productId,
                quantity: 1,
                idempotencyKey: `manual_${userId}_${Date.now()}`
            })
        });
        const data = await res.json();

        if (data.status === 'SUCCESS') {
            alertBox.className = 'alert-box alert-success';
            alertBox.style.display = 'block';
            alertBox.innerText = `✅ Order Confirmed! ID: ${data.order.id} (Latency: ${data.latencyUs}µs)`;
        } else {
            alertBox.className = 'alert-box alert-error';
            alertBox.style.display = 'block';
            alertBox.innerText = `❌ ${data.message || data.status}`;
        }
    } catch (e) {
        alertBox.className = 'alert-box alert-error';
        alertBox.style.display = 'block';
        alertBox.innerText = 'Network error placing order';
    }
}

// Stress Test Benchmark
async function runStressBenchmark() {
    const productId = document.getElementById('stress-product-select').value;
    const totalRequests = document.getElementById('input-total-req').value;
    const concurrency = document.getElementById('input-concurrency').value;
    const rateLimitStrict = document.getElementById('input-bot-spam').checked;
    const btn = document.getElementById('btn-run-stress');

    btn.disabled = true;
    btn.innerText = '⏳ Benchmarking Concurrency Storm...';

    try {
        const res = await fetch('/api/stress-test', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ productId, totalRequests, concurrency, rateLimitStrict })
        });
        const data = await res.json();

        const card = document.getElementById('stress-result-card');
        card.style.display = 'block';
        document.getElementById('res-rps').innerText = Number(data.requestsPerSecond).toLocaleString() + ' RPS';
        document.getElementById('res-latency').innerText = data.avgLatencyMs + ' ms';
        document.getElementById('res-success').innerText = data.successfulOrders;
        document.getElementById('res-rate-limited').innerText = data.rateLimitedRejections;

        const oversoldBadge = document.getElementById('res-oversold');
        if (data.zeroOversellingVerified) {
            oversoldBadge.innerText = 'PASSED: Zero Inventory Overselling Invariant Verified';
            oversoldBadge.style.color = '#34d399';
        } else {
            oversoldBadge.innerText = 'FAILED: Negative Stock Race Condition Detected';
            oversoldBadge.style.color = '#ef4444';
        }
    } catch (e) {
        console.error(e);
    } finally {
        btn.disabled = false;
        btn.innerText = '⚡ Launch Concurrency Storm';
    }
}

// Reset Inventory Cache
async function resetInventory() {
    await fetch('/api/inventory/reset', { method: 'POST' });
    document.getElementById('stress-result-card').style.display = 'none';
}

function initChart() {
    const ctx = document.getElementById('latencyChart').getContext('2d');
    latencyChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Async Pipeline Latency (ms)',
                data: [],
                borderColor: '#06b6d4',
                backgroundColor: 'rgba(6, 182, 212, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#64748b' } },
                x: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#64748b' } }
            }
        }
    });
}

window.addEventListener('DOMContentLoaded', () => {
    initChart();
    initWebSocket();
});
