let socket;
let latencyChart;
const latencyHistory = [];

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
                <span>Stock: <strong>${p.availableStock}</strong> / ${p.initialStock}</span>
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

    // 4. Update Latency Chart
    if (latencyChart) {
        latencyHistory.push(parseFloat(data.queueStats.avgLatencyMs) || 0.5);
        if (latencyHistory.length > 20) latencyHistory.shift();
        latencyChart.data.labels = latencyHistory.map((_, i) => `${i}s`);
        latencyChart.data.datasets[0].data = latencyHistory;
        latencyChart.update();
    }
}

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
            alertBox.innerText = `✅ Order Confirmed! ID: ${data.order.id} (Latency: ${data.latencyUs}µs)`;
        } else {
            alertBox.className = 'alert-box alert-error';
            alertBox.innerText = `❌ ${data.message || data.status}`;
        }
    } catch (e) {
        alertBox.className = 'alert-box alert-error';
        alertBox.innerText = 'Network error placing order';
    }
}

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
        document.getElementById('res-rps').innerText = Number(data.requestsPerSecond).toLocaleString();
        document.getElementById('res-latency').innerText = data.avgLatencyMs;
        document.getElementById('res-success').innerText = data.successfulOrders;
        document.getElementById('res-out-of-stock').innerText = data.outOfStockRejections;
        document.getElementById('res-rate-limited').innerText = data.rateLimitedRejections;

        const oversoldBadge = document.getElementById('res-oversold');
        if (data.zeroOversellingVerified) {
            oversoldBadge.innerText = 'PASSED (Zero Race Conditions / 0 Oversold)';
            oversoldBadge.className = 'badge-safe';
        } else {
            oversoldBadge.innerText = 'FAILED (Oversold detected)';
            oversoldBadge.style.color = '#ef4444';
        }
    } catch (e) {
        console.error(e);
    } finally {
        btn.disabled = false;
        btn.innerText = '⚡ Launch Stress Test';
    }
}

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
                y: { grid: { color: '#1e293b' }, ticks: { color: '#94a3b8' } },
                x: { grid: { color: '#1e293b' }, ticks: { color: '#94a3b8' } }
            }
        }
    });
}

window.addEventListener('DOMContentLoaded', () => {
    initChart();
    initWebSocket();
});
