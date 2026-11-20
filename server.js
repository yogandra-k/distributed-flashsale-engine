const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');

const redis = require('./src/redisEngine');
const eventQueue = require('./src/queueEngine');
const flashSaleService = require('./src/flashSaleService');
const loadTestRunner = require('./src/loadTestRunner');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 4000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Broadcast system telemetry to connected WebSocket clients
function broadcastTelemetry() {
    const payload = JSON.stringify({
        type: 'TELEMETRY_UPDATE',
        products: flashSaleService.getAllProducts(),
        redisStats: redis.getStats(),
        queueStats: eventQueue.getStats(),
        recentOrders: flashSaleService.getRecentOrders(10)
    });

    wss.clients.forEach((client) => {
        if (client.readyState === 1) {
            client.send(payload);
        }
    });
}

// Tick telemetry every 500ms
setInterval(broadcastTelemetry, 500);

// REST API Endpoints
app.get('/api/products', (req, res) => {
    res.json({ status: 'success', data: flashSaleService.getAllProducts() });
});

app.post('/api/checkout', (req, res) => {
    const { userId, productId, quantity, idempotencyKey } = req.body;
    const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;

    const result = flashSaleService.processCheckout({
        userId,
        productId,
        quantity,
        idempotencyKey,
        clientIp
    });

    res.status(result.code || 200).json(result);
});

app.post('/api/inventory/reset', (req, res) => {
    const result = flashSaleService.resetInventory();
    broadcastTelemetry();
    res.json({ status: 'success', ...result });
});

app.post('/api/stress-test', async (req, res) => {
    const { productId, totalRequests, concurrency, rateLimitStrict } = req.body;
    const result = await loadTestRunner.runLoadTest({
        productId,
        totalRequests: Number(totalRequests) || 500,
        concurrency: Number(concurrency) || 50,
        rateLimitStrict: Boolean(rateLimitStrict)
    });

    broadcastTelemetry();
    res.json(result);
});

app.get('/api/stats', (req, res) => {
    res.json({
        products: flashSaleService.getAllProducts(),
        redis: redis.getStats(),
        eventQueue: eventQueue.getStats(),
        recentOrders: flashSaleService.getRecentOrders(20)
    });
});

server.listen(PORT, () => {
    console.log(`=======================================================`);
    console.log(`🚀 Distributed Flash-Sale & Checkout Engine Started!`);
    console.log(`📍 Web Dashboard: http://localhost:${PORT}`);
    console.log(`⚡ WebSocket Stream: ws://localhost:${PORT}`);
    console.log(`=======================================================`);
});
