/**
 * Flash Sale Checkout Service
 * Orchestrates Rate Limiting, Idempotency, In-Memory Atomic Stock Check,
 * Optimistic Database Record Creation, and Async Event Streaming.
 */

const redis = require('./redisEngine');
const eventQueue = require('./queueEngine');

class FlashSaleService {
    constructor() {
        // Initial in-memory relational inventory state
        this.products = new Map([
            ['prod_ps5_pro', { id: 'prod_ps5_pro', name: 'PlayStation 5 Pro Digital Edition', initialStock: 100, price: 699.99 }],
            ['prod_iphone_16', { id: 'prod_iphone_16', name: 'iPhone 16 Pro Max 256GB', initialStock: 50, price: 1199.00 }],
            ['prod_rtx_5090', { id: 'prod_rtx_5090', name: 'NVIDIA GeForce RTX 5090 OC', initialStock: 25, price: 1999.00 }]
        ]);

        this.orders = [];
        this.resetInventory();

        // Listen to event queue confirmation
        eventQueue.on('message_processed', (msg) => {
            if (msg.topic === 'ORDER_CREATED') {
                const order = this.orders.find((o) => o.id === msg.payload.orderId);
                if (order) {
                    order.status = 'CONFIRMED_ASYNC_PERSISTED';
                    order.persistedAt = Date.now();
                }
            }
        });
    }

    resetInventory() {
        this.orders = [];
        for (const [id, prod] of this.products.entries()) {
            redis.set(`stock:${id}`, prod.initialStock);
        }
        return { message: 'Inventory and Redis cache reset successfully.' };
    }

    getProductStatus(productId) {
        const prod = this.products.get(productId);
        if (!prod) return null;
        const currentCachedStock = redis.get(`stock:${productId}`) ?? 0;
        return {
            ...prod,
            availableStock: currentCachedStock,
            totalSold: prod.initialStock - currentCachedStock
        };
    }

    getAllProducts() {
        return Array.from(this.products.keys()).map((id) => this.getProductStatus(id));
    }

    // High-Concurrency Checkout Pipeline
    processCheckout({ userId, productId, quantity = 1, idempotencyKey, clientIp = '127.0.0.1' }) {
        const startTime = process.hrtime.bigint();

        // 1. Rate Limiting Check (Token Bucket)
        const rateLimit = redis.checkRateLimit(`user:${userId || clientIp}`, 20, 5);
        if (!rateLimit.allowed) {
            return {
                status: 'REJECTED_RATE_LIMITED',
                code: 429,
                message: 'Too many checkout requests. Token bucket exhausted.',
                retryAfterMs: rateLimit.retryAfterMs
            };
        }

        // 2. Idempotency Check (prevents double billing if network times out)
        if (idempotencyKey) {
            const idempotencyCheck = redis.checkAndSetIdempotency(idempotencyKey);
            if (idempotencyCheck.isDuplicate) {
                return {
                    status: 'IDEMPOTENT_REPLAY',
                    code: 200,
                    message: 'Order already processed for this idempotency key.',
                    data: idempotencyCheck.cachedResult
                };
            }
        }

        // 3. In-Memory Atomic Inventory Decrement (Lua script simulation)
        const decrResult = redis.atomicStockDecrement(productId, quantity);
        if (!decrResult.success) {
            return {
                status: 'REJECTED_OUT_OF_STOCK',
                code: 410,
                message: decrResult.reason === 'OUT_OF_STOCK' ? 'Flash sale product sold out!' : 'Product not found',
                remainingStock: decrResult.remainingStock
            };
        }

        // 4. Create Order Object
        const prod = this.products.get(productId);
        const orderId = `ORD-${Date.now()}-${Math.floor(Math.random() * 9000 + 1000)}`;
        const order = {
            id: orderId,
            userId,
            productId,
            productName: prod.name,
            quantity,
            totalAmount: Number((prod.price * quantity).toFixed(2)),
            status: 'ORDER_PLACED_CACHE_COMMITTED',
            remainingStock: decrResult.remainingStock,
            timestamp: Date.now()
        };

        this.orders.push(order);

        // 5. Publish to Async Event Queue (Kafka simulation)
        const eventId = eventQueue.publish('ORDER_CREATED', {
            orderId,
            userId,
            productId,
            quantity,
            amount: order.totalAmount
        });

        // 6. Record Idempotency cache
        if (idempotencyKey) {
            redis.recordIdempotencyResult(idempotencyKey, order);
        }

        const endTime = process.hrtime.bigint();
        const executionMicroseconds = Number(endTime - startTime) / 1000;

        return {
            status: 'SUCCESS',
            code: 201,
            message: 'Checkout successful!',
            order,
            eventId,
            latencyUs: executionMicroseconds.toFixed(2)
        };
    }

    getRecentOrders(limit = 20) {
        return this.orders.slice(-limit).reverse();
    }
}

module.exports = new FlashSaleService();
