/**
 * Core In-Memory Distributed Data Store (Simulates Redis Cluster & Cache-Aside Layer)
 * Implements:
 * 1. Atomic Lua Script / Compare-And-Swap (CAS) inventory decrements
 * 2. Distributed Lock with TTL (Redlock algorithm simulation)
 * 3. Token Bucket Rate Limiting per IP/User
 * 4. Idempotency Key validation store
 */

class RedisCacheEngine {
    constructor() {
        this.store = new Map();
        this.locks = new Map();
        this.rateLimiters = new Map();
        this.idempotencyStore = new Map();
        this.metrics = {
            totalHits: 0,
            totalMisses: 0,
            atomicOps: 0,
            locksAcquired: 0,
            locksFailed: 0,
            rateLimitedCount: 0
        };
    }

    // Set Key-Value with optional TTL (ms)
    set(key, value, ttlMs = null) {
        const expiresAt = ttlMs ? Date.now() + ttlMs : null;
        this.store.set(key, { value, expiresAt });
    }

    get(key) {
        const item = this.store.get(key);
        if (!item) {
            this.metrics.totalMisses++;
            return null;
        }
        if (item.expiresAt && Date.now() > item.expiresAt) {
            this.store.delete(key);
            this.metrics.totalMisses++;
            return null;
        }
        this.metrics.totalHits++;
        return item.value;
    }

    // Atomic Lua Script Simulation: Decrement Stock only if stock >= count
    atomicStockDecrement(productId, count = 1) {
        this.metrics.atomicOps++;
        const key = `stock:${productId}`;
        const currentStock = this.get(key);

        if (currentStock === null) {
            return { success: false, reason: 'PRODUCT_NOT_FOUND', remainingStock: 0 };
        }

        if (currentStock < count) {
            return { success: false, reason: 'OUT_OF_STOCK', remainingStock: currentStock };
        }

        const newStock = currentStock - count;
        this.set(key, newStock);
        return { success: true, remainingStock: newStock };
    }

    // Distributed Locking (Redlock style mutex with TTL to prevent deadlocks)
    acquireLock(lockKey, ttlMs = 2000, holderId = 'client') {
        const existing = this.locks.get(lockKey);
        const now = Date.now();

        if (existing && now < existing.expiresAt) {
            this.metrics.locksFailed++;
            return { acquired: false, reason: 'LOCK_HELD' };
        }

        this.locks.set(lockKey, { holderId, expiresAt: now + ttlMs });
        this.metrics.locksAcquired++;
        return { acquired: true, lockKey, holderId };
    }

    releaseLock(lockKey, holderId) {
        const existing = this.locks.get(lockKey);
        if (existing && existing.holderId === holderId) {
            this.locks.delete(lockKey);
            return true;
        }
        return false;
    }

    // Token Bucket Rate Limiting (e.g., max 20 req/sec per user)
    checkRateLimit(clientId, capacity = 15, refillRatePerSec = 5) {
        const now = Date.now();
        let bucket = this.rateLimiters.get(clientId);

        if (!bucket) {
            bucket = { tokens: capacity, lastRefill: now };
            this.rateLimiters.set(clientId, bucket);
        }

        // Refill tokens based on elapsed time
        const elapsedSec = (now - bucket.lastRefill) / 1000;
        const tokensToAdd = elapsedSec * refillRatePerSec;
        bucket.tokens = Math.min(capacity, bucket.tokens + tokensToAdd);
        bucket.lastRefill = now;

        if (bucket.tokens >= 1) {
            bucket.tokens -= 1;
            return { allowed: true, remainingTokens: Math.floor(bucket.tokens) };
        } else {
            this.metrics.rateLimitedCount++;
            return { allowed: false, remainingTokens: 0, retryAfterMs: Math.ceil((1 - bucket.tokens) / refillRatePerSec * 1000) };
        }
    }

    // Idempotency: prevent double order submissions
    checkAndSetIdempotency(key, payload, ttlMs = 60000) {
        const existing = this.idempotencyStore.get(key);
        if (existing && Date.now() < existing.expiresAt) {
            return { isDuplicate: true, cachedResult: existing.result };
        }
        return { isDuplicate: false };
    }

    recordIdempotencyResult(key, result, ttlMs = 60000) {
        this.idempotencyStore.set(key, { result, expiresAt: Date.now() + ttlMs });
    }

    getStats() {
        return {
            ...this.metrics,
            activeKeys: this.store.size,
            activeLocks: this.locks.size,
            activeRateLimiters: this.rateLimiters.size
        };
    }
}

module.exports = new RedisCacheEngine();
