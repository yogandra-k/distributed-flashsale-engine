/**
 * Distributed Load Testing Benchmark Generator
 * Simulates 100 to 5,000 concurrent virtual users storming the checkout API.
 */

const flashSaleService = require('./flashSaleService');

class LoadTestRunner {
    constructor() {
        this.isRunning = false;
    }

    async runLoadTest({ productId = 'prod_ps5_pro', totalRequests = 500, concurrency = 50, rateLimitStrict = false }) {
        if (this.isRunning) {
            return { status: 'error', message: 'A stress test is already in progress.' };
        }

        this.isRunning = true;
        const initialStatus = flashSaleService.getProductStatus(productId);
        const startTime = Date.now();

        const results = {
            totalRequests,
            concurrency,
            successfulOrders: 0,
            outOfStockRejections: 0,
            rateLimitedRejections: 0,
            latenciesMs: [],
            oversoldError: false
        };

        let requestsRemaining = totalRequests;
        const batches = Math.ceil(totalRequests / concurrency);

        for (let b = 0; b < batches; b++) {
            const batchSize = Math.min(concurrency, requestsRemaining);
            requestsRemaining -= batchSize;

            const promises = Array.from({ length: batchSize }).map((_, i) => {
                const userId = rateLimitStrict ? 'bot_user_spam' : `user_${b}_${i}_${Math.random().toString(36).substr(2, 5)}`;
                const reqStart = Date.now();
                const res = flashSaleService.processCheckout({
                    userId,
                    productId,
                    quantity: 1,
                    idempotencyKey: `idemp_${userId}`
                });
                const reqEnd = Date.now();
                results.latenciesMs.push(reqEnd - reqStart);

                if (res.status === 'SUCCESS') results.successfulOrders++;
                else if (res.status === 'REJECTED_OUT_OF_STOCK') results.outOfStockRejections++;
                else if (res.status === 'REJECTED_RATE_LIMITED') results.rateLimitedRejections++;

                return res;
            });

            await Promise.all(promises);
            // Micro pause to simulate network packet interval
            await new Promise((r) => setTimeout(r, 10));
        }

        const endTime = Date.now();
        const finalStatus = flashSaleService.getProductStatus(productId);
        this.isRunning = false;

        const totalDurationSec = (endTime - startTime) / 1000;
        const rps = (totalRequests / (totalDurationSec || 0.001)).toFixed(1);
        const avgLatency = (results.latenciesMs.reduce((a, b) => a + b, 0) / results.latenciesMs.length).toFixed(2);
        const p99Latency = results.latenciesMs.sort((a, b) => a - b)[Math.floor(results.latenciesMs.length * 0.99)] || 0;

        // Check if overselling occurred (Negative stock invariant check)
        if (finalStatus.availableStock < 0 || results.successfulOrders > initialStatus.availableStock) {
            results.oversoldError = true;
        }

        return {
            productId,
            totalRequests,
            concurrency,
            totalDurationSec: totalDurationSec.toFixed(3),
            requestsPerSecond: Number(rps),
            avgLatencyMs: Number(avgLatency),
            p99LatencyMs: p99Latency,
            successfulOrders: results.successfulOrders,
            outOfStockRejections: results.outOfStockRejections,
            rateLimitedRejections: results.rateLimitedRejections,
            finalStock: finalStatus.availableStock,
            zeroOversellingVerified: !results.oversoldError
        };
    }
}

module.exports = new LoadTestRunner();
