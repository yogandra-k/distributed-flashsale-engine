/**
 * Asynchronous Event Queue / Message Broker (Simulates Apache Kafka / RabbitMQ)
 * Decouples the fast in-memory inventory decrement from heavy database writes,
 * payment verification, and invoice notifications.
 */

const EventEmitter = require('events');

class AsyncEventQueue extends EventEmitter {
    constructor() {
        super();
        this.queue = [];
        this.deadLetterQueue = [];
        this.isProcessing = false;
        this.stats = {
            totalPublished: 0,
            totalProcessed: 0,
            totalFailed: 0,
            avgLatencyMs: 0
        };
        this.processedLatencies = [];
    }

    // Publish event into topic/partition
    publish(topic, payload) {
        const message = {
            id: `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            topic,
            payload,
            publishedAt: Date.now(),
            retryCount: 0
        };

        this.queue.push(message);
        this.stats.totalPublished++;
        this.emit('message_queued', message);

        if (!this.isProcessing) {
            this.processQueue();
        }

        return message.id;
    }

    async processQueue() {
        if (this.queue.length === 0) {
            this.isProcessing = false;
            return;
        }

        this.isProcessing = true;
        const msg = this.queue.shift();

        try {
            // Simulated asynchronous database persistence & verification delay (5-15ms)
            await new Promise((resolve) => setTimeout(resolve, Math.floor(Math.random() * 10) + 5));

            const latency = Date.now() - msg.publishedAt;
            this.processedLatencies.push(latency);
            if (this.processedLatencies.length > 100) this.processedLatencies.shift();
            this.stats.avgLatencyMs = (
                this.processedLatencies.reduce((a, b) => a + b, 0) / this.processedLatencies.length
            ).toFixed(1);

            this.stats.totalProcessed++;
            this.emit('message_processed', { ...msg, latency });
        } catch (err) {
            msg.retryCount++;
            if (msg.retryCount <= 3) {
                this.queue.push(msg); // Retry backoff
            } else {
                this.deadLetterQueue.push({ ...msg, error: err.message });
                this.stats.totalFailed++;
                this.emit('message_dead_letter', msg);
            }
        }

        // Keep processing next event
        setImmediate(() => this.processQueue());
    }

    getStats() {
        return {
            ...this.stats,
            queueDepth: this.queue.length,
            dlqDepth: this.deadLetterQueue.length
        };
    }
}

module.exports = new AsyncEventQueue();
