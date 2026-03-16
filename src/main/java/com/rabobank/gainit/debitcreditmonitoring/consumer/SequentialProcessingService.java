package com.rabobank.gainit.debitcreditmonitoring.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service to control sequential processing of Event Hub messages.
 * Ensures that credit and debit process queues are only consumed after
 * the main debit-credit-events queue is completely drained.
 */
@Service
@Slf4j
public class SequentialProcessingService {

    private final AtomicBoolean mainQueueDrained = new AtomicBoolean(false);
    private volatile long lastMainQueueActivity = System.currentTimeMillis();
    private static final long DRAIN_CHECK_INTERVAL_MS = 5000; // 5 seconds

    /**
     * Called when a message is received from the main queue (debit-credit-events).
     * Resets the drained flag and updates activity timestamp.
     */
    public void onMainQueueMessage() {
        mainQueueDrained.set(false);
        lastMainQueueActivity = System.currentTimeMillis();
        log.debug("Main queue activity detected, resetting drained flag");
    }

    /**
     * Called periodically to check if main queue appears to be drained.
     * Uses a time-based heuristic since we can't directly query queue depth.
     */
    public void checkMainQueueDrained() {
        long timeSinceLastActivity = System.currentTimeMillis() - lastMainQueueActivity;

        if (timeSinceLastActivity > DRAIN_CHECK_INTERVAL_MS && !mainQueueDrained.get()) {
            mainQueueDrained.set(true);
            log.info("Main queue (debit-credit-events) appears to be drained. " +
                    "Credit and debit processing queues can now be consumed.");
        }
    }

    /**
     * Checks if secondary queues (credit and debit) can be processed.
     * @return true if main queue is drained and secondary processing is allowed
     */
    public boolean canProcessSecondaryQueues() {
        checkMainQueueDrained(); // Update drained status
        return mainQueueDrained.get();
    }

    /**
     * Forces the main queue to be marked as drained (for testing or manual control).
     */
    public void forceMainQueueDrained() {
        mainQueueDrained.set(true);
        log.warn("Main queue manually marked as drained");
    }

    /**
     * Resets the processing state (for testing or restart scenarios).
     */
    public void reset() {
        mainQueueDrained.set(false);
        lastMainQueueActivity = System.currentTimeMillis();
        log.info("Sequential processing state reset");
    }
}