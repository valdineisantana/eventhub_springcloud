package com.rabobank.gainit.debitcreditmonitoring.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Elegant binding-based sequential processing controller.
 * Ensures three-phase processing: Main -> Credit -> Debit queues.
 */
@Service
@Slf4j
public class SequentialProcessingController {

    private final BindingsLifecycleController bindingsController;

    // Phase tracking
    private final AtomicBoolean mainQueueActive = new AtomicBoolean(false);
    private final AtomicBoolean creditQueueActive = new AtomicBoolean(false);
    private final AtomicBoolean debitQueueActive = new AtomicBoolean(false);

    // Timestamps for activity detection
    private final AtomicLong lastMainQueueMessage = new AtomicLong(0);
    private final AtomicLong lastCreditQueueMessage = new AtomicLong(0);

    // Binding names from application.yml
    private static final String MONITOR_CREDIT_BINDING = "monitorCredit-in-0";
    private static final String MONITOR_DEBIT_BINDING = "monitorDebit-in-0";

    // Phase constants
    private static final long INACTIVITY_THRESHOLD_MS = 3000; // 3 seconds

    public SequentialProcessingController(BindingsLifecycleController bindingsController) {
        this.bindingsController = bindingsController;
        // Start with all secondary queues paused
        pauseAllSecondaryQueues();
    }

    /**
     * Called when a message is received from the main queue.
     * Immediately pauses all secondary queues.
     */
    public void onMainQueueMessage() {
        mainQueueActive.set(true);
        lastMainQueueMessage.set(System.currentTimeMillis());

        if (areAnySecondaryQueuesActive()) {
            log.info("Main queue message received - pausing all secondary queues");
            pauseAllSecondaryQueues();
        }
    }

    /**
     * Called when a message is received from the credit queue.
     * Pauses debit queue if it's active.
     */
    public void onCreditQueueMessage() {
        if (!mainQueueActive.get()) { // Only track if main queue is drained
            creditQueueActive.set(true);
            lastCreditQueueMessage.set(System.currentTimeMillis());

            if (debitQueueActive.get()) {
                log.info("Credit queue message received - pausing debit queue");
                pauseDebitQueue();
            }
        }
    }

    /**
     * Called when a message is received from the debit queue.
     * No action needed as debit is the final phase.
     */
    public void onDebitQueueMessage() {
        if (!mainQueueActive.get() && !creditQueueActive.get()) { // Only track if both previous are drained
            debitQueueActive.set(true);
        }
    }

    /**
     * Periodic check to determine processing phases.
     * Manages the three-phase state machine.
     */
    @Scheduled(fixedDelay = 2000) // Check every 2 seconds
    public void checkProcessingPhases() {
        long currentTime = System.currentTimeMillis();

        // Phase 1: Check if main queue is drained
        if (mainQueueActive.get()) {
            long timeSinceLastMainMessage = currentTime - lastMainQueueMessage.get();
            if (timeSinceLastMainMessage > INACTIVITY_THRESHOLD_MS) {
                mainQueueActive.set(false);
                log.info("Phase 1 complete: Main queue drained after {}ms. Starting Phase 2: Credit processing", timeSinceLastMainMessage);
                resumeCreditQueue();
            }
        }
        // Phase 2: Check if credit queue is drained (only if main is drained)
        else if (creditQueueActive.get()) {
            long timeSinceLastCreditMessage = currentTime - lastCreditQueueMessage.get();
            if (timeSinceLastCreditMessage > INACTIVITY_THRESHOLD_MS) {
                creditQueueActive.set(false);
                log.info("Phase 2 complete: Credit queue drained after {}ms. Starting Phase 3: Debit processing", timeSinceLastCreditMessage);
                resumeDebitQueue();
            }
        }
        // Phase 3: Debit processing is active when both main and credit are drained
        else if (!debitQueueActive.get()) {
            log.info("Phase 3 active: All queues drained. Debit processing enabled.");
            resumeDebitQueue();
        }
    }

    /**
     * Pause all secondary queues (credit and debit).
     */
    private void pauseAllSecondaryQueues() {
        pauseCreditQueue();
        pauseDebitQueue();
    }

    /**
     * Pause only the credit queue.
     */
    private void pauseCreditQueue() {
        try {
            bindingsController.changeState(MONITOR_CREDIT_BINDING, BindingsLifecycleController.State.PAUSED);
            log.debug("Credit queue paused: {}", MONITOR_CREDIT_BINDING);
        } catch (Exception e) {
            log.error("Failed to pause credit queue", e);
        }
    }

    /**
     * Pause only the debit queue.
     */
    private void pauseDebitQueue() {
        try {
            bindingsController.changeState(MONITOR_DEBIT_BINDING, BindingsLifecycleController.State.PAUSED);
            debitQueueActive.set(false);
            log.debug("Debit queue paused: {}", MONITOR_DEBIT_BINDING);
        } catch (Exception e) {
            log.error("Failed to pause debit queue", e);
        }
    }

    /**
     * Resume the credit queue.
     */
    private void resumeCreditQueue() {
        try {
            bindingsController.changeState(MONITOR_CREDIT_BINDING, BindingsLifecycleController.State.RESUMED);
            creditQueueActive.set(true);
            log.info("Credit queue resumed: {}", MONITOR_CREDIT_BINDING);
        } catch (Exception e) {
            log.error("Failed to resume credit queue", e);
        }
    }

    /**
     * Resume the debit queue.
     */
    private void resumeDebitQueue() {
        try {
            bindingsController.changeState(MONITOR_DEBIT_BINDING, BindingsLifecycleController.State.RESUMED);
            debitQueueActive.set(true);
            log.info("Debit queue resumed: {}", MONITOR_DEBIT_BINDING);
        } catch (Exception e) {
            log.error("Failed to resume debit queue", e);
        }
    }

    /**
     * Check if any secondary queues are currently active.
     */
    private boolean areAnySecondaryQueuesActive() {
        return creditQueueActive.get() || debitQueueActive.get();
    }

    /**
     * Force resume all secondary queues (for manual control).
     */
    public void forceResumeAllSecondaryQueues() {
        log.warn("Manually forcing resume of all secondary queues");
        resumeCreditQueue();
        resumeDebitQueue();
    }

    /**
     * Force pause all secondary queues (for manual control).
     */
    public void forcePauseAllSecondaryQueues() {
        log.warn("Manually forcing pause of all secondary queues");
        pauseAllSecondaryQueues();
    }

    /**
     * Force resume only debit queue (for testing).
     */
    public void forceResumeDebitQueue() {
        log.warn("Manually forcing resume of debit queue");
        resumeDebitQueue();
    }

    /**
     * Get current processing status.
     */
    public ProcessingStatus getStatus() {
        return new ProcessingStatus(
            mainQueueActive.get(),
            creditQueueActive.get(),
            debitQueueActive.get(),
            System.currentTimeMillis() - lastMainQueueMessage.get(),
            System.currentTimeMillis() - lastCreditQueueMessage.get()
        );
    }

    public static class ProcessingStatus {
        public final boolean mainQueueActive;
        public final boolean creditQueueActive;
        public final boolean debitQueueActive;
        public final long timeSinceLastMainMessage;
        public final long timeSinceLastCreditMessage;

        public ProcessingStatus(boolean mainQueueActive, boolean creditQueueActive, boolean debitQueueActive,
                              long timeSinceLastMainMessage, long timeSinceLastCreditMessage) {
            this.mainQueueActive = mainQueueActive;
            this.creditQueueActive = creditQueueActive;
            this.debitQueueActive = debitQueueActive;
            this.timeSinceLastMainMessage = timeSinceLastMainMessage;
            this.timeSinceLastCreditMessage = timeSinceLastCreditMessage;
        }
    }
}