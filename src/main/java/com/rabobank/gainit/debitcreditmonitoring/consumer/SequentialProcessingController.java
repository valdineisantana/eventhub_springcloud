package com.rabobank.gainit.debitcreditmonitoring.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Elegant binding-based sequential processing controller.
 * Uses Spring Cloud Stream binding control instead of time-based heuristics.
 */
@Service
@Slf4j
public class SequentialProcessingController {

    private final BindingsLifecycleController bindingsController;
    private final AtomicBoolean mainQueueActive = new AtomicBoolean(false);
    private final AtomicLong lastMainQueueMessage = new AtomicLong(0);
    private final AtomicBoolean secondaryQueuesPaused = new AtomicBoolean(true);

    // Binding names from application.yml
    private static final String MONITOR_CREDIT_BINDING = "monitorCredit-in-0";
    private static final String MONITOR_DEBIT_BINDING = "monitorDebit-in-0";

    public SequentialProcessingController(BindingsLifecycleController bindingsController) {
        this.bindingsController = bindingsController;
        // Start with secondary queues paused
        pauseSecondaryQueues();
    }

    /**
     * Called when a message is received from the main queue.
     * Immediately pauses secondary queues if they were active.
     */
    public void onMainQueueMessage() {
        mainQueueActive.set(true);
        lastMainQueueMessage.set(System.currentTimeMillis());

        if (!secondaryQueuesPaused.get()) {
            log.info("Main queue message received - pausing secondary queues");
            pauseSecondaryQueues();
        }
    }

    /**
     * Called when main queue processing is complete.
     * Enables secondary queue processing.
     */
    public void onMainQueueDrained() {
        mainQueueActive.set(false);
        log.info("Main queue drained - enabling secondary queue processing");
        resumeSecondaryQueues();
    }

    /**
     * Periodic check to determine if main queue is drained.
     * Uses a more sophisticated approach than simple time-based heuristic.
     */
    @Scheduled(fixedDelay = 2000) // Check every 2 seconds
    public void checkMainQueueStatus() {
        if (!mainQueueActive.get()) {
            long timeSinceLastMessage = System.currentTimeMillis() - lastMainQueueMessage.get();

            // If no main queue activity for 3 seconds, consider it drained
            if (timeSinceLastMessage > 3000 && secondaryQueuesPaused.get()) {
                log.info("Main queue appears inactive for {}ms - enabling secondary processing", timeSinceLastMessage);
                onMainQueueDrained();
            }
        }
    }

    /**
     * Pause secondary queue consumers.
     */
    private void pauseSecondaryQueues() {
        try {
            bindingsController.changeState(MONITOR_CREDIT_BINDING, BindingsLifecycleController.State.PAUSED);
            bindingsController.changeState(MONITOR_DEBIT_BINDING, BindingsLifecycleController.State.PAUSED);
            secondaryQueuesPaused.set(true);
            log.info("Secondary queues paused: credit={}, debit={}",
                    MONITOR_CREDIT_BINDING, MONITOR_DEBIT_BINDING);
        } catch (Exception e) {
            log.error("Failed to pause secondary queues", e);
        }
    }

    /**
     * Resume secondary queue consumers.
     */
    private void resumeSecondaryQueues() {
        try {
            bindingsController.changeState(MONITOR_CREDIT_BINDING, BindingsLifecycleController.State.RESUMED);
            bindingsController.changeState(MONITOR_DEBIT_BINDING, BindingsLifecycleController.State.RESUMED);
            secondaryQueuesPaused.set(false);
            log.info("Secondary queues resumed: credit={}, debit={}",
                    MONITOR_CREDIT_BINDING, MONITOR_DEBIT_BINDING);
        } catch (Exception e) {
            log.error("Failed to resume secondary queues", e);
        }
    }

    /**
     * Check if secondary queues are currently active.
     */
    public boolean areSecondaryQueuesActive() {
        return !secondaryQueuesPaused.get();
    }

    /**
     * Force resume secondary queues (for manual control).
     */
    public void forceResumeSecondaryQueues() {
        log.warn("Manually forcing resume of secondary queues");
        onMainQueueDrained();
    }

    /**
     * Force pause secondary queues (for manual control).
     */
    public void forcePauseSecondaryQueues() {
        log.warn("Manually forcing pause of secondary queues");
        pauseSecondaryQueues();
    }

    /**
     * Get current status for monitoring.
     */
    public ProcessingStatus getStatus() {
        return new ProcessingStatus(
            mainQueueActive.get(),
            !secondaryQueuesPaused.get(),
            System.currentTimeMillis() - lastMainQueueMessage.get()
        );
    }

    public static class ProcessingStatus {
        public final boolean mainQueueActive;
        public final boolean secondaryQueuesActive;
        public final long timeSinceLastMainMessage;

        public ProcessingStatus(boolean mainQueueActive, boolean secondaryQueuesActive, long timeSinceLastMainMessage) {
            this.mainQueueActive = mainQueueActive;
            this.secondaryQueuesActive = secondaryQueuesActive;
            this.timeSinceLastMainMessage = timeSinceLastMainMessage;
        }
    }
}