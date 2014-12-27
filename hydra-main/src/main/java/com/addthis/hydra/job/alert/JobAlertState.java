package com.addthis.hydra.job.alert;

/**
 * Any state that has the string "SENDING_" indicates
 * that an email should be sent during this iteration of the
 * alert scan. The "SENDING_" states are transient they must
 * always transition to another state on the next iteration or
 * another email will be delivered.
 *
 * The lifecycle of a typical alert with no delay is:
 *    TRIGGER_SENDING_EMAIL -> TRIGGER_SENT_EMAIL -> (wait for alert to clear)
 *    -> CLEAR_SENDING_EMAIL -> (state deleted)
 *
 * The lifecycle of a typical alert with delay is either:
 *    TRIGGER_DELAY_EMAIL -> (wait for alert to clear) -> (state deleted)
 * or:
 *    TRIGGER_DELAY_EMAIL -> TRIGGER_SENDING_EMAIL ->
 *    TRIGGER_SENT_EMAIL -> (wait for alert to clear) -> CLEAR_SENDING_EMAIL
 *    -> (state deleted)
 */
public enum JobAlertState {

    /**
     * signal that a delayed alert has not yet been delivered
     */
    TRIGGER_DELAY_EMAIL,

    /**
     * signal that an alert message is to be sent at current iteration
     */
    TRIGGER_SENDING_EMAIL,

    /**
     * signal that an alert message was sent in a previous iteration
     */
    TRIGGER_SENT_EMAIL,

    /**
     * signal that an alert changed message is to be sent at current iteration
     */
    TRIGGER_SENDING_CHANGED,

    /**
     * signal that an alert cleared message is to be sent at current iteration
     */
    CLEAR_SENDING_EMAIL,
}
