package dev.whitewater;

/**
 * Base class for every Riverbed failure. {@code status} is the HTTP status;
 * {@code code} is the machine-actionable refusal code when the server
 * supplied one — never branch on {@link #getMessage()} text.
 */
public class WhitewaterException extends RuntimeException {

    private final int status;
    private final String code;

    public WhitewaterException(int status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public int status() {
        return status;
    }

    /** Machine-actionable refusal code (e.g. {@code "lease.busy"}), or null. */
    public String code() {
        return code;
    }

    /** A Writer/Reader session epoch was superseded; reopen the session. */
    public static class StaleSession extends WhitewaterException {
        public StaleSession(int status, String code, String message) {
            super(status, code, message);
        }
    }

    /** The request's outcome is uncertain; identical retry is required. */
    public static class AmbiguousResult extends WhitewaterException {
        public AmbiguousResult(int status, String code, String message) {
            super(status, code, message);
        }
    }

    /** Quorum or replica temporarily unavailable; safe to retry. */
    public static class Unavailable extends WhitewaterException {
        public Unavailable(int status, String code, String message) {
            super(status, code, message);
        }
    }

    /** A Subscription work lease conflict (lease.busy/stale_lease/capacity). */
    public static class LeaseConflict extends WhitewaterException {
        public LeaseConflict(int status, String code, String message) {
            super(status, code, message);
        }
    }

    /** Generic fencing conflict without a finer code. */
    public static class Conflict extends WhitewaterException {
        public Conflict(int status, String code, String message) {
            super(status, code, message);
        }
    }

    /** The server refused the request (bad input, missing auth, not found). */
    public static class Refused extends WhitewaterException {
        public Refused(int status, String code, String message) {
            super(status, code, message);
        }
    }

    /** The contacted Node speaks an incompatible protocol major. */
    public static class IncompatibleServer extends WhitewaterException {
        public IncompatibleServer(int status, String code, String message) {
            super(status, code, message);
        }
    }
}
