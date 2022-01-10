package org.dcache.qos.services.verifier.data;

public enum VerifyOperationState {
    UNINITIALIZED("CONSTRUCTED BUT NOT YET CONFIGURED"),
    READY("READY TO RUN VERIFICATION AGAIN"),
    RUNNING("VERIFICATION SUBMITTED TO THE EXECUTOR"),
    WAITING("LONG RUNNING ADJUSTMENT GOES INTO THIS STATE"),
    DONE("CURRENT ADJUSTMENT COMPLETED WITHOUT ERROR"),
    CANCELED("CURRENT ADJUSTMENT WAS TERMINATED BY USER"),
    FAILED("CURRENT ADJUSTMENT FAILED WITH EXCEPTION"),
    ABORTED("CANNOT DO ANYTHING FURTHER");

    private final String description;

    VerifyOperationState(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
