const SSEStatus = {
    PENDING: "pending",
    CREATED: "created",
    RUNNING: "running",
    DISCONNECTED: "disconnected",
    CANCELLED: "cancelled",
    ERROR: "error",
    SERVER_ERROR: "server-side-error"
};
window.CONFIG["sse"] = {
    "status": SSEStatus.PENDING
};