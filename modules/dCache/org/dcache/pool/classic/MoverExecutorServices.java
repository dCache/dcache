package org.dcache.pool.classic;

import java.util.Map;

/**
 *
 * @since 1.9.11
 */
public class MoverExecutorServices {

    private static final MoverExecutorService DEFAULT = new LegacyMoverExecutorService();
    private static final PostTransferExecutionService DEFAULT_POST = new ClassicPostExecutionService();

    private final Map<String, MoverExecutorService> _executionService;

    public MoverExecutorServices(Map<String, MoverExecutorService> executionService) {
        _executionService = executionService;
    }

    public MoverExecutorService getExecutorService(String protocol) {
        MoverExecutorService service = _executionService.get(protocol);
        if (service != null) {
            return service;
        }

        return DEFAULT;
    }

    public PostTransferExecutionService getPostExecutorService(String protocol) {
        return DEFAULT_POST;
    }
}
