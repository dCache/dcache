package org.dcache.pool.classic;

import java.util.Map;

/**
 *
 * @since 1.9.11
 */
public class MoverExecutorServices {
    private final MoverExecutorService _defaultExecutorService;
    private final PostTransferExecutionService _defaultPostService =
        new ClassicPostExecutionService();
    private final Map<String, MoverExecutorService> _executionService;

    public MoverExecutorServices(MoverExecutorService defaultExecutorService,
                                 Map<String, MoverExecutorService> executionService) {
        _defaultExecutorService = defaultExecutorService;
        _executionService = executionService;
    }

    public MoverExecutorService getExecutorService(String protocol) {
        MoverExecutorService service = _executionService.get(protocol);
        if (service != null) {
            return service;
        }

        return _defaultExecutorService;
    }

    public PostTransferExecutionService getPostExecutorService(String protocol) {
        return _defaultPostService;
    }
}
