package org.dcache.pool.classic;

import java.util.Map;

/**
 *
 * @since 1.9.11
 */
public class MoverExecutorServices {
    private final MoverExecutorService _defaultExecutorService;
    private final PostTransferExecutionService _defaultPostService;
    private final Map<String, MoverExecutorService> _executionService;

    public MoverExecutorServices(MoverExecutorService defaultExecutorService,
                                 Map<String, MoverExecutorService> executionService,
                                 PostTransferExecutionService postTransferExecutionService) {
        _defaultExecutorService = defaultExecutorService;
        _executionService = executionService;
        _defaultPostService = postTransferExecutionService;
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
