package org.dcache.srm;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public class SRMPermissionDeniedException extends SRMException {
        public SRMPermissionDeniedException() {
                super(TStatusCode.SRM_AUTHORIZATION_FAILURE,"Permission denied");
        }

        public SRMPermissionDeniedException(String msg) {
                super(TStatusCode.SRM_AUTHORIZATION_FAILURE,msg);
        }

        public SRMPermissionDeniedException(String message,Throwable cause) {
                super(message,cause);
                status.setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
        }

        public SRMPermissionDeniedException(Throwable cause) {
                super(cause);
                status.setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
        }
}
