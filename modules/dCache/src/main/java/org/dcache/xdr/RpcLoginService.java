package org.dcache.xdr;

import java.security.Principal;
import javax.security.auth.Subject;

public interface RpcLoginService {

    Subject login(Principal principal);
}
