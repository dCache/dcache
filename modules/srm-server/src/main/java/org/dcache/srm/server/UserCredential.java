package org.dcache.srm.server;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;

public class UserCredential {
   public String secureId;
   public GSSCredential credential;
   public GSSContext context;
   public String clientHost;
}

