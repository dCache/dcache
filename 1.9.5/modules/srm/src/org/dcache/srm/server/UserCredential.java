package org.dcache.srm.server;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSContext;

public class UserCredential {
   public String secureId;
   public GSSCredential credential;
   public GSSContext context;
   public String clientHost;
}

