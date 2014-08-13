package org.dcache.srm.server;

import org.ietf.jgss.GSSCredential;

import java.security.cert.X509Certificate;

public class UserCredential {
   public String secureId;
   public GSSCredential credential;
   public X509Certificate[] chain;
   public String clientHost;
}

