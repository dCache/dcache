package diskCacheV111.vehicles;

import java.util.LinkedList;

import gplazma.authz.records.gPlazmaAuthorizationRecord;

public class AuthenticationMessage extends Message {

  long authRequestID;
  LinkedList<gPlazmaAuthorizationRecord> gauthlist;

  public AuthenticationMessage() {
    super();
  }

  public AuthenticationMessage(LinkedList <gPlazmaAuthorizationRecord> gauthlist, long authRequestID) {
    this();
    this.authRequestID = authRequestID;
    this.gauthlist=gauthlist;
  }

  public long getAuthRequestID() {
    return authRequestID;
  }

  public LinkedList <gPlazmaAuthorizationRecord> getgPlazmaAuthRecords() {
    if (gauthlist==null) {
        gauthlist = new LinkedList <gPlazmaAuthorizationRecord> ();
  }

    return gauthlist;
    }

  }
