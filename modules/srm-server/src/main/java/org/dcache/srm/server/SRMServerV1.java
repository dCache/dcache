/**
 * ISRMImpl.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.2RC2 Nov 16, 2004 (12:19:44 EST) WSDL2Java emitter.
 */

package org.dcache.srm.server;

import org.glite.voms.PKIVerifier;
import org.gridforum.jgss.ExtendedGSSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collection;

import org.dcache.auth.util.GSSUtils;
import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.client.ConvertUtil;
import org.dcache.srm.util.Axis;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;


public class SRMServerV1 implements org.dcache.srm.client.axis.ISRM_PortType{

   public Logger log;
   private SrmAuthorizer srmAuth;
   private PKIVerifier pkiVerifier;
   private final SRM srm;
   private final RequestCounters<String> srmServerCounters;
   private final RequestExecutionTimeGauges<String> srmServerGauges;

    public SRMServerV1() throws java.rmi.RemoteException {
       try
       {
          // srmConn = SrmDCacheConnector.getInstance();
          log = LoggerFactory.getLogger(this.getClass().getName());

             srm = Axis.getSRM();
             Configuration config = Axis.getConfiguration();

             srmAuth = new SrmAuthorizer(config.getAuthorization(),
                    srm.getRequestCredentialStorage(),
                    config.isClientDNSLookup());

             // use default locations for cacerts and vomdsdir
             pkiVerifier
                 = GSSUtils.getPkiVerifier(null,null, MDC.getCopyOfContextMap());
             srmServerCounters = srm.getSrmServerV1Counters();
             srmServerGauges = srm.getSrmServerV1Gauges();
       }
       catch ( java.rmi.RemoteException re) { throw re; }
       catch ( Exception e) {
           throw new java.rmi.RemoteException("exception",e);
       }

    }

      /**
       * increment particular request type count
       */

    private void incrementRequest(String operation) {
      srmServerCounters.incrementRequests(operation);
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus put(java.lang.String[] arg0,
            java.lang.String[] arg1, long[] arg2, boolean[] arg3, java.lang.String[] arg4)
            throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "put";
      try (JDC ignored = JDC.createSession("srm1:put")) {
          incrementRequest(methodName);
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;

          try {
              userCred = srmAuth.getUserCredentials();
              Collection roles
                  = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                                  pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
              user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
              if (user.isReadOnly()) {
                  throw new SRMAuthorizationException("Session is read-only");
              }
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {

             requestStatus = srm.put(user,requestCredential,arg0,arg1,arg2,arg3,arg4, userCred.clientHost);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm put failed", e);
          }
          org.dcache.srm.client.axis.RequestStatus response =
            ConvertUtil.RS2axisRS(requestStatus);
            return response;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus get(java.lang.String[] arg0, java.lang.String[] arg1) throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "get";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:get")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;
          try {
             userCred = srmAuth.getUserCredentials();
              Collection roles
                  = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                              pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {

             requestStatus = srm.get(user,
                 requestCredential,
                 arg0,
                 arg1,
                 userCred.clientHost);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm get failed", e);
          }
          org.dcache.srm.client.axis.RequestStatus response =
            ConvertUtil.RS2axisRS(requestStatus);
            return response;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus copy(java.lang.String[] arg0, java.lang.String[] arg1, boolean[] arg2) throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "copy";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:copy")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;
          try {
             userCred = srmAuth.getUserCredentials();
              Collection roles
                  = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                              pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {

             requestStatus = srm.copy(user,
                 requestCredential,
                 arg0,
                 arg1,
                 arg2,
                 userCred.clientHost);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm put failed", e);
          }
          org.dcache.srm.client.axis.RequestStatus response =
            ConvertUtil.RS2axisRS(requestStatus);
            return response;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public boolean ping() throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "ping";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:ping")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user = null;
          org.dcache.srm.request.RequestCredential requestCredential;

          try {
             userCred = srmAuth.getUserCredentials();
             Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                         pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }
          return true;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus pin(java.lang.String[] arg0) throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "pin";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:pin")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user = null;
          org.dcache.srm.request.RequestCredential requestCredential;
          try {
             userCred = srmAuth.getUserCredentials();
             Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                                 pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("SRMServerV1.pin() : role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

             throw new java.rmi.RemoteException("srm pin is not supported");
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus unPin(java.lang.String[] arg0, int arg1) throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "unPin";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:unpin")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user = null;
          org.dcache.srm.request.RequestCredential requestCredential;
          try {
             userCred = srmAuth.getUserCredentials();
             Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                                 pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          throw new java.rmi.RemoteException("srm unPin is not supported");
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus setFileStatus(int arg0, int arg1, java.lang.String arg2) throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "setFileStatus";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:setFileStatus")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;
          try {
             userCred = srmAuth.getUserCredentials();
             Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                                 pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {

             requestStatus = srm.setFileStatus(user,requestCredential,arg0,arg1,arg2);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm setFileStatus failed", e);
          }
          org.dcache.srm.client.axis.RequestStatus response =
            ConvertUtil.RS2axisRS(requestStatus);
            return response;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus getRequestStatus(int arg0) throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "getRequestStatus";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:getRequestStatus")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;
          try {
             userCred = srmAuth.getUserCredentials();
             Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                                 pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {

             requestStatus = srm.getRequestStatus(user,requestCredential,arg0);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm getRequestStatus failed", e);
          }
          org.dcache.srm.client.axis.RequestStatus response =
            ConvertUtil.RS2axisRS(requestStatus);
            return response;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.FileMetaData[] getFileMetaData(
    java.lang.String[] arg0) throws java.rmi.RemoteException {
              log.debug("Entering ISRMImpl.getFileMetaData");
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "mkPermanent";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:getFileMetaData")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;
          try {
             userCred = srmAuth.getUserCredentials();
             Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                                 pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          log.debug("About to call getFileMetaData()");
          diskCacheV111.srm.FileMetaData[] fmdArray;
          try {

             fmdArray = srm.getFileMetaData(user,requestCredential,arg0);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm getFileMetaData failed", e);
          }
          org.dcache.srm.client.axis.FileMetaData[] response =
            ConvertUtil.FMDs2AxisFMDs(fmdArray);
          log.debug("About to return FileMetaData array ");
          return response;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus mkPermanent(java.lang.String[] arg0) throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "mkPermanent";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:mkPermanent")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;

          try {
             userCred = srmAuth.getUserCredentials();
             Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                         pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {

             requestStatus = srm.mkPermanent(user,requestCredential,arg0);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm mkPermanent failed", e);
          }
          org.dcache.srm.client.axis.RequestStatus response =
            ConvertUtil.RS2axisRS(requestStatus);
            return response;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus getEstGetTime(java.lang.String[] arg0, java.lang.String[] arg1) throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "getEstGetTime";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:getEstGetTime")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;

          try {
             userCred = srmAuth.getUserCredentials();
             Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                         pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {

             requestStatus = srm.getEstGetTime(user,requestCredential,arg0,arg1);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm getEstGetTime failed", e);
          }
          org.dcache.srm.client.axis.RequestStatus response =
            ConvertUtil.RS2axisRS(requestStatus);
            return response;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus getEstPutTime(java.lang.String[] arg0, java.lang.String[] arg1, long[] arg2, boolean[] arg3, java.lang.String[] arg4) throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "getEstPutTime";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:getEstPutTime")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;

          try {
             userCred = srmAuth.getUserCredentials();
             Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                                 pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {

             requestStatus = srm.getEstPutTime(user,requestCredential,arg0,arg1,arg2,arg3,arg4);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm put failed", e);
          }
          org.dcache.srm.client.axis.RequestStatus response =
            ConvertUtil.RS2axisRS(requestStatus);
            return response;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public void advisoryDelete(java.lang.String[] arg0) throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "advisoryDelete";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:advisoryDelete")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;
          try {
             userCred = srmAuth.getUserCredentials();
              Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                         pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
              user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
              if (user.isReadOnly()) {
                  throw new SRMAuthorizationException("Session is read-only");
              }
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          try {

              srm.advisoryDelete(user,requestCredential,arg0);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm advisoryDelete failed", e);
          }
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public java.lang.String[] getProtocols() throws java.rmi.RemoteException {
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "getProtocols";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:getProtocols")) {
          org.dcache.srm.server.UserCredential userCred;
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;

          try {
             userCred = srmAuth.getUserCredentials();
             Collection roles
                 = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                                 pkiVerifier);
              String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
              log.debug("SRMServerV1.getProtocols() : role is "+role);
              requestCredential = srmAuth.getRequestCredential(userCred,role);
             user = srmAuth.getRequestUser(requestCredential,null,userCred.context);
          }
          catch (SRMAuthorizationException sae) {
              String msg = "SRM Authorization failed: " + sae.getMessage();
              log.error(msg);
              throw new java.rmi.RemoteException(msg);
          }

          try {

             return srm.getProtocols(user,requestCredential);
          } catch(Exception e) {
             log.error(e.toString());
             throw new java.rmi.RemoteException("srm getProtocols failed", e);
          }
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }
}
