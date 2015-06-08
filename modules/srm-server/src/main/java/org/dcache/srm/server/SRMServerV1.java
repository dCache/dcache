package org.dcache.srm.server;

import com.google.common.net.InetAddresses;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.client.ConvertUtil;
import org.dcache.srm.util.Axis;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;


public class SRMServerV1 implements org.dcache.srm.client.axis.ISRM_PortType{

    private final Logger log;
    private final SrmAuthorizer srmAuth;
    private final SRM srm;
    private final RequestCounters<String> srmServerCounters;
    private final RequestExecutionTimeGauges<String> srmServerGauges;
    private final boolean isClientDNSLookup;
    private final boolean isEnabled;

    public SRMServerV1()
    {
         log = LoggerFactory.getLogger(this.getClass().getName());
         srm = Axis.getSRM();
         Configuration config = Axis.getConfiguration();

         srmAuth = new SrmAuthorizer(config.getAuthorization(),
                srm.getRequestCredentialStorage(),
                config.isClientDNSLookup(),
                config.getVomsdir(),
                config.getCaCertificatePath());
         isClientDNSLookup = config.isClientDNSLookup();
         srmServerCounters = srm.getSrmServerV1Counters();
         srmServerGauges = srm.getSrmServerV1Gauges();
    }

    private void checkEnabled() throws RemoteException
    {
        if (!isEnabled) {
            log.warn("Rejecting SRM v1 client request from '{}' by '{}' because SRM v1 is disabled.",
                     Axis.getRemoteAddress(), Axis.getDN().orElse(""));
            throw new java.rmi.RemoteException("SRM version 1 is not supported by this server.");
        }
    }

    private String getRemoteHost() {
        String remoteIP = Axis.getRemoteAddress();
        return isClientDNSLookup ?
                    InetAddresses.forString(remoteIP).getCanonicalHostName() : remoteIP;
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
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "put";
      try (JDC ignored = JDC.createSession("srm1:put")) {
          incrementRequest(methodName);
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;

          try {
              requestCredential = srmAuth.getRequestCredential();
              user = srmAuth.getRequestUser();
              if (user.isReadOnly()) {
                  throw new SRMAuthorizationException("Session is read-only");
              }
          } catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {
              Long[] sizes = Longs.asList(arg2).toArray(new Long[arg2.length]);
              requestStatus = srm.put(user,requestCredential,arg0,arg1,sizes,arg3,arg4, getRemoteHost());
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
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "get";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:get")) {
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;
          try {
             requestCredential = srmAuth.getRequestCredential();
             user = srmAuth.getRequestUser();
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {
             requestStatus = srm.get(user,
                 requestCredential,
                 arg0,
                 arg1,
                 getRemoteHost());
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
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "copy";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:copy")) {
          SRMUser user;
          org.dcache.srm.request.RequestCredential requestCredential;
          try {
             requestCredential = srmAuth.getRequestCredential();
             user = srmAuth.getRequestUser();
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {
             requestStatus = srm.copy(user,
                 requestCredential,
                 arg0,
                 arg1,
                 arg2,
                 getRemoteHost());
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
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "ping";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:ping")) {
          try {
              if (!srmAuth.isUserAuthorized()) {
                  throw new java.rmi.RemoteException("Not authorized");
              }
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }
          return true;
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus pin(java.lang.String[] arg0) throws java.rmi.RemoteException {
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "pin";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:pin")) {
          try {
             if (!srmAuth.isUserAuthorized()) {
                  throw new java.rmi.RemoteException("Not authorized");
             }
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

             throw new java.rmi.RemoteException("srm pin is not supported");
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus unPin(java.lang.String[] arg0, int arg1) throws java.rmi.RemoteException {
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "unPin";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:unpin")) {
          try {
             if (!srmAuth.isUserAuthorized()) {
                  throw new java.rmi.RemoteException("Not authorized");
             }
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          throw new java.rmi.RemoteException("srm unPin is not supported");
      } finally {
          srmServerGauges.update(methodName, System.currentTimeMillis() -
                  startTimeStamp);
      }
    }

    @Override
    public org.dcache.srm.client.axis.RequestStatus setFileStatus(int arg0, int arg1, java.lang.String arg2) throws java.rmi.RemoteException {
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "setFileStatus";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:setFileStatus")) {
          SRMUser user;
          try {
              user = srmAuth.getRequestUser();
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {
             requestStatus = srm.setFileStatus(user,arg0,arg1,arg2);
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
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "getRequestStatus";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:getRequestStatus")) {
          SRMUser user;
          try {
             user = srmAuth.getRequestUser();
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {
             requestStatus = srm.getRequestStatus(user,arg0);
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
      checkEnabled();
      log.debug("Entering ISRMImpl.getFileMetaData");
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "mkPermanent";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:getFileMetaData")) {
          SRMUser user;
          try {
              user = srmAuth.getRequestUser();
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          log.debug("About to call getFileMetaData()");
          diskCacheV111.srm.FileMetaData[] fmdArray;
          try {
             fmdArray = srm.getFileMetaData(user,arg0);
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
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "mkPermanent";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:mkPermanent")) {
          try {
             if (!srmAuth.isUserAuthorized()) {
                  throw new java.rmi.RemoteException("Not authorized");
             }
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {
             requestStatus = srm.mkPermanent();
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
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "getEstGetTime";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:getEstGetTime")) {
          try {
             if (!srmAuth.isUserAuthorized()) {
                  throw new java.rmi.RemoteException("Not authorized");
             }
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {

             requestStatus = srm.getEstGetTime();
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
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "getEstPutTime";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:getEstPutTime")) {
          try {
             if (!srmAuth.isUserAuthorized()) {
                  throw new java.rmi.RemoteException("Not authorized");
             }
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          diskCacheV111.srm.RequestStatus requestStatus;
          try {

             requestStatus = srm.getEstPutTime();
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
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "advisoryDelete";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:advisoryDelete")) {
          SRMUser user;
          try {
              user = srmAuth.getRequestUser();
              if (user.isReadOnly()) {
                  throw new SRMAuthorizationException("Session is read-only");
              }
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          try {

              srm.advisoryDelete(user,arg0);
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
      checkEnabled();
      long startTimeStamp = System.currentTimeMillis();
      String methodName = "getProtocols";
      incrementRequest(methodName);
      try (JDC ignored = JDC.createSession("srm1:getProtocols")) {
          try {
             if (!srmAuth.isUserAuthorized()) {
                  throw new java.rmi.RemoteException("Not authorized");
             }
          }
          catch (SRMException sae) {
              log.error(sae.getMessage());
              throw new java.rmi.RemoteException(sae.getMessage());
          }

          try {
             return srm.getProtocols();
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
