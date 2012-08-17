// $Id: JobInfo.java,v 1.3 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;
import java.text.SimpleDateFormat ;
import java.util.Date;
import diskCacheV111.util.* ;

public class JobInfo implements java.io.Serializable {

   public static JobInfo newInstance( JobScheduler.Job job ){
      Runnable run = job.getTarget() ;
      if( run instanceof IoBatchable ){
         IoJobInfo info = new IoJobInfo(job) ;
         info.setClient(((Batchable)run).getClient(),
                        ((Batchable)run).getClientId() ) ;
         info.setIoInfo(
                        ((IoBatchable)run).getPnfsId() ,
                        ((IoBatchable)run).getBytesTransferred() ,
                        ((IoBatchable)run).getTransferTime() ,
                        ((IoBatchable)run).getLastTransferred()) ;
         return info ;
      }else if( run instanceof Batchable ){
         JobInfo info = new JobInfo(job) ;
         info.setClient(((Batchable)run).getClient(),
                        ((Batchable)run).getClientId() ) ;
         return info ;
      }else{
         return new JobInfo( job ) ;
      }
   }
   private String _client     = "<unknown>" ;
   private long   _clientId   = 0 ;
   private long   _submitTime = 0 ;
   private long   _startTime  = 0 ;
   private String _status     = null ;
   private long   _jobId      = 0 ;
   JobInfo( JobScheduler.Job job ){
      _submitTime = job.getSubmitTime() ;
      _startTime  = job.getStartTime() ;
      _status     = job.getStatusString() ;
      _jobId      = job.getJobId() ;
   }
   public void setClient( String clientName , long clientId ){
      _client   = clientName ;
      _clientId = clientId ;
   }
   public String getClientName(){ return _client ; }
   public long   getClientId(){ return _clientId ; }
   public long   getStartTime(){ return _startTime ; }
   public long   getSubmitTime(){ return _submitTime ; }
   public String getStatus(){ return _status  ;}
   public long   getJobId(){ return _jobId ; }
   private final static SimpleDateFormat __format =
        new SimpleDateFormat( "MM/dd-HH:mm:ss" ) ;

   private static final long serialVersionUID = 5209798222006083955L;

   public String toString(){
      StringBuffer sb = new StringBuffer();
      sb.append(_jobId).append(";");
      sb.append(_client).append(":").append(_clientId) ;
      synchronized (__format) {
          sb.append(";").append(__format.format(new Date(_startTime))).
                  append(";").append(__format.format(new Date(_submitTime))).
                  append(";").append(_status).append(";") ;
      }
      return sb.toString();
   }
}
