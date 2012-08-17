// $Id: InfoMessage.java,v 1.12 2007-07-31 12:22:34 tigran Exp $

package diskCacheV111.vehicles ;

import java.util.Date;
import java.text.SimpleDateFormat;
import diskCacheV111.util.Transaction;
import org.antlr.stringtemplate.StringTemplate;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.SubjectWrapper;

public class InfoMessage implements java.io.Serializable {
   private static final SimpleDateFormat __dateFormat = new SimpleDateFormat("MM.dd HH:mm:ss");
   private final String    _cellType    ;
   private String    _messageType = null ;
   private final String    _cellName    ;
   private long      _timeQueued  = 0 ;
   private int       _resultCode  = 0 ;
   private String    _message     = "" ;
   private   long    _timestamp   = System.currentTimeMillis() ;
   private String    _transaction  = null;
   private long      _transactionID = Transaction.newID();
   private Subject _subject;

   private static final long serialVersionUID = -8035876156296337291L;

   public InfoMessage( String messageType ,
                       String cellType ,
                       String cellName  ){
     _cellName    = cellName ;
     _cellType    = cellType ;
     _messageType = messageType ;
   }

   protected synchronized static String formatTimestamp(Date timestamp)
   {
       return __dateFormat.format(timestamp);
   }

   public String getInfoHeader(){
      return formatTimestamp(new Date(_timestamp))+" ["+_cellType+":"+_cellName+":"+_messageType+"]" ;
   }
   public String getResult(){
      return "{"+_resultCode+":\""+_message+"\"}" ;
   }
   public String toString(){
      return getInfoHeader()+" "+getResult() ;
   }
    public String getFormattedDate() {
        return formatTimestamp(new Date(_timestamp));
    }

    public void fillTemplate(StringTemplate template)
    {
        template.setAttribute("date", new Date(getTimestamp()));
        template.setAttribute("queuingTime", getTimeQueued());
        template.setAttribute("message", getMessage());
        template.setAttribute("type", getMessageType());
        template.setAttribute("cellName", getCellName());
        template.setAttribute("cellType", getCellType());
        template.setAttribute("rc", getResultCode());
        template.setAttribute("subject", new SubjectWrapper(getSubject()));
    }

   public void setResult( int resultCode , String resultMessage ){
     _message    = resultMessage ;
     _resultCode = resultCode ;
   }
   public void   setTimeQueued( long timeQueued ){ _timeQueued = timeQueued ; }
   public long   getTimeQueued(){ return _timeQueued ; }
   public String getCellType(){ return _cellType ; }
   public void   setMessageType( String messageType ){
      _messageType = messageType ;
   }
   public String getMessageType(){ return _messageType ; }
   public String getCellName(){ return _cellName ; }
   public String getMessage(){ return _message ; }
   public int    getResultCode(){ return _resultCode ; }
   public long   getTimestamp(){ return _timestamp ; }
   public synchronized void   setTransaction( String transaction ) {
       _transaction = transaction;
   }
   public synchronized String getTransaction() {

       if(_transaction == null) {
           StringBuffer sb = new StringBuffer();
           sb.append(this.getCellType()).append(":").append(this.getCellName()).append(":");
           sb.append(this.getTimestamp()).append("-").append(_transactionID);
           _transaction = sb.toString();
       }
       return _transaction ;
   }

    public void setSubject(Subject subject)
    {
        _subject = subject;
    }

    public Subject getSubject()
    {
        /* The null check ensures compatibility with pools earlier
         * than version 2.1. Those pools do not include a subject
         * field.
         */
        return (_subject == null) ? Subjects.ROOT : _subject;
    }
}
