package dmg.cells.nucleus ;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellMessage implements Cloneable , Serializable {

  private static final long serialVersionUID = -5559658187264201731L;

  private CellPath    _source , _destination ;
  private CellPath    _markSource, _markDestination;
  private Object      _message ;
  private long        _creationTime ;
  private long        _ttl = Long.MAX_VALUE;
  private int         _mode ;
  private UOID        _umid , _lastUmid ;
  private final byte[] _messageStream;
  private boolean     _isRouted;
  private int         _hopCount;
  private boolean     _isAcknowledge;
  private boolean     _isPersistent;
  private Object      _session;
  private static final int   ORIGINAL_MODE  = 0 ;
  private static final int   STREAM_MODE    = 1 ;
  private static final int   DUMMY_MODE     = 2 ;
  private transient long _receivedAt;

  public CellMessage( CellPath addr , Serializable msg ){

     _destination  = addr ;
     _message      = msg ;
     _source       = new CellPath() ;
     _creationTime = System.currentTimeMillis() ;
     _receivedAt   = _creationTime;
     _mode         = ORIGINAL_MODE ;
     _umid         = new UOID() ;
     _lastUmid     = _umid ;
     _session      = CDC.getSession();
     _messageStream = null;
  }
  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder() ;
    sb.append( "<CM: S=" ).append( _source.toString() ).
       append( ";D=").append( _destination.toString() ) ;
    if( _markSource != null ) {
        sb.append(";MS=").append(_markSource.toString());
    }
    if( _markDestination != null ) {
        sb.append(";MD=").append(_markDestination.toString());
    }
    if( _mode == ORIGINAL_MODE ) {
        sb.append(";C=").
                append(_message.getClass().getName());
    } else {
        sb.append(";C=Stream");
    }

    sb.append( ";O=" ).append( _umid ).append( ";LO=" ).append( _lastUmid );
    if (_session != null) {
        sb.append(";SID=").append(_session);
    }
    if (_ttl < Long.MAX_VALUE) {
        sb.append(";TTL=").append(_ttl);
    }
    sb.append( ">" ) ;
    return sb.toString() ;
  }
  @Override
public int     hashCode(){ return _umid.hashCode() ; }
  @Override
public boolean equals( Object obj ){
      if( obj instanceof CellMessage ) {
          return ((CellMessage) obj)._umid.equals(_umid);
      } else if( obj instanceof UOID ) {
          return obj.equals(_umid);
      }

      return false ;
  }
  public void        setAcknowledge( boolean ack ){ _isAcknowledge = ack ; }
  public boolean     isAcknowledge(){ return _isAcknowledge ; }
  public boolean     isReply() { return _isPersistent; }
  public int         getHopCount(){ return _hopCount ; }
  public void        incHopCount(){ _hopCount++ ; }
  public UOID        getUOID() { return _umid ; }
  public UOID        getLastUOID() { return _lastUmid ; }
  public void        setUOID( UOID umid ) {
     _umid = umid ;
  }
  public void        setLastUOID( UOID lastUOID ) {
     _lastUmid = lastUOID ;
  }
  public Serializable getSession() { return (Serializable) _session; }
  public void        setSession(Serializable session) { _session = session; }
  public void        setTtl(long ttl) { _ttl = ttl; }
  public long        getTtl() { return _ttl; }
  public CellAddressCore getSourceAddress() { return _source.getSourceAddress(); }
  public CellPath    getDestinationPath(){ return _destination ; }
  public CellPath    getSourcePath(){ return _source ; }
  public Serializable getMessageObject(){ return (Serializable) _message  ; }
  public void        setMessageObject( Serializable obj ){ _message = obj ; }
  public void        revertDirection(){
     _destination = _source ;
     _destination.revert() ;
     _source      = new CellPath() ;
     _lastUmid    = _umid ;
     _isPersistent = true;
  }
  public void        markLocation(){
     _markDestination = (CellPath)_destination.clone() ;
     _markSource      = (CellPath)_source.clone() ;
  }
  public void        resetLocation(){
     if( ( _markDestination == null ) ||
         ( _markSource      == null )    ) {
         return;
     }
     _destination = _markDestination ;
     _source      = _markSource ;
  }
  public boolean isFinalDestination(){ return _destination.isFinalDestination() ; }
  public boolean isFirstDestination(){ return _destination.isFirstDestination() ; }
  public boolean nextDestination(){ return _destination.next() ; }
  //
  // package methods
  //
  void    isRouted( boolean r ){ _isRouted = r ; }
  boolean wasRouted(){ return _isRouted ; }
  boolean isStreamMode(){ return _mode == STREAM_MODE  ; }
  void touch(){
    if( _destination.isFirstDestination() ){
        _umid = new UOID() ;
    }
  }
  CellMessage(){
     _mode = DUMMY_MODE ;
     _messageStream = null;
  }
  public CellMessage( CellMessage cm ) throws SerializationException {
     if( cm._mode == ORIGINAL_MODE ){
        _messageStream = _originalToStream( cm ) ;
     }else{
         _streamToOriginal( cm ) ;
         _messageStream = null;
     }
  }
  public void addSourceAddress( CellAddressCore source ){
      _source.add( source ) ;
  }
  /*
  void markPersistent(){ _source.wasStored() ; }
  */
  //
  // and private
  //
  private void _copyInternalStuff(CellMessage cm){
     _destination   = (CellPath)cm._destination.clone() ;
     _source        = (CellPath)cm._source.clone() ;
     _markDestination = cm._markDestination==null?null:
                        (CellPath)cm._markDestination.clone() ;
     _markSource      = cm._markSource==null?null:
                        (CellPath)cm._markSource.clone() ;
     _creationTime  = cm._creationTime ;
     _receivedAt    = cm._receivedAt;
     _umid          = cm._umid ;    // UOID is immutable
     _lastUmid      = cm._lastUmid ;
     _hopCount      = cm._hopCount ;
     _isPersistent  = cm._isPersistent ;
     _isAcknowledge = cm._isAcknowledge ;
     _session       = cm._session;
     _ttl           = cm._ttl;
  }
  private byte[] _originalToStream( CellMessage cm )
      throws SerializationException {
     _copyInternalStuff( cm ) ;
     _mode          = STREAM_MODE ;
     //
     // here we have to make a bytestream out of the message object ;
     //
     ObjectOutputStream    out;
     ByteArrayOutputStream array;
     try{
         array = new ByteArrayOutputStream() ;
         out   = new ObjectOutputStream( array ) ;
         out.writeObject( cm._message ) ;
     } catch (InvalidClassException e) {
         throw new SerializationException("Failed to serialize object: "
                                          + e + "(this is usually a bug)", e);
     } catch (NotSerializableException e) {
         throw new SerializationException("Failed to serialize object because the object is not serializable (this is usually a bug)", e);
     } catch (IOException e) {
         throw new SerializationException("Failed to serialize object: " + e, e);
     }

     return array.toByteArray() ;
  }
  private void _streamToOriginal( CellMessage cm )
      throws SerializationException {
     _copyInternalStuff( cm ) ;
     _mode         = ORIGINAL_MODE ;
     ByteArrayInputStream in;
     ObjectInputStream    stream;
     try{
        in       = new ByteArrayInputStream( cm._messageStream ) ;
        stream   = new ObjectInputStream( in ) ;
        _message = stream.readObject() ;
     } catch (ClassNotFoundException e) {
         throw new SerializationException("Failed to deserialize object: The class could not be found. Is there a software version mismatch in your installation?", e);
     } catch (IOException e) {
         throw new SerializationException("Failed to deserialize object: " + e, e);
     }

  }

    private void readObject(ObjectInputStream stream)
        throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        _receivedAt = System.currentTimeMillis();
        if (_ttl == 0) {
            _ttl = Long.MAX_VALUE;
        }
    }

    /**
     * Returns the number of milliseconds since this message was
     * received by the local domain. If the message created in the
     * local domain, then the method returns the number of
     * milliseconds since it was created.
     */
    public long getLocalAge()
    {
        return System.currentTimeMillis() - _receivedAt;
    }
}
