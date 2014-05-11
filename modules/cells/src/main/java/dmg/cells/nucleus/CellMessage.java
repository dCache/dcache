package dmg.cells.nucleus ;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkState;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellMessage implements Cloneable , Serializable {

  private static final long serialVersionUID = -5559658187264201731L;

  /**
   * Maximum TTL adjustment in milliseconds.
   */
  private static final int TTL_BUFFER_MAXIMUM = 10000;

  /**
   * Maximum TTL adjustment as a fraction of TTL.
   */
  private static final float TTL_BUFFER_FRACTION = 0.10f;

  private CellPath    _source , _destination ;
  private Object      _message ;
  private long        _creationTime ;
  private long        _ttl = Long.MAX_VALUE;
  private int         _mode ;
  private UOID        _umid , _lastUmid ;
  private byte[]      _messageStream;
  private boolean     _isPersistent;
  private Object      _session;
  private static final int   ORIGINAL_MODE  = 0 ;
  private static final int   STREAM_MODE    = 1 ;
  private static final int   DUMMY_MODE     = 2 ;
  private transient long _receivedAt;

  public CellMessage( CellPath addr , Serializable msg ){

     _destination  = (CellPath) addr.clone();
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
  public boolean     isReply() { return _isPersistent; }
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
     _destination = _source.revert();
     _source      = new CellPath() ;
     _lastUmid    = _umid ;
     _isPersistent = true;
  }
  public boolean isFinalDestination(){ return _destination.isFinalDestination() ; }
  public boolean isFirstDestination(){ return _destination.isFirstDestination() ; }
  public boolean nextDestination(){ return _destination.next() ; }
  //
  // package methods
  //
  boolean isStreamMode(){ return _mode == STREAM_MODE  ; }
  void touch(){
    if( _destination.isFirstDestination() ){
        _umid = new UOID() ;
    }
  }

    private CellMessage()
    {
        _mode = DUMMY_MODE;
        _messageStream = null;
    }

    private CellMessage cloneWithoutPayload()
    {
        CellMessage copy = new CellMessage();
        copy._destination = (CellPath) _destination.clone();
        copy._source = (CellPath) _source.clone();
        copy._creationTime = _creationTime;
        copy._receivedAt = _receivedAt;
        copy._umid = _umid;    // UOID is immutable
        copy._lastUmid = _lastUmid;
        copy._isPersistent = _isPersistent;
        copy._session = _session;
        copy._ttl = _ttl;
        return copy;
    }

    public CellMessage encode() throws SerializationException
    {
        checkState(_mode == ORIGINAL_MODE);
        CellMessage encoded = cloneWithoutPayload();
        encoded._mode = STREAM_MODE;
        ByteArrayOutputStream array = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(array)) {
            out.writeObject(_message);
        } catch (InvalidClassException e) {
            throw new SerializationException("Failed to serialize object: "
                    + e + "(this is usually a bug)", e);
        } catch (NotSerializableException e) {
            throw new SerializationException("Failed to serialize object because the object is not serializable (this is usually a bug)", e);
        } catch (IOException e) {
            throw new SerializationException("Failed to serialize object: " + e, e);
        }

        encoded._messageStream = array.toByteArray();
        return encoded;
    }

    public CellMessage decode() throws SerializationException
    {
        checkState(_mode == STREAM_MODE);
        CellMessage decoded = cloneWithoutPayload();
        decoded._mode = ORIGINAL_MODE;
        ByteArrayInputStream in;
        ObjectInputStream stream;
        try {
            in = new ByteArrayInputStream(_messageStream);
            stream = new ObjectInputStream(in);
            decoded._message = stream.readObject();
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Failed to deserialize object: The class could not be found. Is there a software version mismatch in your installation?", e);
        } catch (IOException e) {
            throw new SerializationException("Failed to deserialize object: " + e, e);
        }

        return decoded;
    }

    public void addSourceAddress( CellAddressCore source ){
      _source.add(source) ;
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

    /**
     * Returns the adjusted TTL of a message. The adjusted TTL is the
     * TTL with some time subtracted to allow for cell communication
     * overhead. Returns Long.MAX_VALUE if the TTL is infinite.
     */
    public long getAdjustedTtl()
    {
        return (_ttl == Long.MAX_VALUE)
                ? Long.MAX_VALUE
                : _ttl - Math.min(TTL_BUFFER_MAXIMUM, (long) (_ttl * TTL_BUFFER_FRACTION));
    }

}
