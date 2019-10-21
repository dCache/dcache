package dmg.cells.nucleus ;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import static dmg.cells.nucleus.SerializationHandler.Serializer;

/**
  * Do not subclass - otherwise raw encoding in LocationMgrTunnel will break.
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public final class CellMessage implements Cloneable , Serializable {

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
  /** Unique Mesage ID */
  private UOID        _umid , _lastUmid ;
  private byte[]      _messageStream;
  private boolean     _isPersistent;
  private Object      _session;
  /** Indicates deserialized message format */
  private static final int   ORIGINAL_MODE  = 0 ;
  /** Indicates serialized message format */
  private static final int   STREAM_MODE    = 1 ;
  private transient long _receivedAt;

    public CellMessage(CellAddressCore address, Serializable msg)
    {
        this(new CellPath(address));
        _message = msg;
    }

    public CellMessage(CellAddressCore address)
    {
        this(new CellPath(address));
    }

    public CellMessage(CellPath path, Serializable msg)
    {
        this(path.clone());
        _message = msg;
    }

    public CellMessage(CellPath path)
    {
        _source = new CellPath();
        _destination = path;
        _creationTime = System.currentTimeMillis();
        _receivedAt = _creationTime;
        _mode = ORIGINAL_MODE;
        _umid = new UOID();
        _lastUmid = _umid;
        _session = CDC.getSession();
    }

    public CellMessage()
    {
        this(new CellPath());
    }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder() ;
    sb.append( "<CM: S=" ).append(_source).
       append( ";D=").append(_destination) ;
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
    sb.append('>') ;
    return sb.toString() ;
  }
  @Override
public int     hashCode(){ return _umid.hashCode() ; }
  @Override
public boolean equals( Object obj ){

    if (obj == this) {
        return true;
    }

    if (obj == null || obj.getClass() != this.getClass()) {
        return false;
    }

    return ((CellMessage) obj)._umid.equals(_umid);
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
  public void        setTtl(long ttl) { _ttl = ttl; _receivedAt = System.currentTimeMillis(); }
  public long        getTtl() { return _ttl; }
  public CellAddressCore getSourceAddress() { return _source.getSourceAddress(); }
  public CellPath    getDestinationPath(){ return _destination ; }
  public CellPath    getSourcePath(){ return _source ; }
  public Serializable getMessageObject(){ return (Serializable) _message  ; }

  public void setMessageObject(Serializable obj)
  {
      checkState(_mode == ORIGINAL_MODE);
      _message = obj;
  }

  public void        revertDirection(){
      checkState(!_source.getSourceAddress().isDomainAddress(), "Cannot return envelope to a domain address.");
     _destination = _source.revert();
     _source      = new CellPath() ;
     _lastUmid    = _umid ;
     _umid = new UOID();
     _isPersistent = true;
  }
  public boolean isFinalDestination(){ return _destination.isFinalDestination() ; }
  public boolean isFirstDestination(){ return _destination.isFirstDestination() ; }
  public boolean nextDestination(){ return _destination.next() ; }
  //
  // package methods
  //
  boolean isStreamMode(){ return _mode == STREAM_MODE  ; }

    /**
     * The method does not copy the message object - only the encoded message
     * stream (if any).
     */
    @Override
    public CellMessage clone()
    {
        try {
            CellMessage copy = (CellMessage) super.clone();
            copy._destination = _destination.clone();
            if (_source != null) {
                copy._source = _source.clone();
            }
            copy._messageStream = _messageStream;
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public CellMessage encodeWith(Serializer serializer)
    {
        checkState(_mode == ORIGINAL_MODE);
        CellMessage encoded = clone();
        encoded._mode = STREAM_MODE;
        encoded._messageStream = SerializationHandler.encode(this._message, serializer);
        return encoded;
    }

    // For LocationManagerTunnel to reencode using JOS
    public CellMessage ensureEncodedWith(Serializer handler)
    {
        checkState(_mode == STREAM_MODE, "not encoded");
        checkArgument(handler != null, "Cannot ensure CellMessage is encoded. The given msg payload serializer is null.");

        if (!SerializationHandler.isEncodedWith(_messageStream, handler)) {
            Object payload = SerializationHandler.decode(_messageStream);
            _messageStream = SerializationHandler.encode(payload, handler);
        }
        return this;
    }

    public CellMessage decode() throws SerializationException
    {
        checkState(_mode == STREAM_MODE);
        CellMessage decoded = clone();
        decoded._mode = ORIGINAL_MODE;
        decoded._messageStream = null;
        decoded._message = SerializationHandler.decode(_messageStream);
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

    /**
     * Writes CellMessage to a data output stream.
     *
     * The CellMessage must be in stream mode.
     *
     * This is the raw encoding used by tunnels since release 3.0.
     */
    public void writeTo(DataOutput out) throws IOException
    {
        checkState(_mode == STREAM_MODE);

        out.writeByte(_mode);
        out.writeBoolean(_isPersistent);
        out.writeLong(_creationTime);
        out.writeLong(_ttl);
        _umid.writeTo(out);
        _lastUmid.writeTo(out);
        _source.writeTo(out);
        _destination.writeTo(out);

        out.writeUTF(Objects.toString(_session, ""));
        out.writeInt(_messageStream.length);
        out.write(_messageStream);
    }

    /**
     * Reads CellMessage from a data input stream.
     *
     * This is the raw encoding used by tunnels since release 3.0.
     */
    public static CellMessage createFrom(DataInput in) throws IOException
    {
        CellMessage message = new CellMessage();
        message._mode = in.readByte();
        if (message._mode != STREAM_MODE) {
            throw new IOException("Invalid message tunnel wire format.");
        }
        /* Need to initialize the transient reception time after the first field is read as
         * this function may have been called while the input stream is empty.
         */
        message._receivedAt = System.currentTimeMillis();
        message._isPersistent = in.readBoolean();
        message._creationTime = in.readLong();
        message._ttl = in.readLong();
        message._umid = UOID.createFrom(in);
        message._lastUmid = UOID.createFrom(in);
        message._source = CellPath.createFrom(in);
        message._destination = CellPath.createFrom(in);
        message._session = Strings.emptyToNull(in.readUTF());
        int len = in.readInt();
        message._messageStream = new byte[len];
        in.readFully(message._messageStream);
        return message;
    }
}
