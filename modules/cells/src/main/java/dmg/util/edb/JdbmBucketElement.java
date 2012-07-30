package dmg.util.edb ;

import java.io.* ;

public class JdbmBucketElement implements JdbmSerializable {
    private final static int KEY_START_SIZE = 4 ;
    int  _hash;
    int  _keySize;
    int  _valueSize;
    long _dataAddress;
    byte [] _keyStart = new byte[KEY_START_SIZE] ;
    public JdbmBucketElement(){}
    @Override
    public void writeObject( ObjectOutput out )
           throws java.io.IOException {
       out.writeInt( _hash ) ;
       out.writeInt( _keySize ) ;
       out.writeInt( _valueSize ) ;
       out.writeLong( _dataAddress ) ;
       out.write( _keyStart ) ;
    }
    @Override
    public void readObject( ObjectInput in )
           throws java.io.IOException, ClassNotFoundException {
       _hash        = in.readInt() ;
       _keySize     = in.readInt() ;
       _valueSize   = in.readInt() ;
       _dataAddress = in.readLong() ;
       in.read( _keyStart ) ;
    }
    @Override
    public int getPersistentSize() {
       return 3 * 4 + 8 + KEY_START_SIZE ; 
    }

}
