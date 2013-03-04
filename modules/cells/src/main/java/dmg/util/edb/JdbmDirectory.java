package dmg.util.edb ;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class JdbmDirectory implements JdbmSerializable {
    private static final long serialVersionUID = -5850714968851810542L;
    private int     _size;
    private long [] _addr;
    private int     _bits;
    public JdbmDirectory(){}
    public JdbmDirectory( int maxBytes ){
       int offset = JdbmSerializable.HEADER_SIZE + 2 * 4  ;

       int n = 8 ;
       int bits = 3 ;
       while( ( offset + n * 8 ) <= maxBytes ){
          bits++ ;
          n *= 2 ;
       }
       if( n < 8 ) {
           throw new
                   IllegalArgumentException("Block to small " + maxBytes);
       }
       _bits = bits - 1 ;
       _size = n / 2 * 8 ;

       _addr = new long[_size] ;
    }
    public void expand(){
       _size *= 2 ;
       long [] newAddr = new long[_size] ;
       int n = 0 ;
       for( int i = 0 ; i < _size ; i+= 2 , n++ ) {
           newAddr[i] = newAddr[i + 1] = _addr[n];
       }
       _bits ++ ;
       _addr = newAddr ;
    }
    @Override
    public void writeObject( ObjectOutput out )
           throws IOException {
       if( _size > _addr.length ) {
           throw new
                   IllegalArgumentException("PANIC : _size > _addr");
       }

       out.writeInt(_size) ;
       for( int i = 0 ; i < _size ; i++ ) {
           out.writeLong(_addr[i]);
       }
    }
    @Override
    public void readObject( ObjectInput in )
           throws IOException, ClassNotFoundException {

       _size = in.readInt() ;
       _addr = new long[_size] ;
       for( int i = 0 ; i < _size ; i++) {
           _addr[i] = in.readLong();
       }
    }
    public String toString(){
        return "dir{b="+_bits+";e="+_size+";s="+getPersistentSize()+"}" ;
    }
    @Override
    public int getPersistentSize() {
       return JdbmSerializable.HEADER_SIZE + 2 * 4  + 8 * _size  ;
    }

}
