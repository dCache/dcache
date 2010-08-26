package dmg.util.edb ;

import java.io.* ;

public class JdbmDirectory implements JdbmSerializable {
    private int     _size = 0 ;
    private long [] _addr = null ;
    private int     _bits = 0 ;
    public JdbmDirectory(){}
    public JdbmDirectory( int maxBytes ){
       int offset = JdbmSerializable.HEADER_SIZE + 2 * 4  ;
       
       int n = 8 ;
       int bits = 3 ; 
       while( ( offset + n * 8 ) <= maxBytes ){
          bits++ ;
          n *= 2 ;
       }
       if( n < 8 )
          throw new
          IllegalArgumentException( "Block to small "+maxBytes ) ;
       _bits = bits - 1 ;
       _size = n / 2 * 8 ;
       
       _addr = new long[_size] ;
    }
    public void expand(){
       _size *= 2 ;
       long [] newAddr = new long[_size] ;
       int n = 0 ;
       for( int i = 0 ; i < _size ; i+= 2 , n++ )
           newAddr[i] = newAddr[i+1] = _addr[n] ;
       _bits ++ ; 
       _addr = newAddr ;
    }
    public void writeObject( ObjectOutput out )
           throws java.io.IOException {
       if( _size > _addr.length )
         throw new
         IllegalArgumentException( "PANIC : _size > _addr" ) ;
         
       out.writeInt(_size) ;
       for( int i = 0 ; i < _size ; i++ )out.writeLong( _addr[i] ) ;
       return ;   
    }
    public void readObject( ObjectInput in )
           throws java.io.IOException, ClassNotFoundException {
           
       _size = in.readInt() ;
       _addr = new long[_size] ;
       for( int i = 0 ; i < _size ; i++)_addr[i] = in.readLong() ;
       return ;
    }
    public String toString(){
        return "dir{b="+_bits+";e="+_size+";s="+getPersistentSize()+"}" ;
    }
    public int getPersistentSize() { 
       return JdbmSerializable.HEADER_SIZE + 2 * 4  + 8 * _size  ;
    }

}
