package dmg.util.edb ;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class JdbmAvElementList implements JdbmSerializable {
    private static final long serialVersionUID = 2153966749036458851L;
    private int     _size;
    private int     _count;
    private long    _next;
    private JdbmAvElement [] _list;
    public JdbmAvElementList(){}
    public JdbmAvElementList( int size ){
       _next  = 0L ;
       _size  = size ;
       _count = 0 ;
       _list  = new JdbmAvElement[size] ;
       for( int i = 0 ; i < _size ; i++ ) {
           _list[i] = new JdbmAvElement();
       }
    }
    @Override
    public void writeObject( ObjectOutput out )
           throws IOException {
       out.writeLong(_next) ;
       out.writeInt(_size) ;
       out.writeInt(_count) ;
       for( int i = 0 ; i < _size ; i++ ) {
           out.writeObject(_list[i]);
       }
    }
    @Override
    public void readObject( ObjectInput in )
           throws IOException, ClassNotFoundException {

       _next  = in.readLong() ;
       _size  = in.readInt() ;
       _count = in.readInt() ;
       _list  = new JdbmAvElement[_size] ;
       for( int i = 0 ; i < _size ; i++ ) {
           _list[i] = (JdbmAvElement) in.readObject();
       }

    }
    @Override
    public int getPersistentSize() { return 0 ; }

}
