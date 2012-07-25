package dmg.util.edb ;

import java.io.* ;

public class JdbmAvElementList implements JdbmSerializable {
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
           throws java.io.IOException {
       out.writeLong(_next) ;
       out.writeInt(_size) ;
       out.writeInt(_count) ;
       for( int i = 0 ; i < _size ; i++ ) {
           out.writeObject(_list[i]);
       }
       return ;   
    }
    @Override
    public void readObject( ObjectInput in )
           throws java.io.IOException, ClassNotFoundException {
           
       _next  = in.readLong() ;    
       _size  = in.readInt() ;
       _count = in.readInt() ;
       _list  = new JdbmAvElement[_size] ;
       for( int i = 0 ; i < _size ; i++ ) {
           _list[i] = (JdbmAvElement) in.readObject();
       }
       
       return ;
    }
    @Override
    public int getPersistentSize() { return 0 ; }

}
