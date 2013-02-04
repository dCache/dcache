package dmg.util.edb ;

import java.io.* ;

public class JdbmBucket implements JdbmSerializable {
    private final static int KEY_START_SIZE = 4 ;
    private static final long serialVersionUID = -4503700195072086822L;
    int _size;
    int _count;
    int _bits;
    JdbmBucketElement [] _list;
    public JdbmBucket(){}
    public JdbmBucket( int size ){
       _size  = size ;
       _count = 0 ;
       _bits  = 0 ;
       _list  = new JdbmBucketElement[_size] ;
       for( int i = 0 ; i < _list.length ; i++ ) {
           _list[i] = new JdbmBucketElement();
       }
    }
    @Override
    public void writeObject( ObjectOutput out )
           throws IOException {
       out.writeInt(_size);
       out.writeInt(_count) ;
       out.writeInt(_bits) ;
       for( int i = 0 ; i < _size ; i++ ) {
           out.writeObject(_list[i]);
       }
    }
    @Override
    public void readObject( ObjectInput in )
           throws IOException, ClassNotFoundException {
       _size  = in.readInt() ;
       _count = in.readInt() ;
       _bits  = in.readInt() ;
       _list  = new JdbmBucketElement[_size] ;
       for( int i = 0 ; i < _size ; i++ ) {
           _list[i] = (JdbmBucketElement) in.readObject();
       }
    }
    @Override
    public int getPersistentSize() {
       return 3 * 4 + _size * ( new JdbmBucketElement() ).getPersistentSize() ; 
    }

}
