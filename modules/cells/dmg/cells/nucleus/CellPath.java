package dmg.cells.nucleus ;
import  java.util.* ;
import  java.io.* ;
/**
  *  
  *  The CellPath is an abstraction of the path a CellMessage is
  *  assumed to travel. The path consists of a defined sequence of
  *  cell hops and a current position. The last hop which might
  *  as well be the only one, is called the FinalDestination.
  *  At any point a new Cell Hop can be added in two wayys :
  *  <ul>
  *  <li>At the end of the sequence. The added Cell becomes the
  *      new FinalDestination.
  *  <li>Insert the new Cell behind the current position. The new
  *      Hop becomes the next hop.
  *  </ul>
  *  The string representation of a cell path can have the formate:
  *  <pre>
  *    path :  &lt;addr1&gt;[:&lt;&lt;addr2&gt;[...]]
  *    addr :  &lt;cellName&gt; | &lt;cellName@domainName&gt; | &lt;cellName@local&gt;
  *  </pre>
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
  /*
  */
public class CellPath  implements Cloneable , Serializable {
   private static final long serialVersionUID =  -4922955783102747577L;

   private Vector _list     = new Vector() ;
   private Vector _mark     = null ;
   private int    _position = -1 ;
   private int    _storage  = -1 ;

   protected CellPath(){}
   protected CellPath( CellPath addr ){
      //
      // this only works because _list contains 
      // imutable objects
      //
      _list     = (Vector)addr._list.clone() ;
      _position = addr._position ;
      _storage  = addr._storage ;
   }
   /**
     *  Creates a CellAddress with an initial path of &lt;path&gt;
     *
     *  @param path The initial cell travel path.
     */
   public CellPath( String path ){
      add( path ) ;
   }
   public CellPath( String cellName , String domainName ){
       add( new CellAddressCore( cellName , domainName ) ) ; ;
   }
   public int hops(){ return _list.size() ; }
   synchronized void add( CellAddressCore core ){ 
      _list.addElement( core ) ;
      if( _position < 0 )_position = 0 ;
   }
   public synchronized void add( CellPath addr ){ 
      for( int i = 0 ; i < addr._list.size() ; i++ )
         _list.addElement( addr._list.elementAt(i) ) ;
   }
   public void add( String cell , String domain ){
       add( new CellAddressCore( cell , domain ) ) ;
   }
   /**
     *  Adds a cell path &lt;path&gt; to the end of the current path.
     *
     *  @param path The added cell travel path.
     */
   public synchronized void add( String path ){
        StringTokenizer st = new StringTokenizer( path ,":" ) ;
        for( ; st.hasMoreTokens() ; ){     
           add( new CellAddressCore( st.nextToken() ) ) ;
        }
   }
   /**
     *  Creates a CellAddress with a single cell as initial destination.
     *  The cell is represented by its name and the name of its domain.
     *
     *  @param cellName The name of the initial destination cell.
     *  @param domainName The name of the initial destination cells domain.
     */
   public Object clone(){
       CellPath addr = new CellPath() ;
       for( int i = 0 ; i < _list.size() ; i++ ){
          addr.add( (CellAddressCore)_list.elementAt(i) ) ;
       }
       addr._position = _position ;
       addr._storage  = _storage ;
       return addr ; 
   }
   /**
     *  Adds a cell path &lt;path&gt; to the end of the current path.
     *
     *  @param path The added cell travel path.
     */
   synchronized void insert( CellAddressCore core ){ 
      _list.insertElementAt( core , _position + 1 ) ;
      if( _position < 0 )_position = 0 ;
   }
   public synchronized void insert( String path ){
      StringTokenizer st = new StringTokenizer( path ,":" ) ;
      for( ; st.hasMoreTokens() ; ){     
         insert( new CellAddressCore( st.nextToken() ) ) ;
      }
   }
   public void mark(){
      _mark    = (Vector)_list.clone() ;
      _storage = _position ;
   }
   public void reset(){
      if( _mark == null )return ;
      _list     = _mark ;
      _position = _storage ;
   }
   public void insert( String cell , String domain ){
       add( new CellAddressCore( cell , domain ) ) ;
   }
   /**
     * Incremeant the current cell position by one.
     *
     *  @return true if the cell hops could be shifted, false if
     *          current cell was the final destination.
     */
   public synchronized boolean next(){
      if( _position >= ( _list.size() - 1 ) )return false ;
      _position++ ;
      return true;
   }
   public synchronized void toFirstDestination(){
      _position = _list.size() == 0 ? -1 : 0 ;
   }
   public synchronized void revert(){
     Vector list = new Vector();
     for( int i = _list.size() -1 ; i >= 0 ; i -- )
        list.addElement( _list.elementAt(i) ) ;
     _list = list ;
     toFirstDestination() ;
   }
   public synchronized boolean isFinalDestination(){
      return _position >= ( _list.size() - 1 ) ;
   }
   public synchronized boolean isFirstDestination(){
      return _position == 0 ;
   }
   CellAddressCore getCurrent(){
     if( ( _list.size() == 0            ) ||
         ( _position    <  0            ) || 
          ( _position    >=_list.size()  )     )return null ; 
     return (CellAddressCore)_list.elementAt( _position ) ;
   }
   public CellAddressCore getDestinationAddress(){ 
      return (CellAddressCore)_list.elementAt(_list.size()-1);
   }
   void replaceCurrent( CellAddressCore core ){
     if( ( _list.size() == 0            ) ||
         ( _position    <  0            ) || 
         ( _position    >=_list.size()  )     )return  ; 
     _list.setElementAt( core , _position ) ;
   }
   public String getCellName(){ 
     CellAddressCore core = getCurrent() ;
     return core == null ? null : core.getCellName() ; 
   }
   public String getCellDomainName(){ 
     CellAddressCore core = getCurrent() ;
     return core == null ? null : core.getCellDomainName() ; 
   }
   /*
   public void wasStored(){ 
       _storage = _list.size()-1 ; 
   }
   public synchronized CellAddress getPreviousStorageAddress(){
     if( _storage < 0 )return null ;
     CellAddress addr = new CellAddress();
     for( int i = _list.size() -1 ; i >= _storage ; i -- )
        addr.add( (CellAddressCore)_list.elementAt(i) ) ;
     return addr ;
   }
   */
   public String toSmallString(){
      int size = _list.size() ;
      if( size == 0 )return "[empty]" ;
      if( ( _position >= size ) || ( _position < 0 ) )return "[INVALID]" ;
      
      CellAddressCore core = (CellAddressCore)_list.elementAt(_position) ;
      
      if( size == 1 ){
         return  "["+core.toString()+"]" ;
      }else{
         if( _position == 0 )
            return  "["+core.toString()+":...("+(size-1)+")...]" ;
         if( _position == (size-1) )
            return  "[...("+(size-1)+")...:"+core.toString()+"]" ;
            
         return  "[...("+_position+")...:"+
                 core.toString()+
                 "...("+(size-_position-1)+")...]" ;
         
      }
   }
   public String toString(){ return toFullString() ; }
   public String toFullString(){
      int size = _list.size() ;
      if( size == 0 )return "[empty]" ;
      if( ( _position >= size ) || ( _position < 0 ) )return "[INVALID]" ;
      
      StringBuffer sb = new StringBuffer() ;
      
      sb.append("[") ;
      for( int i = 0 ; i < _list.size() ; i ++ ){
         if( i > 0 )sb.append(":") ;
         if( i == _position )sb.append(">") ;
         if( ( _storage > -1 ) && ( i == _storage ) )sb.append("*") ;
         sb.append(((CellAddressCore)_list.elementAt(i)).toString());
      }
      sb.append("]") ;
      return sb.toString();
   }
   public synchronized boolean equals( Object obj ){
      if( ! ( obj instanceof CellPath ) )return false ;
      CellPath other = (CellPath)obj ;
      synchronized( other ){ // not deadlock free
          int s = 0 ;
          if( ( s = _list.size() ) != other._list.size() )return false ;
       
          for( int i = 0 ; i < s ; i++ )
             if( 
               ! _list.elementAt(i).equals(other._list.elementAt(i)) 
             )return false;  
             
          return true ;  
      }
   }
   public synchronized int hashCode(){ 
      int sum = 0 ;
      for( int i = 0 ; i < _list.size() ; i++ )
        sum += _list.elementAt(i).hashCode() ;
      return sum ;
   }
   
   public static void main( String [] args ){
      CellPath addr = new CellPath() ;
      for( int i = 0 ; i < args.length ; i ++ ){
         addr.add( args[i] ) ;
      
      }
      System.out.println( addr.toFullString() ) ;
      System.out.println( addr.toString() ) ;
      while( addr.next() )System.out.println( addr.toString() ) ;
   }


}
