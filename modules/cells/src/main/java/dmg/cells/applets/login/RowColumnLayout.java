package dmg.cells.applets.login ;
import java.awt.* ;

public class RowColumnLayout implements LayoutManager, java.io.Serializable {

    private static final long serialVersionUID = -2955752259527033593L;
    private int _vGap = 10 , _hGap = 10 ;
    private int _fitsAllSizes = NONE ;
   
    private int _columns;
    public static final int NONE = -1 ;
    public static final int LAST = -2 ;
    public RowColumnLayout( int columns ){
       _columns = columns ;
    }
    public RowColumnLayout( int columns , int fits ){
       _columns = columns ;
       _fitsAllSizes = fits == LAST ? ( _columns - 1 ) : fits ;
    }
    
    public void setFitsAllSizes( int fits ){ 
       _fitsAllSizes = fits == LAST ? ( _columns - 1 ) : fits ;
    }
    public void setHgap( int hGap ){ _hGap = hGap ; }
    public void setVgap( int vGap ){ _vGap = vGap ; }
    @Override
    public void addLayoutComponent(String name, Component comp) {
    }
    @Override
    public void removeLayoutComponent(Component comp) {
    }
    @Override
    public Dimension preferredLayoutSize(Container target) {
       return minimumLayoutSize( target ) ;
    }
    @Override
    public Dimension minimumLayoutSize(Container target) {
      synchronized (target.getTreeLock()) {
         int [] [] dim = getMinimumDimensions(target );
         Insets    insets  = target.getInsets();
         int     maxwidth  = 0 ;
         int     maxheight = 0 ;
         
         if( ( _fitsAllSizes >= 0 ) && ( _fitsAllSizes < _columns ) ){
           Dimension d = target.getSize() ;
           maxwidth = d.width ;
         }else{        
           for( int column = 0 ; column < _columns ; column++ ) {
               maxwidth += dim[1][column];
           }
           maxwidth += ( insets.left + insets.right + ( _columns -1 )* _hGap ) ;
         }

         for( int row = 0 ; row < dim[0].length ; row++ ) {
             maxheight += dim[0][row];
         }
            
         maxheight += ( insets.top + insets.bottom + ( dim[0].length - 1 ) * _vGap ) ;
         
//         System.out.println( "minimumLayoutSize : "+maxwidth+" "+maxheight ) ;                
         return new Dimension( maxwidth , maxheight ) ;
      }
    }
    private int [] [] getMinimumDimensions(Container target ){
       int [] [] dim = new int [2][] ;
       
       synchronized (target.getTreeLock()) {
       
 	  int components = target.getComponentCount() / _columns * _columns ;
          
          if( components == 0 ) {
              return dim;
          }
          
          dim[0] = new int[components/_columns] ; // rows  
          dim[1] = new int[_columns] ;            // columns
                   
          for( int i = 0 ; i < components  ; i ++ ){    
             Component m  = target.getComponent(i) ;
             Dimension d  = m.getMinimumSize() ;
//             System.out.println( ""+i+" : "+d ) ;
             int column = i % _columns ;
             int row    = i / _columns ;
             dim[0][row]     = Math.max( dim[0][row]    , d.height ) ;
             dim[1][column]  = Math.max( dim[1][column] , d.width ) ;
            
          }
//          for( int i = 0 ; i < dim[0].length ; i++ )
//            System.out.println( "dim[0]["+i+"] : "+dim[0][i] ) ;
//          for( int i = 0 ; i < dim[1].length ; i++ )
//            System.out.println( "dim[1]["+i+"] : "+dim[1][i] ) ;
          return dim ;
       }
    }
    @Override
    public void layoutContainer(Container target) {
//      System.out.println( "layoutContainer" ) ;
      synchronized (target.getTreeLock()) {
	Insets    insets  = target.getInsets();
        Dimension t_dim   = target.getSize() ;
	int     maxwidth  = t_dim.width  - (insets.left + insets.right );
	int     maxheight = t_dim.height - (insets.top  + insets.bottom );
	int components = target.getComponentCount();
        if( components < 1 ) {
            return;
        }
        
        int [] [] dim = getMinimumDimensions(target );
        
        components = components / _columns * _columns ;
        
        if( ( _fitsAllSizes >= 0 ) && ( _fitsAllSizes < _columns ) ){        
           int sum = 0 ;
           for( int column = 0 ; column < _columns ; column++ ) {
               sum += dim[1][column];
           }
           sum -= dim[1][_fitsAllSizes] ;
           dim[1][_fitsAllSizes] = t_dim.width - 
                                   insets.left - insets.right - sum -
                                   ( _columns -1 ) * _hGap ;
        }
        
        int rows = components / _columns ;
        int y    = insets.top ;
        int element = 0 ;
        for( int row = 0 ; row < rows ; row ++ ){
            
           int x  = insets.left ;
           for( int column = 0 ; column < _columns  ; column ++ ){  
           
              Component m  = target.getComponent( element++ ) ;
	      if (m.isVisible()) {
                  m.validate() ;  
                  m.setSize( dim[1][column] , dim[0][row] ); 
                  m.setLocation( x  , y ) ;             
              }
              x += ( dim[1][column] + _hGap ) ;
           }
           y += ( dim[0][row] + _vGap ) ;
        }
        
      }
    }
    
    public String toString() {
	return getClass().getName() ;
    }
}
