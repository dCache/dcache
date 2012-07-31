package dmg.cells.applets.spy ;

import java.awt.* ;
import java.awt.event.* ;

public class BorderPanel extends Panel {
    private int _b = 4 ;
    public BorderPanel( Component c ){
       super( new BorderLayout() ) ;
       add( c , "Center" ) ;
    }
    public BorderPanel( Component c , int frame ){
       super( new BorderLayout() ) ;
       _b = frame ;
       add( c , "Center" ) ;

    }
    public synchronized void actionPerformed( ActionEvent event ){
       String command = event.getActionCommand() ;
       System.out.println( "Action x : "+command ) ;
    }

    @Override
    public Insets getInsets(){ return new Insets( _b , _b ,_b , _b ) ; }
    @Override
    public void paint( Graphics g ){
       Dimension   d    = getSize() ;
       Color base = getBackground() ;
       for( int i = 0 ; i < _b ; i++ ){
          g.setColor( base ) ;
          g.drawRect( i , i , d.width-2*i-1 , d.height-2*i-1 ) ;
          base = base.darker() ;
       }
    }
   
}


 
