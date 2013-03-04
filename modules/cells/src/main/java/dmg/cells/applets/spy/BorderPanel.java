package dmg.cells.applets.spy ;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Insets;
import java.awt.Panel;
import java.awt.event.ActionEvent;

public class BorderPanel extends Panel {
    private static final long serialVersionUID = -4311790038120869338L;
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



