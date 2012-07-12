package dmg.cells.applets.login ;

import java.awt.* ;
import java.awt.event.* ;
import java.net.* ;
import java.io.*;
import dmg.util.* ;
import dmg.protocols.ssh.* ;

public class      SshLoginOKPanel 
       extends    SshActionPanel              {
       
//   private Font      _font = new Font( "TimesRoman" , Font.BOLD , 20 ) ;
   private Canvas    _canvas = null ;
   
   SshLoginOKPanel(){  
       add( _canvas = new MyCanvas( getPreferredSize() ) ) ; 
       System.out.println( "Canvas added" ) ;
   }
   private void mouseSaysExit(){
      informActionListeners("exit") ;
   }
//   public void paint( Graphics g ){
//      _canvas.paint( g ) ;
//   }
   class MyCanvas extends Canvas implements MouseListener {
      private Dimension _dim = null ;
      private boolean   _mouseIsIn = false ;
      MyCanvas( Dimension d ){
         _dim = d ;
         addMouseListener( this ) ;
      }
      @Override
      public Dimension getPreferredSize(){ return _dim = new Dimension(100,100) ; }
      @Override
      public void paint( Graphics g ){
        Dimension d = getSize() ;
        System.out.println( "Painting width="+d.width+";height="+d.height ) ;
   //     g.setFont( _font ) ;
        g.setColor( _mouseIsIn ? Color.red : Color.blue  ) ;
        g.fillRect( 0 , 0 , d.width-1 , d.height-1 ) ;
        int cxa = d.width / 3 ;
        int cya = d.height / 2 ;
        int cxb = 2 * cxa ;
        int cyb = cya ;
        int r = Math.min( d.width / 6 , d.height / 6 ) / 2 ;
        g.setColor( _mouseIsIn ? Color.blue : Color.red  ) ;
        for( int i=  0 ; i < 4 ; i++ ) {
            g.drawOval(cxa - r - i, cya - r - i, 2 * (r + i), 2 * (r + i));
        }
        for( int i=  0 ; i < 4 ; i++ ) {
            g.drawOval(cxb - r - i, cyb - r - i, 2 * (r + i), 2 * (r + i));
        }
        g.drawLine( cxa + r , cya , cxb - r , cyb ) ;
      }
      @Override
      public void mouseClicked(MouseEvent e){
         System.out.println( "Mouse clicked" ) ;
         mouseSaysExit() ;
      }
      @Override
      public void mousePressed(MouseEvent e){
         System.out.println( "Mouse whatever" ) ;
      }
      @Override
      public void mouseReleased(MouseEvent e){
         System.out.println( "Mouse whatever" ) ;
      }
      @Override
      public void mouseEntered(MouseEvent e){
         _mouseIsIn = true ;
         System.out.println( "Mouse whatever" ) ;
         repaint() ;
      }
      @Override
      public void mouseExited(MouseEvent e){
         _mouseIsIn = false ;
         System.out.println( "Mouse whatever" ) ;
         repaint() ;
      }
   }

  
}
 
