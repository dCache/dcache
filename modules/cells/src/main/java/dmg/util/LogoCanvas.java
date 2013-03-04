package dmg.util ;

import java.awt.Canvas;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.Random;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      LogoCanvas
       extends    Canvas
       implements MouseListener , Runnable {

   private static final long serialVersionUID = -4709372485548974258L;
   private String     _string = "PnfsSpy" , _result ;
   private Font       _font   , _smallFont  ;
   private Toolkit    _toolkit ;
   private ActionListener _actionListener;
   //
   //  all the choise stuff
   //
   private boolean    _makeChoiseMode;
   private String []  _choises ;
   private final Object     _choiseLock = new Object() ;
   private int    []  _offsets ;
   //
   //  all the animation
   //
   private Thread     _worker;
   private int        _animationMode;
   private int        _animationState, _edges = 3 ;
   private Image      _offImage ;
   private final Object     _animationLock = new Object() ;
   //
   // snow
   //
   private Image []  _snowImages ;
   private int       _snowImagesCount = 8 ,
                     _snowImagesWidth ,
                     _snowImagesHeight ;
   //
   // the animation modes
   //
   public static final int INFINIT    = 1 ;
   public static final int GROWING    = 2 ;
   public static final int SHRINKING  = 3 ;
   public static final int SNOW       = 4 ;

   public LogoCanvas( String title ){
      super() ;
      _string = title == null ? "" : title ;
//      setSize( width , height  );

      _toolkit = getToolkit() ;

      setBackground( Color.blue ) ;
      addMouseListener( this ) ;
      _font      = new Font( "TimesRoman" , Font.BOLD , 20 ) ;
      _smallFont = new Font( "TimesRoman" , Font.ITALIC , 20 ) ;

      _makeChoiseMode = false ;
      _animationMode  = 0 ;

   }
   public void setActionListener( ActionListener l ){
        _actionListener  = l ;
   }
   public void setString( String str ){
     _makeChoiseMode  = false ;
//      _animationMode  = 0 ;
     System.out.println( "Going into stop animation" ) ;
     _stopAnimation() ;
     System.out.println( "Going into repaint" ) ;
     _string = str ;
     repaint() ;
     _toolkit.sync() ;
     System.out.println( "Leaving repaint" ) ;
   }
   public void animation( int mode ){
     _makeChoiseMode  = false ;
     _animationMode   = mode ;
     if( ( _animationMode == INFINIT ) ||
         ( _animationMode == GROWING )     ){
        _animationState = 0 ;
     }else{
        _animationState = 1000 ;
     }
     synchronized( _animationLock ){
        if( _worker != null ){ _worker.interrupt() ; }
        _worker = new Thread( this ) ;
        _worker.start();
     }
   }
   public String makeChoise( String title , String [] choises ){
      _stopAnimation() ;
      _makeChoiseMode = true ;
      _string         = title ;
      _choises        = choises ;
      _offsets        = new int[choises.length] ;

      synchronized( _choiseLock ){
         System.out.println( " choise : repaint " ) ;
         repaint() ;
         _toolkit.sync() ;
         try { _choiseLock.wait() ; }
         catch( InterruptedException ie ){}
      }
      _makeChoiseMode = false ;
      _string = "" ;

      return _result ;
   }
   @Override
   public void paint( Graphics g ){
      Dimension   d  = getSize() ;
      if( _makeChoiseMode ){
         g.setColor( Color.red ) ;
         g.setFont( _font ) ;
         FontMetrics fm = g.getFontMetrics() ;
         int y = 10 , x = 10  ;

         y  += fm.getHeight() ;
         g.drawString(  _string , x , y ) ;

         x += 10 ;
         g.setFont( _smallFont ) ;
         fm = g.getFontMetrics() ;
         int height     = fm.getHeight() ;
         int leading    = fm.getLeading() ;
         for( int i = 0 ; i < _choises.length ; i++ ){
            y += ( height + 2*leading );
            g.drawString(  _choises[i] , x , y ) ;
            _offsets[i] = y ;
         }
         _toolkit.sync() ;
      }else if( _animationMode > 0 ) {
         if( _animationMode == SNOW ){
            _createSnowImages(d) ;
            _drawSnowImages(g);
         }else{
//            if( _offImage == null )
            _offImage = createImage( d.width , d.height ) ;
            _makeFun( g ) ;
         }
      }else {
         if( _string == null ) {
             return;
         }
         g.setFont( _font ) ;
         FontMetrics fm = g.getFontMetrics() ;
         int height     = fm.getHeight() ;
         int width      = fm.stringWidth( _string ) ;
         int y = ( d.height + height ) / 2 ;
         int x = ( d.width  - width  ) / 2 ;
         g.setColor( Color.red ) ;
         g.drawString(  _string , x , y ) ;
      }
   }
   @Override
   public void update( Graphics g ){
      Dimension   d  = getSize() ;
      if( _animationMode > 0 ) {
         if( _animationMode == SNOW ){
            _createSnowImages(d) ;
            _drawSnowImages(g);

         }else{
            if( _offImage == null ) {
                _offImage = createImage(d.width, d.height);
            }
            _makeFun( g ) ;
         }
      }else{
         g.setColor( Color.blue ) ;
         g.fillRect( 0 , 0 , d.width , d.height ) ;
         paint( g ) ;
      }
   }
   private void _drawSnowImages( Graphics g ){
     int s = _animationState % _snowImagesCount ;
     g.drawImage( _snowImages[s] , 0 , 0 , null ) ;
   }
   private void _createSnowImages( Dimension d ){
     Random r = new Random();
     if( ( _snowImages == null ) ||
         ( d.height != _snowImagesHeight ) ||
         ( d.width  != _snowImagesWidth  )    ){

        _snowImages = new Image[_snowImagesCount] ;
        _snowImagesHeight = d.height ;
        _snowImagesWidth  = d.width ;

        for( int i = 0 ; i < _snowImagesCount ; i++ ){
            _snowImages[i] = createImage( d.width , d.height ) ;
            Graphics g = _snowImages[i].getGraphics() ;
            g.setColor( Color.white ) ;
            g.fillRect( 0 , 0 , d.width , d.height ) ;
            g.setColor( Color.black ) ;
            System.out.println( " Creating snow image "+i);
            for( int w = 0 ; w < d.width ; w++ ){
              for( int h = 0 ; h < d.height ; h++ ){
                 if( (r.nextInt()%2)>0 ) {
                     g.drawLine(w, h, w, h);
                 }
              }
            }
            System.out.println( " Ready");

        }
     }
   }
   private void _stopAnimation(){
     _animationMode = 0 ;
     System.out.println( "Trying to enter animationLock " ) ;
     synchronized( _animationLock ){

        System.out.println( "AnimationLock entered " ) ;
        if( _worker != null ){
             System.out.println( "Worker not yet zero " ) ;
//            _worker.stop() ;
            _worker = null ;
        }else{
             System.out.println( "Worker is zero ( doing nothing) " ) ;
        }
     }
     System.out.println( "AnimationLock left " ) ;

   }
   @Override
   public void run(){
     Thread worker ;
     synchronized( _animationLock ){
        worker = _worker ;
     }
     if( Thread.currentThread() == worker ){
        System.out.println( " run (mode="+_animationMode+")" ) ;
        if( _animationMode == INFINIT ){
           while(true){
              _runUp() ;
              _runDown() ;
             _edges ++ ;
             if( _edges > 5 ) {
                 _edges = 3;
             }
           }
        }else if( _animationMode == GROWING ){
           _edges = 3 ;
           _runUp();
           System.out.println( "RunUp stopped, going into ActionListner" ) ;
           if( _actionListener != null ) {
               _actionListener.actionPerformed(
                       new ActionEvent(this, 0, "finished"));
           }
           System.out.println( "ActionListner returned" ) ;
        }else if( _animationMode == SHRINKING ){
           _edges = 3 ;
           _runDown();
           if( _actionListener != null ) {
               _actionListener.actionPerformed(
                       new ActionEvent(this, 0, "finished"));
           }
        }else if( _animationMode == SNOW ){
           _runSnow();
           if( _actionListener != null ) {
               _actionListener.actionPerformed(
                       new ActionEvent(this, 0, "finished"));
           }
        }
     }
     synchronized( _animationLock ){
        _worker = null ;
     }
   }
   private void _runSnow(){
       for(  _animationState = 0 ; true ;
             _animationState ++ ){

           repaint() ;
           _toolkit.sync() ;
           try{ Thread.sleep(100) ; }
           catch( InterruptedException ie ){}
       }
   }
   private void _runDown(){
       for(  _animationState = 1000 ;
             _animationState >= 0 ;
             _animationState -= 10 ){

           repaint() ;
           _toolkit.sync() ;
           try{ Thread.sleep(100) ; }
           catch( InterruptedException ie ){}
       }
   }
   private void _runUp(){
       for( _animationState = 0 ;
            _animationState < 1001 ;
            _animationState += 10 ){

            repaint() ;
            _toolkit.sync() ;
            try{ Thread.sleep(100) ; }
            catch( InterruptedException ie ){}
       }

   }
   private void _makeFun( Graphics g ){

      double fraction  = (double) (_animationState) / 1000. ;
      Dimension     d  = getSize() ;
      if( _offImage == null ) {
          return;
      }
      Graphics offGraphics = _offImage.getGraphics() ;
      offGraphics.setColor( Color.blue ) ;
      offGraphics.fillRect( 0 , 0 , d.width , d.height ) ;
//      offGraphics.setColor( new Color(
//                      Color.HSBtoRGB( (float)0.5 ,
//                                      (float)0.5 ,
//                                      (float)0.9  ) ) );
      offGraphics.setColor( Color.red ) ;
      _drawPolygon( offGraphics , fraction ) ;
      offGraphics.setColor( Color.blue ) ;
      _drawPolygon( offGraphics , fraction*0.5 ) ;
      offGraphics.setColor( Color.yellow ) ;
      _drawPolygon( offGraphics , fraction*0.25 ) ;
      g.drawImage( _offImage , 0 , 0 , null ) ;
   }
   public void _drawPolygon( Graphics g , double fraction ){
      Dimension   d  = getSize() ;
      double y0 = (double)d.height / 2. ;
      double x0 = (double)d.width  / 2. ;
      double r  = Math.min( x0 , y0 ) * fraction ;
      double a  = fraction * 2. *  Math.PI ;
      int    n  = _edges ;
      double diff =  2. *  Math.PI / (double) n;
      int [] x  = new int [n] ;
      int [] y  = new int [n] ;

      for( int i = 0 ; i < n ; i ++ , a+= diff  ){
         x[i]  = (int)( x0 + r * Math.sin( a ) );
         y[i]  = (int)( y0 - r * Math.cos( a ) );
      }
      g.fillPolygon( x , y , n ) ;
   }
   @Override
   public void mouseClicked( MouseEvent e ){
     if( _makeChoiseMode ){
       synchronized( _choiseLock ){
          int i ;
          _result = null ;
          if( _offsets == null ) {
              return;
          }
          for( i = 0 ; i < _offsets.length ; i++ ){
             if( _offsets[i] > e.getY() ) {
                 break;
             }
          }
          if( i == _offsets.length ) {
              return;
          }
          _result = _choises[i] ;
          _choiseLock.notifyAll() ;
       }
     }else if( _animationMode > 0 ){
       _stopAnimation() ;
       if( _actionListener != null ) {
           _actionListener.actionPerformed(
                   new ActionEvent(this, 0, "clicked"));
       }
     }else{
       if( _actionListener != null ) {
           _actionListener.actionPerformed(
                   new ActionEvent(this, 0, "clicked"));
       }
     }
   }
   @Override
   public void mouseExited( MouseEvent e ){
   }
   @Override
   public void mouseEntered( MouseEvent e ){
   }
   @Override
   public void mousePressed( MouseEvent e ){
   }
   @Override
   public void mouseReleased( MouseEvent e ){

   }

}
