/*
 * Found this at: http://www.codeguru.com/java/articles/199.shtml
 */
package org.globus.tools.ui.swing; 

import javax.swing.plaf.basic.BasicLabelUI;
import java.awt.geom.AffineTransform;
import java.awt.Rectangle;
import java.awt.Insets;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.FontMetrics;
import java.awt.Dimension;
import javax.swing.JComponent;
import javax.swing.Icon;
import javax.swing.JLabel;

public class VerticalLabelUI extends BasicLabelUI
{
	static {
		labelUI = new VerticalLabelUI(false);
	}
	
	protected boolean clockwise;
  public VerticalLabelUI( boolean clockwise )
  {
    super();
    this.clockwise = clockwise;
  }
	

    public Dimension getPreferredSize(JComponent c) 
    {
    	Dimension dim = super.getPreferredSize(c);
    	return new Dimension( dim.height, dim.width );
    }	

    private static Rectangle paintIconR = new Rectangle();
    private static Rectangle paintTextR = new Rectangle();
    private static Rectangle paintViewR = new Rectangle();
    private static Insets paintViewInsets = new Insets(0, 0, 0, 0);

	public void paint(Graphics g, JComponent c) 
    {

    	
        JLabel label = (JLabel)c;
        String text = label.getText();
        Icon icon = (label.isEnabled()) ? label.getIcon() : label.getDisabledIcon();

        if ((icon == null) && (text == null)) {
            return;
        }

        FontMetrics fm = g.getFontMetrics();
        paintViewInsets = c.getInsets(paintViewInsets);

        paintViewR.x = paintViewInsets.left;
        paintViewR.y = paintViewInsets.top;
    	
    	// Use inverted height & width
        paintViewR.height = c.getWidth() - (paintViewInsets.left + paintViewInsets.right);
        paintViewR.width = c.getHeight() - (paintViewInsets.top + paintViewInsets.bottom);

        paintIconR.x = paintIconR.y = paintIconR.width = paintIconR.height = 0;
        paintTextR.x = paintTextR.y = paintTextR.width = paintTextR.height = 0;

        String clippedText = 
            layoutCL(label, fm, text, icon, paintViewR, paintIconR, paintTextR);

    	Graphics2D g2 = (Graphics2D) g;
    	AffineTransform tr = g2.getTransform();
    	if( clockwise )
    	{
	    	g2.rotate( Math.PI / 2 ); 
    		g2.translate( 0, - c.getWidth() );
    	}
    	else
    	{
	    	g2.rotate( - Math.PI / 2 ); 
    		g2.translate( - c.getHeight(), 0 );
    	}

    	if (icon != null) {
            icon.paintIcon(c, g, paintIconR.x, paintIconR.y);
        }

        if (text != null) {
            int textX = paintTextR.x;
            int textY = paintTextR.y + fm.getAscent();

            if (label.isEnabled()) {
                paintEnabledText(label, g, clippedText, textX, textY);
            }
            else {
                paintDisabledText(label, g, clippedText, textX, textY);
            }
        }
    	
    	
    	g2.setTransform( tr );
    }
}

