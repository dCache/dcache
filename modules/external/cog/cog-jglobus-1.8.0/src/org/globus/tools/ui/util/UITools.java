package org.globus.tools.ui.util;

import java.awt.Component;
import java.awt.Container;
import java.awt.Rectangle;
import java.awt.Label;
import java.awt.Dimension;
import java.awt.Frame;

/*

  A tool to center windows relative to another window
  or relative to the screen 
  
 -----------------------------------------------------------------------------
  (c) Copyright IBM Corp. 1998  All rights reserved.

 The sample program(s) is/are owned by International Business Machines
 Corporation or one of its subsidiaries ("IBM") and is/are copyrighted and
 licensed, not sold.

 You may copy, modify, and distribute this/these sample program(s) in any form
 without payment to IBM, for any purpose including developing, using, marketing
 or distributing programs that include or are derivative works of the sample
 program(s).

 The sample program(s) is/are provided to you on an "AS IS" basis, without
 warranty of any kind.  IBM HEREBY EXPRESSLY DISCLAIMS ALL WARRANTIES, EITHER
 EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.  Some jurisdictions do
 not allow for the exclusion or limitation of implied warranties, so the above
 limitations or exclusions may not apply to you.  IBM shall not be liable for
 any damages you suffer as a result of using, modifying or distributing the
 sample program(s) or its/their derivatives.

 Each copy of any portion of this/these sample program(s) or any derivative
 work, must include the above copyright notice and disclaimer of warranty.

 -----------------------------------------------------------------------------

 Type: "java UITools"  from the command line to test
         
*/

/**
 * This class provides method to center a Component in a center
 * of a given container
 *
 * @version 1.0
 * @author Hung Dinh, David Carew
 *
 */
public abstract class UITools {

   /**
    * This method positions a Component in a center of a given
    * Container.
    *  
    * If the Container is null or the the Component is larger 
    * than the Container, the  Component is centered relative 
    * to the screen.
    *
    * @param parent The Container relative to which
    *               the Component is centered. 
    * @param comp   The Component to be centered
    *
    */
   public static void center(Container parent, Component comp) {
      int x, y;
      Rectangle parentBounds;
      Dimension compSize = comp.getSize();

      // If Container is null or smaller than the component
      // then our bounding rectangle is the
      // whole screen
      if ((parent == null) || (parent.getBounds().width < compSize.width) ||
          (parent.getBounds().height < compSize.height)) {
         parentBounds = new Rectangle(comp.getToolkit().getScreenSize());
         parentBounds.setLocation(0,0);
      }
      // Else our bounding rectangle is the Container
      else {
          parentBounds = parent.getBounds();
      }

      // Place the component so its center is the same
      // as the center of the bounding rectangle
      x = parentBounds.x + ((parentBounds.width/2) - (compSize.width/2));
      y = parentBounds.y + ((parentBounds.height/2) - (compSize.height/2));
      comp.setLocation(x, y);
   }

   // For testing purposes only 

   public static void main(String args[]) {
      // Create a frame and center it relative to the 
      // screen
      Frame f = new Frame("This should be centered");
      f.add(new Label("Press Ctrl-C from command line to exit"));
      f.setSize(f.getPreferredSize());
      f.pack();
      center(null, f);
      f.show();
   }
}
