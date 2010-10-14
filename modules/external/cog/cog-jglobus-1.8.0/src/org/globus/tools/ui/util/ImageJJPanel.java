/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.tools.ui.util;

import java.awt.Image;
import java.awt.Graphics;
import java.awt.Dimension;
import javax.swing.ImageIcon;

public class ImageJJPanel extends JJPanel {
    
    protected Image image;
    
    public void setImage(ImageIcon icon) {
	image = icon.getImage();
    }
    
    public void paintComponent(Graphics g) {
	super.paintComponent(g);
	if (image == null) return ;
	Dimension d = getSize();
	g.drawImage(image, 0, 0, (int)d.getWidth(), 
		    (int)d.getHeight(), this);
    }
    
}
