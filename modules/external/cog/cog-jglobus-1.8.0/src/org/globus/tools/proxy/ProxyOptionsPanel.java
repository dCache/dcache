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
package org.globus.tools.proxy;

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;

import javax.swing.JOptionPane;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.ButtonGroup;
import javax.swing.JTextField;

import org.globus.common.CoGProperties;
import org.globus.tools.ui.util.JJPanel;
import org.globus.tools.ui.util.FileBrowser;

public class ProxyOptionsPanel extends JJPanel {

    protected FileBrowser proxyFileFB;

    protected ButtonGroup hoursGroup;

    protected ButtonGroup bitsGroup;

    protected JRadioButton shRB, mhRB, lhRB, ghRB, otherRB;

    protected JRadioButton sbRB, mbRB, lbRB, gbRB;

    private JTextField otherTF;

    public ProxyOptionsPanel() {

        setAnchor(GridBagConstraints.EAST);

        add(new JLabel("Proxy Lifetime: "), 0, 0, 1, 1);

        add(new JLabel("Strength: "), 0, 1, 1, 1);

        add(new JLabel("Proxy File: "), 0, 2, 1, 1);

        setAnchor(GridBagConstraints.WEST);

        // this makes the component to fill the entrie available space
        gbc.weightx = 1;

        setFill(GridBagConstraints.HORIZONTAL);

        JPanel hoursPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 2, 0));

        shRB = new JRadioButton("12 h");
        mhRB = new JRadioButton("24 h", true);
        lhRB = new JRadioButton("1 week");
        ghRB = new JRadioButton("1 month");
        otherRB = new JRadioButton();

        shRB.setActionCommand("12");
        mhRB.setActionCommand("24");
        lhRB.setActionCommand("168");
        ghRB.setActionCommand("672");
        otherRB.setActionCommand("other");

        // Group the radio buttons.
        hoursGroup = new ButtonGroup();
        hoursGroup.add(shRB);
        hoursGroup.add(mhRB);
        hoursGroup.add(lhRB);
        hoursGroup.add(ghRB);
        hoursGroup.add(otherRB);

        otherTF = new JTextField(3);
        otherTF.addFocusListener(new FocusListener() {
            public void focusGained(FocusEvent e) {
                otherRB.setSelected(true);
            }

            public void focusLost(FocusEvent e) {
            }

        });

        hoursPanel.add(shRB);
        hoursPanel.add(mhRB);
        hoursPanel.add(lhRB);
        hoursPanel.add(ghRB);
        hoursPanel.add(otherRB);
        hoursPanel.add(otherTF);
        hoursPanel.add(new JLabel("h"));

        add(hoursPanel, 1, 0, 1, 1);

        JPanel bitsPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 2, 0));

        sbRB = new JRadioButton("512", true);
        mbRB = new JRadioButton("1024");
        lbRB = new JRadioButton("2048");
        gbRB = new JRadioButton("4096");
        sbRB.setActionCommand("512");
        mbRB.setActionCommand("1024");
        lbRB.setActionCommand("2048");
        gbRB.setActionCommand("4096");

        // Group the radio buttons.
        bitsGroup = new ButtonGroup();
        bitsGroup.add(sbRB);
        bitsGroup.add(mbRB);
        bitsGroup.add(lbRB);
        bitsGroup.add(gbRB);

        bitsPanel.add(sbRB);
        bitsPanel.add(mbRB);
        bitsPanel.add(lbRB);
        bitsPanel.add(gbRB);

        add(bitsPanel, 1, 1, 1, 1);

        proxyFileFB = new FileBrowser("Select Grid Proxy File", "Select");

        add(proxyFileFB, 1, 2, 1, 1);
    }

    protected boolean error(String msg) {
        JOptionPane.showMessageDialog(this, msg, "Need More Information",
                JOptionPane.WARNING_MESSAGE);
        return false;
    }

    public boolean validateSettings() {
        if (proxyFileFB.getFile().equals("")) {
            return error("Please enter the proxy file location");
        }
        return true;
    }

    public void set(CoGProperties props) {

        proxyFileFB.setFile(props.getProxyFile());

        int value = props.getProxyLifeTime();

        if (value == 12)
            shRB.setSelected(true);
        else if (value == 24)
            mhRB.setSelected(true);
        else if (value == 168)
            lhRB.setSelected(true);
        else if (value == 672)
            ghRB.setSelected(true);
        else {
            otherTF.setText(String.valueOf(value));
            otherRB.setSelected(true);
        }

        value = props.getProxyStrength();

        if (value == 512)
            sbRB.setSelected(true);
        else if (value == 1024)
            mbRB.setSelected(true);
        else if (value == 2048)
            lbRB.setSelected(true);
        else if (value == 4096)
            gbRB.setSelected(true);
        else
            sbRB.setSelected(true);

    }

    public void get(CoGProperties props) {
        props.setProxyStrength(Integer.parseInt(bitsGroup.getSelection()
                .getActionCommand()));
        props.setProxyFile(proxyFileFB.getFile());

        if (otherRB.isSelected()) {
            props.setProxyLifeTime(Integer.parseInt(otherTF.getText().trim()));
        } else {
            props.setProxyLifeTime(Integer.parseInt(hoursGroup.getSelection()
                    .getActionCommand()));
        }
    }

}
