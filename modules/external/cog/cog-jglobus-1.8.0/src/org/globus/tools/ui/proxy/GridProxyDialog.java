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
package org.globus.tools.ui.proxy;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import org.globus.tools.ui.util.FileBrowser;
import org.globus.tools.ui.util.JJPanel;

public class GridProxyDialog extends JDialog implements ActionListener {

    private GridProxyProperties gridProps;

    private FileBrowser certFileFB, keyFileFB, proxyFileFB, caCertFileFB;

    private ButtonGroup hoursGroup = new ButtonGroup();

    private ButtonGroup bitsGroup = new ButtonGroup();

    private JButton okButton, saveButton, exitButton;
    
    private JRadioButton otherRB;
    private JTextField otherTF;

    public GridProxyDialog(GridProxyProperties props) {
        this.gridProps = props;
        
        setTitle("Credential Properties");
        setSize(520, 300);

        Container contentPane = getContentPane();

        JJPanel gridPanel = new JJPanel();
        gridPanel.setInsets(2, 2, 2, 2);
        gridPanel.setAnchor(GridBagConstraints.EAST);
        gridPanel.setBorder(BorderFactory.createEtchedBorder());

        gridPanel.add(new JLabel("Proxy Lifetime: "), 0, 0, 1, 1);

        gridPanel.add(new JLabel("Key length: "), 0, 1, 1, 1);

        gridPanel.add(new JLabel("Location of User Certificate: "), 0, 2, 1, 1);

        gridPanel.add(new JLabel("Location of User Private Key: "), 0, 3, 1, 1);

        gridPanel.add(new JLabel("Location of CA Certificate: "), 0, 4, 1, 1);

        gridPanel.add(new JLabel("Location of Grid Proxy File: "), 0, 5, 1, 1);

        gridPanel.setAnchor(GridBagConstraints.WEST);

        // this makes the component to fill the entrie available space
        gridPanel.gbc.weightx = 1;

        gridPanel.setFill(GridBagConstraints.HORIZONTAL);

        JPanel hoursPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 2, 0));

        JRadioButton shRB = new JRadioButton("12 h");
        JRadioButton mhRB = new JRadioButton("24 h");
        JRadioButton lhRB = new JRadioButton("1 week");
        JRadioButton ghRB = new JRadioButton("1 month");
        otherRB = new JRadioButton();

        shRB.setActionCommand("12");
        mhRB.setActionCommand("24");
        lhRB.setActionCommand("168");
        ghRB.setActionCommand("672");
        otherRB.setActionCommand("other");
        
        otherTF = new JTextField(3);
        otherTF.addFocusListener(new FocusListener() {
            public void focusGained(FocusEvent e) {
                otherRB.setSelected(true);
            }

            public void focusLost(FocusEvent e) {
            }

        });

        int time = this.gridProps.getHours();
        if (time == 12)
            shRB.setSelected(true);
        else if (time == 24)
            mhRB.setSelected(true);
        else if (time == 168)
            lhRB.setSelected(true);
        else if (time == 672)
            ghRB.setSelected(true);
        else {
            otherTF.setText(String.valueOf(time));
            otherRB.setSelected(true);
        }

        // Group the radio buttons.
        hoursGroup.add(shRB);
        hoursGroup.add(mhRB);
        hoursGroup.add(lhRB);
        hoursGroup.add(ghRB);
        hoursGroup.add(otherRB);

        hoursPanel.add(shRB);
        hoursPanel.add(mhRB);
        hoursPanel.add(lhRB);
        hoursPanel.add(ghRB);
        hoursPanel.add(otherRB);
        hoursPanel.add(otherTF);
        hoursPanel.add(new JLabel("h"));

        gridPanel.add(hoursPanel, 1, 0, 1, 1);

        JPanel bitsPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 2, 0));

        JRadioButton sbRB = new JRadioButton("512");
        JRadioButton mbRB = new JRadioButton("1024");
        JRadioButton lbRB = new JRadioButton("2048");
        JRadioButton gbRB = new JRadioButton("4096");
        sbRB.setActionCommand("512");
        mbRB.setActionCommand("1024");
        lbRB.setActionCommand("2048");
        gbRB.setActionCommand("4096");

        if (this.gridProps.getBits() == 512)
            sbRB.setSelected(true);
        else if (this.gridProps.getBits() == 1024)
            mbRB.setSelected(true);
        else if (this.gridProps.getBits() == 2048)
            lbRB.setSelected(true);
        else if (this.gridProps.getBits() == 4096)
            gbRB.setSelected(true);

        // Group the radio buttons.
        bitsGroup.add(sbRB);
        bitsGroup.add(mbRB);
        bitsGroup.add(lbRB);
        bitsGroup.add(gbRB);

        bitsPanel.add(sbRB);
        bitsPanel.add(mbRB);
        bitsPanel.add(lbRB);
        bitsPanel.add(gbRB);

        gridPanel.add(bitsPanel, 1, 1, 1, 1);

        certFileFB = new FileBrowser("Select User Certificate", "Select");
        certFileFB.setFile(this.gridProps.getUserCertFile());

        gridPanel.add(certFileFB, 1, 2, 1, 1);

        keyFileFB = new FileBrowser("Select User Private Key", "Select");
        keyFileFB.setFile(this.gridProps.getUserKeyFile());

        gridPanel.add(keyFileFB, 1, 3, 1, 1);

        caCertFileFB = new FileBrowser("Select CA Certificate", "Select");
        caCertFileFB.setFile(this.gridProps.getCACertFile());

        gridPanel.add(caCertFileFB, 1, 4, 1, 1);

        proxyFileFB = new FileBrowser("Select Grid Proxy File", "Select");
        proxyFileFB.setFile(this.gridProps.getProxyFile());

        gridPanel.add(proxyFileFB, 1, 5, 1, 1);

        contentPane.setLayout(new BorderLayout());
        contentPane.add(gridPanel, BorderLayout.CENTER);

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        okButton = new JButton("OK");
        saveButton = new JButton("Save");
        exitButton = new JButton("Cancel");

        buttonPanel.add(okButton);
        buttonPanel.add(saveButton);
        buttonPanel.add(exitButton);

        okButton.addActionListener(this);
        saveButton.addActionListener(this);
        exitButton.addActionListener(this);

        contentPane.add(buttonPanel, BorderLayout.SOUTH);
    }

    private boolean error(String msg) {
        JOptionPane.showMessageDialog(this, msg, "Need More Information",
                JOptionPane.WARNING_MESSAGE);
        return false;
    }

    private boolean validateSettings() {
        if (proxyFileFB.getFile().equals("")) {
            return error("Please enter the proxy file location");
        } else if (certFileFB.getFile().equals("")) {
            return error("Please enter the certificate file location");
        } else if (keyFileFB.getFile().equals("")) {
            return error("Please enter the private key file location");
        } else if (caCertFileFB.getFile().equals("")) {
            return error("Please enter the CA certificate file location");
        }

        if (otherRB.isSelected()) {
            this.gridProps.setHours(Integer.parseInt(otherTF.getText().trim()));
        } else {
            this.gridProps.setHours(Integer.parseInt(hoursGroup.getSelection().getActionCommand()));
        }
        
        this.gridProps.setBits(Integer.parseInt(bitsGroup.getSelection().getActionCommand()));
        
        this.gridProps.setProxyFile(proxyFileFB.getFile());

        this.gridProps.setUserCertFile(certFileFB.getFile());

        this.gridProps.setUserKeyFile(keyFileFB.getFile());
        
        this.gridProps.setCACertFile(caCertFileFB.getFile());

        return true;
    }

    /**
     * Handles button events for saving and exiting
     * 
     * @param evt
     *            an ActionEvent
     */
    public void actionPerformed(ActionEvent evt) {
        Object source = evt.getSource();
        if (source == okButton) {
            if (!validateSettings()) {
                return;
            }
            setVisible(false);
        } else if (source == saveButton) {
            if (!validateSettings()) {
                return;
            }
            if (this.gridProps.saveProperties()) {
                JOptionPane.showMessageDialog(this,
                        "Successfully saved properties", "Information",
                        JOptionPane.INFORMATION_MESSAGE);
            } else {
                JOptionPane.showMessageDialog(this,
                        "Failed to save properties", "Information",
                        JOptionPane.WARNING_MESSAGE);
            }
        } else if (source == exitButton) {

            setVisible(false);
        } else {
            System.err.println("Unidentified event in GridProxyDialog");
        }
    }

}
