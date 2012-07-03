package org.dcache.webadmin.view.pages.billingplots;

import java.io.File;
import java.io.IOException;

import org.apache.wicket.markup.html.image.Image;
import org.dcache.services.billing.cells.BillingDatabase;
import org.dcache.webadmin.view.pages.basepage.BasePage;

public class BillingPlots extends BasePage {

    private final String imageName = "image_";

    public BillingPlots() throws IOException {
        String imageFormat = getWebadminApplication().getExportExt();
        String plotsDirectoryPath = getWebadminApplication().getPlotsDir();
        String[] type = BillingDatabase.getTYPE().toArray(new String[0]);
        String[] ext = BillingDatabase.getEXT().toArray(new String[0]);
        File dir = new File(plotsDirectoryPath);
        for (int t = 0; t < type.length; t++) {
            for (int e = 0; e < ext.length; e++) {

                final FileResource file
                    = new FileResource(new File(dir,
                                                type[t] + ext[e] + imageFormat));
                final Image image = new Image(imageName + t + e, file);
                add(image);
            }
        }
    }
}
