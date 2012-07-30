package org.dcache.webadmin.view.pages.billingplots;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.wicket.markup.html.image.Image;
import org.apache.wicket.request.resource.IResource;
import org.apache.wicket.request.resource.ResourceStreamResource;
import org.apache.wicket.util.resource.FileResourceStream;
import org.dcache.services.billing.cells.BillingDatabase;
import org.dcache.webadmin.view.pages.basepage.BasePage;

public class BillingPlots extends BasePage {

    private final String imageName = "image_";

    public BillingPlots()
    {
        String imageFormat = getWebadminApplication().getExportExt();
        String plotsDirectoryPath = getWebadminApplication().getPlotsDir();
        List<String> type = BillingDatabase.getTYPE();
        List<String> ext = BillingDatabase.getEXT();
        File dir = new File(plotsDirectoryPath);
        for (int t = 0; t < type.size(); t++) {
            for (int e = 0; e < ext.size(); e++) {
                final IResource file = new ResourceStreamResource(
                        new FileResourceStream(new File(dir, type.get(t) + ext.get(e) + imageFormat)));
                add(new Image(imageName + t + e, file));
            }
        }
    }
}
