package org.dcache.commons.plot.dao;

import org.dcache.commons.plot.ParamPlotType;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

/**
 * default Dao factory for getting DAO (Data Access Object) from xml bean file
 * @author timur and tao
 */
public class SpringPlotDaoFactory extends PlotDaoFactory {

    private String xmlBeanFile =
            System.getProperty(
            "org.dcache.commons.plot.dao.SpringPlotDaoFactory.configuration",
            System.getProperty("dcache.home", "/opt/d-cache") + "/etc/PlotConfiguration.xml");
    private XmlBeanFactory beanFactory = null;

    public String getXmlBeanFile() {
        return xmlBeanFile;
    }

    public void setXmlBeanFile(String xmlBeanFile){
        this.xmlBeanFile = xmlBeanFile;
    }

    public PlotDao getPlotDao(ParamPlotType paramPlotType) {
        if (beanFactory == null) {
            beanFactory = new XmlBeanFactory(new FileSystemResource(xmlBeanFile));
        }
        return (PlotDao) beanFactory.getBean(paramPlotType.getType(),
                PlotDao.class);
    }
}
