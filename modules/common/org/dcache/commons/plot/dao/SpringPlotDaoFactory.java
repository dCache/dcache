package org.dcache.commons.plot.dao;

import org.dcache.commons.plot.ParamPlotType;
import org.dcache.commons.plot.PlotException;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

/**
 * default Dao factory for getting DAO (Data Access Object) from xml bean file
 * @author timur and tao
 */
public class SpringPlotDaoFactory extends PlotDaoFactory {

    private String xmlBeanFile =
            System.getProperty(
            "org.dcache.commons.plot.dao.SpringPlotDaoFactory.configuration",
            "org/dcache/commons/plot/PlotConfiguration.xml");
    private XmlBeanFactory beanFactory = null;

    public String getXmlBeanFile() {
        return xmlBeanFile;
    }

    public void setXmlBeanFile(String xmlBeanFile) throws PlotException {
        this.xmlBeanFile = xmlBeanFile;
        try {
            beanFactory = new XmlBeanFactory(new ClassPathResource(xmlBeanFile));
        } catch (Exception e) {
            throw new PlotException("failed in setting xml file: " + e, e);
        }
    }

    public PlotDao getPlotDao(ParamPlotType paramPlotType) {
        if (beanFactory == null) {
            beanFactory = new XmlBeanFactory(new ClassPathResource(xmlBeanFile));
        }
        return (PlotDao) beanFactory.getBean(paramPlotType.getType(),
                PlotDao.class);
    }
}
