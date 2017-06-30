/**
 * 
 */
package io.mycat.config.loader.zkprocess.entity.server.user;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import io.mycat.config.loader.zkprocess.entity.Named;

/**
 * @author zhaoshan<br>
 * @version 1.0
 * 2017年6月30日 下午1:59:35<br>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "table", namespace = "privilege")
public class TablePrivilege implements Named{
    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute
    protected String dml;
    
    @Override
    public String getName() {
        return name;
    }

    public void setName(String name){
        this.name = name;
    }

    /**
     * @return the dml
     */
    public String getDml() {
        return dml;
    }

    /**
     * @param dml the dml to set
     */
    public void setDml(String dml) {
        this.dml = dml;
    }
    
    @Override
    public String toString() {
        return "Schema{" + "name='" + name + '\'' + ", dml=" + dml + '}';
    }
    
}
