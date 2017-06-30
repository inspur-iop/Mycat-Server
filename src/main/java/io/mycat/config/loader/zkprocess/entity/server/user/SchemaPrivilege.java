/**
 * 
 */
package io.mycat.config.loader.zkprocess.entity.server.user;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import io.mycat.config.loader.zkprocess.entity.Named;

/**
 * @author zhaoshan<br>
 * @version 1.0
 * 2017年6月30日 下午1:59:19<br>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "schema", namespace="privileges")
public class SchemaPrivilege implements Named{
    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute
    protected String dml;
    protected List<TablePrivilege> table;
    
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

    /**
     * @return the tablePrivileges
     */
    public List<TablePrivilege> getTable() {
        return table;
    }

    /**
     * @param tablePrivileges the tablePrivileges to set
     */
    public void setTable(List<TablePrivilege> table) {
        this.table = table;
    }
    
    @Override
    public String toString() {
        return "Schema{" + "name='" + name + '\'' + ", dml=" + dml + ", table=" + table +'}';
    }

}
