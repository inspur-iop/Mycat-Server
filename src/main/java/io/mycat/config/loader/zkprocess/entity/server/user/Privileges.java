/**
 * 
 */
package io.mycat.config.loader.zkprocess.entity.server.user;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

/**
 * @author zhaoshan<br>
 * @version 1.0
 * 2017年6月30日 下午1:55:11<br>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "privileges")
public class Privileges {
    @XmlAttribute
    protected String check;
    
    protected List<SchemaPrivilege> schema;

    /**
     * @return the check
     */
    public String getCheck() {
        return check;
    }

    /**
     * @param check the check to set
     */
    public void setCheck(String check) {
        this.check = check;
    }

    /**
     * @return the schemaPrivileges
     */
    public List<SchemaPrivilege> getSchema() {
        return schema;
    }

    /**
     * @param schemaPrivileges the schemaPrivileges to set
     */
    public void setSchema(List<SchemaPrivilege> schema) {
        this.schema = schema;
    }
    
    @Override
    public String toString() {
        return "Privileges{" + "check='" + check + '\'' + ", schema=" + schema + '}';
    }
}
