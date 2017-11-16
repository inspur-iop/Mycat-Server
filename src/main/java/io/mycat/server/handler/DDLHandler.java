/**
 * 
 */
package io.mycat.server.handler;

import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParseDDL;
import io.mycat.util.StringUtil;

/**
 * @author zhaoshan<br>
 * @version 1.0
 * 2017年11月1日 下午5:14:53<br>
 */
public final class DDLHandler {

	/**
	 * DDL语句处理
	 * 参考 https://dev.mysql.com/doc/refman/5.7/en/sql-syntax-data-definition.html
	 * @param sql
	 * @param c
	 */
	public static void handle(String sql, ServerConnection c, int rs) {
		if(StringUtil.isEmpty(sql)) {
			return;
		}
		switch (sql.charAt(0)) {
		case 'C':
		case 'c':
			//TODO: CREATE 
//						DATABASE 
//						EVENT 
//						FUNCTION 
//						INDEX 
//						LOGFILE 拦截
//						PROCEDURE 
//						SERVER 拦截
//						TABLE 
//						TABLESPACE
//						TRIGGER 
//						VIEW 
			
			break;
		case 'A':
		case 'a':
			//TODO: ALTER
			ServerParseDDL.checkAlterSQL(sql,c,rs);
			break;
		case 'D':
		case 'd':
			//TODO: DROP
//			break;
		case 'T':
		case 't':
			//TODO: TRUNCATE
//			break;
		case 'R':
		case 'r':
			//TODO: RENAME
//			break;
		default:
			c.execute(sql, rs & 0xff);
			break;
		}
		
	}
	
	
}
