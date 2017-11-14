package io.mycat.server.parser;

import java.util.Map;

import io.mycat.MycatServer;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.server.ServerConnection;
import io.mycat.server.util.SchemaUtil;

//import io.mycat.server.ServerConnection;

public class ServerParseDDL {
	
	private static int offset_total;


	public static void check(String sql,ServerConnection c,int offset) {
		String tableName = null;
		String columnName = null;
		
		 offset_total=offset;//其实就是0
		 
		 offset_total=alterCheck(sql,offset_total);
		 if(offset_total==-1) {
				c.execute(sql, ServerParse.DDL);
				return ;
				}
			 
		offset_total=get(sql,offset_total);
		if(offset_total==-1) {
			c.execute(sql, ServerParse.DDL);
			return ;
			}
	     offset_total=tableCheck(sql,offset_total);
		if(offset_total!=-1) {
			tableName=gettabName(sql,offset_total);
	        tableName=tableName.toLowerCase();
		}else {
			c.execute(sql, ServerParse.DDL);
			return ;
		}
		offset_total=get(sql,offset_total);
		if(offset_total==-1) {
			c.execute(sql, ServerParse.DDL);
			return ;
			}
		boolean flg=columnParse(sql,offset_total);
		if(flg==true) {
			columnName=getcolName(sql,offset_total);
            columnName=columnName.toLowerCase();
			
		}
		else {
			c.execute(sql, ServerParse.DDL);
			return ;
		}
//在这之上是对sql语句的拦截，分析，获取表名和列名。在这之后是对schema中表信息的获取，表对应的分片字段，主键等。
        String showSchemal= SchemaUtil.parseShowTableSchema(sql) ;
        String cSchema =showSchemal==null? c.getSchema():showSchemal;
        SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(cSchema);
        if(schema == null) {
        	 c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,"No database selected");
             return ;
            }
        Map<String, TableConfig> tables=schema.getTables();
        for (TableConfig tablekey : tables.values()) {
        String tabname=tablekey.getName();
        tabname=tabname.toLowerCase();

        if(tabname.equals(tableName)) {
            String tabPrimaryKey=tablekey.getPrimaryKey();
            if(tabPrimaryKey!=null) {tabPrimaryKey=tabPrimaryKey.toLowerCase();}

            String tabjoinKey=tablekey.getJoinKey();
            if(tabjoinKey!=null) {tabjoinKey=tabjoinKey.toLowerCase();}
            
            String tabparentKey=tablekey.getParentKey();
            if(tabparentKey!=null) {tabparentKey=tabparentKey.toLowerCase();}
            
            String tabRuleColumn = null;
            RuleConfig rule=tablekey.getRule();
            if(rule!=null) { 
                tabRuleColumn=rule.getColumn();
            	if(tabRuleColumn!=null) {tabRuleColumn=tabRuleColumn.toLowerCase();}         
            }
                      
			if((columnName.equals(tabPrimaryKey))||(columnName.equals(tabjoinKey))//判断所要操作的列是否是主键，分片字段等
            		||(columnName.equals(tabparentKey))||(columnName.equals(tabRuleColumn))) {
            	c.writeErrMessage(ErrorCode.ER_NO_USEKEY_ERROR,"Alter column cannot be a primarykey or joinkey or parentkey or rulecolumn.");
            	return;
            }
        }
     }
    c.execute(sql, ServerParse.DDL);      
}
	
/**
 * 判断这个sql语句是否是alter操作。
 * @param sql
 * @param offset
 * @return
 */
	private static int alterCheck(String sql, int offset) {
		if (sql.length() > offset + 4) {
			char c1 = sql.charAt(++offset);
			char c2 = sql.charAt(++offset);
			char c3 = sql.charAt(++offset);
			char c4 = sql.charAt(++offset);
			char c5 = sql.charAt(++offset);
			if ((c1 == 'L' || c1 == 'l')
					&& (c2 == 'T' || c2 == 't')
					&& (c3 == 'E' || c3 == 'e')
					&& (c4 == 'R' || c4 == 'r')
					&& (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
				return offset;
			}
		}
		return -1;
	}

/**
 * 判断sql语句中alter关键字之后是否是table关键字
 * @param sql
 * @param offset
 * @return
 */
	public static int tableCheck(String sql,int offset) {
		if(sql.length()>offset+5) {
			char c0 =sql.charAt(offset);
			char c1 = sql.charAt(++offset);
			char c2 = sql.charAt(++offset);
			char c3 = sql.charAt(++offset);
			char c4 = sql.charAt(++offset);
			char c5 = sql.charAt(++offset);
			if ((c0 == 'T' || c0 == 't')
					&&(c1 == 'A' || c1 == 'a')
					&& (c2 == 'B' || c2 == 'b')
					&& (c3 == 'L' || c3 == 'l')
					&& (c4 == 'E' || c4 == 'e')
					&& (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
			  return offset;
			}
		}
		return -1;
	}


/**
 * 获取alter操作的表名
 * @param sql
 * @param offset
 * @return
 */
	private static String gettabName(String sql, int offset) {
		// TODO Auto-generated method stub
		String tabName="";
		offset_total=get(sql,offset);
		
		for(int i=offset_total;i<sql.length();i++) {	
			char ch=sql.charAt(i);
			if(ch== ' ' || ch == '\t' || ch == '\r' || ch == '\n') {offset_total=i;return tabName;}
			else {tabName=tabName+ch;}
		}
		
		return tabName;
	}

/**
 * 判断alter table之后列操作：add，change，drop？
 * @param sql
 * @param offset
 * @return
 */
	private static boolean columnParse(String sql,int offset) {
		boolean rt=false;
		switch (sql.charAt(offset)) {
			case 'A':
			case 'a':
				rt = addCheck(sql, offset);
				return rt;
			case 'C':
			case 'c':
				rt = changeCheck(sql, offset);
				return rt;
			case 'D':
			case 'd':
				rt = dropCheck(sql, offset);
				return rt;
			default:
				return rt;
			
	}
}

/**
 * 
 * @param sql
 * @param offset
 * @return 获取change，add，drop后面的列名
 */

	private static String getcolName(String sql, int offset) {
			// TODO Auto-generated method stub
			String colName="";
			int jj=get(sql,offset);
			int jjj=jj;
			if(sql.charAt(jjj)=='C'||sql.charAt(jjj)=='c')
			{
				if (sql.length() > jjj + 5) {
					char ch1=sql.charAt(++jjj);
					char ch2=sql.charAt(++jjj);
					char ch3=sql.charAt(++jjj);
					char ch4=sql.charAt(++jjj);
					char ch5=sql.charAt(++jjj);
					char ch6=sql.charAt(++jjj);
					if((ch1=='O'||ch1=='o')
							&&(ch2=='L'||ch2=='l')
							&&(ch3=='U'||ch3=='u')
							&&(ch4=='M'||ch4=='m')
							&&(ch5=='N'||ch5=='n')
							&&(ch6==' ' || ch6 == '\t' || ch6 == '\r' || ch6 == '\n')) {
						jj=get(sql,jjj);
					}
				}
				
			}
				
			for(int i=jj;i<sql.length();i++) {	
				char ch=sql.charAt(i);
				if(ch== ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch ==';') {return colName;}
				else {colName=colName+ch;}
		}
			return colName;
		}

/**
 * 
 * @param sql
 * @param offset
 * @return 判断是否为drop操作
 */
	private static boolean dropCheck(String sql, int offset) {
		// TODO Auto-generated method stub
		if (sql.length() > offset + 3) {
			char c1 = sql.charAt(++offset);
			char c2 = sql.charAt(++offset);
			char c3 = sql.charAt(++offset);
			char c4 = sql.charAt(++offset);
			if ((c1 == 'R' || c1 == 'r')
				&& (c2 == 'O' || c2 == 'o')
				&& (c3 == 'P' || c3 == 'p')
				&& (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) 
			{
				offset_total=offset; 
				return true;
			}
		}
		return false;
	}

/**
 * 
 * @param sql
 * @param offset
 * @return 判断是否是change操作
 */
	private static boolean changeCheck(String sql, int offset) {
		// TODO Auto-generated method stub
		if (sql.length() > offset + 5) {
			char c1 = sql.charAt(++offset);
			char c2 = sql.charAt(++offset);
			char c3 = sql.charAt(++offset);
			char c4 = sql.charAt(++offset);
			char c5 = sql.charAt(++offset);
			char c6 = sql.charAt(++offset);
			if ((c1 == 'H' || c1 == 'h')
				&& (c2 == 'A' || c2 == 'a')
				&& (c3 == 'N' || c3 == 'n')
				&& (c4 == 'G' || c4 == 'g')
				&& (c5 == 'E' || c5 == 'e')
				&& (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {offset_total=offset; return true;}
		}
		return false;
	}

/**
 * 
 * @param sql
 * @param offset
 * @return 判断是否是add操作。
 */
	private static boolean addCheck(String sql, int offset) {
		// TODO Auto-generated method stub
		if (sql.length() > offset + 2) {
			char c1 = sql.charAt(++offset);
			char c2 = sql.charAt(++offset);
			char c3 = sql.charAt(++offset);
			if ((c1 == 'D' || c1 == 'd')
				&& (c2 == 'D' || c2 == 'd')
				&& (c3 == ' ' || c3 == '\t' || c3 == '\r' || c3 == '\n')) {offset_total=offset; return true;}
		}
		return false;
	}
	
	/**
	 * 跳过sql语句中的空格、空白等字符。	
	 * @param sql
	 * @param offset
	 * @return
	 */
	public static int get(String sql,int offset) {
			for(int j=offset;j<sql.length();j++) {
				switch (sql.charAt(j)) {
				case ' ':
				case '\t':
				case '\r':
				case '\n':
					continue;
				default:
				  return j;
					}
				
			}
			return 0;
		}

	
}
