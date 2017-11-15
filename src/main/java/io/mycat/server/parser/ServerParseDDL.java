package io.mycat.server.parser;

import java.util.HashMap;
import java.util.Map;

import io.mycat.MycatServer;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.server.ServerConnection;
import io.mycat.server.util.SchemaUtil;
import io.mycat.util.StringUtil;

public class ServerParseDDL {

	private static int offset_total;

	public static void checkAlterSQL(String sql, ServerConnection c, int offset) {

		String tableName = null;
		String columnName = null;
		offset_total = offset;// 其实就是0
		Map<String,Object> map= parseSQL(sql, c);
		if ((boolean) map.get("flag")== false) {
			c.execute(sql, ServerParse.DDL);
			return;
		}else {
			tableName=(String) map.get("tableName");
			columnName=(String) map.get("colName");
		}
		String showSchema = SchemaUtil.parseShowTableSchema(sql);
		String SchemaName = showSchema == null ? c.getSchema() : showSchema;
		SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(SchemaName);
		if (schema == null) {
			c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
			return;
		}
		Map<String, TableConfig> tables = schema.getTables();
		for (TableConfig tablekey : tables.values()) {
			String tabName = tablekey.getName();
			if (StringUtil.equalsIgnoreCase(tabName, tableName)) {
				String tabPrimaryKey = tablekey.getPrimaryKey();
				String tabJoinKey = tablekey.getJoinKey();
				String tabParentKey = tablekey.getParentKey();

				String tabRuleColumn = null;
				RuleConfig rule = tablekey.getRule();
				if (rule != null) {
					tabRuleColumn = rule.getColumn();
				}

				if (StringUtil.equalsIgnoreCase(columnName, tabPrimaryKey)// 判断所要操作的列是否是主键，分片字段等。
						|| StringUtil.equalsIgnoreCase(columnName, tabJoinKey)
						|| StringUtil.equalsIgnoreCase(columnName, tabParentKey)
						|| StringUtil.equalsIgnoreCase(columnName, tabRuleColumn)) {
					c.writeErrMessage(ErrorCode.ER_CANT_ALTER_COLUMN_ERROR,
							"Alter column cannot be a primary key or join key or parent key or rule column.");
					return;
				}
			}
		}
		c.execute(sql, ServerParse.DDL);
	}

	/*
	 * 判断这个sql语句的正确性和获取alter的表名和列名。
	 * 
	 * @param sql
	 * 
	 * @param c
	 * 
	 * @return
	 */
	private static Map<String,Object> parseSQL(String sql, ServerConnection c) {
		Map<String,Object> params= new HashMap<String,Object>();
		String tabName = null;
		String colName = null;
		boolean flag_golal = false;
		offset_total = alterCheck(sql, offset_total);
		if (offset_total == -1) {
			flag_golal=false;
		}

		offset_total = get(sql, offset_total);
		if (offset_total == -1) {
			flag_golal=false;
		}
		offset_total = tableCheck(sql, offset_total);
		if (offset_total != -1) {
			tabName = gettabName(sql, offset_total);
			flag_golal=true;
		} else {
			flag_golal=false;
			params.put("flag",flag_golal);
			params.put("tableName", tabName);
			params.put("colName", colName);
			return params;
		}
		offset_total = get(sql, offset_total);
		if (offset_total == -1) {
			flag_golal=false;
		}
		boolean flg = columnParse(sql, offset_total);
		if (flg) {
			colName = getcolName(sql, offset_total);
			flag_golal=true;

		} else {
			flag_golal=false;
		}
		params.put("flag",flag_golal);
		params.put("tableName", tabName);
		params.put("colName", colName);
		return params;
	}

	/*
	 * 判断这个sql语句是否是alter操作。
	 * 
	 * @param sql
	 * 
	 * @param offset
	 * 
	 * @return
	 */
	private static int alterCheck(String sql, int offset) {
		if (sql.length() > offset + 4) {
			char c1 = sql.charAt(++offset);
			char c2 = sql.charAt(++offset);
			char c3 = sql.charAt(++offset);
			char c4 = sql.charAt(++offset);
			char c5 = sql.charAt(++offset);
			if ((c1 == 'L' || c1 == 'l') && (c2 == 'T' || c2 == 't') && (c3 == 'E' || c3 == 'e')
					&& (c4 == 'R' || c4 == 'r') && (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
				return offset;
			}
		}
		return -1;
	}

	/*
	 * 判断sql语句中alter关键字之后是否是table关键字。
	 * 
	 * @param sql
	 * 
	 * @param offset
	 * 
	 * @return
	 */
	private static int tableCheck(String sql, int offset) {
		if (sql.length() > offset + 5) {
			char c0 = sql.charAt(offset);
			char c1 = sql.charAt(++offset);
			char c2 = sql.charAt(++offset);
			char c3 = sql.charAt(++offset);
			char c4 = sql.charAt(++offset);
			char c5 = sql.charAt(++offset);
			if ((c0 == 'T' || c0 == 't') && (c1 == 'A' || c1 == 'a') && (c2 == 'B' || c2 == 'b')
					&& (c3 == 'L' || c3 == 'l') && (c4 == 'E' || c4 == 'e')
					&& (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
				return offset;
			}
		}
		return -1;
	}

	/*
	 * 获取alter操作的表名。
	 * 
	 * @param sql
	 * 
	 * @param offset
	 * 
	 * @return
	 */
	private static String gettabName(String sql, int offset) {
		// TODO Auto-generated method stub
		String tabName = "";
		offset_total = get(sql, offset);

		for (int i = offset_total; i < sql.length(); i++) {
			char ch = sql.charAt(i);
			if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n') {
				offset_total = i;
				return tabName;
			} else {
				tabName = tabName + ch;
			}
		}

		return tabName;
	}

	/*
	 * 判断alter table之后列操作：add，change，drop？
	 * 
	 * @param sql
	 * 
	 * @param offset
	 * 
	 * @return
	 */
	private static boolean columnParse(String sql, int offset) {
		boolean rt = false;
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

	/*
	 * 获取change，add，drop后面的列名。
	 * 
	 * @param sql
	 * 
	 * @param offset
	 * 
	 * @return
	 */

	private static String getcolName(String sql, int offset) {
		// TODO Auto-generated method stub
		String colName = "";
		offset = get(sql, offset);
		int temp=offset;
		if (sql.charAt(temp) == 'C' || sql.charAt(temp) == 'c') {
			if (sql.length() > temp + 5) {
				char c1 = sql.charAt(++temp);
				char c2 = sql.charAt(++temp);
				char c3 = sql.charAt(++temp);
				char c4 = sql.charAt(++temp);
				char c5 = sql.charAt(++temp);
				char c6 = sql.charAt(++temp);
				if ((c1 == 'O' || c1 == 'o') && (c2 == 'L' || c2 == 'l') && (c3 == 'U' || c3 == 'u')
						&& (c4 == 'M' || c4 == 'm') && (c5 == 'N' || c5 == 'n')
						&& (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
					offset = get(sql, temp);
				}
			}

		}

		for (int i = offset; i < sql.length(); i++) {
			char ch = sql.charAt(i);
			if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == ';') {
				return colName;
			} else {
				colName = colName + ch;
			}
		}
		return colName;
	}

	/*
	 * 判断是否为drop操作。
	 * 
	 * @param sql
	 * 
	 * @param offset
	 * 
	 * @return
	 */
	private static boolean dropCheck(String sql, int offset) {
		// TODO Auto-generated method stub
		if (sql.length() > offset + 3) {
			char c1 = sql.charAt(++offset);
			char c2 = sql.charAt(++offset);
			char c3 = sql.charAt(++offset);
			char c4 = sql.charAt(++offset);
			if ((c1 == 'R' || c1 == 'r') && (c2 == 'O' || c2 == 'o') && (c3 == 'P' || c3 == 'p')
					&& (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
				offset_total = offset;
				return true;
			}
		}
		return false;
	}

	/*
	 * 判断是否是change操作。
	 * 
	 * @param sql
	 * 
	 * @param offset
	 * 
	 * @return
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
			if ((c1 == 'H' || c1 == 'h') && (c2 == 'A' || c2 == 'a') && (c3 == 'N' || c3 == 'n')
					&& (c4 == 'G' || c4 == 'g') && (c5 == 'E' || c5 == 'e')
					&& (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
				offset_total = offset;
				return true;
			}
		}
		return false;
	}

	/*
	 * 判断是否是add操作。
	 * 
	 * @param sql
	 * 
	 * @param offset
	 * 
	 * @return
	 */
	private static boolean addCheck(String sql, int offset) {
		// TODO Auto-generated method stub
		if (sql.length() > offset + 2) {
			char c1 = sql.charAt(++offset);
			char c2 = sql.charAt(++offset);
			char c3 = sql.charAt(++offset);
			if ((c1 == 'D' || c1 == 'd') && (c2 == 'D' || c2 == 'd')
					&& (c3 == ' ' || c3 == '\t' || c3 == '\r' || c3 == '\n')) {
				offset_total = offset;
				return true;
			}
		}
		return false;
	}

	/*
	 * 跳过sql语句中的空格、空白等字符。
	 * 
	 * @param sql
	 * 
	 * @param offset
	 * 
	 * @return
	 */
	public static int get(String sql, int offset) {
		for (int i = offset; i < sql.length(); i++) {
			switch (sql.charAt(i)) {
			case ' ':
			case '\t':
			case '\r':
			case '\n':
				continue;
			default:
				return i;
			}
		}
		return -1;
	}
}
