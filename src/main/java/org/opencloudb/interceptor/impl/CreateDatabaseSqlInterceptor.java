package org.opencloudb.interceptor.impl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.interceptor.SQLInterceptor;

public class CreateDatabaseSqlInterceptor implements SQLInterceptor {
	private static final Logger LOGGER = Logger
			.getLogger(CreateDatabaseSqlInterceptor.class);

	private final class CreateDatabaseSqlRunner implements Runnable {

		private int sqltype = 0;
		private String sqls = "";

		public CreateDatabaseSqlRunner(int sqltype, String sqls) {
			this.sqltype = sqltype;
			this.sqls = sqls;
		}

		public void run() {
			try {
				SystemConfig sysconfig = MycatServer.getInstance().getConfig()
						.getSystem();
				String sqlInterceptorType = sysconfig.getDefaultSqlParser();
				if (sqlInterceptorType.equals("createDatabase")) {
					sqls = sqls.trim().toUpperCase();
					Pattern p = Pattern.compile("(CREATE\\s+DATABASE)");
					Matcher m = p.matcher(sqls);
					while (m.find()) {
						try {
							throw new Exception("不支持创建数据库！！");
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

			} catch (Exception e) {
				LOGGER.error("interceptSQL error:" + e.getMessage());
			}
		}
	}

	/**
	 * escape mysql create database etc
	 */
	@Override
	public String interceptSQL(String sql, int sqlType) {
		// other interceptors put in here ....
		LOGGER.debug("sql interceptSQL:");
		final int sqltype = sqlType;
		final String sqls = DefaultSqlInterceptor.processEscape(sql);
		MycatServer.getInstance().getBusinessExecutor()
				.execute(new CreateDatabaseSqlRunner(sqltype, sqls));
		return sql;
	}

}
