/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.server.handler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.QuarantineConfig;
import org.opencloudb.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.support.json.JSONUtils;
import com.alibaba.druid.wall.Violation;
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.violation.IllegalSQLObjectViolation;
import com.alibaba.druid.wall.violation.SyntaxErrorViolation;

/**
 * @author songwie
 */
public final class QuarantineHandler {

	private static Logger logger = LoggerFactory.getLogger(QuarantineHandler.class);
	private static boolean check = false;
	private static final String logPath = "/var/log/mycat/";
	
	private final static ThreadLocal<WallProvider> contextLocal = new ThreadLocal<WallProvider>();
	
	public static boolean handle(String sql, ServerConnection c) {
		if(contextLocal.get()==null){
			QuarantineConfig quarantineConfig = MycatServer.getInstance().getConfig().getQuarantine();
            if(quarantineConfig!=null){
            	if(quarantineConfig.isCheck()){
            	   contextLocal.set(quarantineConfig.getProvider());
            	   check = true;
            	}
            }
		}
		if(check){
			WallCheckResult result = contextLocal.get().check(sql);
			if (!result.getViolations().isEmpty()) {
				Violation violation = result.getViolations().get(0);
				
				//Violation有2个实现类：SyntaxErrorViolation 和 IllegalSQLObjectViolation
				//此处只统计和记录IllegalSQLObjectViolation类型的。
				if(violation instanceof IllegalSQLObjectViolation){
					
					//更新统计信息
					File statFile = new File(logPath + "stat.log");
					if(!statFile.exists()){
						try {
							//先创建目录
							if(! statFile.getParentFile().exists()){
								statFile.getParentFile().mkdirs();
							}
							//后创建文件
							statFile.createNewFile();
						} catch (IOException e) {
							logger.error("create file stat.log error.", e);
						}
					}
					String fileContent = file2String(statFile);
					Map statMap = null;
					if(fileContent == null || fileContent.isEmpty()){
						statMap = new HashMap();
					}else{
						statMap = (Map)JSONUtils.parse(fileContent);
					}
					if(statMap.get(violation.getMessage()) == null){
						statMap.put(violation.getMessage(), 1);
					}else{
						statMap.put(violation.getMessage(), Integer.parseInt(String.valueOf(statMap.get(violation.getMessage()))) + 1);
					}
					String newFileContent = JSONUtils.toJSONString(statMap);
					string2File(newFileContent, statFile, false);
					
					//记录日志信息 
					File auditFile = new File(logPath + "audit.log");
					if(!auditFile.exists()){
						try {
							//先创建目录
							if(! auditFile.getParentFile().exists()){
								auditFile.getParentFile().mkdirs();
							}
							//后创建文件
							auditFile.createNewFile();
						} catch (IOException e) {
							logger.error("create file audit.log error.", e);
						}
					}
					Map audit = new HashMap();
					audit.put("date", getDateTimeStr());
					audit.put("user", c.getUser());
					audit.put("host", c.getHost());
					audit.put("schema", c.getSchema());
					audit.put("sql", sql);
					audit.put("message", violation.getMessage());
					String newAudit = JSONUtils.toJSONString(audit);
					string2File(newAudit, auditFile, true);
				}
				//公共
	            c.writeErrMessage(ErrorCode.ERR_WRONG_USED, violation.getMessage());
	            return false;
	        }
		}
		return true;
	}
	public static String file2String(File file) {
		StringBuilder result = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String s = null;
			while ((s = br.readLine()) != null) {
				result.append(System.lineSeparator() + s);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("read file " + file.getName() + " error.", e);
		}
		return result.toString();
	}
	public static void string2File(String str, File file,boolean isAppend) {
		FileWriter fw = null;
		try {
			fw = new FileWriter(file,isAppend);
			fw.write(str);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static String getDateTimeStr() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		String res = sdf.format(new Date());
		return res;
	}
}