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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
					
					//更新累计统计信息
					updateTotalStat(violation.getMessage());
					
					//更新每日统计信息
					updateDayStat(violation.getMessage());
					
					//记录日志详情信息 
					logAudit(c, violation.getMessage());
				}
				//公共
	            c.writeErrMessage(ErrorCode.ERR_WRONG_USED, violation.getMessage());
	            return false;
	        }
		}
		return true;
	}
	
	/**
	 * 更新累计统计信息 
	 * @param errMsg
	 */
	private static void updateTotalStat(String errMsg){
		if(errMsg == null || errMsg.isEmpty()){
			return;
		}
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
		if(statMap.get(errMsg) == null){
			statMap.put(errMsg, 1);
		}else{
			statMap.put(errMsg, Integer.parseInt(String.valueOf(statMap.get(errMsg))) + 1);
		}
		String newFileContent = JSONUtils.toJSONString(statMap);
		newFileContent = newFileContent + System.lineSeparator();
		string2File(newFileContent, statFile, false);
	}
	
	/**
	 * 更新每日统计信息
	 * @param errMsg
	 */
	private static void updateDayStat(String errMsg){
		File statDayFile = new File(logPath + "stat_day.log");
		if(!statDayFile.exists()){
			try {
				//先创建目录
				if(! statDayFile.getParentFile().exists()){
					statDayFile.getParentFile().mkdirs();
				}
				//后创建文件
				statDayFile.createNewFile();
			} catch (IOException e) {
				logger.error("create file stat_day.log error.", e);
			}
		}
		String statDayFileContent = file2String(statDayFile);
		Map<String,Object> statDayMap = null;
		if(statDayFileContent == null || statDayFileContent.isEmpty()){
			statDayMap = new HashMap();
		}else{
			statDayMap = (Map)JSONUtils.parse(statDayFileContent);
		}
		//新增或更新当前天的统计数据
		String currentDateStr = getDateStr();
		if(statDayMap.get(currentDateStr) == null){
			Map localStatMap = new HashMap();
			localStatMap.put(errMsg, 1);
			statDayMap.put(currentDateStr, localStatMap);
		}else{
			Map localStatMap = (Map)statDayMap.get(currentDateStr);
			if(localStatMap.get(errMsg) == null){
				localStatMap.put(errMsg, 1);
			}else{
				localStatMap.put(errMsg, Integer.parseInt(String.valueOf(localStatMap.get(errMsg))) + 1);
			}
		}
		//删除6个月之前的统计数据
		long currentTimeMillis = getTimeInMillis(currentDateStr,"yyyy-MM-dd");
		List<String> removeKey = new ArrayList<String>(); 
		for(String dateStr : statDayMap.keySet()){  
			//((long)180)*24*3600*1000 的值已超过int最大值，所以需要将第一个乘数转为long
			if(currentTimeMillis - getTimeInMillis(dateStr,"yyyy-MM-dd") > ((long)180)*24*3600*1000){
				removeKey.add(dateStr);
			}
        }  
		for(String key : removeKey){
			statDayMap.remove(key);
		}
		
		//将每日统计信息更新到文件系统
		String newDayFileContent = JSONUtils.toJSONString(statDayMap);
		newDayFileContent = newDayFileContent + System.lineSeparator();
		string2File(newDayFileContent, statDayFile, false);
	}
	
	/**
	 * 记录日志详情
	 * @param c
	 * @param errMsg
	 */
	private static void logAudit(ServerConnection c,String errMsg){
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
		audit.put("sql", c.getExecuteSql());
		audit.put("message", errMsg);
		String newAudit = JSONUtils.toJSONString(audit);
		newAudit = newAudit + System.lineSeparator();
		string2File(newAudit, auditFile, true);
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
	//yyyy-MM-dd HH:mm:ss
	public static String getDateTimeStr() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		String res = sdf.format(new Date());
		return res;
	}
	//yyyy-MM-dd 
	public static String getDateStr() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		String res = sdf.format(new Date());
		return res;
	}
	
	public static long getTimeInMillis(String dateStr,String format){
		if(format == null || format.isEmpty()){
			format = "yyyy-MM-dd";
		}
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		Date date = null;
		try {
			date = sdf.parse(dateStr);
			return date.getTime();
		} catch (ParseException e) {
			throw new RuntimeException("日期时间格式化错误",e);
		}
	}
	
}