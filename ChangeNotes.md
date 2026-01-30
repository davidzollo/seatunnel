1、 日期：20160130 
原因：ST-3352【商业】【申万】ADB(pgsql)同步到Hologres(pgsql)时，选择copy方式导入时，数据导入格式不对源端是text类型 目标端是text类型，字段如果携带双引号那边写入sink会多出2个双引号，更换 jar 包解决.
更换包名：connector-jdbc-2.6.467-release.jar
