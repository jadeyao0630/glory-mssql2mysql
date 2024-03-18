const express = require('express');
const app = express();
const path = require('path');
const http = require('http');
const server = http.createServer(app);
const dotenv = require('dotenv');
const { env } = process;
dotenv.config({
  path: path.resolve(
      __dirname,
      `./.env${process.env.NODE_ENV ? "."+process.env.NODE_ENV : ""}`
    ),
});
const DbService = require('./dbService');
const db= DbService.getDbServiceInstance();


var tasks={};
var isRunFullSync=false;
async function pollDatabaseChanges(table) {
    try {

      return await new Promise(async(resolve,reject)=>{
        EnableChangeTracking(table);
        const tableInfo=await GetTableInfo(table)
        //console.log('tableInfo',tableInfo);
        var _lastSyncVersion = await db.getLastUpdatedVersion(table);
        //console.log('_lastSyncVersion',_lastSyncVersion);
        //let pool = new mssql.ConnectionPool(mssqlConfig);
        //await pool.connect();

        
        // 示例查询: 替换为实际的变更检测逻辑
        let CURRENT_VERSION = await db.getCurrentUpdatedVersion();
        
        if (tableInfo.primaryKey.length===0) tableInfo.primaryKey=['ProjGUID'];
        console.log(tasks[table]+"--"+table+'_lastSyncVersion',_lastSyncVersion,CURRENT_VERSION,tableInfo.primaryKey);
        if (_lastSyncVersion < CURRENT_VERSION){
            
              if(tableInfo.primaryKey.length>0){
                const pk=tableInfo.primaryKey[0];
                let query='';
                const isFrist=_lastSyncVersion===-1
                if(isFrist){
                  isRunFullSync=true;
                  // Object.keys(tasks).forEach(task=>{
                  //   clearInterval(tasks[task]);
                  // })
                  
                  query = `SELECT * FROM dbo.${table}`;
                }else{
                  isRunFullSync=false;
                  query = `SELECT T.* FROM CHANGETABLE(CHANGES dbo.${table}, ${_lastSyncVersion}) AS CT JOIN dbo.${table} AS T ON T.${pk} = CT.${pk}`;
                }
                // 处理结果...
                
                let result=await db.mssqlGet(query);
                if(result.success){
                  let index = 0;
                  var waitTask=setInterval(async()=>{
                    console.log(table+"---->"+(Math.round(index/result.data.recordset.length*10000)/100)+"%");
                    //process.stdout.write('\r'+table+"---->"+(Math.round(index/result.data.recordset.length*10000)/100)+"% "+(index===result.data.recordset.length));
                    if(index===result.data.recordset.length){
                      
                      clearInterval(waitTask);
                      _lastSyncVersion=Number(CURRENT_VERSION);
                      await db.mysqlExcute("REPLACE INTO sync_version (table_name,last_sync_version) VALUES ('"+table+"',"+_lastSyncVersion+");");
                      if(isFrist){
                        //runTask();
                        isRunFullSync=false;
                        console.log(table+' has been synced...');
                      }
                      resolve(true)
                    }
                  },1000);
                  while(index < result.data.recordset.length){
                    const row = result.data.recordset[index];
                    //console.log(row['SYS_CHANGE_OPERATION'],row[pk],row); // Process each row here
                    const Keys=Object.keys(row);
                    const Values=[];
                    //console.log('tableInfo.columnTypes',tableInfo.columnTypes);
                    Keys.forEach(key=>{
                      if (tableInfo.columnTypes.hasOwnProperty(key)){
                        var value=row[key];
                        //console.log('value',value===null);
                        if(value===null) {Values.push('NULL');}
                        else{
                          value=value.constructor ===String?value.replaceAll("'","\\\'"):value;
                          switch (tableInfo.columnTypes[key].toLowerCase())
                          {
                              case "int":
                                  Values.push(value);
                                  break;
                              case "tinyint":
                                  Values.push(value);
                                  break;
                              case "text":
                                  Values.push("'" + value + "'");
                                  break;
                              case "money":
                                  Values.push(value);
                                  break;
                              case "datetime":
                                  Values.push("'" + formatDateTimeStr2Mysql(value) + "'");
                                  break;
                              case "timestamp":
                                  value = convertTimestampToHexString(value);
                                  Values.push("'" + value + "'");
                                  break;
                              case "uniqueidentifier":
                                  Values.push("'" + value + "'");
                                  break;
                              case "nvarchar":
                                  Values.push("'" + value + "'");
                                  break;
                              case "varchar":
                                  Values.push("'" + value + "'");
                                  break;
                              default:
                                  Values.push("'" + value + "'");
                                  break;
                          }
                        }
                        
                      }
                    })
                    var query_replaceinto=`REPLACE INTO ${table} (${Keys.join(',')}) VALUES (${Values.join(',')});`;
                    
                    //console.log(query_replaceinto);
                    var replaceIntoTask=await db.mysqlExcute(query_replaceinto);
                    if(!replaceIntoTask.success){
                      console.log(res);
                    }else{
                      if(!isFrist) 
                        console.log(`${table} ---> ${pk}=${row[pk]}`)
                    }
                    index++;
                  }
                  
                  
                }else{
                  console.log(result.data.message);
                  resolve(true)
                }
              
            }
            // 示例查询: 替换为实际的变更检测逻辑
            //var pks=await GetPrimaryKey(table,pool);
            //console.log('pks',pks)
            
            
            
            
            
            
        }else{
          resolve(true)
        }
      });
  
      // 同步到MySQL...
      //let mysqlConnection = await mysql.createConnection(mysqlConfig);
      // 使用result记录执行MySQL更新
      //await mysqlConnection.query('SELECT * FROM sync_version WHERE table_name = "'+table+'"');
      
      //pool.close();
      //mysqlConnection.end();
    } catch (err) {
      console.error('Database polling error:', err);
    }
  }
  //tasks=setInterval(listenChanges, 1000); // 每10秒检查一次
// query('select * from names', (err,results)=>{
//     console.log(results)
// });
const tables=['p_Room','p_project','s_Order','s_Contract','p_Building','cb_Product','myBusinessUnit','s_Getin','s_fee']
EnableChangeTracking();
runTask();
function runTask(){
  tables.forEach(async(table,index)=>{
    //setTimeout(async() => {
      // var wait=await setInterval(async()=>{
      //   //console.log(table,isRunFullSync)
      //   if(!isRunFullSync){
      //     isRunFullSync=true;

      //     clearInterval(wait)
          
          
      //   }
      // })
      //var isDone=await pollDatabaseChanges(table);
            if(await pollDatabaseChanges(table)){
              tasks[table]=setInterval(async()=>{
                await pollDatabaseChanges(table);
              }, 1000);
            }
    //}, index*1000);
    
  })
}
async function GetPrimaryKey(table){
  var pks=[];
  const query=`SELECT 
  k.COLUMN_NAME
FROM 
  INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
JOIN 
  INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
ON 
  t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
WHERE 
  t.CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND t.TABLE_NAME = '${table}'
  AND t.TABLE_SCHEMA = 'dbo';`
  let result = await db.mssqlGet(query);
            
    // 处理结果...
    if(result.success){
      //console.log(result);
      let index = 0;
      while(index < result.data.recordset.length){
        const row = result.data.recordset[index];
        const columnName=row["COLUMN_NAME"];
        //console.log(row['SYS_CHANGE_OPERATION'],row['RoomGUID']); // Process each row here
        //console.log('columnName',columnName);
        pks.push(columnName);
        index++;
      }
    }
    //console.log('GetPrimaryKey',result,pks);
    return pks;
}
async function GetTableInfo(table){
  var primaryKey=await GetPrimaryKey(table);
  
  //console.log('GetPrimaryKey',primaryKey);
  var createTableSql='';
  var query = `SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '${table}'`;
  let result = await db.mssqlGet(query);
  var mysqlDataTypes=[]
  var columnTypes={}
  //console.log('GetTableInfo',result,result.success && result.data.length>0);
  if(result.success && result.data.recordset.length>0){
    
    createTableSql = `CREATE TABLE IF NOT EXISTS ${table} (`;
    result.data.recordset.forEach(data => {
      var columnName = data["COLUMN_NAME"];
      var isPk = primaryKey.includes(columnName);
      var dataType=data["DATA_TYPE"];
      var charMaxLength=data["CHARACTER_MAXIMUM_LENGTH"]===null?"255":data["CHARACTER_MAXIMUM_LENGTH"];
      columnTypes[columnName]=dataType;
      mysqlDataTypes.push(columnName+" "+ConvertMssql2MysqlDataType(dataType, charMaxLength)+(isPk? " PRIMARY KEY" : ""));
    });
    createTableSql+=mysqlDataTypes.join(",")+") ROW_FORMAT=COMPRESSED;";
    //console.log(createTableSql);
    db.mysqlExcute(createTableSql).then((res)=>{
      if(!res.success) console.log(res)
    });
  }else{
    
    //console.log(result);
  }
  
  return {columnTypes:columnTypes,primaryKey:primaryKey};
}
function ConvertMssql2MysqlDataType(dataType,charMaxLength){
  switch (dataType.toLowerCase())
  {
      case "int":
          return "INT";
      case "tinyint":
          return "TINYINT";
      case "text":
          return "TEXT";
      case "money":
          return "DECIMAL(19, 4)";
      case "datetime":
          return "DATETIME";
      case "timestamp":
          return "VARCHAR(36)";
      case "uniqueidentifier":
          return "VARCHAR(36)";
      case "nvarchar":
          return "VARCHAR(" + charMaxLength + ") CHARACTER SET utf8mb4";
      case "varchar":
          return "VARCHAR("+ charMaxLength+")";
      // Add more conversions as needed
      default:
          return "VARCHAR("+ charMaxLength+")"; // Default conversion
  }
}
function convertTimestampToHexString(timestamp) {
  const sqlServerTimestamp = Buffer.from(timestamp);
  return sqlServerTimestamp.toString('hex');
}
function EnableChangeTracking(table=null){
  //console.log('EnableChangeTracking',table===null,env.MS_DATABASE,process.env.MS_DATABASE);
  var query=table===null?"ALTER DATABASE "+ env.MS_DATABASE + " SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);":
  "ALTER TABLE dbo." + table + " ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);";
  //console.log('EnableChangeTracking',query);
  db.mssqlExcute(query).then((res)=>{
    if(!res.success){
      //console.log('enable change tracking on '+(table===null?env.MS_DATABASE:table)+ " failed.");
      //console.log('enable change tracking on '+(table===null?env.MS_DATABASE:table)+ " failed.",res.data);
    }
    
  });
}
function formatDateTimeStr2Mysql(dateTimeStr){
  var datetime=new Date(dateTimeStr).toLocaleString();
  //datetime=formatAMPM()
  return datetime.substr(0,20);
}
server.listen(env.PORT, () => console.log('app is runing at port: '+env.PORT,'mysql host: '+env.HOST))


