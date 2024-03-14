const express = require('express');
const app = express();
const path = require('path');
const http = require('http');
const mssql = require('mssql');
const server = http.createServer(app);
require('dotenv').config({
    path: path.resolve(
        __dirname,
        `./.env`
    ),
});
const { env } = process;
const DbService = require('./dbService');
const db= DbService.getDbServiceInstance();

const mssqlConfig = {
    user: env.MS_USER,
    password: env.MS_PASSWORD,
    server: env.MS_HOST,
    database: env.MS_DATABASE,
    options: {
        encrypt: false,
      //encrypt: true, // for azure
      //trustServerCertificate: true // for self-signed certificates
    }
  };

async function pollDatabaseChanges() {
    try {
      var table='p_Room';
      var _lastSyncVersion = await db.getLastUpdatedVersion(table);
      console.log('_lastSyncVersion',_lastSyncVersion);
      let pool = new mssql.ConnectionPool(mssqlConfig);
      await pool.connect();

      
      // 示例查询: 替换为实际的变更检测逻辑
      let CURRENT_VERSION = await db.getCurrentUpdatedVersion();
      
      console.log('_lastSyncVersion',_lastSyncVersion,CURRENT_VERSION);


      if (_lastSyncVersion < CURRENT_VERSION){
          // 示例查询: 替换为实际的变更检测逻辑
          //var pks=await GetPrimaryKey(table,pool);
          //console.log('pks',pks)
          let result = await pool.request().query(`SELECT CT.* FROM CHANGETABLE(CHANGES dbo.${table}, ${_lastSyncVersion}) AS CT`);
            
          // 处理结果...
          //console.log(result);
          let index = 0;
          while(index < result.data.recordset.length){
            const row = result.data.recordset[index];
            console.log(row['SYS_CHANGE_OPERATION'],row['RoomGUID']); // Process each row here
            index++;
          }
          
          _lastSyncVersion=Number(CURRENT_VERSION);
          db.mysqlExcute("REPLACE INTO sync_version (table_name,last_sync_version) VALUES ('"+table+"',"+_lastSyncVersion+");")
            .then((res)=>console.log(res));
          
      }
      
  
      // 同步到MySQL...
      //let mysqlConnection = await mysql.createConnection(mysqlConfig);
      // 使用result记录执行MySQL更新
      //await mysqlConnection.query('SELECT * FROM sync_version WHERE table_name = "'+table+'"');
      
      pool.close();
      //mysqlConnection.end();
    } catch (err) {
      console.error('Database polling error:', err);
    }
  }
  setInterval(pollDatabaseChanges, 1000); // 每10秒检查一次
// query('select * from names', (err,results)=>{
//     console.log(results)
// });
async function GetPrimaryKey(table,pool){
  var pks=[];
  let result = await pool.request().query(`SELECT 
  kcu.TABLE_NAME,
  kcu.COLUMN_NAME
FROM 
  INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN 
  INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
  AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
  AND tc.TABLE_NAME = kcu.TABLE_NAME
WHERE 
  tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND tc.TABLE_NAME = ${table};`);
            
    // 处理结果...
    console.log(result);
    let index = 0;
    while(index < result.recordset.length){
      const row = result.recordset[index];
      const columnName=row["COLUMN_NAME"];
      //console.log(row['SYS_CHANGE_OPERATION'],row['RoomGUID']); // Process each row here
      console.log('columnName',columnName);
      pks.push(columnName);
      index++;
    }
    return pks;
}
server.listen(env.PORT, () => console.log('app is runing at port: '+env.PORT,'mysql host: '+env.HOST))


