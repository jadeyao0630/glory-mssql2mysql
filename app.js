const express = require('express');
const app = express();
const path = require('path');
const http = require('http');
const server = http.createServer(app);
require('dotenv').config({
    path: path.resolve(
        __dirname,
        `./.env`
    ),
});
const { env } = process;
var mysql=require("mysql");
var mssql=require("mssql");
var pool = mysql.createPool({
    host: env.HOST,
    user:env.HOST==='192.168.10.241'||env.HOST==='192.168.10.242'?'glory':env.USER,
    password:env.PASSWORD,
    database:env.DATABASE,
    port:env.DB_PORT,
});
const mssqlConfig = {
    user: 'sa',
    password: 'qijiashe6',
    server: '192.168.10.122',
    database: 'glory',
    options: {
        encrypt: false,
      //encrypt: true, // for azure
      //trustServerCertificate: true // for self-signed certificates
    }
  };
var query=function(sql,options,callback){

    pool.getConnection(function(err,conn){
        if(err){
            console.log(err);
            if(callback!=undefined) callback(err,null,null);
        }else{
            conn.query(sql,options,function(err,results,fields){
                //事件驱动回调
                if(callback!=undefined) callback(err,results,fields);
            });
            //释放连接，需要注意的是连接释放需要在此处释放，而不是在查询回调里面释放
            conn.release();
        }
    });
};
async function pollDatabaseChanges() {
    try {
      let pool = new mssql.ConnectionPool(mssqlConfig);
      await pool.connect();
        var table='sales';
        var _lastSyncVersion=0;
      // 示例查询: 替换为实际的变更检测逻辑
      let result = await pool.request().query(`SELECT CT.* FROM CHANGETABLE(CHANGES dbo.${table}, ${_lastSyncVersion}) AS CT`);
  
      // 处理结果...
      console.log(result);
  
      // 同步到MySQL...
      //let mysqlConnection = await mysql.createConnection(mysqlConfig);
      // 使用result记录执行MySQL更新
      // await mysqlConnection.query('INSERT INTO YourMySQLTable ...');
  
      //pool.close();
      //mysqlConnection.end();
    } catch (err) {
      console.error('Database polling error:', err);
    }
  }
  setInterval(pollDatabaseChanges, 1000); // 每10秒检查一次
// query('select * from names', (err,results)=>{
//     console.log(results)
// });

server.listen(env.PORT, () => console.log('app is runing at port: '+env.PORT,'mysql host: '+env.HOST))