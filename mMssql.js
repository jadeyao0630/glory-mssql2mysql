var mssql=require("mssql");
const dotenv = require('dotenv');
const path = require("path");
const { env } = process;
dotenv.config({
    path: path.resolve(
        __dirname,
        `./.env${process.env.NODE_ENV ? "."+process.env.NODE_ENV : ""}`
      ),
});
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
  let pool = new mssql.ConnectionPool(mssqlConfig);
  var query=function(sql,options,callback){
    pool.connect().then(()=>{
        pool.request().query(sql,options,function(err,results,fields){
            if(callback!=undefined) callback(err,results,fields);
            pool.close();
        })
    });
    
    
  }
  module.exports=query;