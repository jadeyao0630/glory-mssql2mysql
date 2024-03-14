const mysqlQuery = require('./mMysql.js');
const mssqlQuery = require('./mMssql.js');
let instance = null;

class DbService{
    static getDbServiceInstance(){
        return instance ? instance : new DbService();
    }
    async getLastUpdatedVersion(table){
        try{
            const LAST_VERSION_QUERY='SELECT * FROM sync_version WHERE table_name = "'+table+'"';
            return await new Promise(async(resolve,reject)=>{
                mysqlQuery(LAST_VERSION_QUERY, (err,results)=>{
                    if (err) reject(new Error(err.message));
                    console.log('results',results)
                    if(results.length>0)
                        resolve(Number(results[0]['last_sync_version']));
                    else
                        resolve(0);
                });
            })
        }catch(error){
            console.log(error);
            return 0;
        }
    }
    async getCurrentUpdatedVersion(){
        try{
            const CURRENT_VERSION_QUERY='SELECT CHANGE_TRACKING_CURRENT_VERSION() AS CurrentVersion;';
            return await new Promise(async(resolve,reject)=>{
                mssqlQuery(CURRENT_VERSION_QUERY, (err,results)=>{
                    if (err) reject(new Error(err.message));
                    //console.log('results',results)
                    if(results.recordset.length>0)
                        resolve(Number(results.recordset[0]['CurrentVersion']));
                    else
                        resolve(0);
                });
            })
        }catch(error){
            console.log(error);
            return 0;
        }
    }
    async mysqlExcute(query){
        try{
            const response = await new Promise(async(resolve,reject)=>{
                mysqlQuery(query, (err,results)=>{
                    if (err) reject(new Error(err.message));
                    //console.log('results',results)
                    resolve(results);
                });
            })
            return {
                'success':true,
                'data':response
            }
        }catch(error){
            console.log(error);
            return {
                'success':false,
                'data':error
            }
        }
    }
    async mssqlExcute(query){
        try{
            const response = await new Promise(async(resolve,reject)=>{
                mssqlQuery(query, (err,results)=>{
                    if (err) reject(new Error(err.message));
                    //console.log('results',results)
                    resolve(results);
                });
            })
            return {
                'success':true,
                'data':response
            }
        }catch(error){
            console.log(error);
            return {
                'success':false,
                'data':error
            }
        }
    }
    async mssqlGet(table){
        try{
            //const LAST_VERSION_QUERY='SELECT * FROM sync_version WHERE table_name = "'+table+'"';
            const response =  new Promise(async(resolve,reject)=>{
                mysqlQuery(LAST_VERSION_QUERY, (err,results)=>{
                    if (err) reject(new Error(err.message));
                    resolve(results);
                });
            })
            return {
                success:true,
                data:response
            }
        }catch(error){
            console.log(error);
            return {
                success:false,
                data:error
            }
        }
    }
}

module.exports = DbService;