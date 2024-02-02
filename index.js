'use strict'
const { DataApiClient } = require('rqlite-js');
const RQLITE_HOST = process.env.RQLITE_HOST || 'http://rqlite-svc-internal:4001'

const queryOpts = { level: 'weak' }
if(process.env.RQLITE_READONLY) queryOpts.level = 'none'
const client = new DataApiClient(RQLITE_HOST);
module.exports.checkTableExists = async(tableName)=>{
  try{
    let result = await client.query(`SELECT name FROM sqlite_master WHERE type='table' AND name='${tableName}'`)
    if(!result) return
    if(result?.get(0)?.data?.name === tableName) return true
  }catch(e){
    console.log(`CheckTable Error`)
    throw(e)
  }
}
module.exports.createTable = async(sql)=>{
  try{
    let result = await client.execute(sql)
    if(result?.get(0)?.rowsAffected) return true
  }catch(e){
    console.log(`createTable Error`)
    throw(e)
  }
}
module.exports.setJSON = async(table, value, altValue, data, expireSeconds)=>{
  try{
    if(!table || !value || !data) return
    let ttl, string = JSON.stringify(string), expireTime = expireSeconds || tables[table]?.ttl
    if(expireTime) ttl = Date.now() + expireTime * 1000
    let sql = `INSERT OR REPLACE INTO ${table} VALUES('${value}', '${altValue}', '${JSON.stringify(data)}', ${ttl})`
    let results = await client.execute(sql)
    return results?.get(0)?.rowsAffected
  }catch(e){
    throw(e)
  }
}
module.exports.getJSON = async(table, key, value, ttl = true)=>{
  try{
    let sql = `SELECT * FROM ${table} WHERE ${key}='${value}'`
    if(ttl) sql += ` AND ttl>${Date.now()}`
    let result = await client.query(sql, queryOpts)
    let json = results.get(0)?.data.data
    if(json) return JSON.parse(json)
  }catch(e){
    throw(e)
  }
}
module.exports.set = async(table, value, altValue, data, expireSeconds)=>{
  try{
    if(!table || !value || !data) return
    let ttl, expireTime = expireSeconds || tables[table]?.ttl
    if(expireTime) ttl = Date.now() + expireTime * 1000
    let sql = `INSERT OR REPLACE INTO ${table} VALUES('${value}', '${altValue}', '${data}', ${ttl})`
    let results = await client.execute(sql)
    return results?.get(0)?.rowsAffected
  }catch(e){
    throw(e)
  }
}
module.exports.get = async(table, key, value, ttl = true)=>{
  try{
    let sql = `SELECT * FROM ${table} WHERE ${key}='${value}'`
    if(ttl) sql += ` AND ttl>${Date.now()}`
    let result = await client.query(sql, queryOpts)
    return result.get(0)?.data?.data
  }catch(e){
    throw(e)
  }
}
