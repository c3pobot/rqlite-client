'use strict'
const { DataApiClient } = require('rqlite-js');
module.exports = class rqlite{
  constructor({ host, readOnly = false }){
    this.queryOpts = { level: 'weak' }
    if(readOnly) this.queryOpts.level = 'none'
    this.client = new DataApiClient(host)
  }
  async checkTableExists(tableName){
    try{
      let result = await this.client?.query(`SELECT name FROM sqlite_master WHERE type='table' AND name='${tableName}'`)
      if(!result) return
      if(result?.get(0)?.data?.name === tableName) return true
    }catch(e){
      throw(e)
    }
  }
  async createTable(sql){
    try{
      let result = await this.client.execute(sql)
      if(result?.get(0)?.rowsAffected) return true
    }catch(e){
      throw(e)
    }
  }
  async get(table, key, value, ttl = true){
    try{
      let sql = `SELECT * FROM ${table} WHERE ${key}='${value}'`
      if(ttl) sql += ` AND ttl>${Date.now()}`
      let results = await this.client?.query(sql, this.queryOpts)
      return results.get(0)?.data?.data
    }catch(e){
      throw(e)
    }
  }
  async getJSON(table, key, value, ttl = true){
    try{
      let sql = `SELECT * FROM ${table} WHERE ${key}='${value}'`
      if(ttl) sql += ` AND ttl>${Date.now()}`
      let results = await this.client.query(sql, this.queryOpts)
      let json = results.get(0)?.data?.data
      if(json) return JSON.parse(json)
    }catch(e){
      throw(e)
    }
  }
  async set(table, value, altValue, data, expireSeconds){
    try{
      if(!table || !value || !data) return
      let ttl
      if(expireSeconds) ttl = Date.now() + (expireSeconds * 1000)
      let sql = `INSERT OR REPLACE INTO ${table} VALUES('${value}', '${altValue}', '${data}', ${ttl})`
      let result = await this.client?.execute(sql)
      return result?.get(0)?.rowsAffected
    }catch(e){
      throw(e)
    }
  }
  async setJSON(table, value, altValue, data, expireSeconds){
    try{
      if(!table || !value || !data) return
      let ttl
      if(expireSeconds) ttl = Date.now() + (expireSeconds * 1000)
      let sql = `INSERT OR REPLACE INTO ${table} VALUES('${value}', '${altValue}', '${JSON.stringify(data)}', ${ttl})`
      let result = await this.client?.execute(sql)
      return result?.get(0)?.rowsAffected
    }catch(e){
      throw(e)
    }
  }
}
