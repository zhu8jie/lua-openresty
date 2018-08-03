local mysql = require 'resty.mysql'
local MysqlConf = require 'lua.conf.mysql_conf'
local LocalHelper = require 'lua.common.log_helper'


local _M = {}
local mt = {__index = _M}


function _M.new(self, conf)
    local db, err = mysql:new()
    if not db then
        return nil
    end

    db:set_timeout(MysqlConf.TIMEOUT)

    return setmetatable({db = db, conf = conf}, mt)
end


function _M.instance(self)
    local res, err, errno, sqlstate = self.db:connect(self.conf)
    if not res then
        return false
    end
    return true
end


function _M.keepalive(self)
    local res, err = self.db:set_keepalive(MysqlConf.MAX_IDLE_TIMEOUT, MysqlConf.POOL_SIZE)
    return res
end


function _M.getAll(self, table_name, where_cond, field_arr, limit_arr, order_str)
    local field_row = '*'
    if type(field_arr) == 'string' then
        field_row = field_arr
    elseif type(field_arr) == 'table' and table.getn(field_arr) > 0 then
        field_row = ''
        for i in pairs(field_arr) do
            field_row = field_row .. field_arr[i] .. ','
        end
        field_row = string.sub(field_row, 1, -2)
    end
    
    local where_c = ''
    if type(where_cond) == 'string' then
        where_c = where_cond
    else
        for i,v in pairs(where_cond) do
            where_c = where_c .. i ..' = ' .. ngx.quote_sql_str(v) .. ' and '
        end
        where_c = string.sub(where_c, 1, -5)
    end

    if order_str ~= '' and type(order_str) == 'string' then
        order_str = ' order by '..order_str
    else
        order_str = ''
    end

    local limit_str = ''
    if type(limit_arr) == 'string' then
        limit_str = ' limit '..limit_arr
    elseif type(limit_arr) == 'table' and table.getn(limit_arr) > 0 then
        limit_str = ' limit '
        for i in pairs(limit_arr) do
            limit_str = limit_str .. limit_arr[i] .. ','
        end
        limit_str = string.sub(limit_str, 1, -2)
    end

    local sql = string.format("select %s from %s where %s %s %s", field_row, table_name, where_c, order_str, limit_str)
    return self:query(sql)
end


function _M.getRow(self, table_name, where_cond, field_arr)
    local field_row = '*'
    if type(field_arr) == 'string' then
        field_row = field_arr
    elseif type(field_arr) == 'table' and table.getn(field_arr) > 0 then
        field_row = ''
        for i in pairs(field_arr) do
            field_row = field_row .. field_arr[i] .. ','
        end
        field_row = string.sub(field_row, 1, -2)
    end
    
    local where_c = ''
    if type(where_cond) == 'string' then
        where_c = where_cond
    else
        for i,v in pairs(where_cond) do
            where_c = where_c .. i ..' = ' .. ngx.quote_sql_str(v) .. ' and '
        end
        where_c = string.sub(where_c, 1, -5)
    end

    local sql = string.format("select %s from %s where %s limit 1", field_row, table_name, where_c)
    local dbret = self:query(sql)
    if next(dbret) == nil then
        return false
    else
        return dbret[1]
    end
end


function _M.add(self, table_name, field_arr)
    local field_deal = ''
    for i, v in pairs(field_arr) do
        field_deal = field_deal .. i ..' = ' .. ngx.quote_sql_str(v) .. ',' 
    end
    field_deal = string.sub(field_deal, 1, -2)

    local sql = string.format("insert into %s set %s", table_name, field_deal)
    return self:query(sql)
end

function _M.delete(self, table_name, where_cond)
    local where_c = ''
    if type(where_cond) == 'string' then
        where_c = where_cond
    else
        for i,v in pairs(where_cond) do
            where_c = where_c .. i ..' = ' .. ngx.quote_sql_str(v) .. ' and '
        end
        where_c = string.sub(where_c, 1, -5)
    end

    local sql = string.format("delete from %s where %s", table_name, where_c)
    return self:query(sql)
end


function _M.update(self, table_name, where_cond, field_arr)
    local where_c = ''
    if type(where_cond) == 'string' then
        where_c = where_cond
    else
        for i,v in pairs(where_cond) do
            where_c = where_c .. i ..' = ' .. ngx.quote_sql_str(v) .. ' and '
        end
        where_c = string.sub(where_c, 1, -5)
    end

    local field_deal = ''
    for i, v in pairs(field_arr) do
        field_deal = field_deal .. i ..' = ' .. ngx.quote_sql_str(v) .. ',' 
    end
    field_deal = string.sub(field_deal, 1, -2)

    local sql = string.format("update %s set %s where %s", table_name, field_deal, where_c)
    return self:query(sql)
end


function _M.save(self, table_name, where_cond, field_arr)
    local where_c = ''
    local field_deal = ''
    if type(where_cond) == 'string' then
        where_c = where_cond
        field_deal = where_cond .. ','
    else
        for i,v in pairs(where_cond) do
            where_c = where_c .. i ..' = ' .. ngx.quote_sql_str(v) .. ' and '
            field_deal = field_deal .. i ..' = ' .. ngx.quote_sql_str(v) .. ',' 
        end
        where_c = string.sub(where_c, 1, -5)
    end

    for i, v in pairs(field_arr) do
        field_deal = field_deal .. i ..' = ' .. ngx.quote_sql_str(v) .. ',' 
    end
    field_deal = string.sub(field_deal, 1, -2)

    local sql = string.format("select count(1) ct from %s where %s", table_name, where_c)
    local dbret = self:query(sql)
    if dbret[1]['ct'] == '0' then
        sql = string.format("insert into %s set %s", table_name, field_deal)
    else
        sql = string.format("update %s set %s where %s", table_name, field_deal, where_c)
    end
    return self:query(sql)
end


function _M.close(self)
    local res, err = self.db:close()
    return res
end


function _M.query(self, sql)
    local res = self:instance()
    if not res then
        LocalHelper:error("db connect error")
        return false
    end

    local res, err, errno, sqlstate = self.db:query(sql)
    if err == nil then
        self:keepalive()
    end

    if not res then
        local error_message = string.format("db query error.sql is %s", sql)
        LocalHelper:error(error_message)
        return false
    end

    return res
end


return _M
