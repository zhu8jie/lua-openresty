local redis = require 'resty.redis'
local RedisConf = require 'lua.conf.redis_conf'
local EncodeHelper = require 'lua.common.encode_helper'
    

local _M = {}
local mt = {__index = _M}


function _M.new(self, conf)
    local db = redis:new()
    if not db then
        return false
    end

    db:set_timeout(RedisConf.TIMEOUT)

    return setmetatable({db = db, conf = conf}, mt)
end


function _M.default_shard(self, key)
    local shard_seed = 0
    for i = 1, string.len(key) do
        shard_seed = shard_seed + string.byte(key, i)
    end
    local redis_node_index = shard_seed % table.getn(self.conf) + 1
    return redis_node_index
end


function _M.mod_shard(self, key)
    local redis_node_index = tonumber(key) % table.getn(self.conf) + 1
    return redis_node_index
end


function _M.md5_shard(self, key)
    local redis_node_index = EncodeHelper:md5(key) % table.getn(self.conf) + 1
    return redis_node_index
end


function _M.instance(self, method, ...)
    local args = {...}
    local redis_node_index = method(unpack(args, 1, table.maxn(args)))

    local res, err = self.db:connect(self.conf[redis_node_index]['host'], self.conf[redis_node_index]['port'])
    if not res then
        return false
    end

    if self.conf[redis_node_index]['password'] ~= nil then
        local res, err = self.db:auth(self.conf[redis_node_index]['password'])
        if not res then
            return false
        end
    end

    local res, err = self.db:select(self.conf[redis_node_index]['db'])
    if not res then
        return false
    end

    return true
end


function _M.keepalive(self)
    local res, err = self.db:set_keepalive(RedisConf.MAX_IDLE_TIMEOUT, RedisConf.POOL_SIZE)
    return res
end


function _M.del(self, key, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:del(key)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    return true
end


function _M.incr(self, key, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:incr(key)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    return res
end


function _M.set(self, key, value, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:set(key, value)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    return res
end

function _M.expire(self, key, value, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:expire(key, value)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    return res
end

function _M.get(self, key, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:get(key)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    if res == ngx.null then
        return false
    end

    return res
end


function _M.rpush(self, key, value, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:rpush(key, value)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    return res
end


function _M.hset(self, key, field, value, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:hset(key, field, value)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    return res
end


function _M.hget(self, key, field, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:hget(key, field)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    if res == ngx.null then
        return false
    end

    return res
end


function _M.hdel(self, key, field, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:hdel(key, field)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    return res
end


function _M.hmset(self, key, tbl, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local fields = {}
    for k, v in pairs(tbl) do
        table.insert(fields, k)
        table.insert(fields, v)
    end

    local res, err = self.db:hmset(key, unpack(fields, 1, table.maxn(fields)))
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    return res
end


function _M.hmget(self, key, tbl, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:hmget(key, unpack(tbl, 1, table.maxn(tbl)))
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    local flag = false
    for _, v in ipairs(res) do
        if v ~= ngx.null then
            flag = true
        end
    end

    if flag == false then
        return false
    end

    local ret = {}
    for i, v in ipairs(tbl) do
        ret[v] = res[i]
    end

    return ret
end


function _M.hgetall(self, key, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:hgetall(key)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    if next(res) == nil then
        return {}
    end

    local ret = {}
    for i = 1, table.maxn(res), 2 do
        ret[res[i]] = res[i + 1]
    end

    return ret
end


function _M.zadd(self, key, value, score, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end
    local res, err = self.db:zadd(key, tonumber(score), value)
    if err == nil then
        self:keepalive()
    end
    if not res then
        return false
    end
    return res
end


function _M.zrem(self, key, value, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end
    local res, err = self.db:zrem(key, value)
    if err == nil then
        self:keepalive()
    end
    if not res then
        return false
    end
    return res
end


function _M.zrank(self, key, value, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:zrank(key, value)
    if err == nil then
        self:keepalive()
    end
    if not res then
        return false
    end
    return res
end


function _M.zrevrank(self, key, value, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:zrevrank(key, value)
    if err == nil then
        self:keepalive()
    end
    if not res then
        return false
    end
    return res
end


function _M.zscore(self, key, value, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:zscore(key, value)
    if err == nil then
        self:keepalive()
    end
    if not res then
        return false
    end
    return res
end


function _M.zrevrange_withscores(self, key, start, stop, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:zrevrange(key, start, stop, 'withscores')
    if err == nil then
        self:keepalive()
    end
    if not res then
        return false
    end

    local ret = {}
    for i = 1, table.maxn(res), 2 do
        table.insert(ret, {member = res[i], score = res[i + 1]})
    end

    return ret
end


function _M.zrevrange(self, key, start, stop, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:zrevrange(key, start, stop)
    if err == nil then
        self:keepalive()
    end
    if not res then
        return false
    end
    return res
end


function _M.srandmember(self, key, count, method, ...)
    local method = method or self.default_shard
    local args = {...}
    if next(args) == nil then
        args = {key}
    end

    local res = self:instance(method, self, unpack(args, 1, table.maxn(args)))
    if not res then
        return false
    end

    local res, err = self.db:srandmember(key, count)
    if err == nil then
        self:keepalive()
    end

    if not res then
        return false
    end

    return res
end


return _M

