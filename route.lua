function common.route(self, dispatch, ctrl_dir)
    -- ngx.say(type(dispatch))
    -- ngx.say(ngx.var.act)
    -- ngx.say(dispatch[ngx.var.act]['url'])
    if dispatch[ngx.var.act] then
        ngx.ctx._REQUEST = {}

        local require_str = "lua."..ctrl_dir
        local tmp = self:split(dispatch[ngx.var.act]['url'], '/')
        local action = tmp[#tmp] .. '_' .. ngx.var.version
        tmp[#tmp] = nil
        for k, v in pairs(tmp) do
            require_str = require_str .."."..v
        end
        -- ngx.say(require_str)

        ngx.ctx._GET = ngx.req.get_uri_args()
        for k, v in pairs(ngx.ctx._GET) do
            ngx.ctx._REQUEST[k] = v
        end

        local content_type = ngx.var.content_type or ''

        local start_index, end_index = content_type:find('boundary')
        if start_index ~= nil and end_index ~= nil then
            local upload_helper = UploadHelper:new(UploadConf.CHUNK_SIZE)
            local ret = upload_helper:read()
            for k, v in pairs(ret) do
                ngx.ctx._REQUEST[k] = v
            end
        else
            ngx.req.read_body()
            ngx.ctx._POST = ngx.req.get_post_args()
            for k, v in pairs(ngx.ctx._POST) do
                ngx.ctx._REQUEST[k] = v
            end
        end

        if dispatch[ngx.var.act]['auth'] then
            local uid = ngx.ctx._REQUEST['uid'] or false
            local pc = ngx.ctx._REQUEST['pc'] or false

            if not uid or not pc then
                pc = ''
                uid = ''
                LogHelper:error('auth failed param deficiency:' .. require_str .. '---' .. action .. '-uid-' .. uid .. '-pc-' .. pc)
                return ResponseHelper:fail_v2('DEFAULT', -10001)
            end

            local redis_cli = RedisHelper:new(RedisConf.PC_CACHE)
            local rds_key = 'pc:' .. uid
            
            local rds_pc = redis_cli:get(rds_key, RedisHelper.mod_shard, uid)
            
            if not rds_pc then
                LogHelper:error('auth failed rds pc deficiency:' .. require_str .. '---' .. action .. '-uid-' .. uid .. '-pc-' .. pc)
                local mysql_helper = MysqlHelper:new(MysqlConf.LEDONGLI_WALKER)    
                local user_info = mysql_helper:getRow('users', 'uid='..uid..' and is_login=1 and is_delete=0', 'pc')
                if not user_info then
                    LogHelper:error('auth failed by db:' .. require_str .. '---' .. action .. '-uid-' .. uid .. '-pc-' .. pc)
                    return ResponseHelper:fail_v2('DEFAULT', -100003)
                end
                correct_pc = user_info['pc']
                self:set_pc_cache(uid, correct_pc)
            else
                correct_pc = rds_pc
            end

            if pc ~= correct_pc then
                LogHelper:error('auth failed api pc error:' .. require_str .. '---' .. action .. '-uid-' .. uid .. '-pc-' .. pc .. '-correct_pc-' .. correct_pc)
                return ResponseHelper:fail_v2('DEFAULT', -10001)
            end

            if self:in_table_key('state', dispatch[ngx.var.act]) and dispatch[ngx.var.act]['state'] then
                local service_status = ngx.ctx._REQUEST['service_status'] or false
                if not service_status then
                    return ResponseHelper:fail_v2('DEFAULT', -100000)
                end

                local mysql_helper = MysqlHelper:new(MysqlConf.LEDONGLI_WALKER)    
                local user_info = mysql_helper:getRow('sprogram_user_business', 'uid='..uid)
                
                if user_info['service_status'] ~= tonumber(service_status) then
                    return ResponseHelper:fail_v2('DEFAULT', -10002)
                end
            end
        end

        if dispatch[ngx.var.act]['auth_coach'] then
            local res = _auth_coach()
            if res == false then
                return ResponseHelper:fail_v2('DEFAULT', -10001)
            end
        end

        if dispatch[ngx.var.act]['auth_step_challenge'] then
            local res = _auth_step_challenge()
            if res == false then
                return ResponseHelper:fail_v2('DEFAULT', -10001)
            end
        end

        local status, controller = pcall(require, require_str)
        
        if not status then
            return ResponseHelper:fail('DEFAULT', -100000, nil, controller)
        elseif not self:in_table_key(action, controller) then
            return ResponseHelper:fail('DEFAULT', -100404)
        else
            return controller[action]()
        end
    else
        return ResponseHelper:fail('DEFAULT', -100404)
    end
end
