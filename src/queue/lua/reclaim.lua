local jobs = redis.call('LRANGE', KEYS[1], 0, -1)
local reclaimed = 0

for i = 1, #jobs do
  local jobId = jobs[i]
  local meta = ARGV[1] .. jobId
  local status = redis.call('HGET', meta, 'status')
  local workerId = redis.call('HGET', meta, 'workerId')

  if status == 'completed' or status == 'failed' then
    redis.call('LREM', KEYS[1], 1, jobId)
  else
    local heartbeatExists = 0
    if workerId then
      heartbeatExists = redis.call('EXISTS', ARGV[2] .. workerId .. ARGV[3])
    end

    if heartbeatExists == 0 then
      redis.call('HSET', meta, 'status', 'waiting')
      redis.call('HDEL', meta, 'workerId', 'startedAt')
      redis.call('LREM', KEYS[1], 1, jobId)
      redis.call('LPUSH', KEYS[2], jobId)
      reclaimed = reclaimed + 1
    end
  end
end

return reclaimed
