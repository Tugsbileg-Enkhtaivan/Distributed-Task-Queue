local jobId = redis.call('RPOPLPUSH', KEYS[1], KEYS[2])
if not jobId then
  return false
end

local meta = ARGV[3] .. jobId
if redis.call('EXISTS', meta) == 0 then
  redis.call('LREM', KEYS[2], 1, jobId)
  return false
end

redis.call('HSET', meta, 'status', 'active', 'startedAt', ARGV[1], 'workerId', ARGV[2])
return jobId
