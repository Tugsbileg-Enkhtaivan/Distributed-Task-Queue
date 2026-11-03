local meta = ARGV[3] .. ARGV[4]
if redis.call('EXISTS', meta) == 0 then
  redis.call('LREM', KEYS[1], 1, ARGV[4])
  return false
end

redis.call('HSET', meta, 'status', 'active', 'startedAt', ARGV[1], 'workerId', ARGV[2])
return ARGV[4]
