local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local moved = 0

for i = 1, #due do
  local jobId = due[i]
  if redis.call('ZREM', KEYS[1], jobId) == 1 then
    redis.call('HSET', ARGV[2] .. jobId, 'status', 'waiting')
    redis.call('LPUSH', KEYS[2], jobId)
    moved = moved + 1
  end
end

return moved
