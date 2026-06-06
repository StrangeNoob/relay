package broker

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

// Lua scripts are kept as standalone .lua files and embedded here, rather than
// inlined as Go string literals, so they stay readable and reviewable as Redis
// code. Each is wrapped in a *redis.Script, which runs via EVALSHA and falls
// back to EVAL automatically when the server has not cached the script yet.

//go:embed scripts/claim.lua
var claimSrc string

var claimScript = redis.NewScript(claimSrc)

//go:embed scripts/ack.lua
var ackSrc string

var ackScript = redis.NewScript(ackSrc)

//go:embed scripts/nack.lua
var nackSrc string

var nackScript = redis.NewScript(nackSrc)

//go:embed scripts/reaper.lua
var reaperSrc string

var reaperScript = redis.NewScript(reaperSrc)
