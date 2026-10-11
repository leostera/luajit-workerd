local DEFAULT_ITERATIONS = 10000
local MAX_ITERATIONS = 1000000
local MODULUS = 1000000007

local function response(body, contentType)
  return {
    status = 200,
    headers = {{ name = "content-type", value = contentType }},
    body = body,
  }
end

local function runCpuLoop(iterations)
  local result = 0
  for i = 1, iterations do
    result = (result + i * 17) % MODULUS
  end
  return result
end

return {
  fetch = function(request)
    local path = string.match(request.url, "^[^:]+://[^/]+([^?#]*)")
    if path == "/hello" then
      return response("hello\n", "text/plain")
    end

    local iterationParameter = string.match(request.url, "[?&]iters=([^&#]*)")
    local iterations = tonumber(iterationParameter) or DEFAULT_ITERATIONS
    if iterations < 1 or iterations > MAX_ITERATIONS or
        iterations ~= math.floor(iterations) then
      iterations = DEFAULT_ITERATIONS
    end

    local result = runCpuLoop(iterations)
    local body = string.format("{\"iters\":%d,\"result\":%.0f}", iterations, result)
    return response(body, "application/json")
  end,
}
