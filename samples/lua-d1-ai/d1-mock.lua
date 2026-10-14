return {
  fetch = function(request)
    if request.url:find("/query", 1, true) then
      return {
        status = 200,
        headers = { { name = "content-type", value = "application/json" } },
        body = [=[{"success":true,"results":{"columns":["answer"],"rows":[["Describe how D1 and Workers AI cooperate in this mock sample."]]},"meta":{"duration":0,"changes":0}}]=],
      }
    end

    return {
      status = 404,
      body = "Unsupported mock D1 endpoint",
    }
  end,
}
