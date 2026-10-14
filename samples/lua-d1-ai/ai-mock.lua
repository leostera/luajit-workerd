return {
  fetch = function(request)
    return {
      status = 200,
      headers = { { name = "content-type", value = "application/json" } },
      body = [[{"response":"This is a local Workers AI mock response."}]],
    }
  end,
}
