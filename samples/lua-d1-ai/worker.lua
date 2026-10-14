return {
  fetch = function(request, env)
    local row = env.DB:prepare("SELECT answer FROM prompts WHERE id = ?")
      :bind("welcome"):first()
    if row == nil then
      return { status = 404, body = "No prompt found" }
    end

    local result = env.AI:run("@cf/meta/llama-3.1-8b-instruct-fast", {
      prompt = row.answer,
    })
    if type(result.response) ~= "string" then
      return { status = 502, body = "Unexpected AI response" }
    end
    return { status = 200, body = result.response }
  end,
}
