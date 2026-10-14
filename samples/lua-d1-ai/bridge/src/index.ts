import { env } from 'cloudflare:workers';

type JsonObject = Record<string, unknown>;

class BadRequestError extends Error {}

function isJsonObject(value: unknown): value is JsonObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function jsonResponse(value: unknown, status = 200): Response {
  return Response.json(value, { status });
}

async function readJsonObject(request: Request): Promise<JsonObject> {
  let value: unknown;
  try {
    value = await request.json();
  } catch {
    throw new BadRequestError('Expected a JSON object');
  }

  if (!isJsonObject(value)) {
    throw new BadRequestError('Expected a JSON object');
  }
  return value;
}

function bindStatement(sql: unknown, params: unknown): D1PreparedStatement {
  if (typeof sql !== 'string' || !Array.isArray(params)) {
    throw new BadRequestError(
      'D1 request requires SQL text and a params array'
    );
  }
  const statement = env.DB.prepare(sql);
  return params.length === 0 ? statement : statement.bind(...params);
}

export default {
  async fetch(request: Request): Promise<Response> {
    const { pathname } = new URL(request.url);

    try {
      if (pathname === '/query' && request.method === 'POST') {
        const { sql, params = [] } = await readJsonObject(request);
        const result = await bindStatement(sql, params).all();
        const columns =
          result.results.length === 0 ? [] : Object.keys(result.results[0]);
        const rows = result.results.map((record) =>
          columns.map((column) => record[column] ?? null)
        );

        return jsonResponse({
          success: result.success,
          results: { columns, rows },
          meta: result.meta,
        });
      }

      if (pathname === '/execute' && request.method === 'POST') {
        const { sql, params = [] } = await readJsonObject(request);
        return jsonResponse(await bindStatement(sql, params).run());
      }

      if (pathname === '/run' && request.method === 'POST') {
        const model = request.headers.get('cf-consn-model-id');
        if (model === null || model.length === 0) {
          throw new BadRequestError('Missing AI model header');
        }

        const body = await readJsonObject(request);
        if (!isJsonObject(body.inputs)) {
          throw new BadRequestError('AI request requires an inputs object');
        }
        if (body.options !== undefined && !isJsonObject(body.options)) {
          throw new BadRequestError('AI options must be an object');
        }

        const result = await env.AI.run(model, body.inputs, body.options);
        return jsonResponse(result);
      }

      return jsonResponse({ error: 'Not found' }, 404);
    } catch (error) {
      return jsonResponse(
        { error: error instanceof Error ? error.message : String(error) },
        error instanceof BadRequestError ? 400 : 500
      );
    }
  },
} satisfies ExportedHandler;
