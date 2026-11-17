export class Drive9Error extends Error {
  constructor(message: string) {
    super(message);
    this.name = "Drive9Error";
  }
}

export class StatusError extends Drive9Error {
  statusCode: number;
  code?: string;
  constructor(message: string, statusCode: number, code?: string) {
    super(message);
    this.name = "StatusError";
    this.statusCode = statusCode;
    this.code = code;
  }
}

export class ConflictError extends StatusError {
  serverRevision?: number;
  constructor(message: string, statusCode = 409, serverRevision?: number, code?: string) {
    super(message, statusCode, code);
    this.name = "ConflictError";
    this.serverRevision = serverRevision;
  }
}

export async function checkError(resp: Response): Promise<Response> {
  if (resp.ok) return resp;
  let message = `HTTP ${resp.status}`;
  let serverRevision: number | undefined;
  let code: string | undefined;
  try {
    const body = (await resp.json()) as {
      error?: string;
      message?: string;
      code?: string;
      server_revision?: number;
    };
    if (body.error) message = body.error;
    else if (body.message) message = body.message;
    if (typeof body.code === "string" && body.code) {
      code = body.code;
    }
    if (typeof body.server_revision === "number") {
      serverRevision = body.server_revision;
    }
  } catch {
    // ignore
  }
  if (resp.status === 409) {
    throw new ConflictError(message, resp.status, serverRevision, code);
  }
  throw new StatusError(message, resp.status, code);
}
