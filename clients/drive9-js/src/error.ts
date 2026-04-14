export class Drive9Error extends Error {
  constructor(message: string) {
    super(message);
    this.name = "Drive9Error";
  }
}

export class StatusError extends Drive9Error {
  statusCode: number;
  constructor(message: string, statusCode: number) {
    super(message);
    this.name = "StatusError";
    this.statusCode = statusCode;
  }
}

export class ConflictError extends StatusError {
  constructor(message: string, statusCode = 409) {
    super(message, statusCode);
    this.name = "ConflictError";
  }
}

export async function checkError(resp: Response): Promise<Response> {
  if (resp.ok) return resp;
  let message = `HTTP ${resp.status}`;
  try {
    const body = (await resp.json()) as { error?: string; message?: string };
    if (body.error) message = body.error;
    else if (body.message) message = body.message;
  } catch {
    // ignore
  }
  if (resp.status === 409) {
    throw new ConflictError(message, resp.status);
  }
  throw new StatusError(message, resp.status);
}
