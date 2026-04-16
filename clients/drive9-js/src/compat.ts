/** Return a BodyInit accepted by fetch in strict Node typings. */
export function bodyInit(data: Uint8Array): BodyInit {
  return Buffer.from(data);
}

/** Return an ArrayBuffer accepted by crypto.subtle.digest in strict Node typings. */
export function bufferSource(data: Uint8Array): ArrayBuffer {
  return data.slice().buffer;
}
