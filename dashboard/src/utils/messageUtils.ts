/**
 * Utility functions for message handling
 */

/**
 * Decode base64 message ID to readable numeric string
 * Converts 8-byte big-endian integer from base64 to decimal string
 */
export const decodeMessageId = (b64: string): string => {
  try {
    const bin = atob(b64);
    if (bin.length !== 8) return b64;
    let seq = 0n;
    for (let i = 0; i < 8; i++) {
      seq = (seq << 8n) | BigInt(bin.charCodeAt(i));
    }
    return seq.toString();
  } catch {
    return b64;
  }
};

/**
 * Convert sequence number string to base64 token
 * Converts decimal string to 8-byte big-endian integer in base64
 */
export const seqToBase64Token = (seqStr: string): string => {
  const n = BigInt(seqStr);
  const bytes = new Uint8Array(8);
  for (let i = 7; i >= 0; i--) {
    bytes[i] = Number(n >> BigInt((7 - i) * 8) & 0xffn);
  }
  let bin = '';
  for (let i = 0; i < 8; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
};

/**
 * Convert bytes array to hex string
 */
export const bytesToHex = (bytes: Uint8Array) => 
  Array.from(bytes, b => b.toString(16).padStart(2, '0')).join(' ');

/**
 * Convert big-endian uint64 bytes to number
 */
export const beUint64ToNumber = (bytes: Uint8Array) => {
  let v = 0n;
  for (let i = 0; i < 8; i++) v = (v << 8n) | BigInt(bytes[i]);
  const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
  return Number(v > maxSafe ? maxSafe : v);
};
