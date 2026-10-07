const sharp = require('sharp');
const prepared = new WeakMap();
const MAX_PIXELS = 40000000;
const MAX_BYTES = 20 * 1024 * 1024;

// Decode the bytes, never infer the provider MIME type from a filename.
// Originals remain untouched; unsupported formats become an in-memory PNG.
async function normalizeSource(source) {
  if (!Buffer.isBuffer(source?.buf) || !source.buf.length) throw invalidSource();
  if (prepared.has(source.buf)) return prepared.get(source.buf);
  const task = (async () => {
    try {
      const input = sharp(source.buf, { limitInputPixels: MAX_PIXELS, failOn: 'error', animated: false });
      const metadata = await input.metadata();
      if (!metadata.width || !metadata.height) throw new Error('Missing dimensions');
      const supported = ['jpeg', 'png', 'webp'].includes(metadata.format);
      const converted = !supported || (metadata.orientation && metadata.orientation !== 1) || metadata.width > 4096 || metadata.height > 4096 || (metadata.pages || 1) > 1 || source.buf.length >= MAX_BYTES;
      let buf;
      if (converted) {
        buf = await input.rotate().resize({ width: 4096, height: 4096, fit: 'inside', withoutEnlargement: true }).toColourspace('srgb').png().toBuffer();
      } else {
        // Metadata alone accepts some truncated files. Fully decode before any API call.
        await input.stats();
        buf = source.buf;
      }
      if (buf.length >= MAX_BYTES) throw new Error('Image too large');
      const format = converted ? 'png' : metadata.format;
      return { buf, mime: 'image/' + format, originalFormat: metadata.format === 'heif' && metadata.compression === 'av1' ? 'avif' : metadata.format, format, converted, width: metadata.width, height: metadata.height };
    } catch {
      throw invalidSource();
    }
  })();
  prepared.set(source.buf, task);
  try { return await task; } catch (error) { prepared.delete(source.buf); throw error; }
}

function invalidSource() {
  const error = new Error('The source photo could not be decoded or is too large. Upload a readable PNG, JPEG or WebP photo before generating. No image-provider request was sent.');
  error.api = { status: 400, code: 'invalid_source_image', type: 'local_image_error' };
  return error;
}

module.exports = { normalizeSource };
