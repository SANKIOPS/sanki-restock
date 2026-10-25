const test = require('node:test');
const assert = require('node:assert/strict');
const Jimp = require('jimp');
const { decodeCatalogueImage, prepareInput, imageHeader } = require('../modules/inventory-visual-search');
let sharp;
try { sharp = require('sharp'); } catch { /* Native format tests require the declared production dependency. */ }

test('Shopify WebP catalogue images decode through native validation and resize proportionally', { skip: !sharp }, async () => {
  const webp = await sharp({ create: { width: 32, height: 20, channels: 3, background: '#fdfdfd' } }).webp().toBuffer();
  const image = await decodeCatalogueImage(webp, 16);
  assert.equal(image.bitmap.width, 16); assert.equal(image.bitmap.height, 10);
  assert.equal(imageHeader(webp).type, 'webp');
  assert.equal(imageHeader(Buffer.from(await prepareInput(webp), 'base64')).type, 'jpeg');
});

test('Shopify AVIF catalogue images remain searchable while direct upload signature validation stays strict', { skip: !sharp }, async () => {
  const avif = await sharp({ create: { width: 32, height: 20, channels: 3, background: '#22aabb' } }).avif().toBuffer();
  const metadata = await sharp(avif).metadata();
  assert.ok(metadata.format === 'avif' || metadata.format === 'heif');
  const image = await decodeCatalogueImage(avif, 16);
  assert.equal(image.bitmap.width, 16); assert.equal(image.bitmap.height, 10);
  assert.throws(() => imageHeader(avif), /readable product photo/);
  await assert.rejects(prepareInput(avif), /readable product photo/);
});

test('rotated catalogue and uploaded JPEGs preserve EXIF orientation before preparing AI input', { skip: !sharp }, async () => {
  const width = 40, height = 20, raw = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) for (let x = 0; x < width; x++) {
    const at = (y * width + x) * 3; raw[at] = x < width / 2 ? 255 : 0; raw[at + 2] = x < width / 2 ? 0 : 255;
  }
  const jpeg = await sharp(raw, { raw: { width, height, channels: 3 } }).jpeg({ quality: 95 }).withMetadata({ orientation: 6 }).toBuffer();
  assert.equal((await sharp(jpeg).metadata()).orientation, 6);
  for (const image of [await decodeCatalogueImage(jpeg, 100), await Jimp.read(Buffer.from(await prepareInput(jpeg), 'base64'))]) {
    assert.equal(image.bitmap.width, 20); assert.equal(image.bitmap.height, 40);
    const top = Jimp.intToRGBA(image.getPixelColor(10, 5)), bottom = Jimp.intToRGBA(image.getPixelColor(10, 35));
    assert.ok(top.r > top.b + 150, 'Original left side becomes the top after EXIF rotation');
    assert.ok(bottom.b > bottom.r + 150, 'Original right side becomes the bottom after EXIF rotation');
  }
});

test('catalogue decoding rejects SVG and corrupt or excessive image dimensions before rasterization', { skip: !sharp }, async () => {
  const svg = Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="32" height="20"><rect width="32" height="20" fill="red"/></svg>');
  await assert.rejects(decodeCatalogueImage(svg, 16), /Unsupported catalogue image format/);
  await assert.rejects(decodeCatalogueImage(Buffer.from('not an AVIF image'), 16));
  const oversized = await sharp({ create: { width: 32, height: 20, channels: 3, background: '#fff' } }).png().toBuffer();
  oversized.writeUInt32BE(100000, 16); oversized.writeUInt32BE(100000, 20);
  await assert.rejects(decodeCatalogueImage(oversized, 16));
});
