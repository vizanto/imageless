import { s3, IS_OFFLINE } from './model'
import * as getStream from 'get-stream'

describe('S3 operations', () => {
  it('should create valid URLs from URL-safe Base64 SHA256 Hash and Mimetype', () => {
    expect(s3.urlOf({ sha256: "HASH", mimetype: "image/png" }))
      .toBe(IS_OFFLINE
        ? "http://localhost:4572/images/HASH.png"
        : "https://s3.amazonaws.com/images/HASH.png")
  });
  it('should roundtrip the same meta-data', async () => {
    const id = 'not-an-image'
    const data = 'This is not an image'
    let image = await s3.putUpload(id, data).promise()
    // Quotes: https://github.com/aws/aws-sdk-net/issues/815
    // ETag:   echo -n 'This is not an image'|md5
    expect(image.ETag).toStrictEqual('\"2c3cd6f2e5c6fc39aba84f8a622450cd\"')
    let image_fetched = await getStream(s3.getUpload(id).createReadStream())
    expect(image_fetched).toEqual('This is not an image')
  });
});
