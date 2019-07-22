import { s3, images, readableBody, IS_OFFLINE } from './model'
import * as getStream from 'get-stream'
import base64url from 'base64url';

describe('S3 operations', () => {
  it('should create valid URLs from URL-safe Base64 SHA256 Hash and Mimetype', () => {
    expect(s3.urlOf({ sha256: "HASH", mimetype: "image/png" }))
      .toBe(IS_OFFLINE
        ? "http://localhost:4572/images/HASH.png"
        : "https://s3.amazonaws.com/images/HASH.png")
  });

  it('should store objects correctly, and roundtrips result in the same meta-data', async () => {
    const id = 'not-an-image'
    const data = 'This is not an image'
    let body = readableBody(data)
    let imageMetaStream = images.streamMetadata(body)
    let putResponse = await s3.upload(id, body).promise()

    // Quotes: https://github.com/aws/aws-sdk-net/issues/815
    // ETag:   echo -n 'This is not an image' |openssl dgst -md5
    expect(putResponse.ETag).toStrictEqual('\"2c3cd6f2e5c6fc39aba84f8a622450cd\"')
    let image_fetched = await getStream(s3.getUpload(id).createReadStream())
    expect(image_fetched).toEqual(data)

    // Roundtrip check
    let imageMeta = await imageMetaStream
    let imageComputed = await images.calculateMetadataFromUpload(id)

    // echo -n 'This is not an image' |openssl dgst -sha256
    expect(base64url.toBuffer(imageComputed.sha256).toString('hex'))
      .toEqual('fdc5aca2dc1c8602fcdb5c458ee9deaa6001eb672006026b6adb4274b0f73151')
    expect(base64url.toBuffer(imageMeta.sha256).toString('hex'))
      .toEqual('fdc5aca2dc1c8602fcdb5c458ee9deaa6001eb672006026b6adb4274b0f73151')

    expect(imageMeta).toEqual(imageComputed)
  });
});
