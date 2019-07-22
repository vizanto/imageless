import { s3, images, readableBody, IS_OFFLINE } from './model'
import * as getStream from 'get-stream'
import base64url from 'base64url';
import * as AWS from 'aws-sdk';
import { PromiseResult } from 'aws-sdk/lib/request';

/*-------
  Helpers
---------*/

// Quotes: https://github.com/aws/aws-sdk-net/issues/815
// ETag:   echo -n 'This is not an image' |openssl dgst -md5
const notImageData = 'This is not an image'
const notImageETag = '\"2c3cd6f2e5c6fc39aba84f8a622450cd\"'

const expectNoSuchKey = (promise: Promise<PromiseResult<object, AWS.AWSError>>) =>
  expect(promise).rejects.toMatchObject({ code: "NoSuchKey", message: "The specified key does not exist." });
const expectUploadToBeDeleted = (id) => expectNoSuchKey(s3.getUpload(id).promise());
const expectImageToBeDeleted = (key) => expect(s3.headImage(key).promise()).rejects.toMatchObject({ code: "NotFound" });


/*-----
  Specs
-------*/

describe('S3 operations', () => {
  it('should create valid URLs from URL-safe Base64 SHA256 Hash and Mimetype', () => {
    expect(s3.urlOf({ sha256: "HASH", mimetype: "image/png" }))
      .toBe(IS_OFFLINE
        ? "http://localhost:4572/images/HASH.png"
        : "https://s3.amazonaws.com/images/HASH.png")
  });

  it('should store objects correctly, and roundtrips result in the same meta-data', async () => {
    const id = 'not-an-image'
    let body = readableBody(notImageData)
    let imageMetaStream = images.streamMetadata(body)
    let putResponse = await s3.upload(id, body).promise()

    expect(putResponse.ETag).toStrictEqual(notImageETag)
    let image_fetched = await getStream(s3.getUpload(id).createReadStream())
    expect(image_fetched).toEqual(notImageData)

    // Roundtrip check
    let imageMeta = await imageMetaStream
    let imageComputed = await images.calculateMetadataFromUpload(id)

    // echo -n 'This is not an image' |openssl dgst -sha256
    expect(base64url.toBuffer(imageComputed.sha256).toString('hex'))
      .toEqual('fdc5aca2dc1c8602fcdb5c458ee9deaa6001eb672006026b6adb4274b0f73151')
    expect(base64url.toBuffer(imageMeta.sha256).toString('hex'))
      .toEqual('fdc5aca2dc1c8602fcdb5c458ee9deaa6001eb672006026b6adb4274b0f73151')

    expect(imageMeta).toEqual(imageComputed)

    // Cleanup after above tests
    await s3.deleteFromUploadBucket(id).promise()
    expectUploadToBeDeleted(id)
  });

  describe('should support moving an uploaded image to content-addressable storage', () => {
    it('should delete the upload after copying to image bucket', async () => {
      const id = 'some-upload-to-be-moved'
      const data = 'This is not an image'
      const image = { sha256: 'not really a hash', mimetype: 'application/octet-stream' }
      await s3.upload(id, data).promise()
      let { awsResults: { s3Copy } } = await s3.moveUploadToImageBucket(id, notImageETag, image, async () => {
        let uploaded = await s3.getUpload(id).promise()
        expect(uploaded.ETag).toBe(notImageETag) // Check the upload still exists
      })
      expect(s3Copy.CopyObjectResult.ETag).toBe(notImageETag)
      expectUploadToBeDeleted(id)

      // Cleanup after above tests
      await s3.deleteUnreferencedImage(image).promise()
      expectImageToBeDeleted(image)
    });

    it('should not fail when upload is already deleted (due to Streams handler or other process) after copying', async () => {
      const id = 'some-upload-to-be-moved-2'
      const data = 'This is not an image 2'
      const notImageETag = '\"a260985349903b35c47eb6f29f64bd4f\"'
      const image = { sha256: 'not really a hash 2', mimetype: 'application/octet-stream' }
      await s3.upload(id, data).promise()
      let { awsResults: { s3Copy } } = await s3.moveUploadToImageBucket(id, notImageETag, image, async () => {
        await s3.deleteFromUploadBucket(id).promise()
        expectUploadToBeDeleted(id)
      })
      expect(s3Copy.CopyObjectResult.ETag).toBe(notImageETag)
      expectUploadToBeDeleted(id)
      expect(await s3.headImage(image).promise()).toMatchObject({ ContentLength: notImageData.length + 2, ETag: notImageETag })

      // Cleanup after above tests
      await s3.deleteUnreferencedImage(image).promise()
      expectImageToBeDeleted(image)
    });
  });
});
