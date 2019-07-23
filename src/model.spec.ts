import { s3, readableBody, IS_OFFLINE, S3ImageKey, db, S3ReferenceItem } from './model'
import * as getStream from 'get-stream'
import base64url from 'base64url';

/*-------
  Helpers
---------*/

// Quotes: https://github.com/aws/aws-sdk-net/issues/815
// ETag:   echo -n 'This is not an image' |openssl dgst -md5
// SHA256: echo -n 'This is not an image' |openssl dgst -sha256
const notImageData = 'This is not an image'
const notImageETag = '\"2c3cd6f2e5c6fc39aba84f8a622450cd\"'
const notImageSHA256 = base64url.encode(Buffer.from('fdc5aca2dc1c8602fcdb5c458ee9deaa6001eb672006026b6adb4274b0f73151', 'hex'))
const notImageS3Key = { sha256: notImageSHA256, mimetype: 'application/octet-stream' }

const expectNoSuchKey = (promise: Promise<object>) =>
  expect(promise).rejects.toMatchObject({ code: "NoSuchKey", message: "The specified key does not exist." });
const expectUploadToNotExist = (id: string) => expectNoSuchKey(s3.getUpload(id).promise());
const expectImageToNotExist = (key: S3ImageKey) => expect(s3.headImage(key).promise()).rejects.toMatchObject({ code: "NotFound" });


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
    let imageMetaStream = s3.streamMetadata(body)
    let putResponse = await s3.upload(id, body).promise()

    expect(putResponse.ETag).toStrictEqual(notImageETag)
    let image_fetched = await getStream(s3.getUpload(id).createReadStream())
    expect(image_fetched).toEqual(notImageData)

    // Roundtrip check
    let imageMeta = await imageMetaStream
    let imageComputed = await s3.calculateMetadataFromUpload(id)

    expect(imageComputed.sha256).toEqual(notImageSHA256)
    expect(imageMeta.sha256).toEqual(notImageSHA256)
    expect(imageMeta).toEqual(imageComputed)
    expect(`"${imageMeta.md5}"`).toEqual(notImageETag)

    // Cleanup after above tests
    await s3.deleteFromUploadBucket(id).promise()
    expectUploadToNotExist(id)
  });

  describe('should support moving an uploaded image to content-addressable storage', () => {
    it('should delete the upload after copying to image bucket', async () => {
      const id = 'some-upload-to-be-moved'
      const data = 'This is not an image'
      const image = { sha256: 'not really a hash', mimetype: 'application/octet-stream' }
      let uploadResult = await s3.upload(id, data).promise()
      expect(uploadResult.ETag).toBe(notImageETag)
      let { awsResults: { s3Copy } } = await s3.moveUploadToImageBucket(id, notImageETag, image, async () => {
        let uploaded = await s3.getUpload(id).promise()
        expect(uploaded.ETag).toBe(notImageETag) // Check the upload still exists
      })
      expect(s3Copy.CopyObjectResult.ETag).toBe(notImageETag)
      expectUploadToNotExist(id)

      // Cleanup after above tests
      await s3.deleteUnreferencedImage(image).promise()
      expectImageToNotExist(image)
    });

    it('should FAIL when both upload (ID) and destination (SHA-256) do not exist', async () => {
      const key = { sha256: 'FAKE-SHA256', mimetype: 'French' }
      expectImageToNotExist(key)
      try {
        await s3.moveUploadToImageBucket('never-uploaded', 'bogus', key, () => Promise.reject(new Error('Test failed: Copy should not succeed')))
        throw 'Test failed';
      }
      catch (reason) {
        expect(reason).toMatchObject({ code: "NoSuchKey", message: "The specified key does not exist." });
      }
    });

    it('should SUCCEED when upload (ID) does not exist, but destination (SHA-256) does', async () => {
      await s3.upload('just-uploaded', notImageData).promise()
      await s3.moveUploadToImageBucket('just-uploaded', notImageETag, notImageS3Key, () => Promise.resolve())
      let result = await s3.moveUploadToImageBucket('never-uploaded', 'bogus', notImageS3Key, () => Promise.resolve('Post copy op'))
      expect(result.afterCopyCompleted).toBe('Post copy op')
      expect(result.awsResults.s3Delete).not.toBeNull()
      expect(result.awsResults.s3Head.ETag).toBe(notImageETag)

      // Cleanup after above tests
      await s3.deleteUnreferencedImage(notImageS3Key).promise()
      expectImageToNotExist(notImageS3Key)
    });

    it('should not fail when upload is already deleted (due to Streams handler or other process) after copying', async () => {
      const id = 'some-upload-to-be-moved-2'
      const data = 'This is not an image 2'
      const notImageETag = '\"a260985349903b35c47eb6f29f64bd4f\"'
      const image = { sha256: 'not really a hash 2', mimetype: 'application/octet-stream' }
      await s3.upload(id, data).promise()
      let { awsResults: { s3Copy } } = await s3.moveUploadToImageBucket(id, notImageETag, image, async () => {
        await s3.deleteFromUploadBucket(id).promise()
        expectUploadToNotExist(id)
      })
      expect(s3Copy.CopyObjectResult.ETag).toBe(notImageETag)
      expectUploadToNotExist(id)
      expect(await s3.headImage(image).promise()).toMatchObject({ ContentLength: notImageData.length + 2, ETag: notImageETag })

      // Cleanup after above tests
      await s3.deleteUnreferencedImage(image).promise()
      expectImageToNotExist(image)
    });
  });
});

describe('DynamoDB ImageRepository Table operations', () => {
  const imageID = 'ref-' + Math.random()
  const imageID2 = 'ref2-' + Math.random()
  const sha = notImageSHA256
  let now = new Date()
  let imageRef: S3ReferenceItem = {
    sha256: sha,
    md5: notImageETag,
    mimetype: 'application/octet-stream',
    size: notImageData.length,
    width: 123,
    height: 456,
    uploadCompletedAt: now,
    images: [imageID],
    lastFileName: 'not-an-image.txt'
  }

  describe('for managing references to content-addressable de-duplicated S3 objects', () => {

    it('should not crash when deleting a non-existent reference item', async () => {
      // Ensure key does not exist, deleting it twice
      await db.deleteReferenceItem(sha).promise();
      await db.deleteReferenceItem(sha).promise();
      let existingRefs = await db.getReferenceItem(sha).promise()
      expect(existingRefs).toStrictEqual({});
    });

    it('should support adding one or more references', async () => {
      // Create the fake reference
      let addRef = db.addReferences(imageRef)
      let result = await addRef.promise()
      expect(result.ConsumedCapacity.CapacityUnits).toBe(1)
      // Ensure key does now exist
      let expectedImageRef = { ...imageRef, uploadCompletedAt: imageRef.uploadCompletedAt.toISOString() }
      let storedRefs = await db.getReferenceItem(sha).promise()
      expect({ ...storedRefs.Item, images: storedRefs.Item.images.values }).toMatchObject(expectedImageRef);
      // Add another ref
      let imageRef2 = { ...imageRef, images: [imageID2] } // Only 1 imageID, it should merge with (add to) `images` set
      let updateResult = await db.addReferences(imageRef2).promise()
      expect(updateResult.ConsumedCapacity.CapacityUnits).toBe(1)
      // Check update result included the new set
      expect(updateResult.Attributes.images.values).toStrictEqual([imageID, imageID2])
      // Ensure imageID was added to set
      let { Item: { images, ...updatedItem } } = await db.getReferenceItem(sha).promise()
      expect({ ...updatedItem, images: undefined }).toMatchObject({ ...expectedImageRef, images: undefined });
      let receivedImageSet: string[] = images.values
      expect(receivedImageSet).toStrictEqual([imageID, imageID2])
    })

    it('should support getting reference items', async () => {
      // Ensure key exists (from previous test)
      let storedRefs = await db.getReferenceItem(sha).promise()
      expect(storedRefs.Item.images).not.toBeNull();
    })

    it('should support removing one or more references', async () => {
      // Delete the first test reference and ensure it is removed
      await db.removeReferences(sha, [imageID]).promise();
      let { Item } = await db.getReferenceItem(sha).promise()
      expect(Item.images.values).toStrictEqual([imageID2]);
    })

    it('should support removing reference items', async () => {
      // Delete the test reference and ensure it is removed
      await db.deleteReferenceItem(sha).promise();
      let item = await db.getReferenceItem(sha).promise()
      expect(item).toStrictEqual({});
    })
  })
})
