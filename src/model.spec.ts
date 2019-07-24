import { defaultStores, readableBody, IS_OFFLINE, S3ImageKey, S3ReferenceItem } from './model'
import * as getStream from 'get-stream'
import base64url from 'base64url';
const { s3, db } = defaultStores
var scuid: () => string = require('scuid');

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

  it('should store objects correctly, and roundtrips result in the same meta-data (allowing uploadCompleteAt to be off by 1 second)', async () => {
    const id = 'not-an-image'
    let body = readableBody(notImageData)
    let imageMetaStream = s3.streamMetadata(body)
    let putResponse = await s3.upload(id, body).promise()

    expect(putResponse.ETag).toStrictEqual(notImageETag)
    let image_fetched = await getStream(s3.getUpload(id).createReadStream())
    expect(image_fetched).toEqual(notImageData)

    // Roundtrip check
    let { uploadCompletedAt: uploadCompletedStamp, ...imageMeta } = await imageMetaStream
    let { uploadCompletedAt: lastModifiedStamp, ...imageComputed } = await s3.calculateMetadataFromUpload(id)
    // Sometimes the Readable ends (upload completes) in the second just before S3 finishes
    // and S3's Last-Modified timestamp ends up as that next second.
    expect(uploadCompletedStamp.valueOf() + 1000).toBeGreaterThanOrEqual(lastModifiedStamp.valueOf())
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
        await s3.moveUploadToImageBucket('never-uploaded', 'bogus', key, (action) => Promise.reject(new Error('Test failed: Copy should not succeed, but: ' + action)))
        throw 'Test failed';
      }
      catch (reason) {
        expect(reason).toMatchObject({ code: "NoSuchKey", message: "The specified key does not exist." });
      }
    });

    it('should SUCCEED when upload (ID) does not exist, but destination (SHA-256) does', async () => {
      await s3.upload('just-uploaded', notImageData).promise()
      let copied = await s3.moveUploadToImageBucket('just-uploaded', notImageETag, notImageS3Key, (action) => Promise.resolve(action))
      expect(copied.afterCopyCompleted).toBe('copied')
      let result = await s3.moveUploadToImageBucket('never-uploaded', 'bogus', notImageS3Key, (action) => Promise.resolve(action))
      expect(result.afterCopyCompleted).toBe('existed')
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
  let now = new Date();
  now.setMilliseconds(0);
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
      let result = await db.addReferences(imageRef)
      expect(result).toStrictEqual(imageRef)
      // Ensure key does now exist
      let expectedImageRef = { ...imageRef, uploadCompletedAt: imageRef.uploadCompletedAt.toISOString() }
      let storedRefs = await db.getReferenceItem(sha).promise()
      expect({ ...storedRefs.Item, images: storedRefs.Item.images.values }).toMatchObject(expectedImageRef);
      // Add another ref
      let imageRef2 = { ...imageRef, images: [imageID2] } // Only 1 imageID, it should merge with (add to) `images` set
      let updateResult = await db.addReferences(imageRef2)
      // Check update result included the new set
      expect(updateResult).toStrictEqual({ ...imageRef2, images: [imageID, imageID2] })
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

    it('should FAIL when S3-key and checksum related ImageBlobData for a reference changes', async () => {
      expect(db.addReferences({ ...imageRef, md5: "bogus" })).rejects.toMatchObject({ message: "The conditional request failed" })
      expect(db.addReferences({ ...imageRef, size: 1234 })).rejects.toMatchObject({ message: "The conditional request failed" })
      expect(db.addReferences({ ...imageRef, mimetype: "text/plain" })).rejects.toMatchObject({ message: "The conditional request failed" })
      expect(db.addReferences({ ...imageRef, width: 1337 })).resolves.toMatchObject({ width: 1337 })
    });

    it('should keep the first stored `uploadCompleteAt` timestamp', async () => {
      let updateResult = await db.addReferences({ ...imageRef, height: 1337, uploadCompletedAt: new Date(2019, 7, 23) })
      expect(updateResult.uploadCompletedAt).toEqual(imageRef.uploadCompletedAt)
      expect(imageRef.height).not.toBe(1337)
      expect(updateResult.height).toBe(1337)
    });

    it('should support removing one or more references', async () => {
      // Delete the first test reference and ensure it is removed
      let { images } = await db.removeReferences(sha, [imageID]);
      expect(images).toStrictEqual([imageID2]);
      let { Item } = await db.getReferenceItem(sha).promise()
      expect(Item.images.values).toStrictEqual(images);
    })

    it('should support removing reference items', async () => {
      // Delete the test reference and ensure it is removed
      await db.deleteReferenceItem(sha).promise();
      let item = await db.getReferenceItem(sha).promise()
      expect(item).toStrictEqual({});
    })
  })

  describe('for `Image` create/read/update/delete (CRUD) functionality', () => {
    const cuid = scuid()
    let { images, lastFileName, ...imageInput } = imageRef;
    let input = { ...imageInput, title: lastFileName };

    it('should support creating an Image', async () => {
      let result = await db.createImage(cuid, input, "now")
      // console.log(result)
      expect(result.image).toMatchObject(input)
    });

    it("should reject replacing an Image's hashes outside of a transaction that updates the related `S3ReferenceItem.images`", async () => {
      const inputInvalid = { message: "Change of ImageBlobData requires a valid sha256, md5, and size" }
      const changeInvalid = { message: "Changes to ImageBlobData must atomically update related S3ReferenceItems" }
      const shouldRejectBlobData = (p) => expect(db.createImage(cuid, p, "now")).rejects
      // Test changing hash with incomplete data
      shouldRejectBlobData({ ...imageInput, title: lastFileName, sha256: "DIFFERENT BLOB", md5: undefined }).toMatchObject(inputInvalid)
      shouldRejectBlobData({ ...imageInput, title: lastFileName, md5: "DIFFERENT BLOB", sha256: undefined }).toMatchObject(inputInvalid)
      shouldRejectBlobData({ ...imageInput, title: lastFileName, md5: "DIFFERENT", sha256: "DIFFERENT", size: undefined }).toMatchObject(inputInvalid)
      // Test changing only 1 hash
      shouldRejectBlobData({ ...imageInput, title: lastFileName, sha256: "DIFFERENT BLOB WITHOUT CHANGING MD5" }).toMatchObject(changeInvalid)
      shouldRejectBlobData({ ...imageInput, title: lastFileName, md5: "DIFFERENT BLOB WITHOUT CHANGING SHA256" }).toMatchObject(changeInvalid)
      // Changing both hashes should only be allowed in a transaction
      shouldRejectBlobData({ ...imageInput, title: lastFileName, sha256: "DIFFERENT BLOB", md5: "DIFFERENT MD5" }).toMatchObject(changeInvalid)
    })

    it('should support deleting an Image', async () => {
      let result = await db.deleteImageItem(cuid).promise()
      expect(result).not.toBeNull()
      //TODO: Update Collections in DynamoDB Streams handler
    });
  })
})
