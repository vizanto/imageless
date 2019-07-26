import { defaultStores, readableBody, IS_OFFLINE, S3ImageKey, S3ReferenceItem, stream } from './model'
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

  it('upload->download roundtrips should result in the same meta-data (allowing uploadCompleteAt to be off by 1 second)', async () => {
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

  describe('should support moving an uploaded image to immutable content-addressable storage', () => {
    it('should delete the upload after copying to immutable image bucket', async () => {
      const id = 'some-upload-to-be-moved'
      const data = 'This is not an image'
      const image = { sha256: 'not really a hash', mimetype: 'application/octet-stream' }
      let uploadResult = await s3.upload(id, data).promise()
      expect(uploadResult.ETag).toBe(notImageETag)
      let { awsResults: { s3Copy } } = await s3.moveUploadToImageBucket(id, notImageETag, image, false, async () => {
        let uploaded = await s3.getUpload(id).promise()
        expect(uploaded.ETag).toBe(notImageETag) // Check the upload still exists
      })
      expect(s3Copy.CopyObjectResult.ETag).toBe(notImageETag)
      expectUploadToNotExist(id)

      // Check object is immutable
      let immutableImage = await s3.headImage(image).promise()
      expect(immutableImage.CacheControl).toBe("public,max-age=31536000,immutable")

      // Cleanup after above tests
      await s3.deleteUnreferencedImage(image).promise()
      expectImageToNotExist(image)
    });

    it('should FAIL when both upload (ID) and destination (SHA-256) do not exist', async () => {
      const key = { sha256: 'FAKE-SHA256', mimetype: 'French' }
      expectImageToNotExist(key)
      try {
        await s3.moveUploadToImageBucket('never-uploaded', 'bogus', key, false, (action) => Promise.reject(new Error('Test failed: Copy should not succeed, but: ' + action)))
        fail("Move completed when it shouldn't");
      }
      catch (reason) {
        expect(reason).toMatchObject({ relatedObject: { uploadMissing: true, image: key }, s3Response: { code: "NotFound" } });
      }
    });

    it('should SUCCEED when upload (ID) does not exist, but destination (SHA-256 and MD5-ETag) does', async () => {
      /**
       * - This can happen when an S3ReferenceItem is processed where the uploaded S3-object is already deleted or expired
       */
      for (let headFirst of [false, true]) try {
        // Sanity check upload and move works properly, and create the image for `notImageS3Key`
        let id = 'just-uploaded-' + headFirst
        await s3.upload(id, notImageData).promise()
        let copied = await s3.moveUploadToImageBucket(id, notImageETag, notImageS3Key, true, (action) => Promise.resolve(action))
        expect(copied.afterCopyCompleted).toBe(headFirst ? 'existed' : 'copied')
        expect(copied.awsResults.s3Delete).not.toBeNull()
        // Make sure ETag (MD5) is checked
        let bogusMD5 = s3.moveUploadToImageBucket('never-uploaded', "bogus", notImageS3Key, headFirst, (action) => Promise.resolve(action))
        expect(bogusMD5).rejects.toMatchObject({ relatedObject: { eTagMismatch: true, eTag: "bogus", s3Head: { ETag: notImageETag } } })
        // Now check not-uploaded but target `notImageS3Key` exists
        let result = await s3.moveUploadToImageBucket('never-uploaded', notImageETag, notImageS3Key, headFirst, (action) => Promise.resolve(action))
        expect(result.afterCopyCompleted).toBe('existed')
        expect(result.awsResults.s3Delete).not.toBeNull()
        expect(result.awsResults.s3Head.ETag).toBe(notImageETag)
      } catch (error) {
        fail({ headFirst, error })
      }

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
      let { awsResults: { s3Copy } } = await s3.moveUploadToImageBucket(id, notImageETag, image, false, async () => {
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
    let createdAt: Date;

    it('should support creating an Image', async () => {
      let result = await db.createOrUpdateImage(cuid, input, "now")
      createdAt = result.image.createdAt
      // console.log(result)
      expect(result.image).toMatchObject(input)
    });

    it("should reject replacing an Image's hashes outside of a transaction that updates the related `S3ReferenceItem.images`", async () => {
      const inputInvalid = { message: "Change of ImageBlobData requires a valid sha256, md5, and size" }
      const changeInvalid = { message: "Changes to ImageBlobData must atomically update related S3ReferenceItems" }
      const shouldRejectBlobData = (p) => expect(db.createOrUpdateImage(cuid, p, "now")).rejects
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

    it('should support reading an Image', async () => {
      expect(await db.getImage(cuid, true)).toEqual({ ...input, cuid, createdAt })
    });

    it('should support deleting an Image', async () => {
      let result = await db.deleteImageItemOptimistically(cuid)
      expect(result.deletedImage.Attributes).not.toBeNull()
      expect(await db.getImage(cuid, true)).toBeNull()
    });
  })

  describe('for Collections create/read/update/delete (CRUD) functionality', () => {
    const cuid = scuid()
    let createdAt: Date;

    it('should support creating a Collection', async () => {
      let title = "Gif files"
      let now = new Date();
      let { lastModifiedAt, ...result } = await db.createOrUpdateCollection(cuid, title)
      createdAt = result.createdAt
      expect(createdAt.valueOf() + 1000).toBeGreaterThanOrEqual(now.valueOf())
      expect(lastModifiedAt.valueOf() + 2).toBeGreaterThanOrEqual(now.valueOf())
      expect(result).toEqual({ cuid, title, createdAt })
    });

    it('should support removing a Collection', async () => {
      let result = await db.deleteCollectionOptimistically(cuid)
      expect(result.deletedCollection).not.toBeNull()
      expect(await db.getCollection(cuid, true)).toBeNull()
    });
  })

  describe('should manage Image <---> Collections Set membership', () => {
    let
      imageCuid = scuid(),
      nonExistentImageCuid = scuid(),
      collectionCuid = scuid();

    it('adding an Image to a Collection', async () => {
      let
        image = (await db.createOrUpdateImage(imageCuid, { ...imageRef, title: "Not really an Image" }, "now", true)).image,
        collection = await db.createOrUpdateCollection(collectionCuid, "Text files");
      expect(image.collections).toBeUndefined()
      expect(collection.images).toBeUndefined()

      let addResult = await db.addToCollection(collectionCuid, [imageCuid]).promise()
      expect(addResult.ConsumedCapacity.length).toBe(2)
      expect(addResult.ConsumedCapacity[0].WriteCapacityUnits).toBe(4)

      let addResult2 = await db.addToCollection(collectionCuid, [nonExistentImageCuid]).promise()
      expect(addResult2.ConsumedCapacity.length).toBe(2)
      expect(addResult2.ConsumedCapacity[0].WriteCapacityUnits).toBe(4)

      let updatedCollection = await db.getCollection(collectionCuid, true)
      //console.log(updatedCollection)
      expect(updatedCollection.images).toEqual([imageCuid, nonExistentImageCuid])

      let updatedImage = await db.getImage(imageCuid, true)
      expect(updatedImage.collections).toEqual([collectionCuid])

      let nonExistentImage = await db.getImage(nonExistentImageCuid, true)
      expect(nonExistentImage.collections).toEqual([collectionCuid])
    });

    it('removing an Image from a Collection', async () => {
      let removeResult = await db.removeFromCollection(collectionCuid, [nonExistentImageCuid]).promise()
      expect(removeResult.ConsumedCapacity.length).toBe(2)
      expect(removeResult.ConsumedCapacity[0].WriteCapacityUnits).toBe(4)

      let updatedCollection = await db.getCollection(collectionCuid, true)
      //console.log(updatedCollection)
      expect(updatedCollection.images).toEqual([imageCuid]) // from previous test

      let updatedImage = await db.getImage(nonExistentImageCuid, true)
      expect(updatedImage.collections).toBeUndefined()
    })

    it('update referred Images when removing a Collection', async () => {
      let result = await db.deleteCollectionOptimistically(collectionCuid)
      //console.log("Delete result", result)
      expect(result.deletedCollection.Attributes.images.values).toEqual([imageCuid]) // from previous test

      let updatedImage = await db.getImage(imageCuid, true)
      expect(updatedImage.collections).toBeUndefined()
    });
  })
})

describe('DynamoDB Stream event handler', () => {
  const { Records } = require('../resources/S3Refs-test-stream.json')

  describe('synchronizing changes between S3 and the S3-Reference DynamoDB Table', () => {
    it('should ignore missing uploads for which the target immutable object is also missing', async () => {
      /**
       * - This can happen if the `image` is deleted after moving from `upload`,
       *    but before the reference record is processed by the Stream handler.
      */
      let moveOp = spyOn(s3, "moveUploadToImageBucket").and.callThrough()
      try {
        await stream.handleS3ReferenceTableEventBatch(Records)
        expect(moveOp.calls.count()).toBe(4)
      } catch (reason) {
        fail(reason)
      }
    });
  });
})
