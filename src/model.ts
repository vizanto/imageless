import { S3, DynamoDBStreams, DynamoDB, AWSError } from "aws-sdk";
import { PromiseResult } from "aws-sdk/lib/request";
import { Body } from "aws-sdk/clients/s3";
import { createHash } from "crypto";
import base64url from "base64url";
import { Readable, PassThrough } from "stream";
import * as streamMeter from "stream-meter";
var scuid: () => string = require('scuid');

/*----------
  Data Types
------------*/
type CUID = string
type URL_Safe_Base64_SHA256 = string
type Base16_MD5 = string

export const mimetypeFileExtension = {
  "image/png":  ".png",
  "image/jpeg": ".jpg",
  "image/gif":  ".gif"
}

export interface S3ImageKey {
  sha256: URL_Safe_Base64_SHA256
  mimetype: string
}
export interface ImageBlobData extends S3ImageKey {
  md5: Base16_MD5
  uploadCompletedAt: Date
  size: number
  width: number
  height: number
}
export interface S3ReferenceItem extends ImageBlobData {
  images: CUID[];
  lastFileName: string;
}
export interface ImageInput extends ImageBlobData {
  name: string
}
export interface Image extends ImageBlobData {
  cuid: CUID
  createdAt: Date
  collections?: CUID[]
  name: string
  s3url: string
}
export interface CollectionItem {
  cuid: CUID;
  name: string;
  images: CUID[];
}


/*------
  S3 ops
--------*/
export class S3ImageRepositoryBuckets {
  private s3: S3;
  readonly uploadBucket: string;
  readonly imagesBucket: string;
  /**
   * Size threshold when to start uploading in chunks.
   * See `partSize` option: https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3/ManagedUpload.html#constructor-property */
  readonly imagePartSize: number;

  constructor(s3: S3, imagesBucketName?: string, uploadBucketName?: string, imagePartSizeInBytes?: number) {
    this.s3 = s3;
    this.uploadBucket = uploadBucketName || 'upload';
    this.imagesBucket = imagesBucketName || 'images';
    this.imagePartSize = imagePartSizeInBytes || /* 50MB */50 * 1024 * 1024;
  }

  imageS3Key({ sha256, mimetype }: S3ImageKey) {
    return sha256 + (mimetypeFileExtension[mimetype] || '')
  }

  urlOf(key: S3ImageKey) {
    return this.s3.config.endpoint + '/' + this.imagesBucket + '/' + this.imageS3Key(key)
  }

  upload(id: CUID, body: Body) {
    let params: S3.PutObjectRequest = {
      Bucket: this.uploadBucket,
      Key: id,
      Body: body
    }
    return this.s3.upload(params, { partSize: this.imagePartSize, leavePartsOnError: false })
  }

  async streamMetadata(readable: Readable): Promise<ImageBlobData> {
    let meter = streamMeter(this.imagePartSize) // Limit uploads to a single part. Ensures S3 ETags are always an MD5 hash
    let SHA256 = createHash('SHA256')
    let MD5 = createHash('MD5')
    readable.on('data', (chunk) => {
      // Don't expect wrapping above the definitions in readable.pipe() to just work, because NodeJS is shit.
      // We can pipe after (!!!) defining the promise, but that's not less lines of code or less error prone.
      meter.write(chunk)
      SHA256.write(chunk)
      MD5.write(chunk)
    })
    return new Promise((resolve, reject) => {
      readable.on('error', reject) //TODO: Test if size limit rejects!
      readable.on('end', () => {
        let now = new Date();
        now.setMilliseconds(0); //S3 stores Last-Modified with per-second precision
        resolve({
          size: meter.bytes,
          md5: MD5.digest().toString("hex"),
          sha256: base64url.encode(SHA256.digest()),
          uploadCompletedAt: now,
          mimetype: "application/octet-stream", //FIXME
          width: 0xDEADBEEF, //FIXME
          height: 0xDEADBEEF, //FIXME
        });
      });
    });
  }

  getUpload(id: CUID) {
    return this.s3.getObject({ Bucket: this.uploadBucket, Key: id }, undefined);
  }

  /**
   * Read an already created object from S3 to calculate its metadata.
   * Especially useful to process Pre-signed URL client uploads.
   * @param cuid The upload S3-key
   */
  async calculateMetadataFromUpload(cuid: CUID): Promise<ImageBlobData> {
    let upload = await this.getUpload(cuid).promise();
    // console.log("Got object", cuid, upload)
    let readable = readableBody(upload.Body);
    let meta = await this.streamMetadata(readable);
    if (meta.size != upload.ContentLength) {
      throw new Error(`Expected upload:${cuid} to be of size: ${meta.size}, but S3 size is: ${upload.ContentLength}`);
    }
    return { ...meta, uploadCompletedAt: upload.LastModified };
  }

  protected copyFromUploadBucket(cuid: string, eTag: string, key: S3ImageKey) {
    return this.s3.copyObject({
      CopySource: this.uploadBucket + "/" + cuid,
      CopySourceIfMatch: eTag,
      ContentType: key.mimetype,
      Bucket: this.imagesBucket,
      Key: this.imageS3Key(key)
    }, null)
  }

  /**
   * Moves an upload to image bucket
   * @param cuid uploaded object (source)
   * @param eTag checksum by S3, usually Base16 MD5 String wrapped in double quotes (")
   * @param image destination
   * @param onImageReady called as soon as object in image bucket exists, usually to Create or Update an Image DynamoDB item for this upload
   */
  async moveUploadToImageBucket<T>(cuid: string, eTag: string, image: S3ImageKey, onImageReady: (image: "copied" | "existed") => Promise<T>) {
    // 1. Copy from upload to image bucket if not exists
    let copyOp = this.copyFromUploadBucket(cuid, eTag, image).promise();
    let copyResult: PromiseResult<S3.CopyObjectOutput, AWSError>;
    try {
      copyResult = await copyOp;
    } catch (reason) {
      if (reason.statusCode == 404) {
        try {
          // Check if the intended destination exists
          let headResult = await s3.headImage(image).promise();
          // It exists! Continue as if upload was copied...
          return {
            awsResults: { s3Head: headResult },
            afterCopyCompleted: await onImageReady("existed")
          }
        } catch {
          throw reason; // Nope. Abort!
        }
      }
    }
    // 2. Notify caller copy was completed
    let handlerResult = await onImageReady("copied");
    // 3. Success! Clean up the S3 upload bucket
    let deleteOp = this.deleteFromUploadBucket(cuid).promise();
    let deleteResult = await deleteOp;
    return {
      awsResults: { s3Copy: copyResult, s3Delete: deleteResult },
      afterCopyCompleted: handlerResult
    }
  }

  headImage(key: S3ImageKey) {
    return this.s3.headObject({ Bucket: this.imagesBucket, Key: this.imageS3Key(key) }, undefined);
  }

  deleteFromUploadBucket(cuid: string) {
    return this.s3.deleteObject({ Bucket: this.uploadBucket, Key: cuid }, null)
  }

  deleteUnreferencedImage(key: S3ImageKey) {
    return this.s3.deleteObject({ Bucket: this.imagesBucket, Key: this.imageS3Key(key) }, null);
  }
}


/*---------------
  DynamoDB Tables
-----------------*/
export const object_to_updateItemInput = (tableName: string, key: DynamoDB.DocumentClient.Key, SET: object, ADD?: object, REMOVE?: object): DynamoDB.DocumentClient.UpdateItemInput => {
  var expr: string = "";
  let values = {};
  for (const keyName in SET) {
    if (SET.hasOwnProperty(keyName)) {
      const ifNotExists = keyName.startsWith("?")
      const key = ifNotExists ? keyName.substr(1) : keyName;
      const setAttr = ifNotExists ? "if_not_exists(" + key + ", :" + key + ")" : key + " = :" + key
      if ("" === expr) expr = "SET "; else expr += ", "
      expr += setAttr;
      const value = SET[key];
      values[":" + key] = value;
    }
  }
  if (ADD) {
    expr += " ADD " + Object.keys(ADD).map(k => { values[":" + k] = ADD[k]; return k + " :" + k; }).join(", ")
  }
  if (REMOVE) {
    expr += " REMOVE " + Object.keys(REMOVE).map(k => { values[":" + k] = REMOVE[k]; return k + " :" + k; }).join(", ")
  }
  return { TableName: tableName, Key: key, UpdateExpression: expr, ExpressionAttributeValues: values }
}

const { S3REFS_TABLE, IMAGES_TABLE, COLLECTIONS_TABLE } = process.env;

export class DynamoDBImageRepositoryTables {
  readonly db: DynamoDB.DocumentClient;

  constructor(dynamoDb: DynamoDB.DocumentClient) {
    this.db = dynamoDb;
  }

  addReference(imageRef: S3ReferenceItem) {
    let { sha256, images, uploadCompletedAt, ...attributes } = imageRef
    let params = object_to_updateItemInput(S3REFS_TABLE, { "sha256": sha256 },
      { ...attributes, uploadCompletedAt: uploadCompletedAt.toISOString() },
      { images: this.db.createSet(images) }
    );
    // console.log("About to update", imageRef, "with", params.UpdateExpression)
    params.ReturnValues = "ALL_NEW"
    params.ReturnConsumedCapacity = "TOTAL"
    params.ReturnItemCollectionMetrics = "SIZE"
    return this.db.update(params)
  }

  getReferences(sha256: URL_Safe_Base64_SHA256, consistentRead = true) {
    return this.db.get({ TableName: S3REFS_TABLE, Key: { "sha256": sha256 }, ConsistentRead: consistentRead })
  }

  deleteReferenceItem(sha256: URL_Safe_Base64_SHA256) {
    return this.db.delete({ TableName: S3REFS_TABLE, Key: { "sha256": sha256 } })
  }

  /**
   * Creates a new Image item,
   *  or if it exists but has a different SHA-256 assigned to it: replaces its ImageBlobData attributes.
   *
   * Does nothing if an Image with given CUID and SHA-256 already exists.
   *
   * @param cuid Image ID
   * @param input Image item attributes, requires sha256 and all S3-object related properties
   */
  createImage(cuid: string, newImage: Readonly<ImageInput>, creationTimestamp: Date | "now") {
    let { uploadCompletedAt } = newImage;
    let createdAt = (creationTimestamp == "now" ? new Date() : creationTimestamp)
    let input = Object.freeze({
      ...newImage,
      "?createdAt": createdAt.toUTCString(),
      uploadCompletedAt: uploadCompletedAt.toUTCString(),
    });
    let params = object_to_updateItemInput(IMAGES_TABLE, { "cuid": { "S": cuid } }, input);
    params.ConditionExpression = 'sha256 != :sha256'
    // Construct an Image object from the database input, excluding ?createdAt
    // See also: https://stackoverflow.com/questions/34698905/clone-a-js-object-except-for-one-key
    const image: Image = (({ '?createdAt': string, ...imageData }) => ({
      ...imageData,
      cuid: cuid,
      createdAt: createdAt,
      uploadCompletedAt: uploadCompletedAt,
      s3url: s3.urlOf(newImage)
    }))(input);
    return { image: Object.freeze(image), dbParams: params, dbUpdateResult: this.db.update(params) };
  }
}


/*------------------------
  DynamoDB Stream handlers
--------------------------*/
type AWSResult<T extends object> = PromiseResult<T, AWSError>
type AWSResults = AWSResult<object>[]
type AWSPromises = Promise<AWSResults>

interface AWSCompositeOp {
  awsResults: { [key: string]: AWSResult<object> }
}

/**
 * Methods to keep S3 buckets and S3Reference Table in sync
 */
export class ImageRepository_DynamoDB_StreamHandler {
  readonly s3: S3ImageRepositoryBuckets;
  readonly db: DynamoDBImageRepositoryTables;

  constructor(s3: S3ImageRepositoryBuckets, db: DynamoDBImageRepositoryTables) {
    this.s3 = s3;
    this.db = db;
  }

  async _moveUploadAndCreateImageItem(cuid: string, newImage: ImageInput) {
    // Prepare to Create (or Update) an Image DynamoDB item using the S3-reference
    let createImage = () => this.db.createImage(cuid, newImage, "now").dbUpdateResult.promise();
    // Move the S3 object from upload to image bucket
    let moveOp = await this.s3.moveUploadToImageBucket(cuid, newImage.md5, newImage, createImage);
    // Success!
    return {
      awsResults: { ...moveOp.awsResults, dbUpdate: moveOp.afterCopyCompleted },
      image: null
    }
  }

  async _concurrentMoveUploadsAndCreateImageItems(cuids: string[], sha256: string, newImage: DynamoDBStreams.AttributeMap): AWSPromises {
    let jobs: Promise<AWSCompositeOp>[] = [];
    for (const cuid of cuids) {
      jobs.push(this._moveUploadAndCreateImageItem(cuid, {
        sha256: sha256,
        md5: newImage.md5.S,
        uploadCompletedAt: new Date(newImage.uploadCompletedAt.S),

        name: newImage.lastFileName.S,
        size: parseInt(newImage.size.N),
        mimetype: newImage.mimetype.S,

        width: parseInt(newImage.width.N),
        height: parseInt(newImage.height.N)
      }));
    }
    return Promise.all(jobs)
      .then(job => job.reduce((array: AWSResults, result) => array.concat(Object.values(result.awsResults)), []))
  }

  async _handleS3ReferenceTableEvent(event: DynamoDBStreams.Record): AWSPromises {
    let record = event.dynamodb;
    let sha256 = record.Keys.sha256.S;
    let newImage = record.NewImage;

    switch (event.eventName) {
      case "INSERT":
        // Copy from upload bucket to images bucket when entirely new references are added.
        // This is to ensure completed uploads are eventually moved to the Image bucket and
        //  corresponding Image items created in DynamoDB.
        // If the upload mutation handler completed successfully, this should end up doing nothing.
        return this._concurrentMoveUploadsAndCreateImageItems(newImage.images.SS, sha256, newImage)
      case "REMOVE":
        // Delete from S3 when all references to an S3-object are removed from DynamoDB
        let deleteOp = s3.deleteUnreferencedImage({ sha256: sha256, mimetype: record.OldImage.mimetype.S }).promise()
        return deleteOp.then(r => [r])
      case "MODIFY":
        // Synchronize changes to S3-references items with actual S3 storage
        // Clearing the set of references will trigger a delete from S3 and item delete from
        let oldImageRef = record.OldImage.images.SS;
        let newImageRef = newImage.images.SS;
        //-- Check for reference count changes
        // A. Empty set, no more references to S3. Delete the blob!
        if (newImageRef.length == 0) {
          return this.db.deleteReferenceItem(sha256).promise().then(r => [r])
          // The "REMOVE" event handler will clean up S3
        }
        // B. Set has grown, copy the uploaded file to long-term S3 bucket
        else if (newImageRef.length > oldImageRef.length) {
          let newRefs = newImageRef.filter(v => oldImageRef.indexOf(v) == -1)
          return this._concurrentMoveUploadsAndCreateImageItems(newRefs, sha256, newImage)
        }
    }

    //-- Ignore all other modifications
    return Promise.resolve([])
  }

  async handleS3ReferenceTableEventBatch(eventsBatch: DynamoDBStreams.Record[]): AWSPromises {
    //1. Group all events by SHA-256 primary key in event order
    let groupedEvents = new Map<string, DynamoDBStreams.Record[]>()
    for (let i = 0; i < eventsBatch.length; i++) {
      const event = eventsBatch[i];
      const dynamoDb = event.dynamodb
      const key = dynamoDb.Keys.sha256.S
      const recordGroup = groupedEvents.get(key)
      if (!recordGroup) {
        groupedEvents.set(key, [event])
      } else {
        recordGroup.push(event)
      }
    }
    //2. Handle all events in parallel that have the same primary key
    let jobs: AWSPromises[] = [];
    for (const events of groupedEvents.values()) {
      if (!events[0]) throw new Error('impossible: zero events in group');
      var lastPromise: AWSPromises = this._handleS3ReferenceTableEvent(events[0]);
      for (let i = 1; i < events.length; i++) {
        const next = events[i];
        lastPromise = lastPromise.then(() => this._handleS3ReferenceTableEvent(next));
      }
      jobs.push(lastPromise)
    }
    //3. Flatten results on completion
    const job = await Promise.all(jobs);
    return job.reduce((result, array) => result.concat(array), []);
  }
}


/*----------------------
  Node.js Stream helpers
------------------------*/
export function readableBody(body: Body): Readable {
  if (body instanceof Readable) {
    return body
  } else {
    let blob = new PassThrough()
    blob.end(body)
    return blob
  }
}


/*--------------------------
  High level ImageRepository
----------------------------*/
export class ImageRepository {
  readonly s3: S3ImageRepositoryBuckets;
  readonly db: DynamoDBImageRepositoryTables;

  constructor(s3: S3ImageRepositoryBuckets, db: DynamoDBImageRepositoryTables) {
    this.s3 = s3;
    this.db = db;
  }

  async createFrom(body: Body, name: string): Promise<Image> {
    const id = scuid()
    let readable = readableBody(body)

    //1. Stream body to temporary upload bucket, while calculating SHA-256 and metadata
    let metadata = s3.streamMetadata(readable)
    let uploadResult = await this.s3.upload(id, readable).promise()
    let uploadCompleted = new Date(); //<-- Can't get from S3 PUT response???

    //2. Check the upload was successful
    //TODO: test what happens when size limit was reached during upload
    let imageBlobData = await metadata
    let { md5 } = imageBlobData
    if (md5 != uploadResult.ETag) {
      // S3's ETag will be a MD5 hash only in certain cases, e.g. no server-side encryption and size below 5GB.
      // See docs for details: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
      throw new Error("Body corrupted during transfer. Expected MD5: " + md5 + ", but S3 got: " + uploadResult.ETag);
    }

    //3. Add a record to reference table to ensure the upload is eventually moved to the images bucket
    let imageRef: S3ReferenceItem = {
      ...imageBlobData,
      uploadCompletedAt: uploadCompleted,
      images: [id],
      lastFileName: name
    };
    await this.db.addReference(imageRef);

    //4. Don't just wait for DynamoDB Streams handler, start the move process immediately!
    //   Create (or Update) an Image DynamoDB item using the S3-reference
    let image: Image;
    let createImage = () => {
      let result = this.db.createImage(id, { ...imageRef, name: imageRef.lastFileName }, "now");
      image = result.image;
      return result.dbUpdateResult.promise();
    }
    await this.s3.moveUploadToImageBucket(id, md5, imageRef, createImage);
    return image;
  }
}


/*------------------------
  Default Image Repository
--------------------------*/
export const IS_OFFLINE = process.env.IS_OFFLINE === 'true';
export const s3 = new S3ImageRepositoryBuckets(
  new S3(IS_OFFLINE ? {
    region: 'eu-central-1',
    s3ForcePathStyle: true,
    accessKeyId: 'S3RVER',
    secretAccessKey: 'S3RVER',
    endpoint: 'http://localhost:4572'
  } : {})
);
export const db = new DynamoDBImageRepositoryTables(
  new DynamoDB.DocumentClient(
    IS_OFFLINE ? {
      region: 'eu-central-1',
      accessKeyId: 'DEFAULT_ACCESS_KEY',
      secretAccessKey: 'DEFAULT_SECRET',
      endpoint: 'http://localhost:8000'
    } : {})
);
export const images = new ImageRepository(s3, db);
export const stream = new ImageRepository_DynamoDB_StreamHandler(s3, db);

console.log(COLLECTIONS_TABLE); // Make TypeScript shut up about unused reference for now
