import { S3, DynamoDBStreams, DynamoDB, AWSError } from "aws-sdk";
import { Key } from "aws-sdk/clients/dynamodb";
import { PromiseResult } from "aws-sdk/lib/request";
import { Body } from "aws-sdk/clients/s3";
import { createHash, Hash } from "crypto";
import base64url from "base64url";
import { Readable } from "stream";
import * as streamMeter from "stream-meter";
var scuid: () => string = require('scuid');

/// --------------
/// DynamoDB and S3 configuration
const S3REFS_TABLE = process.env.S3REFS_TABLE;
const IMAGES_TABLE = process.env.IMAGES_TABLE;
const COLLECTIONS_TABLE = process.env.COLLECTIONS_TABLE;
const UPLOAD_BUCKET = 'upload'
const IMAGES_BUCKET = 'images'
/// --------------


// Image metadata byte-stream handler


/// DynamoDB handlers

// function addImageReference(image: Image) {

// }

/// DynamoDB ops

const object_to_updateItemInput = (tableName: string, key: Key, input: object): DynamoDB.UpdateItemInput => {
  var expr: string = null;
  let values = {};
  for (const keyName in input) {
    if (input.hasOwnProperty(keyName)) {
      const ifNotExists = keyName.startsWith("?")
      const key = ifNotExists ? keyName.substr(1) : keyName;
      const setAttr = ifNotExists ? "if_not_exists(" + key + ", :" + key + ")" : key + " = :" + key
      if (expr == null) expr = "SET "; else expr += ", "
      expr += setAttr;
      const value = input[key];
      values[":" + key] = value;
    }
  }
  return { TableName: tableName, Key: key, UpdateExpression: expr, ExpressionAttributeValues: values }
}

function db_deleteImageReferences(key: string) {
  return dynamoDb.delete({ TableName: S3REFS_TABLE, Key: { "cuid": key } })
}

/**
 * Update the given Image attributes when it does not have the about to be saved SHA-256 assigned to it
 * @param cuid Image ID
 * @param input Image item attributes, requires sha256 and related properties at least
 */
function db_updateImageItem(cuid: string, input: object) {
  // if ((input.sha256 || "").length != )
  let params = object_to_updateItemInput(IMAGES_TABLE, { "cuid": { "S": cuid } }, input)
  params.ConditionExpression = 'sha256 != :sha256'
  return dynamoDb.update(params)
}


/// S3 ops

const mimetypeFileExtension = {
  "image/png": "png",
  "image/jpeg": "jpg",
  "image/gif": "gif"
}

const s3_urlOf = (sha256: string, mimetype: string) => {
  const url = s3.config.endpoint + '/' + IMAGES_BUCKET + '/' + sha256 + '.' + (mimetypeFileExtension[mimetype] || '')
  return url
}

function s3_deleteUnreferencedImage(sha256: string) {
  return s3.deleteObject({ Bucket: IMAGES_BUCKET, Key: sha256 }, null);
}

function s3_copyFromUploadBucket(cuid: string, md5: string, sha256: string) {
  return s3.copyObject({
    CopySource: UPLOAD_BUCKET + "/" + cuid, CopySourceIfMatch: md5,
    Bucket: IMAGES_BUCKET,
    Key: sha256
  }, null)
}

function s3_deleteFromUploadBucket(cuid: string) {
  return s3.deleteObject({ Bucket: UPLOAD_BUCKET, Key: cuid }, null)
}

function s3_ignoreNoSuchKeyError(reason: AWSError) {
  if ("NoSuchKey" != reason.code) throw reason;
  else console.log("Ignored AWSError", reason);
}


/// DynamoDB Stream handlers

interface NewImageInput {
  sha256: string,
  md5: string,

  uploadCompletedAt: Date,

  name: string,
  size: number,
  mimetype: string,

  width: number,
  height: number
}

type AWSResults = PromiseResult<object, AWSError>[]
type AWSPromises = Promise<AWSResults>

async function s3db_moveUploadAndCreateImageItem(cuid: string, newImage: NewImageInput): AWSPromises {
  let results: AWSResults = [];
  let sha256 = newImage.sha256;
  let md5 = newImage.md5;
  // 1. Copy from upload to image bucket if not exists
  let copyOp = s3_copyFromUploadBucket(cuid, md5, sha256).promise()
  copyOp.catch(s3_ignoreNoSuchKeyError)
  await copyOp.then(r => results.push(r))
  // 2. Create or Update an Image DynamoDB item for this reference
  let mimetype = newImage.mimetype;
  await db_updateImageItem(cuid, {
    sha256: sha256,
    md5: md5,

    createdAt: new Date(),
    uploadCompletedAt: newImage.uploadCompletedAt,

    "?name": newImage.name,
    blob: s3_urlOf(cuid, mimetype),
    size: newImage.size,
    mimetype: mimetype,

    width: newImage.width,
    height: newImage.height
  }).promise().then(r => results.push(r));
  // 3. Success! Clean up the S3 upload bucket
  let deleteOp = s3_deleteFromUploadBucket(cuid).promise()
  deleteOp.catch(s3_ignoreNoSuchKeyError)
  return deleteOp.then(r => { results.push(r); return results })
}

function s3db_concurrentMoveUploadsAndCreateImageItems(cuids: string[], sha256: string, newImage: DynamoDBStreams.AttributeMap): AWSPromises {
  let jobs: AWSPromises[] = [];
  for (const cuid of cuids) {
    jobs.push(s3db_moveUploadAndCreateImageItem(cuid, {
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
  const result = Promise.all(jobs).then(job => job.reduce((result, array) => result.concat(array), []))
  return result
}

function handleS3ReferenceTableEvent(event: DynamoDBStreams.Record): AWSPromises {
  let record = event.dynamodb;
  let sha256 = record.Keys.sha256.S;
  let newImage = record.NewImage;

  switch (event.eventName) {
    case "INSERT":
      // Copy from upload bucket to images bucket when entirely new references are added.
      // This is to ensure completed uploads are eventually moved to the Image bucket and
      //  corresponding Image items created in DynamoDB.
      // If the upload mutation handler completed successfully, this should end up doing nothing.
      return s3db_concurrentMoveUploadsAndCreateImageItems(newImage.images.SS, sha256, newImage)
    case "REMOVE":
      // Delete from S3 when all references to an S3-object are removed from DynamoDB
      let deleteOp = s3_deleteUnreferencedImage(sha256).promise()
      deleteOp.catch(s3_ignoreNoSuchKeyError)
      return deleteOp.then(r => [r])
    case "MODIFY":
      // Synchronize changes to S3-references items with actual S3 storage
      // Clearing the set of references will trigger a delete from S3 and item delete from 
      let oldImageRef = record.OldImage.images.SS;
      let newImageRef = newImage.images.SS;
      //-- Check for reference count changes
      // A. Empty set, no more references to S3. Delete the blob!
      if (newImageRef.length == 0) {
        return db_deleteImageReferences(sha256).promise().then(r => [r])
        // The "REMOVE" event handler will clean up S3
      }
      // B. Set has grown, copy the uploaded file to long-term S3 bucket
      else if (newImageRef.length > oldImageRef.length) {
        let newRefs = newImageRef.filter(v => oldImageRef.indexOf(v) == -1)
        return s3db_concurrentMoveUploadsAndCreateImageItems(newRefs, sha256, newImage)
      }
  }

  //-- Ignore all other modifications
  return Promise.resolve([])
}

function handleS3ReferenceTableEventBatch(eventsBatch: DynamoDBStreams.Record[]): AWSPromises {
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
    var lastPromise: AWSPromises;
    for (let i = 0; i < events.length; i++) {
      const next = events[i];
      lastPromise = lastPromise == null ? handleS3ReferenceTableEvent(next) : lastPromise.then(() => handleS3ReferenceTableEvent(next))
    }
    if (!lastPromise) throw new Error('impossible: zero executed jobs')
    jobs.push(lastPromise)
  }
  //3. Flatten results on completion
  return Promise.all(jobs).then(job => job.reduce((result, array) => result.concat(array), []))
}

type CUID = string
type URL_Safe_Base64_SHA256 = string
type URL_Safe_Base64_MD5 = string
type UTC_DateTime = string

interface S3ReferenceItem {
  sha256?: URL_Safe_Base64_SHA256;
  md5?: Base64_MD5;

  images: CUID[];

  size: number;
  mimetype: string;
  width: number;
  height: number;

  uploadCompletedAt: UTC_DateTime;
  lastFileName: String;
}

interface ImageItem {
  cuid: CUID;
  sha256: URL_Safe_Base64_SHA256;
  md5: URL_Safe_Base64_MD5;

  collections?: CUID[];
  createdAt: UTC_DateTime;
  uploadCompletedAt: UTC_DateTime;

  name: string;
  // blob?: URL; // Not actually stored in DynamoDB as it can be constructed from`name` and`mimetype`
  size: number;
  mimetype: string;

  width: number;
  height: number;
}

interface CollectionItem {
  cuid: CUID;
  name: string;
  images: CUID[];
}

function readableBody(body: Body) : Readable {
  let blob: Readable;
  if (body instanceof Readable) {
    blob = body
  } else {
    blob = new Readable()
    blob.push(body)
    blob.push(null)
  }
  return blob
}

function pipePromise<D extends NodeJS.WritableStream, T>(blob: Readable, destination: D, onEnd: (destination: D) => T): Promise<T> {
  blob.pipe(destination)
  return new Promise<T>((resolve, reject) => {
    blob.on('error', reject)
    blob.on('end', () => resolve(onEnd(destination)))
  });
}

export class ImageRepository {
  readonly s3: S3;
  readonly dynamoDb: DynamoDB.DocumentClient;
  readonly imageSizeLimit: number;

  constructor(dynamoDb: DynamoDB.DocumentClient, s3: S3, maxImageSizeInBytes?: number) {
    this.dynamoDb = dynamoDb;
    this.s3 = s3;
    this.imageSizeLimit = maxImageSizeInBytes || /* 50MB */50 * 1024 * 1024;
  }

  async createFrom(body: Body, name: string): Promise<ImageItem> {
    const id = scuid()
    let readable = readableBody(body)
    let meter = streamMeter(this.imageSizeLimit)
    // Stream into counting and hashing functions while uploading to S3
    let bytesCounter = pipePromise(readable, meter,                m => m.bytes)
    let digestSHA256 = pipePromise(readable, createHash('SHA256'), h => h.digest())
    let digestMD5    = pipePromise(readable, createHash('MD5'),    h => h.digest())

    //1. Copy body to temporary upload bucket, while calculating SHA-256 and metadata
    let params: S3.PutObjectRequest = {
      Bucket: UPLOAD_BUCKET,
      Key: id,
      Body: body
    }
    let uploadResult = await this.s3.putObject(params, undefined).promise()

    //2. Check the upload was successful
    let size = await bytesCounter //TODO: test what happens when size limit was reached during upload
    let md5bits = await digestMD5
    let md5 = md5bits.toString("base64")
    if (md5 != uploadResult.ETag && !uploadResult.ServerSideEncryption) {
      // S3 will return the MD5 hash in certain cases, e.g. no server-side encryption and size below 5GB
      // See docs for details: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
      throw new Error("Body corrupted during transfer. Expected MD5: " + md5 + ", but S3 got: " + uploadResult.ETag);
    }
    //3. Add a record to reference table to ensure the upload gets moved to images bucket eventually
    let sha256bits = await digestSHA256
    let sha256 = base64url.encode(sha256bits)
    let imageRef: S3ReferenceItem = {
      sha256: sha256,
      md5: md5,
      images: [id], //fixme: needs a set-type add !
      size: size,
      mimetype: mimetype,
      width: width,
      height: height,
      uploadCompletedAt: new Date().toUTCString(),
      lastFileName: name
    };
    await this.dynamoDb.put({ TableName: S3REFS_TABLE, Item: imageRef })

    //4. Don't just wait for DynamoDB Streams handler, start the upload to permanent copy process immediately!
    // let image: ImageItem = {
    //   cuid: id,
    //   sha256: sha256,
    //   md5: md5,
    //   name: name,
    //   createdAt: new Date().toUTCString(),
    // };
    // let imageParams = object_to_updateItemInput(IMAGES_TABLE, { "cuid": { "S": id } }, image)
    // await this.dynamoDb.update(imageRefParams)
    // return image;
  }
}

export const IS_OFFLINE = process.env.IS_OFFLINE === 'true';

/// ------------------------
/// Default Image Repository
export const images = new ImageRepository(
  new DynamoDB.DocumentClient(
    IS_OFFLINE ? {
      region: 'localhost',
      endpoint: 'http://localhost:8000'
    } : {}),
  new S3(IS_OFFLINE ? { endpoint: "localhost:8001" } : {})
);

console.log(COLLECTIONS_TABLE); // Make TypeScript shut up about unused reference for now
