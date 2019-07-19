import { S3, DynamoDBStreams, DynamoDB, AWSError } from "aws-sdk";
import { Key } from "aws-sdk/clients/dynamodb";
import { PromiseResult } from "aws-sdk/lib/request";

const IS_OFFLINE = process.env.IS_OFFLINE === 'true';

/// --------------
/// DynamoDB setup
const S3REFS_TABLE = process.env.S3REFS_TABLE;
const IMAGES_TABLE = process.env.IMAGES_TABLE;
console.log(IMAGES_TABLE)

let dynamoDb = new DynamoDB.DocumentClient(IS_OFFLINE ? {
  region: 'localhost',
  endpoint: 'http://localhost:8000'
} : {})

/// --------------
/// S3 setup
const UPLOAD_BUCKET = 'upload'
const IMAGES_BUCKET = 'images'

let s3 = new S3(IS_OFFLINE ? { endpoint: "localhost:8001" } : {});
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
    if (!lastPromise) throw 'error'
    jobs.push(lastPromise)
  }
  //3. Flatten results on completion
  return Promise.all(jobs).then(job => job.reduce((result, array) => result.concat(array), []))
}
