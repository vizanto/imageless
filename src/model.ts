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
  title: string
}
export interface Image extends ImageBlobData {
  cuid: CUID
  createdAt: Date
  collections?: CUID[]
  title: string
  s3url?: string
}
export interface CollectionItem {
  cuid: CUID;
  title: string;
  images?: CUID[];
  createdAt: Date;
  lastModifiedAt: Date;
}

export class ValidationError extends Error {
  readonly relatedObject: object;
  constructor(message: string, relatedObject: object) {
    super(message)
    this.relatedObject = relatedObject;
  }
}

/*------
  S3 ops
--------*/
export class S3Error extends Error {
  readonly relatedObject: any;
  readonly s3Response: AWSError;
  constructor(message: string, relatedObject: any, s3Response: AWSError) {
    super(message)
    this.relatedObject = relatedObject;
    this.s3Response = s3Response;
  }
}

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
  async calculateMetadataFromUpload(cuid: CUID): Promise<Readonly<ImageBlobData>> {
    let upload = await this.getUpload(cuid).promise();
    // console.log("Got object", cuid, upload)
    let readable = readableBody(upload.Body);
    let meta = await this.streamMetadata(readable);
    if (meta.size != upload.ContentLength) {
      throw new Error(`Expected upload:${cuid} to be of size: ${meta.size}, but S3 size is: ${upload.ContentLength}`);
    }
    // console.log(meta.uploadCompletedAt, "=", meta.uploadCompletedAt.valueOf(), upload.LastModified, "=", upload.LastModified.valueOf());
    meta.uploadCompletedAt = upload.LastModified;
    return Object.freeze(meta)
  }

  protected copyFromUploadBucket(cuid: string, eTag: string, key: S3ImageKey) {
    return this.s3.copyObject({
      CopySource: this.uploadBucket + "/" + cuid,
      CopySourceIfMatch: eTag,
      ContentType: key.mimetype,
      CacheControl: "public,max-age=31536000,immutable",
      MetadataDirective: "REPLACE",
      Bucket: this.imagesBucket,
      Key: this.imageS3Key(key)
    }, null)
  }

  /**
   * Moves an upload to image bucket
   * @param cuid uploaded object (source)
   * @param eTag checksum by S3, usually Base16 MD5 String wrapped in double quotes (")
   * @param image destination
   * @param s3HeadBeforeCopy if it is likely the `image` already exists, set this to `true` to reduce S3 costs
   * @param onImageReady called as soon as object in image bucket exists, usually to Create or Update an Image DynamoDB item for this upload
   */
  async moveUploadToImageBucket<T>(cuid: string, eTag: string, image: S3ImageKey, s3HeadBeforeCopy = false, onImageReady: (image: "copied" | "existed") => Promise<T>) {
    // 1. Copy from upload to image bucket if not exists
    let s3Head: PromiseResult<S3.HeadObjectOutput, AWSError>;
    let s3HeadError: AWSError;
    const s3HeadRequest = async () => {
      try {
        // Check if destination already exists (HEAD is 12.5x cheaper in USD than a COPY request)
        s3Head = await this.headImage(image).promise();
      } catch (reason) {
        s3HeadError = reason
      }
      if (s3Head && eTag !== s3Head.ETag) {
        const message = `Uploaded file ETag '${s3Head.ETag}' differs from destination ETag '${eTag}' but has the same SHA256: ${image.sha256}`
        throw new ValidationError(message, { eTagMismatch: true, cuid, eTag, image, s3Head })
      }
    }
    if (s3HeadBeforeCopy) await s3HeadRequest();
    // Try to copy from upload to image bucket
    let s3Copy: PromiseResult<S3.CopyObjectOutput, AWSError>;
    try {
      s3Copy = await this.copyFromUploadBucket(cuid, eTag, image).promise();
    } catch (reason) {
      const failureMessage = "Couldn't copy " + cuid + " from upload bucket"
      if (reason.statusCode == 404) {
        if (!s3HeadBeforeCopy) await s3HeadRequest();
        if (s3HeadError) {
          let uploadMissing = s3HeadError.statusCode == 404;
          const message = failureMessage + (uploadMissing ? " and " + this.urlOf(image) + " does not exist" : "");
          // console.log(cuid, eTag, failureMessage, "reason=", reason, "s3Head=", s3Head, "s3HeadError=", s3HeadError)
          throw new S3Error(message, { cuid, uploadMissing, image }, s3HeadError);
        }
      } else {
        // console.log(cuid, eTag, failureMessage, "reason=", reason, "s3Head=", s3Head, "s3HeadError=", s3HeadError)
        throw new S3Error(failureMessage, { cuid, image }, s3HeadError);
      }
    }
    // 2. Notify caller copy was completed
    let handlerResult = await onImageReady(s3Head ? "existed" : "copied");
    // 3. Success! Clean up the S3 upload bucket
    let s3Delete = await this.deleteFromUploadBucket(cuid).promise();
    return {
      awsResults: { s3Head, s3Copy, s3Delete },
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
export const object_to_updateItemInput = (tableName: string, key: DynamoDB.DocumentClient.Key, SET?: object, ADD?: object, DELETE?: object): DynamoDB.DocumentClient.UpdateItemInput => {
  var expr: string = "";
  let values = {};
  for (const keyName in SET) {
    if (SET.hasOwnProperty(keyName)) {
      const ifNotExists = keyName.startsWith("?")
      const key = ifNotExists ? keyName.substr(1) : keyName;
      const setAttr = ifNotExists ? key + " = if_not_exists(" + key + ", :" + key + ")" : key + " = :" + key
      if ("" === expr) expr = "SET "; else expr += ", "
      expr += setAttr;
      const value = SET[keyName];
      values[":" + key] = value;
    }
  }
  if (ADD) {
    expr += " ADD " + Object.keys(ADD).map(k => { values[":" + k] = ADD[k]; return k + " :" + k; }).join(", ")
  }
  if (DELETE) {
    expr += " DELETE " + Object.keys(DELETE).map(k => { values[":" + k] = DELETE[k]; return k + " :" + k; }).join(", ")
  }
  return { TableName: tableName, Key: key, UpdateExpression: expr, ExpressionAttributeValues: values }
}

const { S3REFS_TABLE, IMAGES_TABLE, COLLECTIONS_TABLE } = process.env;

export class DynamoDBImageRepositoryTables {
  readonly db: DynamoDB.DocumentClient;

  constructor(dynamoDb: DynamoDB.DocumentClient) {
    this.db = dynamoDb;
  }

  /*-------------------
    S3 Reference Table
  --------------------*/

  async _updateReferenceItem(sha256: URL_Safe_Base64_SHA256, SET?, ADD?, DELETE?): Promise<S3ReferenceItem> {
    let params = object_to_updateItemInput(S3REFS_TABLE, { sha256 }, SET, ADD, DELETE);
    params.ReturnValues = "UPDATED_NEW"
    if (SET) {
      if (SET.md5 && SET.size && SET.mimetype) {
        params.ConditionExpression = '(attribute_not_exists(md5) OR md5 = :md5) AND (attribute_not_exists(size) OR size = :size) AND (attribute_not_exists(mimetype) OR mimetype = :mimetype)';
      } else if (SET.md5 || SET.size || SET.mimetype) {
        throw new Error('Invalid S3ReferenceItem update: ' + SET);
      }
    }
    // console.log("About to update", params, "with", params.UpdateExpression)
    let { Attributes } = await this.db.update(params).promise();
    return {
      sha256,
      mimetype: Attributes.mimetype,
      md5: Attributes.md5,
      uploadCompletedAt: new Date(Attributes.uploadCompletedAt),
      size: Attributes.size,
      width: Attributes.width,
      height: Attributes.height,
      images: Attributes.images.values,
      lastFileName: Attributes.lastFileName
    }
  }

  async addReferences(imageRef: S3ReferenceItem) {
    let { sha256, images, uploadCompletedAt, ...attributes } = imageRef
    return this._updateReferenceItem(
      sha256,
      { ...attributes, "?uploadCompletedAt": uploadCompletedAt.toISOString() },
      { images: this.db.createSet(images) }
    );
  }

  getReferenceItem(sha256: URL_Safe_Base64_SHA256, consistentRead: DynamoDB.DocumentClient.ConsistentRead = true) {
    return this.db.get({ TableName: S3REFS_TABLE, Key: { sha256 }, ConsistentRead: consistentRead })
  }

  async removeReferences(sha256: URL_Safe_Base64_SHA256, refs: CUID[]) {
    return this._updateReferenceItem(sha256, undefined, undefined, { images: this.db.createSet(refs) })
  }

  deleteReferenceItem(sha256: URL_Safe_Base64_SHA256) {
    return this.db.delete({ TableName: S3REFS_TABLE, Key: { sha256 } })
  }

  /*-----------
    Image Table
  -------------*/

  async _updateImageItem(cuid: CUID, returnValues: "ALL_NEW" | "NONE", input) {
    let params = object_to_updateItemInput(IMAGES_TABLE, { cuid }, input);
    params.ReturnValues = returnValues
    let { sha256, md5, size } = input;
    if ((sha256 || md5 || size)) {
      if (!(sha256 && md5 && size)) {
        throw new ValidationError("Change of ImageBlobData requires a valid sha256, md5, and size", { input });
      }
      params.ConditionExpression = '(attribute_not_exists(sha256) AND attribute_not_exists(md5)) OR (sha256 = :sha256 AND md5 = :md5)'
    }
    let dbResult: PromiseResult<DynamoDB.DocumentClient.UpdateItemOutput, AWSError>;
    try {
      dbResult = await this.db.update(params).promise();
    }
    catch (e) {
      if ((e as AWSError).code == 'ConditionalCheckFailedException') {
        // console.error(cuid, e, input)
        throw new ValidationError("Changes to ImageBlobData must atomically update related S3ReferenceItems", { awsError: e, cuid: cuid, input: input })
      } else {
        throw e
      }
    }
    if (dbResult.Attributes) {
      return { dbResult, image: this._imageItem(cuid, dbResult.Attributes) }
    } else {
      return { dbResult };
    }
  }

  protected _imageItem(cuid: CUID, Attributes) {
    if (!Attributes) return null;
    let image: Image = {
      sha256: Attributes.sha256,
      mimetype: Attributes.mimetype,
      md5: Attributes.md5,
      uploadCompletedAt: new Date(Attributes.uploadCompletedAt),
      size: Attributes.size,
      width: Attributes.width,
      height: Attributes.height,
      cuid: cuid,
      createdAt: new Date(Attributes.createdAt),
      title: Attributes.title
    }
    if (Attributes.collections) {
      image.collections = Attributes.collections.values;
    }
    return image;
  }

  /**
   * Creates a new Image item,
   *  or if it exists but has a different SHA-256 assigned to it: replaces its ImageBlobData attributes.
   *
   * Does nothing if an Image with given CUID and SHA-256 already exists.
   *
   * @param cuid Image ID
   * @param input Image item attributes, requires sha256 and all S3-object related properties
   * @param returnImage true: return the updated Image, false: resolve Promise to null
   */
  async createOrUpdateImage(cuid: CUID, newImage: Readonly<ImageInput>, creationTimestamp: Date | "now", returnImage = true) {
    let { uploadCompletedAt } = newImage;
    let createdAt = (creationTimestamp == "now" ? new Date() : creationTimestamp)
    let input = Object.freeze({
      ...newImage,
      "?createdAt": createdAt.toUTCString(),
      uploadCompletedAt: uploadCompletedAt.toUTCString(),
    });
    return this._updateImageItem(cuid, returnImage ? "ALL_NEW" : "NONE", input);
  }

  async getImage(cuid: CUID, consistentRead: DynamoDB.DocumentClient.ConsistentRead = false) {
    let response = await this.db.get({ TableName: IMAGES_TABLE, Key: { cuid }, ConsistentRead: consistentRead }).promise()
    return this._imageItem(cuid, response.Item)
  }

  /**
   * Deletes an Image and updates all Collections that it was a part of.
   *
   * Because of DynamoDB's limit of 25 operations for a single transaction,
   *  and an Image potentially being added to more than 25 collections,
   *  the Image delete could succeed while some Images may not be updated due to timeouts or network issues.
   *
   * There is currently no automatic fixing of "Collections that point to a deleted Image" implemented.
   * @param cuid Image ID
   */
  async deleteImageItemOptimistically(cuid: CUID) {
    let deletedImage = await this.db.delete({ TableName: IMAGES_TABLE, Key: { cuid }, ReturnValues: "ALL_OLD" }).promise()
    if (deletedImage.Attributes.collections) {
      let
        collections: CUID[] = deletedImage.Attributes.collections.values,
        UpdateExpression = 'DELETE images :image',
        ExpressionAttributeValues = { ':image': this.db.createSet([cuid]) },
        imageUpdates = await Promise.all(
          collections.map((cuid) => this.db.update({
            TableName: COLLECTIONS_TABLE, Key: { cuid }, ReturnConsumedCapacity: "TOTAL",
            UpdateExpression, ExpressionAttributeValues
          }).promise())
        )
      return { deletedImage, imageUpdates }
    } else {
      return { deletedImage };
    }
  }

  /*-----------------
    Collections Table
  -------------------*/

  protected _collectionItem(cuid: CUID, Attributes, lastModifiedAt?: Date) {
    if (!Attributes) return null;
    let collection: CollectionItem = {
      cuid: cuid,
      title: Attributes.title,
      createdAt: new Date(Attributes.createdAt),
      lastModifiedAt: lastModifiedAt || new Date(Attributes.lastModifiedAt)
    }
    if (Attributes.images) {
      collection.images = Attributes.images.values
    }
    return collection;
  }

  async createOrUpdateCollection(cuid: CUID, title: string): Promise<CollectionItem> {
    let lastModifiedAtDate = new Date(),
      lastModifiedAt = lastModifiedAtDate.toISOString(),
      input = { title, lastModifiedAt, "?createdAt": lastModifiedAt },
      params = object_to_updateItemInput(COLLECTIONS_TABLE, { cuid }, input);
    params.ReturnValues = "ALL_NEW"
    let result = await this.db.update(params).promise()
    return this._collectionItem(cuid, result.Attributes, lastModifiedAtDate);
  }

  async getCollection(cuid: CUID, consistentRead: DynamoDB.DocumentClient.ConsistentRead = false) {
    let response = await this.db.get({ TableName: COLLECTIONS_TABLE, Key: { cuid }, ConsistentRead: consistentRead }).promise()
    return this._collectionItem(cuid, response.Item)
  }

  _collectionTransaction(action: 'ADD' | 'DELETE', collection: CUID, images: CUID[]): DynamoDB.DocumentClient.TransactWriteItemsInput {
    let
      collectionSet = { ':collection': this.db.createSet([collection]) },
      imageExpr = action + ' collections :collection',
      collectionExpr = action + ' images :images',
      imageSet = { ':images': this.db.createSet(images) },
      items: DynamoDB.DocumentClient.TransactWriteItemList = images.map((cuid) => ({
        Update: {
          TableName: IMAGES_TABLE,
          Key: { cuid },
          UpdateExpression: imageExpr,
          ExpressionAttributeValues: collectionSet
        }
      }))
    items.push({
      Update: {
        TableName: COLLECTIONS_TABLE,
        Key: { cuid: collection },
        UpdateExpression: collectionExpr,
        ExpressionAttributeValues: imageSet
      }
    })
    return { TransactItems: items, ReturnItemCollectionMetrics: "SIZE", ReturnConsumedCapacity: "TOTAL" }
  }

  addToCollection(collection: CUID, images: CUID[]) {
    if (images.length > 12) throw new Error((images.length - 12) + " images too many! DynamoDB only supports up to 25 items in a transaction")
    let params = this._collectionTransaction('ADD', collection, images)
    return this.db.transactWrite(params)
  }

  removeFromCollection(collection: CUID, images: CUID[]) {
    if (images.length > 12) throw new Error((images.length - 12) + " images too many! DynamoDB only supports up to 25 items in a transaction")
    let params = this._collectionTransaction('DELETE', collection, images)
    return this.db.transactWrite(params)
  }

  /**
   * Deletes a Collection and updates all Images that are part of it.
   *
   * Because of DynamoDB's limit of 25 operations for a single transaction,
   *  and a Collection potentially having more than 25 images,
   *  the Collection delete could succeed while some Images may not be updated due to timeouts or network issues.
   *
   * There is currently no automatic fixing of "Images that point to a deleted Collection" implemented.
   * @param cuid Collection ID
   */
  async deleteCollectionOptimistically(cuid: CUID) {
    let deletedCollection = await this.db.delete({ TableName: COLLECTIONS_TABLE, Key: { cuid }, ReturnValues: "ALL_OLD" }).promise()
    if (deletedCollection.Attributes.images) {
      let
        images: CUID[] = deletedCollection.Attributes.images.values,
        UpdateExpression = 'DELETE collections :collection',
        ExpressionAttributeValues = { ':collection': this.db.createSet([cuid]) },
        imageUpdates = await Promise.all(
          images.map((cuid) => this.db.update({
            TableName: IMAGES_TABLE, Key: { cuid }, ReturnConsumedCapacity: "TOTAL",
            UpdateExpression, ExpressionAttributeValues
          }).promise())
        )
      return { deletedCollection, imageUpdates }
    } else {
      return { deletedCollection };
    }
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

  async _moveUploadAndCreateImageItem(cuid: string, newImage: ImageInput, s3HeadBeforeCopy: boolean) {
    // Prepare to Create (or Update) an Image DynamoDB item using the S3-reference
    let createImage = () => this.db.createOrUpdateImage(cuid, newImage, "now", false);
    // Move the S3 object from upload to image bucket
    try {
      let moveOp = await this.s3.moveUploadToImageBucket(cuid, newImage.md5, newImage, s3HeadBeforeCopy, createImage);
      // Success!
      return {
        awsResults: moveOp.awsResults,
        image: moveOp.afterCopyCompleted
      }
    } catch (reason) {
      if (reason instanceof S3Error && reason.relatedObject.uploadMissing) {
        return {
          awsResults: {},
          skipped: true,
          uploadMissing: true,
          reason
        }
      }
    }
  }

  async _concurrentMoveUploadsAndCreateImageItems(cuids: string[], sha256: string, s3HeadBeforeCopy:boolean, newImage: DynamoDBStreams.AttributeMap): AWSPromises {
    let jobs: Promise<AWSCompositeOp>[] = [];
    for (const cuid of cuids) {
      let input: ImageInput = {
        sha256: sha256,
        md5: newImage.md5.S,
        uploadCompletedAt: new Date(newImage.uploadCompletedAt.S),

        title: newImage.lastFileName.S,
        size: parseInt(newImage.size.N),
        mimetype: newImage.mimetype.S,

        width: parseInt(newImage.width.N),
        height: parseInt(newImage.height.N)
      }
      jobs.push(this._moveUploadAndCreateImageItem(cuid, input, s3HeadBeforeCopy));
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

        // Before COPY, check if this sha256 already exists in the images bucket.
        // It's likely to exist, because normally the mutation handler will complete
        // before this Stream handler is triggered.
        return this._concurrentMoveUploadsAndCreateImageItems(newImage.images.SS, sha256, true, newImage)
      case "REMOVE":
        // Delete from S3 when all references to an S3-object are removed from DynamoDB
        let deleteOp = this.s3.deleteUnreferencedImage({ sha256, mimetype: record.OldImage.mimetype.S }).promise()
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
          // Before COPY, check if this sha256 already exists in the images bucket.
          // It's likely to exist, because normally the mutation handler will complete
          // before this Stream handler is triggered.
          return this._concurrentMoveUploadsAndCreateImageItems(newRefs, sha256, true, newImage)
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
    let metadata = this.s3.streamMetadata(readable)
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
    let imageRef: S3ReferenceItem = await this.db.addReferences({
      ...imageBlobData,
      uploadCompletedAt: uploadCompleted,
      images: [id],
      lastFileName: name
    });

    //4. Don't just wait for DynamoDB Streams handler, start the move process immediately!
    //   Create (or Update) an Image DynamoDB item using the S3-reference
    let createImage = () => this.db.createOrUpdateImage(id, { ...imageRef, title: imageRef.lastFileName }, "now", true);
    // COPY immediately without checking if this sha256 already exists in the images bucket.
    // It's not likely to exist, because normally this method is used to create entirely new files.
    let moveResult = await this.s3.moveUploadToImageBucket(id, md5, imageRef, false, createImage);
    return moveResult.afterCopyCompleted.image;
  }
}


/*------------------------
  Default Image Repository
--------------------------*/
export const IS_OFFLINE = process.env.IS_OFFLINE === 'true';
const default_s3 = new S3ImageRepositoryBuckets(
  new S3(IS_OFFLINE ? {
    region: 'eu-central-1',
    s3ForcePathStyle: true,
    accessKeyId: 'S3RVER',
    secretAccessKey: 'S3RVER',
    endpoint: 'http://localhost:4572'
  } : {})
);
const default_db = new DynamoDBImageRepositoryTables(
  new DynamoDB.DocumentClient(
    IS_OFFLINE ? {
      region: 'eu-central-1',
      accessKeyId: 'DEFAULT_ACCESS_KEY',
      secretAccessKey: 'DEFAULT_SECRET',
      endpoint: 'http://localhost:8000'
    } : {})
);
export const defaultStores = { s3: default_s3, db: default_db };
export const images = new ImageRepository(default_s3, default_db);
export const stream = new ImageRepository_DynamoDB_StreamHandler(default_s3, default_db);
