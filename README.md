# Image GraphQL service

This Serverless®™ Lambda™® implements a GraphQL™ service to manage "collections" of images.
Images that are uploaded are stored in a content-addressable manner into Amazon S3.
[Content-addressable](https://en.wikipedia.org/wiki/Content-addressable_storage) means that the image file contents is cryptographically hashed and the hash used as unique-identifier.
Uploading the same file twice will actually store it only once on S3.



# Usage

The GraphQL schema is defined in `schema.graphql` with supported types and mutations.

## Building

Open this repository in a Terminal with [direnv](https://direnv.net) and [the Nix package manager](https://nixos.org/nixpkgs/) installed
or ensure that the dependencies listed in `shell.nix` are installed in some other way.

Then run `yarn install` to fetch Node.js build and runtime dependencies.

To compile the sources to javascript run `serverless webpack -o out` and the result should be in `out/service/handler.js`.


## Exploring

To explore the schema locally in a web browser run `serverless offline` and open the URL printed.



# Design tradeoffs

Distributed systems are hard.

Ensuring atomic updates of both DynamoDB and S3 together would be much easier if GraphQL mutations were routed to the same long running process to linearize operations. Classic server architecture would use something like sticky sessions, while for scalibility mutations could be parallelized by sharding on the SHA-256 hash of the image.
However, Amazon Lambda is event based and Lambdas can be started/stopped on any machine and events routed anywhere.

The next best thing is a single DynamoDB streams Lambda function per partition that keeps changes to DynamoDB tables in sync with S3. And that's exactly what this system does using a S3-references table.

Image and Collection tables only keep data about images.

For keeping changes to Image data and S3 in sync, the S3-references table keeps track of:
- image-data stored in S3 (the SHA-256 of it at least)
- what Image items are referring to the same image-data

Only when no Image items refer to a file on S3 can it be safely removed. This cleanup process is implemented as a DynamoDB streams handler to ensure it is [serializable](https://en.wikipedia.org/wiki/Serializability) and run at least once.



## Eventual consistency

- Image objects can only be added to a collection unordered once: Collections are a named set of images.
  Clients are responsible for sorting.
- The DynamoDB table design favors read performance over writes, storing Collection membership in both Image and Collection items
- Writes (Create, Update, Delete) are processed in DynamoDB streams handler and may eventually create or delete objects in S3
- DynamoDB transactions are used to update both Image and Collection sets atomically
  - Note: Transaction could be replaced by a Streams handler to save costs (DynamoDB Write Units)
- DynamoDB transactions are used to change what blob an Image refers to (likely an uncommon operation)


### Image uploads

Images are added to S3 and the database in 2-phases:

#### Phase 1, The GraphQL Lambda:

Upload mutation handler T B D


#### Phase 2, The DynamoDB Streams handler:

S3 sync, Modifications and Delete handler T B D


## Immutable image storage

S3 does not allow renaming of keys, thus we have to compute the hash somehow before (or after) uploading to S3.
AWS Lambda functions are limited to keep 512MB of data in `/tmp`.
To avoid long running Lambdas and overflowing `/tmp`, image uploads are limited to 50MB or 5 minutes per HTTP request.
The upload is hashed while being saved in /tmp. Thereafter it is transferred to S3 if it wasn't already there.


### Designing for slow uploads, or uploads larger than 50 MB

The user can first create an Image and in return get a pre-signed S3 URL (anonymously named, e.g. UUID) where to upload to.
This requires another step, a Lambda to:
- compute the Hash of the just uploaded file
- update the Image item and add a reference to the S3 URL
- or delete the file if it was already uploaded to S3 before (hash exists)
  and update the Image item to use the oldest S3 URL instead

By using 2 buckets: an upload bucket where files expire (e.g. after a day) and the content-addressable bucket, slow uploads won't be charged for expensive Lambda cycles.



# Garbage collection

There are 3 known possible instances of garbage:

### DynamoDB items referencing image blobs that failed to upload to S3

S3 does not guarantee that a Lambda will be executed for every event or created object.

A periodic lambda could query DynamoDB for all images uploaded before the previous hour that have not been marked as completed and remove those items. Alternatively S3 could be periodically scanned (a list of entire bucket checked against DynamoDB) but this process could be expensive and is less scalable.

But, more simply using expiry for the upload bucket, any unused files should eventually disappear.


### DynamoDB items referencing image blobs that were removed from S3

This would most likely be a bug or caused by manual action. A scan Images that checks S3 find unavailable blobs and write `Delete` commands to remove the Image items.

Alternatively the GraphQL Lambda could log a `BlobMissing` command that (after some time passed to allow for eventual consistency) would mark the Image item as dead and update related Collections after checking S3 once more for a 404 response.

Depending on the practical circumstances, the dead Image items could be deleted or data re-uploaded to S3 and Image items marked live again.


### Files on S3 that have no DynamoDB metadata items

This would most likely be a bug or caused by manual action. A periodic scan of S3 could check a corresponding Image DynamoDB item exists (after some delay to allow for eventual consistency) and write `Delete` commands to remove the unavailable Images.



## Preventing abusive GraphQL queries

Images keep track to which collections they are added, and Collections keep a list of images.
Because of the potential infinite nesting of querying this graph relationship, GraphQL query depth is limited to 3.
See `schema.graphql` for `Image <--> Collection` relationship details.



# Project Background

This is my first ever project using:

- TypeScript
- GraphQL
- Node.js
- DynamoDB
- DynamoDB Streams
- AWS S3
- AWS Lambda
- AWS API Gateway
- Serverless tooling

To speed up development I thought I would use some libraries. `graphql-yoga` looked to be a relatively complete library (without additional ORM overhead) to get started. Unfortunately they have not updated their `apollo-graphql-upload` dependency.

For production use, more work is required as they foolishly supply a handler for AWS Lambda that only implements a `POST` handler for GraphQL queries and returns the `playgroundHandler` for all `GET` requests. This decision prevents or complicates Browser, API-Gateway and Cloudfront caching. GraphQL supports GET queries using request parameters (query strings) that can be cached by traditional HTTP caching methods.


## Tools that I've never used before this project

I might have done something weird by the standards of experienced users of the following:

- serverless
- node
- webpack
- yarn:
  - Seems to be the recommended package manager for js
  - Also tried pnpm
    - pnpm is slightly faster installing packages
    - but, failed to run serverless
      - pnpm uses a different node_modules layout
        and this causes serverless to go into infinite recursion while monkey-patching console output functions


## Tools I did use before 💓

- [direnv](https://direnv.net)
- [the Nix package manager](https://nixos.org/nixpkgs/)


# Some Notes for myself

Upload bucket:

- Expire uploads after 1 day automatically:
  - https://aws.amazon.com/blogs/aws/amazon-s3-object-expiration/
  - https://stackoverflow.com/questions/46769995/temporarily-upload-on-s3-and-remove-if-its-not-used
- LIST manageble on upload bucket, use to garbage collect abandoned uploads. Not needed with expiry?
- Copy between buckets in same region incur no charge for bandwidth
  - https://serverfault.com/questions/349460/how-to-move-files-between-two-s3-buckets-with-minimum-cost
  - Lambda computes SHA-256, transfer is free from S3
    - https://www.quora.com/What-are-the-data-transfer-charges-for-reading-file-from-S3-within-AWS-Lambda-function
    - https://aws.amazon.com/lambda/pricing/
    - "Data transferred between Amazon S3, Amazon Glacier, Amazon DynamoDB, Amazon SES, Amazon SQS, Amazon Kinesis, Amazon ECR, Amazon SNS, or Amazon SimpleDB and AWS Lambda functions in the same AWS Region is free."


- [AppSync Limitations (1)](https://medium.com/@dadc/aws-appsync-the-unexpected-a430ff7180a3)
- [AppSync Limitations (2)](https://dev.to/raoulmeyer/appsync-basically-graphql-as-a-service-3bp1)
- "Appsync doesn't fully support graphQl spec"
