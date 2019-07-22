import {s3} from './model'

describe('S3 operations', () => {
  it('should create valid URLs from URL-safe Base64 SHA256 Hash and Mimetype', () => {
    expect(s3.urlOf({ sha256: "HASH", mimetype: "image/png" }))
      .toBe(process.env.IS_OFFLINE
        ? "https://localhost:8001/images/HASH.png"
        : "https://s3.amazonaws.com/images/HASH.png")
  });
});
