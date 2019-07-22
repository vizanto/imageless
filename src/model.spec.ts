import { s3, IS_OFFLINE } from './model'

describe('S3 operations', () => {
  it('should create valid URLs from URL-safe Base64 SHA256 Hash and Mimetype', () => {
    expect(s3.urlOf({ sha256: "HASH", mimetype: "image/png" }))
      .toBe(IS_OFFLINE
        ? "http://localhost:4572/images/HASH.png"
        : "https://s3.amazonaws.com/images/HASH.png")
  });
});
