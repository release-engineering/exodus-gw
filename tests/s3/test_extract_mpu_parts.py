from exodus_gw.s3.util import extract_mpu_parts


def test_typical_body():
    """extract_mpu_parts can extract data from a typical request body."""

    body = """
        <?xml version="1.0" encoding="UTF-8"?>
        <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Part>
                <ETag>someval</ETag>
                <PartNumber>123</PartNumber>
            </Part>
            <Part>
                <!-- note ETags produced by S3 genuinely do seem to include
                     this extra level of quoting -->
                <ETag>"otherval"</ETag>
                <PartNumber>234</PartNumber>
            </Part>
        </CompleteMultipartUpload>
    """.strip()

    parts = extract_mpu_parts(body)

    # It should accurately parse content from the request
    assert parts == [
        {"ETag": "someval", "PartNumber": 123},
        {"ETag": '"otherval"', "PartNumber": 234},
    ]
