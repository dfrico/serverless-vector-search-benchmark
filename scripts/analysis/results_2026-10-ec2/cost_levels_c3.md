# Cost levels per cell (2026-10-ec2)

Level 1 = CloudWatch billed duration x memory x rate + request fee. Level 2 = Level 1 + S3 requests (access logs) + DynamoDB reads + S3 Vectors request and data fees. Per query.

| exp | cell | n | p50 s | inv/q | GB-s/q | L1 $/q | S3 GET/q | S3 PUT+LIST/q | S3 $/q | DDB RRU/q | S3V $/q | L2 $/q | logs ok |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| c3 | blocks_c1 | 131 | 6.03 | 21.0 | 110.27 | 0.0018421 | 749 | 59 | 0.0005947 | 0.00 | 0.0000000 | 0.0024368 | yes |
| c3 | lancedb_c1 | 54 | 51.4 | 1.0 | 105.14 | 0.0014020 | 5019 | 1 | 0.0020126 | 0.00 | 0.0000000 | 0.0034147 | NO |
| c3 | lancedb_c10 | 60 | 51.5 | 1.0 | 106.50 | 0.0014202 | 5773 | 1 | 0.0023150 | 0.00 | 0.0000000 | 0.0037353 | yes |
| c3 | lancedb_c4 | 56 | 51.4 | 1.0 | 105.09 | 0.0014014 | 5021 | 1 | 0.0020135 | 0.00 | 0.0000000 | 0.0034149 | yes |
| c3 | s3vectors_c1 | 1057 | 0.325 | 0.0 | 0.00 | 0.0000000 | 0 | 0 | 0.0000000 | 0.00 | 0.0000103 | 0.0000103 | yes |
| c3 | s3vectors_c4 | 1785 | 0.208 | 0.0 | 0.00 | 0.0000000 | 0 | 0 | 0.0000000 | 0.00 | 0.0000103 | 0.0000103 | yes |
