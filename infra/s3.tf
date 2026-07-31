# CLAUDE.md의 S3 설계 결정 사항을 그대로 반영합니다:
# - 라이프사이클 규칙 없음 (같은 시뮬레이션 데이터로 인프라 변경 전후 성능을 비교해야 하므로 만료/전환 금지)
# - 퍼블릭 액세스 전면 차단 (백엔드만 접근, 트레이더 앱은 백엔드 API를 통해서만 접근)
resource "aws_s3_bucket" "market_data" {
  bucket = "team1-truss-market-data"

  tags = {
    Team = "team1"
    Name = "team1-truss-market-data"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "market_data" {
  bucket = aws_s3_bucket.market_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "market_data" {
  bucket = aws_s3_bucket.market_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
