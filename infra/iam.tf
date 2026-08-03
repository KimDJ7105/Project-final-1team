# EC2가 team1-truss-market-data 버킷에만 접근할 수 있도록 스코프한 IAM 역할.
# CLAUDE.md의 "백엔드는 인스턴스 프로파일로 S3에 접근, 개인 자격증명 사용 안 함" 결정을 반영한다.

resource "aws_iam_role" "team1_ec2_role" {
  name = "team1-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = {
    Team = "team1"
    Name = "team1-ec2-role"
  }
}

resource "aws_iam_role_policy" "team1_ec2_s3_policy" {
  name = "team1-ec2-s3-market-data"
  role = aws_iam_role.team1_ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListMarketDataBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.market_data.arn]
      },
      {
        Sid      = "ReadWriteMarketDataObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = ["${aws_s3_bucket.market_data.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "team1_ec2_profile" {
  name = "team1-ec2-profile"
  role = aws_iam_role.team1_ec2_role.name

  tags = {
    Team = "team1"
    Name = "team1-ec2-profile"
  }
}
