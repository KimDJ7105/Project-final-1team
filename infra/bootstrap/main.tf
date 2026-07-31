terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # 처음 apply할 때는 이 버킷 자체가 존재하지 않아 로컬 state로 1회성 적용했지만,
  # 버킷 생성 후에는 이 state도 같은 버킷(다른 key)에 올려서 로컬 파일에 의존하지
  # 않게 합니다 (로컬 state 파일은 유실되기 쉬움 — 실제로 한 번 유실된 적 있음).
  backend "s3" {
    bucket = "team1-terraform-state-s3"
    key    = "bootstrap/terraform.tfstate"
    region = "ap-northeast-2"
  }
}

provider "aws" {
  region = "ap-northeast-2"
}

resource "aws_s3_bucket" "terraform_state" {
  bucket = "team1-terraform-state-s3"

  tags = {
    Team = "team1"
    Name = "team1-terraform-state-s3"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
