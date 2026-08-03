# team1 전용 네트워크 인프라 기반 (VPC + 퍼블릭 서브넷 1개 + IGW + 라우팅 테이블).
# 계정을 여러 팀이 공유하므로 CIDR은 기존 팀들이 쓰지 않는 10.100.0.0/16을 사용한다.
# 현재는 ap-northeast-2a 퍼블릭 서브넷 1개뿐이며, 추후 2개 AZ로 확장하고
# 프라이빗 서브넷 + NAT 게이트웨이를 추가할 예정이다.

resource "aws_vpc" "team1_vpc" {
  cidr_block           = "10.100.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Team = "team1"
    Name = "team1-vpc"
  }
}

resource "aws_internet_gateway" "team1_igw" {
  vpc_id = aws_vpc.team1_vpc.id

  tags = {
    Team = "team1"
    Name = "team1-igw"
  }
}

resource "aws_subnet" "team1_public_a" {
  vpc_id                  = aws_vpc.team1_vpc.id
  cidr_block              = "10.100.1.0/24"
  availability_zone       = "ap-northeast-2a"
  map_public_ip_on_launch = true

  tags = {
    Team = "team1"
    Name = "team1-public-subnet-a"
  }
}

resource "aws_route_table" "team1_public_rt" {
  vpc_id = aws_vpc.team1_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.team1_igw.id
  }

  tags = {
    Team = "team1"
    Name = "team1-public-rt"
  }
}

resource "aws_route_table_association" "team1_public_a" {
  subnet_id      = aws_subnet.team1_public_a.id
  route_table_id = aws_route_table.team1_public_rt.id
}
