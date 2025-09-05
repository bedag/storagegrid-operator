package s3

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// var (
// 	s3Client *s3.Client
// )

type S3Client = s3.Client

func InitS3Client(ctx context.Context, endpoint string, accessKey string, secret string, region string, pathStyle bool) (*S3Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
		return nil, err
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.Region = region
		// leaving as comment if need to be made variable in the future
		// o.EndpointOptions.DisableHTTPS = true
		o.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secret, "")
		o.UsePathStyle = pathStyle
	})

	return s3Client, nil
}
