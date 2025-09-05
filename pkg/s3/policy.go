package s3

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func ApplyPolicy(ctx context.Context, bucket string, policy string, s3Client *S3Client) error {
	log := log.FromContext(ctx).WithValues("func", "applyPolicy")
	log.V(1).Info("Creating policy", "bucket", bucket)

	_, err := s3Client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(policy),
	})
	if err != nil {
		log.Error(err, "Failed to apply policy")
		return err
	}

	log.V(1).Info("Policy created")
	return nil
}

func GetPolicy(ctx context.Context, bucket string, s3Client *S3Client) (string, error) {
	log := log.FromContext(ctx).WithValues("func", "getPolicy")
	log.V(1).Info("Fetching policy", "bucket", bucket)

	policy, err := s3Client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		if err.Error() != "NoSuchBucketPolicy: The bucket policy does not exist" {
			log.V(1).Info("No policy found, returning empty string")
			return "", nil
		}
		log.Error(err, "Failed to get policy")
		return "", err
	}

	log.V(1).Info("Policy fetched")

	out := bytes.Buffer{}
	policyStr := *policy.Policy
	if err := json.Indent(&out, []byte(policyStr), "", "  "); err != nil {
		log.Error(err, "Failed to pretty print bucket policy")
	}

	return policyStr, nil
}

func DeletePolicy(ctx context.Context, bucket string, s3Client *S3Client) error {
	log := log.FromContext(ctx).WithValues("func", "deletePolicy")
	log.V(1).Info("Deleting policy", "bucket", bucket)

	_, err := s3Client.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.Error(err, "Failed to delete policy")
		return err
	}

	log.V(1).Info("Policy deleted")
	return nil
}
