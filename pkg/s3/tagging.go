/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package s3

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func AppendTagMap(ctx context.Context, bucket string, tags map[string]string, s3Client *S3Client) error {
	log := log.FromContext(ctx).WithValues("func", "applyPolicy")
	log.V(1).Info("Creating policy", "bucket", bucket)

	// log tags to apply
	log.V(1).Info("Tags to apply", "tags", tags)

	// get existing tags
	currentTags, err := getBucketTags(ctx, bucket, s3Client)
	if err != nil {
		log.Error(err, "Failed to get existing tags")
		return err
	}

	tagMap := s3TagsToMap(currentTags)

	// Merge existing tags with new tags
	for k, v := range tags {
		tagMap[k] = v
	}

	// prepare for put
	putParams := &s3.PutBucketTaggingInput{
		Bucket: aws.String(bucket),
		Tagging: &s3types.Tagging{
			TagSet: mapToS3Tags(tagMap),
		},
	}

	// apply tags
	_, err = s3Client.PutBucketTagging(ctx, putParams)
	if err != nil {
		log.Error(err, "Failed to apply bucket tags")
		return err
	}

	// get new tags for verification
	newTags, err := getBucketTags(ctx, bucket, s3Client)
	if err != nil {
		log.Error(err, "Failed to get new bucket tags for verification")
		return err
	}

	log.V(1).Info("New bucket tags", "tags", s3TagsToMap(newTags))

	log.V(1).Info("Successfully applied bucket tags", "bucket", bucket)
	return nil
}

func ReplaceTagMap(ctx context.Context, bucket string, tags map[string]string, s3Client *S3Client) error {
	log := log.FromContext(ctx).WithValues("func", "ReplaceTagMap")
	log.V(1).Info("Replacing bucket tags", "bucket", bucket)

	// prepare for put
	putParams := &s3.PutBucketTaggingInput{
		Bucket: aws.String(bucket),
		Tagging: &s3types.Tagging{
			TagSet: mapToS3Tags(tags),
		},
	}

	// apply tags
	_, err := s3Client.PutBucketTagging(ctx, putParams)
	if err != nil {
		log.Error(err, "Failed to replace bucket tags")
		return err
	}

	log.V(1).Info("Successfully replaced bucket tags", "bucket", bucket)
	return nil
}

func getBucketTags(ctx context.Context, bucket string, s3Client *S3Client) ([]s3types.Tag, error) {
	log := log.FromContext(ctx).WithValues("func", "getBucketTags")
	log.V(1).Info("Getting bucket tags", "bucket", bucket)

	output, err := s3Client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchTagSet") {
			log.V(1).Info(fmt.Sprintf("No tags found for bucket %s", bucket))
			return []s3types.Tag{}, nil
		}
		log.Error(err, "Failed to get bucket tags")
		return nil, err
	}

	return output.TagSet, nil
}

func GetBucketTagMap(ctx context.Context, bucket string, s3Client *S3Client) (map[string]string, error) {
	log := log.FromContext(ctx).WithValues("func", "GetBucketTagMap")
	log.V(1).Info("Getting bucket tag map", "bucket", bucket)

	tags, err := getBucketTags(ctx, bucket, s3Client)
	if err != nil {
		log.Error(err, "Failed to get bucket tags")
		return nil, err
	}

	tagMap := s3TagsToMap(tags)
	return tagMap, nil
}

func mapToS3Tags(tagMap map[string]string) []s3types.Tag {
	tags := make([]s3types.Tag, 0, len(tagMap))
	for k, v := range tagMap {
		tags = append(tags, s3types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return tags
}

func s3TagsToMap(s3Tags []s3types.Tag) map[string]string {
	tagMap := make(map[string]string)
	for _, tag := range s3Tags {
		tagMap[*tag.Key] = *tag.Value
	}
	return tagMap
}
