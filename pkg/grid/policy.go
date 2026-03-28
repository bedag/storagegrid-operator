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

package grid

import (
	"bytes"
	"encoding/json"

	models "github.com/bedag/storagegrid-sdk-go/models"
)

type Policy struct {
	Version   *string           `json:"Version,omitempty"`
	Statement []PolicyStatement `json:"Statement"`
}

type PolicyStatement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

func GeneratePolicyDocument(policy Policy) string {
	policyBytes, _ := json.Marshal(policy)
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, policyBytes, "", "  "); err != nil {
		return string(policyBytes)
	}
	return prettyJSON.String()
}

func policyStatementsToS3Statements(statements []PolicyStatement) []models.S3Statement {
	s3Statements := make([]models.S3Statement, 0, len(statements))
	for _, ps := range statements {
		s3Statements = append(s3Statements, models.S3Statement{
			Effect:   ps.Effect,
			Action:   &ps.Action,
			Resource: ps.Resource,
		})
	}
	return s3Statements
}
