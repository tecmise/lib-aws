package sqsclient

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	transport "github.com/aws/smithy-go/endpoints"
)

type (
	sqsEndpointResolver struct {
	}
)

func (s sqsEndpointResolver) ResolveEndpoint(ctx context.Context, params sqs.EndpointParameters) (transport.Endpoint, error) {
	fmt.Printf("The endpoint provided in config is %s\n", *params.Endpoint)
	return sqs.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, params)
}
