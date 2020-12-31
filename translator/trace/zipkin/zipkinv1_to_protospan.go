// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zipkin

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/jaegertracing/jaeger/thrift-gen/zipkincore"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.opentelemetry.io/collector/consumer/consumerdata"
	"go.opentelemetry.io/collector/consumer/pdata"
	tracetranslator "go.opentelemetry.io/collector/translator/trace"
)

var (
	// ZipkinV1 friendly conversion errors
	msgZipkinV1JSONUnmarshalError = "zipkinV1"
	msgZipkinV1BinAnnotationError = "zipkinV1 span binary annotation"
	// Generic hex to ID conversion errors
	errHexTraceIDWrongLen = errors.New("hex traceId span has wrong length (expected 16 or 32)")
	errHexTraceIDParsing  = errors.New("failed to parse hex traceId")
	errHexTraceIDZero     = errors.New("traceId is zero")
	errHexIDWrongLen      = errors.New("hex Id has wrong length (expected 16)")
	errHexIDParsing       = errors.New("failed to parse hex Id")
	errHexIDZero          = errors.New("ID is zero")
)

// Trace translation from Zipkin V1 is a bit of special case since there is no model
// defined in golang for Zipkin V1 spans and there is no need to define one here, given
// that the zipkinV1Span defined below is as defined at:
// https://zipkin.io/zipkin-api/zipkin-api.yaml
type zipkinV1Span struct {
	TraceID           string              `json:"traceId"`
	Name              string              `json:"name,omitempty"`
	ParentID          string              `json:"parentId,omitempty"`
	ID                string              `json:"id"`
	Timestamp         int64               `json:"timestamp"`
	Duration          int64               `json:"duration"`
	Debug             bool                `json:"debug,omitempty"`
	Annotations       []*annotation       `json:"annotations,omitempty"`
	BinaryAnnotations []*binaryAnnotation `json:"binaryAnnotations,omitempty"`
}

// endpoint structure used by zipkinV1Span.
type endpoint struct {
	ServiceName string `json:"serviceName"`
	IPv4        string `json:"ipv4"`
	IPv6        string `json:"ipv6"`
	Port        int32  `json:"port"`
}

// annotation struct used by zipkinV1Span.
type annotation struct {
	Timestamp int64     `json:"timestamp"`
	Value     string    `json:"value"`
	Endpoint  *endpoint `json:"endpoint"`
}

// binaryAnnotation used by zipkinV1Span.
type binaryAnnotation struct {
	Key      string    `json:"key"`
	Value    string    `json:"value"`
	Endpoint *endpoint `json:"endpoint"`
}

func v1JSONBatchToZipkinV1Spans(blob []byte) ([]*zipkinV1Span, error) {
	var zSpans []*zipkinV1Span
	if err := json.Unmarshal(blob, &zSpans); err != nil {
		return nil, fmt.Errorf("%s: %w", msgZipkinV1JSONUnmarshalError, err)
	}

	return zSpans, nil
}

func zipkinV1SpanToInternal(zSpan *zipkinV1Span, parseStringTags bool, dest pdata.Span) error {
	if err := zipkinV1SpanIDsToInternal(zSpan, dest); err != nil {
		return err
	}

	parsedAnnotations := parseV1Annotations(zSpan.Annotations)
	localComponent, err := parseV1BinAnnotations(zSpan.BinaryAnnotations, parseStringTags, dest)
	if err != nil {
		return err
	}
	if parsedAnnotations.endpoint.ServiceName == unknownServiceName && localComponent != "" {
		parsedAnnotations.endpoint.ServiceName = localComponent
	}

	dest.SetKind(parsedAnnotations.kind)
	dest.SetName(zSpan.Name)

	var startTime, endTime pdata.TimestampUnixNano
	if zSpan.Timestamp == 0 {
		startTime = parsedAnnotations.earlyAnnotationTime
		endTime = parsedAnnotations.lateAnnotationTime
	} else {
		startTime = microsecondsToUnixNano(zSpan.Timestamp)
		endTime = microsecondsToUnixNano(zSpan.Timestamp + zSpan.Duration)
	}
	dest.SetStartTime(startTime)
	dest.SetEndTime(endTime)

	return nil
}

// microsecondsToUnixNano converts epoch microseconds to pdata.TimestampUnixNano
// TODO: copypasted from jaeger converter - should it be moved to separate package?
func microsecondsToUnixNano(ms int64) pdata.TimestampUnixNano {
	return pdata.TimestampUnixNano(uint64(ms) * 1000)
}

// v1AnnotationParseResult stores the results of examining the original annotations,
// this way multiple passes on the annotations are not needed.
type v1AnnotationParseResult struct {
	endpoint            *endpoint
	events              pdata.SpanEventSlice
	kind                pdata.SpanKind
	extendedKind        tracetranslator.OpenTracingSpanKind
	earlyAnnotationTime pdata.TimestampUnixNano
	lateAnnotationTime  pdata.TimestampUnixNano
}

func parseV1Annotations(annotations []*annotation) *v1AnnotationParseResult {
	earlyAnnotationTimestamp := int64(math.MaxInt64)
	lateAnnotationTimestamp := int64(math.MinInt64)

	events := pdata.NewSpanEventSlice()
	events.Resize(len(annotations))

	res := &v1AnnotationParseResult{
		events: pdata.NewSpanEventSlice(),
		kind:   pdata.SpanKindUNSPECIFIED,
	}

	// We want to set the span kind from the first annotation that contains information
	// about the span kind. This flags ensures we only set span kind once from
	// the first annotation.
	spanKindIsSet := false

	eventsCount := 0
	for _, currAnnotation := range annotations {
		if currAnnotation == nil || currAnnotation.Value == "" {
			continue
		}

		endpointName := unknownServiceName
		if currAnnotation.Endpoint != nil && currAnnotation.Endpoint.ServiceName != "" {
			endpointName = currAnnotation.Endpoint.ServiceName
		}

		// Check if annotation has span kind information.
		annotationHasSpanKind := false
		switch currAnnotation.Value {
		case "cs", "cr", "ms", "mr", "ss", "sr":
			annotationHasSpanKind = true
		}

		// Populate the endpoint if it is not already populated and current endpoint
		// has a service name and span kind.
		if res.endpoint == nil && endpointName != unknownServiceName && annotationHasSpanKind {
			res.endpoint = currAnnotation.Endpoint
		}

		if !spanKindIsSet && annotationHasSpanKind {
			// We have not yet populated span kind, do it now.
			// Translate from Zipkin span kind stored in Value field to Kind/ExternalKind
			// pair of internal fields.
			switch currAnnotation.Value {
			case "cs", "cr":
				res.kind = pdata.SpanKindCLIENT
				res.extendedKind = tracetranslator.OpenTracingSpanKindClient

			case "ms":
				// "ms" and "mr" are PRODUCER and CONSUMER kinds which have no equivalent
				// representation in OC. We keep res.Kind unspecified and will use
				// ExtendedKind for translations.
				res.extendedKind = tracetranslator.OpenTracingSpanKindProducer

			case "mr":
				res.extendedKind = tracetranslator.OpenTracingSpanKindConsumer

			case "ss", "sr":
				res.kind = pdata.SpanKindSERVER
				res.extendedKind = tracetranslator.OpenTracingSpanKindServer
			}

			// Remember that we populated the span kind, so that we don't do it again.
			spanKindIsSet = true
		}

		ts := microsecondsToUnixNano(currAnnotation.Timestamp)
		if currAnnotation.Timestamp < earlyAnnotationTimestamp {
			earlyAnnotationTimestamp = currAnnotation.Timestamp
			res.earlyAnnotationTime = ts
		}
		if currAnnotation.Timestamp > lateAnnotationTimestamp {
			lateAnnotationTimestamp = currAnnotation.Timestamp
			res.lateAnnotationTime = ts
		}

		if annotationHasSpanKind {
			// If this annotation is for the send/receive timestamps, no need to create the annotation
			continue
		}

		event := pdata.NewSpanEvent()
		event.SetTimestamp(ts)
		event.SetName(currAnnotation.Value)
		res.events.Append(pdata.NewSpanEvent())
		eventsCount++
	}

	// Truncate the events slice to only include populated items.
	res.events.Resize(eventsCount)

	if res.endpoint == nil {
		res.endpoint = &endpoint{
			ServiceName: unknownServiceName,
		}
	}

	return res
}

// parseV1BinAnnotations parses binary annotations to populate the provided pdata.Span
// with pdata.AttributeMap and pdata.SpanStatus and to return the fallbackServiceName.
func parseV1BinAnnotations(binAnnotations []*binaryAnnotation, parseStringTags bool, dest pdata.Span) (string, error) {
	var localComponent, fallbackServiceName string
	attrs := dest.Attributes()
	sMapper := &statusMapper{}

	for _, binAnnotation := range binAnnotations {
		if binAnnotation.Endpoint != nil && binAnnotation.Endpoint.ServiceName != "" {
			fallbackServiceName = binAnnotation.Endpoint.ServiceName
		}

		key := binAnnotation.Key
		if key == zipkincore.LOCAL_COMPONENT {
			// TODO: (@pjanotti) add reference to OpenTracing and change related tags to use them
			key = "component"
			localComponent = binAnnotation.Value
		}

		attrVal, err := parseInternalAttributeValue(binAnnotation.Value, parseStringTags)
		if err != nil {
			return "", err
		}

		if drop := sMapper.fromAttributeValue(key, attrVal); drop {
			continue
		}

		attrs.Upsert(key, attrVal)
	}

	// Populate SpanStatus.
	sMapper.toInternal(dest.Status())

	if len(binAnnotations) == 0 {
		return "", nil
	}
	if fallbackServiceName == "" {
		fallbackServiceName = localComponent
	}

	return fallbackServiceName, nil
}

// parseInternalAttributeValue prepares a single pdata.AttributeValue from the provided raw string input.
func parseInternalAttributeValue(value string, parseStringTags bool) (pdata.AttributeValue, error) {
	attributeVal := pdata.NewAttributeValueNull()

	if !parseStringTags {
		attributeVal.SetStringVal(value)
		return attributeVal, nil
	}

	switch tracetranslator.DetermineValueType(value, false) {
	case pdata.AttributeValueINT:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return attributeVal, fmt.Errorf("%s: %w", msgZipkinV1BinAnnotationError, err)
		}
		attributeVal.SetIntVal(v)

	case pdata.AttributeValueDOUBLE:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return attributeVal, fmt.Errorf("%s: %w", msgZipkinV1BinAnnotationError, err)
		}
		attributeVal.SetDoubleVal(v)

	case pdata.AttributeValueBOOL:
		v, err := strconv.ParseBool(value)
		if err != nil {
			return attributeVal, fmt.Errorf("%s: %w", msgZipkinV1BinAnnotationError, err)
		}
		attributeVal.SetBoolVal(v)

	default:
		attributeVal.SetStringVal(value)
	}

	return attributeVal, nil
}

// zipkinV1SpanIDsToInternal populates pdata.Span with TraceID, SpanID and ParentID
// from an instance of *zipkinV1Span.
func zipkinV1SpanIDsToInternal(zSpan *zipkinV1Span, dest pdata.Span) error {
	traceID, err := hexTraceIDToInternal(zSpan.TraceID)
	if err != nil {
		return err
	}
	dest.SetTraceID(traceID)

	spanID, err := hexSpanIDToInternalID(zSpan.ID)
	if err != nil {
		return err
	}
	dest.SetSpanID(spanID)

	if zSpan.ParentID != "" {
		parentID, err := hexSpanIDToInternalID(zSpan.ParentID)
		if err != nil {
			return err
		}
		dest.SetParentSpanID(parentID)
	}

	return nil
}

// hexTraceIDToInternal transforms hex string trace ID representation
// into a [16]byte internal pdata.TraceID.
// Extra data is truncated to 16 bytes.
func hexTraceIDToInternal(hex string) (pdata.TraceID, error) {
	// Per info at https://zipkin.io/zipkin-api/zipkin-api.yaml it should be 16 or 32 characters.
	hexLen := len(hex)
	if hexLen != 16 && hexLen != 32 {
		return pdata.InvalidTraceID(), errHexTraceIDWrongLen
	}

	var (
		high, low uint64
		err       error
	)
	if hexLen == 32 {
		if high, err = strconv.ParseUint(hex[:16], 16, 64); err != nil {
			return pdata.InvalidTraceID(), errHexTraceIDParsing
		}
	}

	if low, err = strconv.ParseUint(hex[hexLen-16:], 16, 64); err != nil {
		return pdata.InvalidTraceID(), errHexTraceIDParsing
	}

	if high == 0 && low == 0 {
		return pdata.InvalidTraceID(), errHexTraceIDZero
	}

	tidBytes := tracetranslator.UInt64ToByteTraceID(high, low)

	tid := [16]byte{}
	copy(tid[:], tidBytes[:])

	return pdata.NewTraceID(tid), nil
}

// hexSpanIDToInternalID transforms hex string span ID representation
// into a [8]byte internal pdata.SpanID.
// Extra data is truncated to 8 bytes.
func hexSpanIDToInternalID(hex string) (pdata.SpanID, error) {
	// Per info at https://zipkin.io/zipkin-api/zipkin-api.yaml it should be 16 characters
	if len(hex) != 16 {
		return pdata.InvalidSpanID(), errHexIDWrongLen
	}

	idValue, err := strconv.ParseUint(hex, 16, 64)
	if err != nil {
		return pdata.InvalidSpanID(), errHexIDParsing
	}

	if idValue == 0 {
		return pdata.InvalidSpanID(), errHexIDZero
	}

	idBytes := tracetranslator.UInt64ToByteSpanID(idValue)

	sid := [8]byte{}
	copy(sid[:], idBytes[:])

	return pdata.NewSpanID(sid), nil
}

type ocSpanAndParsedAnnotations struct {
	ocSpan            *tracepb.Span
	parsedAnnotations *annotationParseResult
}

func zipkinToOCProtoBatch(ocSpansAndParsedAnnotations []ocSpanAndParsedAnnotations) ([]consumerdata.TraceData, error) {
	// Service to batch maps the service name to the trace request with the corresponding node.
	svcToTD := make(map[string]*consumerdata.TraceData)
	for _, curr := range ocSpansAndParsedAnnotations {
		req := getOrCreateNodeRequest(svcToTD, curr.parsedAnnotations.Endpoint)
		req.Spans = append(req.Spans, curr.ocSpan)
	}

	tds := make([]consumerdata.TraceData, 0, len(svcToTD))
	for _, v := range svcToTD {
		tds = append(tds, *v)
	}
	return tds, nil
}

func setSpanKind(ocSpan *tracepb.Span, kind tracepb.Span_SpanKind, extendedKind tracetranslator.OpenTracingSpanKind) {
	if kind == tracepb.Span_SPAN_KIND_UNSPECIFIED &&
		extendedKind != tracetranslator.OpenTracingSpanKindUnspecified {
		// Span kind has no equivalent in OC, so we cannot represent it in the Kind field.
		// We will set a TagSpanKind attribute in the span. This will successfully transfer
		// in the pipeline until it reaches the exporter which is responsible for
		// reverse translation.
		if ocSpan.Attributes == nil {
			ocSpan.Attributes = &tracepb.Span_Attributes{}
		}
		if ocSpan.Attributes.AttributeMap == nil {
			ocSpan.Attributes.AttributeMap = make(map[string]*tracepb.AttributeValue, 1)
		}
		ocSpan.Attributes.AttributeMap[tracetranslator.TagSpanKind] =
			&tracepb.AttributeValue{Value: &tracepb.AttributeValue_StringValue{
				StringValue: &tracepb.TruncatableString{Value: string(extendedKind)},
			}}
	}
}

// annotationParseResult stores the results of examining the original annotations,
// this way multiple passes on the annotations are not needed.
type annotationParseResult struct {
	Endpoint            *endpoint
	TimeEvents          *tracepb.Span_TimeEvents
	Kind                tracepb.Span_SpanKind
	ExtendedKind        tracetranslator.OpenTracingSpanKind
	EarlyAnnotationTime *timestamppb.Timestamp
	LateAnnotationTime  *timestamppb.Timestamp
}

// Unknown service name works both as a default value and a flag to indicate that a valid endpoint was found.
const unknownServiceName = "unknown-service"

func parseZipkinV1Annotations(annotations []*annotation) *annotationParseResult {
	// Zipkin V1 annotations have a timestamp so they fit well with OC TimeEvent
	earlyAnnotationTimestamp := int64(math.MaxInt64)
	lateAnnotationTimestamp := int64(math.MinInt64)
	res := &annotationParseResult{}
	timeEvents := make([]*tracepb.Span_TimeEvent, 0, len(annotations))

	// We want to set the span kind from the first annotation that contains information
	// about the span kind. This flags ensures we only set span kind once from
	// the first annotation.
	spanKindIsSet := false

	for _, currAnnotation := range annotations {
		if currAnnotation == nil || currAnnotation.Value == "" {
			continue
		}

		endpointName := unknownServiceName
		if currAnnotation.Endpoint != nil && currAnnotation.Endpoint.ServiceName != "" {
			endpointName = currAnnotation.Endpoint.ServiceName
		}

		// Check if annotation has span kind information.
		annotationHasSpanKind := false
		switch currAnnotation.Value {
		case "cs", "cr", "ms", "mr", "ss", "sr":
			annotationHasSpanKind = true
		}

		// Populate the endpoint if it is not already populated and current endpoint
		// has a service name and span kind.
		if res.Endpoint == nil && endpointName != unknownServiceName && annotationHasSpanKind {
			res.Endpoint = currAnnotation.Endpoint
		}

		if !spanKindIsSet && annotationHasSpanKind {
			// We have not yet populated span kind, do it now.
			// Translate from Zipkin span kind stored in Value field to Kind/ExternalKind
			// pair of internal fields.
			switch currAnnotation.Value {
			case "cs", "cr":
				res.Kind = tracepb.Span_CLIENT
				res.ExtendedKind = tracetranslator.OpenTracingSpanKindClient

			case "ms":
				// "ms" and "mr" are PRODUCER and CONSUMER kinds which have no equivalent
				// representation in OC. We keep res.Kind unspecified and will use
				// ExtendedKind for translations.
				res.ExtendedKind = tracetranslator.OpenTracingSpanKindProducer

			case "mr":
				res.ExtendedKind = tracetranslator.OpenTracingSpanKindConsumer

			case "ss", "sr":
				res.Kind = tracepb.Span_SERVER
				res.ExtendedKind = tracetranslator.OpenTracingSpanKindServer
			}

			// Remember that we populated the span kind, so that we don't do it again.
			spanKindIsSet = true
		}

		ts := epochMicrosecondsToTimestamp(currAnnotation.Timestamp)
		if currAnnotation.Timestamp < earlyAnnotationTimestamp {
			earlyAnnotationTimestamp = currAnnotation.Timestamp
			res.EarlyAnnotationTime = ts
		}
		if currAnnotation.Timestamp > lateAnnotationTimestamp {
			lateAnnotationTimestamp = currAnnotation.Timestamp
			res.LateAnnotationTime = ts
		}

		if annotationHasSpanKind {
			// If this annotation is for the send/receive timestamps, no need to create the annotation
			continue
		}

		timeEvent := &tracepb.Span_TimeEvent{
			Time: ts,
			// More economically we could use a tracepb.Span_TimeEvent_Message, however, it will mean the loss of some information.
			// Using the more expensive annotation until/if something cheaper is needed.
			Value: &tracepb.Span_TimeEvent_Annotation_{
				Annotation: &tracepb.Span_TimeEvent_Annotation{
					Description: &tracepb.TruncatableString{Value: currAnnotation.Value},
				},
			},
		}

		timeEvents = append(timeEvents, timeEvent)
	}

	if len(timeEvents) > 0 {
		res.TimeEvents = &tracepb.Span_TimeEvents{TimeEvent: timeEvents}
	}

	if res.Endpoint == nil {
		res.Endpoint = &endpoint{
			ServiceName: unknownServiceName,
		}
	}

	return res
}

func epochMicrosecondsToTimestamp(msecs int64) *timestamppb.Timestamp {
	if msecs <= 0 {
		return nil
	}
	t := &timestamppb.Timestamp{}
	t.Seconds = msecs / 1e6
	t.Nanos = int32(msecs%1e6) * 1e3
	return t
}

func getOrCreateNodeRequest(m map[string]*consumerdata.TraceData, endpoint *endpoint) *consumerdata.TraceData {
	// this private function assumes that the caller never passes an nil endpoint
	nodeKey := endpoint.string()
	req := m[nodeKey]

	if req != nil {
		return req
	}

	req = &consumerdata.TraceData{
		Node: &commonpb.Node{
			ServiceInfo: &commonpb.ServiceInfo{Name: endpoint.ServiceName},
		},
	}

	if attributeMap := endpoint.createAttributeMap(); attributeMap != nil {
		req.Node.Attributes = attributeMap
	}

	m[nodeKey] = req

	return req
}

func (ep *endpoint) string() string {
	return fmt.Sprintf("%s-%s-%s-%d", ep.ServiceName, ep.IPv4, ep.IPv6, ep.Port)
}

func (ep *endpoint) createAttributeMap() map[string]string {
	if ep.IPv4 == "" && ep.IPv6 == "" && ep.Port == 0 {
		return nil
	}

	attributeMap := make(map[string]string, 3)
	if ep.IPv4 != "" {
		attributeMap["ipv4"] = ep.IPv4
	}
	if ep.IPv6 != "" {
		attributeMap["ipv6"] = ep.IPv6
	}
	if ep.Port != 0 {
		attributeMap["port"] = strconv.Itoa(int(ep.Port))
	}
	return attributeMap
}

func setTimestampsIfUnset(span *tracepb.Span) {
	// zipkin allows timestamp to be unset, but opentelemetry-collector expects it to have a value.
	// If this is unset, the conversion from open census to the internal trace format breaks
	// what should be an identity transformation oc -> internal -> oc
	if span.StartTime == nil {
		now := timestamppb.New(time.Now())
		span.StartTime = now
		span.EndTime = now

		if span.Attributes == nil {
			span.Attributes = &tracepb.Span_Attributes{}
		}
		if span.Attributes.AttributeMap == nil {
			span.Attributes.AttributeMap = make(map[string]*tracepb.AttributeValue, 1)
		}
		span.Attributes.AttributeMap[StartTimeAbsent] = &tracepb.AttributeValue{
			Value: &tracepb.AttributeValue_BoolValue{
				BoolValue: true,
			}}
	}
}
