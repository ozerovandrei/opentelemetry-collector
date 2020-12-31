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
	"go.opentelemetry.io/collector/consumer/pdata"
)

func V1JSONBatchToInternalTraces(blob []byte, parseStringTags bool) (pdata.Traces, error) {
	traceData := pdata.NewTraces()

	zSpans, err := v1JSONBatchToZipkinV1Spans(blob)
	if err != nil {
		return traceData, err
	}

	// Allocate a slice with a single Resource.
	rss := traceData.ResourceSpans()
	rss.Resize(1)
	rs0 := rss.At(0)

	// Allocate a slice for spans that need to be combined into ResourceSpans.
	ilss := rs0.InstrumentationLibrarySpans()
	ilss.Resize(1)
	ils0 := ilss.At(0)

	// Allocate a slice for all converted spans.
	combinedSpans := ils0.Spans()
	combinedSpans.Resize(len(zSpans))

	for zSpanIdx, zSpan := range zSpans {
		if err := zipkinV1SpanToInternal(zSpan, parseStringTags, combinedSpans.At(zSpanIdx)); err != nil {
			return traceData, err
		}
	}

	return traceData, nil
}
