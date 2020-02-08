// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunks

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestReaderWithInvalidBuffer(t *testing.T) {
	b := realByteSlice([]byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81})
	r := &Reader{bs: []ByteSlice{b}}

	_, err := r.Chunk(0)
	testutil.NotOk(t, err)
}

func TestMergedChunksUnder120Samples(t *testing.T) {
	for _, tc := range []struct {
		input    []Meta
		expected []Meta
	}{
		{
			input: []Meta{
				Meta{MinTime: 1, MaxTime: 120},
				Meta{MinTime: 110, MaxTime: 150},
				Meta{MinTime: 160, MaxTime: 200},
			},
			expected: []Meta{
				Meta{MinTime: 1, MaxTime: 120},
				Meta{MinTime: 121, MaxTime: 150},
				Meta{MinTime: 160, MaxTime: 200},
			},
		},
		{
			input: []Meta{
				Meta{MinTime: 1, MaxTime: 120},
				Meta{MinTime: 90, MaxTime: 200},
				Meta{MinTime: 110, MaxTime: 150},
			},
			expected: []Meta{
				Meta{MinTime: 1, MaxTime: 120},
				Meta{MinTime: 121, MaxTime: 200},
			},
		},
	} {
		// Create chunks for the input.
		for i, meta := range tc.input {
			chk := chunkenc.NewXORChunk()
			app, err := chk.Appender()
			testutil.Ok(t, err)
			for j := meta.MinTime; j <= meta.MaxTime; j++ {
				app.Append(j, float64(j))
			}
			tc.input[i].Chunk = chk
		}

		// Create chunks for the expected.
		for i, meta := range tc.expected {
			chk := chunkenc.NewXORChunk()
			app, err := chk.Appender()
			testutil.Ok(t, err)
			for j := meta.MinTime; j <= meta.MaxTime; j++ {
				app.Append(j, float64(j))
			}
			tc.expected[i].Chunk = chk
		}

		// Test the merging.
		value, err := MergeOverlappingChunks(tc.input)
		testutil.Ok(t, err)

		testutil.Equals(t, len(tc.expected), len(value))

		for i := range tc.expected {
			exp := tc.expected[i]
			act := value[i]
			testutil.Equals(t, exp.MinTime, act.MinTime)
			testutil.Equals(t, exp.MaxTime, act.MaxTime)
			testutil.Equals(t, exp.Chunk.Bytes(), act.Chunk.Bytes())

		}
	}
}
