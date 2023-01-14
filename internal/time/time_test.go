package time

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTime_UnmarshalText(t *testing.T) {
	t.Run("rfc3339", func(t *testing.T) {
		var testData struct {
			T Time `json:"t"`
		}

		err := json.Unmarshal([]byte(`{"t": "2022-12-11T16:44:17+01:00"}`), &testData)
		require.NoError(t, err)
		expectedTimestamp := time.Date(2022, 12, 11, 15, 44, 17, 0, time.UTC)
		require.True(t,
			expectedTimestamp.Equal(time.Time(testData.T)),
			"%s %s",
			expectedTimestamp.String(),
			time.Time(testData.T).String())
	})

	t.Run("rfc3339 no timezone", func(t *testing.T) {
		var testData struct {
			T Time `json:"t"`
		}

		err := json.Unmarshal([]byte(`{"t": "2022-12-11T16:44:17+01:00"}`), &testData)
		require.NoError(t, err)
		expectedTimestamp := time.Date(2022, 12, 11, 16, 44, 17, 0, time.Local)
		require.True(t,
			expectedTimestamp.Equal(time.Time(testData.T)),
			"%s != %s",
			expectedTimestamp.String(),
			time.Time(testData.T).String())
	})

	t.Run("time only", func(t *testing.T) {
		now = func() time.Time {
			return time.Date(2022, 12, 11, 16, 44, 17, 0, time.UTC)
		}
		local = time.FixedZone("test", 3600)
		t.Cleanup(func() {
			now = time.Now
			local = time.Local
		})

		var testData struct {
			T Time `json:"t"`
		}
		err := json.Unmarshal([]byte(`{"t": "00:33"}`), &testData)
		require.NoError(t, err)
		expectedTimestamp := time.Date(2022, 12, 10, 23, 33, 0, 0, time.UTC)
		require.True(t,
			expectedTimestamp.Equal(time.Time(testData.T)),
			"%s != %s",
			expectedTimestamp.String(),
			time.Time(testData.T).String())
	})
}
