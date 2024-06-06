package messenger_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger"
)

func TestMetadata_Get(t *testing.T) {
	t.Parallel()

	md := messenger.Metadata{
		"key": "value",
	}
	for tname, tc := range map[string]struct {
		key      string
		expected string
	}{
		"missing returns empty string": {
			key:      "some-key",
			expected: "",
		},
		"exists returns value": {
			key:      "key",
			expected: "value",
		},
	} {
		t.Run(tname, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, md.Get(tc.key))
		})
	}
}

func TestMetadata_Set(t *testing.T) {
	t.Parallel()
	for tname, tc := range map[string]struct {
		md       messenger.Metadata
		expected messenger.Metadata
	}{
		"not exists adds new value": {
			md: messenger.Metadata{},
			expected: messenger.Metadata{
				"key": "new-value",
			},
		},
		"exists replaces value": {
			md: messenger.Metadata{
				"key": "value",
			},
			expected: messenger.Metadata{
				"key": "new-value",
			},
		},
	} {
		t.Run(tname, func(t *testing.T) {
			t.Parallel()
			tc.md.Set("key", "new-value")
			require.Equal(t, tc.expected, tc.md)
		})
	}
}

func TestMetadata_Value(t *testing.T) {
	md := messenger.Metadata{
		"key": "value",
	}

	v, err := md.Value()
	require.NoError(t, err)

	require.Equal(t, []uint8(`{"key":"value"}`), v)
}

func TestMetadata_Scan(t *testing.T) {
	expectedMD := messenger.Metadata{
		"key": "value",
	}
	for tname, tc := range map[string]struct {
		data       any
		expectErr  bool
		expectedMD messenger.Metadata
	}{
		"type string success": {
			data:       `{"key":"value"}`,
			expectErr:  false,
			expectedMD: expectedMD,
		},
		"type bytes success": {
			data:       []byte(`{"key":"value"}`),
			expectErr:  false,
			expectedMD: expectedMD,
		},
		"other type returns error": {
			data:      123454,
			expectErr: true,
		},
	} {
		t.Run(tname, func(t *testing.T) {
			var md messenger.Metadata

			err := md.Scan(tc.data)
			require.Equal(t, tc.expectErr, err != nil)
			require.Equal(t, tc.expectedMD, md)
		})
	}
}
