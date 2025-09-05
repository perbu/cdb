package cdb_test

import (
	"reflect"
	"testing"
)

func requireNoError(tb testing.TB, err error, msgAndArgs ...interface{}) {
	tb.Helper()
	if err != nil {
		if len(msgAndArgs) > 0 {
			tb.Fatalf("Expected no error, but got: %v. %v", err, msgAndArgs[0])
		} else {
			tb.Fatalf("Expected no error, but got: %v", err)
		}
	}
}

func requireNil(tb testing.TB, value interface{}, msgAndArgs ...interface{}) {
	tb.Helper()

	// Check if value is nil using reflection to handle typed nil pointers
	if value == nil {
		return // Interface{} nil
	}

	// Check for typed nil pointers and empty slices
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		if v.IsNil() {
			return // Typed nil
		}
	}

	// For byte slices, also consider empty slice as nil-like for CDB purposes
	if bytes, ok := value.([]byte); ok && len(bytes) == 0 {
		return
	}

	if len(msgAndArgs) > 0 {
		tb.Fatalf("Expected nil, but got: %v. %v", value, msgAndArgs[0])
	} else {
		tb.Fatalf("Expected nil, but got: %v", value)
	}
}

func requireNotNil(tb testing.TB, value interface{}, msgAndArgs ...interface{}) {
	tb.Helper()
	if value == nil {
		if len(msgAndArgs) > 0 {
			tb.Fatalf("Expected not nil, but got nil. %v", msgAndArgs[0])
		} else {
			tb.Fatal("Expected not nil, but got nil")
		}
	}
}

func assertEqual(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		if len(msgAndArgs) > 0 {
			t.Errorf("Expected %v, but got %v. %v", expected, actual, msgAndArgs[0])
		} else {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	}
}

func assertEqualValues(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		if len(msgAndArgs) > 0 {
			t.Errorf("Expected %v, but got %v. %v", expected, actual, msgAndArgs[0])
		} else {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	}
}

func assertTrue(t *testing.T, condition bool, msgAndArgs ...interface{}) {
	t.Helper()
	if !condition {
		if len(msgAndArgs) > 0 {
			t.Errorf("Expected true, but got false. %v", msgAndArgs[0])
		} else {
			t.Error("Expected true, but got false")
		}
	}
}

func assertError(t *testing.T, err error, msgAndArgs ...interface{}) {
	t.Helper()
	if err == nil {
		if len(msgAndArgs) > 0 {
			t.Errorf("Expected an error, but got nil. %v", msgAndArgs[0])
		} else {
			t.Error("Expected an error, but got nil")
		}
	}
}
