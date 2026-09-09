package serversend

import "fmt"

// routeStoreError adds an operation without hiding the original store error.
// Keeping both wrapped errors lets callers distinguish an unavailable route
// store from cancellation or deadline expiry with errors.Is.
func routeStoreError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %w", ErrRouteStoreUnavailable, operation, err)
}
