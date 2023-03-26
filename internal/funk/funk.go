// Package funk provides functional helpers.
package funk

// CallAbortOnError will call sequentially the list of functions
// given as parameters but stopping the call list at the first encountered error.
func CallAbortOnError(f ...func() error) (err error) {
	for idx := 0; err == nil && idx < len(f); idx++ {
		err = f[idx]()
	}
	return
}
