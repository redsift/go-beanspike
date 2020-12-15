package beanspike

// ErrorLogger defines error logging interface app can implement to catch errors
type ErrorLogger interface {
	LogError(error)
}

// The ErrorLoggerFunc type is an adapter to allow the use of ordinary functions as error logger.
// If f is a function with the appropriate signature, ErrorLoggerFunc(f) is a ErrorLogger that calls f.
type ErrorLoggerFunc func(error)

// LogError calls f(err)
func (f ErrorLoggerFunc) LogError(err error) { f(err) }

// TODO consider replacing this variable with "Options" pattern.
var DefaultErrorLogger ErrorLogger = ErrorLoggerFunc(func(err error) {})
