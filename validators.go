package redstream

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	"time"
)

var (
	Validate *validator.Validate
)

// init initializes the validator and registers the custom duration validation.
//
// This function is automatically called when the package is imported.
// It creates a new validator instance and registers the ValidateDuration
// function for the "duration" tag. If registration fails, it panics with
// an error message.
func init() {
	Validate = validator.New()
	err := Validate.RegisterValidation("duration", ValidateDuration)
	if err != nil {
		panic(fmt.Sprintf("failed to register duration validation: %v", err))
	}
}

// ValidateDuration checks if a given field can be parsed as a valid duration.
//
// It takes a validator.FieldLevel as an argument, which provides access to
// the field being validated and its metadata.
//
// Parameters:
//   - fl: A validator.FieldLevel interface that allows access to the field
//     being validated, as well as the struct it belongs to.
//
// Returns:
//   - bool: true if the field can be parsed as a valid duration, false otherwise.
func ValidateDuration(fl validator.FieldLevel) bool {
	_, err := time.ParseDuration(fl.Field().String())
	return err == nil
}
