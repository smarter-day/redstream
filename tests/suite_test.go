package tests_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"testing"
)

func TestMeRoute(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RedStream Suite")
}
