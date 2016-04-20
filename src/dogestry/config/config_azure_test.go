package config

import (
	"os"
	"testing"
)

func TestNewAzureConfig(t *testing.T) {
	os.Setenv("AZ_ACCOUNT_NAME", "name")
	os.Setenv("AZ_ACCOUNT_KEY", "key")

	c, err := NewAzureConfig()

	if err != nil {
		t.Fatalf("Failed to create config. Error: %v", err)
	}
	if c.Azure.AccountName != "name" {
		t.Error("AccountName should be 'name': " + c.Azure.AccountName)
	}
	if c.Azure.AccountKey != "key" {
		t.Error("AccountKey should be 'key': " + c.Azure.AccountKey)
	}
	if c.Docker.Connection == "" {
		t.Error("config.Docker.Connection should not be empty.")
	}

	os.Unsetenv("AZ_ACCOUNT_NAME")
	os.Unsetenv("AZ_ACCOUNT_KEY")

	c, err = NewAzureConfig()
	if err == nil || err.Error() != "AZ_ACCOUNT_NAME or AZ_ACCOUNT_KEY is missing." {
		t.Error("should return error when evn vars are not set")
	}
}
