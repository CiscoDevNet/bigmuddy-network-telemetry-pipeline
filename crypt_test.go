//
// July 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Test crypto support used to RSA encrypt credentials used to dial in to router.
//
package main

import (
	"github.com/dlintw/goconf"
	"strings"
	"testing"
)

func TestCrypt(t *testing.T) {

	passwords := []string{
		"a",
		"_",
		"aaaaaaaaaaaaaaaaaaaaaaaaaa",
		"1_£$0909jjdsoi08",
		"RANDOMRANDOMRANDOM",
	}

	pemfile_bad := "id_rsa_FOR_TEST_ONLY_BAD"
	err, _ := collect_pkey(pemfile_bad)
	if err == nil {
		t.Fatalf("\nPEM file is nonexistent but collect_pkey did not error")
	}

	pemfile := "crypt_test.go"
	err, _ = collect_pkey(pemfile)
	if err == nil {
		t.Fatalf("\nPEM file passed is bad, but collect_pkey did not error")
	}

	pemfile = "id_rsa_FOR_TEST_ONLY"
	err, _ = collect_pkey(pemfile)
	if err != nil {
		t.Fatalf("\nPrivate key parse failure: %s", err)
	}

	for _, p := range passwords {

		err, _ := encrypt_password(pemfile_bad, []byte(p))
		if err == nil {
			t.Fatalf("\nPEM file is non-existent, but encrypt does no fail")
		}

		err, encoded_cipher := encrypt_password(pemfile, []byte(p))
		if err != nil {
			t.Fatalf("\nFailed to encrypt password: %s", err)
		}

		err, _ = decrypt_password(pemfile_bad, encoded_cipher)
		if err == nil {
			t.Fatalf("\nPEM file is non-existent, but decrypt does no fail")
		}

		err, _ = decrypt_password("id_rsa_FOR_TEST_ONLY_ALT", encoded_cipher)
		if err == nil {
			t.Fatalf("\nDecrypted successfully even if pem mismatched")
		}

		err, out := decrypt_password(pemfile, encoded_cipher)
		if err != nil {
			t.Fatalf("\nFailed to decrypt: %v", err)
		}

		if strings.Compare(p, string(out)) != 0 {
			t.Fatalf("Passwords en/decrypt not symmetric: %v in %s out", p, out)
		} else {
			// t.Logf("Passwords en/decrypt symmetric: %v in %s out", p, out)
		}
	}
}

type cryptTestUserPasswordCollector struct {
	real *cryptUserPasswordCollector
}

func (c *cryptTestUserPasswordCollector) handleConfig(
	nc nodeConfig, name string, server string) error {
	return c.real.handleConfig(nc, name, server)
}

func (c *cryptTestUserPasswordCollector) getUP() (
	string, string, error) {
	return c.real.getUP()
}

func cryptTestUPCollectorFactory() userPasswordCollector {
	return &cryptTestUserPasswordCollector{}
}

func TestCryptCollect(t *testing.T) {

	var nc nodeConfig
	var err error

	nc.config, err = goconf.ReadConfigFile("pipeline_test.conf")
	if err != nil {
		t.Fatalf("Failed to read config [%v]", err)
	}

	c := &cryptTestUserPasswordCollector{
		real: &cryptUserPasswordCollector{},
	}

	//
	// Now that a password is set, let's test the non-interactive
	// negative and positive path.
	name := "crypttest"
	user := "user"
	pw := "mysillysillysillypassword"
	nc.config.AddOption(name, "password", pw)
	err = c.handleConfig(nc, name, "myauthenticatorBAD")
	if err == nil {
		t.Fatalf("Test passed but should fail, no pemfile")
	}

	*pemFileName = "id_rsa_FOR_TEST_ONLY"
	err = c.handleConfig(nc, name, "myauthenticatorBAD")
	if err == nil {
		t.Fatalf("Test passed but should fail, bad password")
	}

	//
	// Rewrite with correct password.
	err, epw := encrypt_password(*pemFileName, []byte(pw))
	if err != nil {
		t.Fatalf("Failed to encrypt password, %v", err)
	}
	nc.config.AddOption(name, "password", epw)

	nc.config.AddOption(name, "username", "")
	err = c.handleConfig(nc, name, "myauthenticatorBAD")
	if err == nil {
		t.Fatalf("Test passed but should fail, empty username")
	}

	nc.config.AddOption(name, "username", user)
	err = c.handleConfig(nc, name, "myauthenticator")
	if err != nil {
		t.Fatalf("Test failed to parse valid config, %v", err)
	}

	ruser, rpw, err := c.getUP()
	if err != nil {
		t.Fatalf("Test failed to return UP, %v", err)
	}

	if ruser != user {
		t.Fatalf("Wrong username returned")
	}
	if rpw != pw {
		t.Fatalf("Wrong password returned")
	}

}
