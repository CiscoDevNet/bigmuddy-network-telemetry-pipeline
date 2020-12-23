//
// July 2016, cisco
//
// Copyright (c) 2016 by cisco Systems, Inc.
// All rights reserved.
//
//
// Crypto support used to RSA encrypt credentials used to dial in to router.
//

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io/ioutil"

	log "github.com/Sirupsen/logrus"
)

func collect_pkey(pemFile string) (err error, key *rsa.PrivateKey) {

	// Read the private key
	pemData, err := ioutil.ReadFile(pemFile)
	if err != nil {
		return fmt.Errorf("Read RSA pem: %v; access to RSA pem required", err), nil
	}

	// Extract the PEM-encoded data block
	block, _ := pem.Decode(pemData)
	if block == nil {
		return fmt.Errorf("Read key data not in PEM format"), nil
	}

	if block.Type != "RSA PRIVATE KEY" {
		return fmt.Errorf("Key type RSA required but found [%v]", block.Type), nil
	}

	pk, err := x509.ParsePKCS1PrivateKey(block.Bytes)

	return err, pk
}

func encrypt_password(pemFile string, password []byte) (error, string) {

	err, pkey := collect_pkey(pemFile)
	if err != nil {
		return fmt.Errorf("Private key parse failure: %s", err), ""
	}

	ciphertxt, err := rsa.EncryptOAEP(sha256.New(), rand.Reader,
		&pkey.PublicKey, password, []byte(""))
	if err != nil {
		return fmt.Errorf("Encryption failed: %s", err), ""
	}

	return nil, base64.StdEncoding.EncodeToString(ciphertxt)
}

func decrypt_password(pemFile string, p string) (error, string) {

	err, pkey := collect_pkey(pemFile)
	if err != nil {
		return fmt.Errorf("\nPrivate key parse failure: %s", err), ""
	}

	pb64, err := base64.StdEncoding.DecodeString(p)
	if err != nil {
		return fmt.Errorf("\nFailed to extract base64 from cipher string: %v", err), ""
	}
	out, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, pkey, pb64, []byte(""))
	if err != nil {
		return fmt.Errorf("\nFailed to decrypt: %v", err), ""
	}

	return nil, string(out)
}

type cryptUserPasswordCollector struct {
	// Username and password. Password is stored in encrypted form if
	// pem file is passed in from cli. pem points at pem file setting.
	username string
	password string
	pem      string
}

//
// Stateless utility to collect username and password
func cryptCollect(name string, authenticator string) (
	error, string, string) {

	// b := bufio.NewReader(os.Stdin)
	// for {
	// 	fmt.Printf("\nCRYPT Client [%s],[%v]\n Enter username: ",
	// 		name, authenticator)
	// 	user, more, err := b.ReadLine()
	// 	if more {
	// 		fmt.Printf("Username too long")
	// 		continue
	// 	}
	// 	if err != nil {
	// 		fmt.Printf("Failed to collect username")
	// 		continue
	// 	}
	// 	if string(user) == "" {
	// 		fmt.Println("Empty username, try again")
	// 		continue
	// 	}
	// 	fmt.Printf(" Enter password: ")
	// 	pw, err := terminal.ReadPassword(int(os.Stdin.Fd()))
	// 	if err != nil {
	// 		fmt.Printf("Failed to collect password")
	// 		continue
	// 	}

	// 	fmt.Printf("\n")

	// 	return nil, string(user), string(pw)
	// }
	return nil, string("admin"), string("admin")

}

func (c *cryptUserPasswordCollector) getUP() (string, string, error) {

	var err error
	var pw string

	if len(c.pem) > 0 {
		err, pw = decrypt_password(c.pem, c.password)
	} else {
		// clear!
		pw = c.password
	}

	return c.username, pw, err
}

//
// Handler of configuration of username and password. Used by various
// module consistently (e.g. grpc dialin, influxdb metrics)
func (c *cryptUserPasswordCollector) handleConfig(
	nc nodeConfig, configSection string, authenticator string) error {

	var user, pw_pem string
	var err error

	logctx := logger.WithFields(log.Fields{
		"name":          configSection,
		"authenticator": authenticator,
	})

	pw, _ := nc.config.GetString(configSection, "password")

	//
	// A password is setup in config. The password is only ever stored
	// encrypted, which means we need the private key to decrypt it.
	// This would have been passed in as a pemfile.
	if len(pw) > 0 {

		if len(*pemFileName) == 0 {
			logctx.Error(
				"Encrypted password included in configuration but '-pem' option not passed in .  " +
					"RSA key pair file used to encrypt password must be passed in as -pem option")
			return fmt.Errorf("Authentication setup inconsistent")
		}

		//
		// Validate decryption works.
		pw_pem = *pemFileName
		user, err = nc.config.GetString(configSection, "username")
		if err == nil {
			err, _ = decrypt_password(pw_pem, pw)
		}
	} else {
		err, user, pw = cryptCollect(configSection, authenticator)
		if err == nil && len(*pemFileName) > 0 {
			var epw string
			//
			// Let's encypt the password, write it to new config, and
			// advise CU accordingly if cu provided keys (pemFileName).
			err, epw = encrypt_password(*pemFileName, []byte(pw))
			if err == nil {
				fmt.Printf("Generating sample config...")
				nc.config.AddOption(configSection, "username", user)
				nc.config.AddOption(configSection, "password", epw)
				// We successfully encrypted the password; may as well
				// revert to storing encrypted password for this run too.
				pw = epw
				pw_pem = *pemFileName
				//
				// Write out a temporary config. We may need to move this
				// to a common point, if multiple sections force rewrite to
				// avoid writing multiple time.
				newconfigfile := *configFileName + "_REWRITTEN"
				err = nc.config.WriteConfigFile(newconfigfile, 0600, "")
				fmt.Printf("A new configuration file [%s] has been written including "+
					"user name and encrypted password.\nIn future, you can run pipeline "+
					"non-interactively.\nDo remember to run pipeline with '-pem %s -config %s' options.\n",
					newconfigfile, *pemFileName, newconfigfile)
			} else {
				fmt.Printf("Failed to encrypt password: [%v]\n", err)
			}
		}
	}

	if err != nil {
		logctx.WithError(err).Error("failed to setup authentication")
		return err
	}

	if len(user) == 0 {
		err = fmt.Errorf("Authentication username zero length")
		logctx.WithError(err).Error("failed to setup authentication")
		return err
	}

	if len(pw) == 0 {
		err = fmt.Errorf("Authentication password zero length")
		logctx.WithError(err).Error("failed to setup authentication")
		return err
	}

	//
	// We have a user, a pw, and, optionally, a pem file. Possibly an
	// error. We're done.
	c.username = user
	c.password = pw
	c.pem = pw_pem
	logctx.WithFields(log.Fields{"username": user, "pem": pw_pem}).Info(
		"setup authentication")

	return nil
}

type userPasswordCollector interface {
	handleConfig(nc nodeConfig, configSection string, authenticator string) error
	getUP() (string, string, error)
}

type userPasswordCollectorFactory func() userPasswordCollector
