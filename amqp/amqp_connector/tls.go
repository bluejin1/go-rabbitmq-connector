package amqp_connector

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
)

// TLSLoadConfig : TLS 인증을 위해 사용한다
func TLSLoadConfig(caCertFileName, certFileName, keyFileName string) (*tls.Config, error) {
	log.Printf("TLSLoadConfig RootCA file: %v, client cert file: %v  client key file: %v", caCertFileName, certFileName, keyFileName)
	cfg := new(tls.Config)
	// client cert
	if certFileName != "" && keyFileName != "" {
		cert, loadErr := tls.LoadX509KeyPair(certFileName, keyFileName)
		if loadErr != nil {
			log.Printf("cert, key load Error: %v", loadErr.Error())
			// certFileName, keyFileName 이 없거나 잘못될 경우
			// InsecureSkipVerify 를 true 로 설정한다.
			// 권장하지 않는 방식이다.
			cfg.InsecureSkipVerify = true
		} else {
			log.Printf("cert, key load")
			cfg.Certificates = []tls.Certificate{cert}
		}

	} else {
		log.Printf("cert, key load empty: %v, %v", certFileName, keyFileName)
		cfg.InsecureSkipVerify = true
	}

	if caCertFileName != "" {
		// server rootCas
		ca, caLoadErr := os.ReadFile(caCertFileName)
		if caLoadErr != nil {
			log.Printf("Root CA: %v", caLoadErr.Error())
		} else {
			log.Printf("RootCAs New")
			cfg.RootCAs = x509.NewCertPool()
			cfg.RootCAs.AppendCertsFromPEM(ca)
		}
	} else {
		log.Printf("caCertFileName empty caCertFileName: %v", caCertFileName)
	}

	return cfg, nil
}
