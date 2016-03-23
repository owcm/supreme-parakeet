package tutorial

/**
 * Created by chrismangold on 11/24/15.
 */
import javax.net.ssl.SSLContext
import spray.io._
import org.apache.camel.util.jsse._

// http://stackoverflow.com/questions/25853953/configuration-issue-for-spray-https-server-with-self-signed-certificate
// http://stackoverflow.com/questions/17695297/importing-the-private-key-public-certificate-pair-in-the-java-keystore
// http://www.akadia.com/services/ssh_test_certificate.html

trait SSLConfig {
  implicit def sslContext: SSLContext = {

    val keyStoreFile = "./surgebackend.jks"

    val ksp = new KeyStoreParameters()
    ksp.setResource(keyStoreFile);
    ksp.setPassword("password")
    val theProvider = ksp.getProvider()

    val kmp = new KeyManagersParameters()
    kmp.setKeyStore(ksp)
    kmp.setKeyPassword("password")

    val scp = new SSLContextParameters()
    scp.setKeyManagers(kmp)

    val context= scp.createSSLContext()

    context
  }


  implicit def sslEngineProvider: ServerSSLEngineProvider = {
    ServerSSLEngineProvider { engine =>
    //  engine.setEnabledCipherSuites(Array("TLS_RSA_WITH_AES_256_CBC_SHA"))
      engine.setEnabledProtocols(Array("SSLv3", "TLSv1"))
      engine
    }
  }

}
